//! Bounded, real-socket UDP comparison probe. Test credentials only; not a device SDK.
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    error::Error,
    net::UdpSocket,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
fn sign(bytes: &[u8]) -> Result<[u8; 32]> {
    let key: [u8; 32] = std::array::from_fn(|n| n as u8);
    let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
    mac.update(bytes);
    Ok(mac.finalize().into_bytes().into())
}
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: udp_ack ADDRESS SECONDS throughput|rtt".into());
    }
    let seconds: u64 = args[2].parse()?;
    if !(1..=120).contains(&seconds) {
        return Err("seconds must be 1..120".into());
    }
    let rtt = match args[3].as_str() {
        "rtt" => true,
        "throughput" => false,
        _ => return Err("invalid mode".into()),
    };
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.connect(&args[1])?;
    socket.set_read_timeout(Some(Duration::from_millis(200)))?;
    let stop = Arc::new(AtomicBool::new(false));
    let reader = if !rtt {
        let receive = socket.try_clone()?;
        let stop = stop.clone();
        Some(thread::spawn(move || {
            let mut count = 0u64;
            let mut input = [0; 65];
            while !stop.load(Ordering::Relaxed) {
                if receive
                    .recv(&mut input)
                    .is_ok_and(|n| n == 64 && &input[..4] == b"NBA1")
                {
                    count += 1;
                }
            }
            count
        }))
    } else {
        None
    };
    let body = format!(
        r#"{{"schema_version":1,"source_message_id":"udp-probe","kind":"telemetry","data":{{"padding":"{}"}}}}"#,
        "x".repeat(158)
    );
    let boot = *uuid::Uuid::new_v4().as_bytes();
    let mut wire = b"NBI1\x0bdemo-device".to_vec();
    wire.extend_from_slice(&1u32.to_be_bytes());
    wire.extend_from_slice(&boot);
    let seq_offset = wire.len();
    wire.extend_from_slice(&[0; 16]);
    wire.extend_from_slice(&(body.len() as u16).to_be_bytes());
    wire.extend_from_slice(body.as_bytes());
    let signed_len = wire.len();
    wire.extend_from_slice(&[0; 32]);
    let started = Instant::now();
    let mut sent = 0u64;
    let mut misses = 0u64;
    let mut latencies = Vec::with_capacity(100_000);
    while started.elapsed() < Duration::from_secs(seconds) && (!rtt || sent < 100_000) {
        sent += 1;
        wire[seq_offset..seq_offset + 8].copy_from_slice(&sent.to_be_bytes());
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
        wire[seq_offset + 8..seq_offset + 16].copy_from_slice(&now.to_be_bytes());
        let tag = sign(&wire[..signed_len])?;
        wire[signed_len..].copy_from_slice(&tag);
        let at = Instant::now();
        socket.send(&wire)?;
        if rtt {
            let mut ack = [0; 65];
            let received = socket.recv(&mut ack);
            let elapsed = at.elapsed().as_nanos() as u64;
            if received.is_ok_and(|n| n == 64)
                && &ack[..4] == b"NBA1"
                && ack[4..8] == 1u32.to_be_bytes()
                && ack[8..24] == boot
                && ack[24..32] == sent.to_be_bytes()
            {
                let key: [u8; 32] = std::array::from_fn(|n| n as u8);
                let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
                mac.update(&ack[..32]);
                mac.verify_slice(&ack[32..64])?;
                latencies.push(elapsed);
            } else {
                misses += 1;
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    let drained = if let Some(reader) = reader {
        reader.join().map_err(|_| "reader panicked")?
    } else {
        latencies.len() as u64
    };
    latencies.sort_unstable();
    let percentile = |fraction: f64| {
        latencies
            .get(((latencies.len().saturating_sub(1)) as f64 * fraction) as usize)
            .copied()
    };
    println!(
        "{}",
        serde_json::json!({"mode": args[3], "sent":sent, "seconds":elapsed,
        "payload_bytes":body.len(), "datagram_bytes":wire.len(), "acks_received":drained, "misses":misses,
        "rtt_p50_ns":percentile(0.50), "rtt_p95_ns":percentile(0.95), "rtt_p99_ns":percentile(0.99)})
    );
    Ok(())
}
