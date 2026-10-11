//! Production probes: independent wire encoding and bounded business ACK observation.
//! The load process owns the sink and each simulated device; no gateway internals are used.
use super::*;
use tokio::net::TcpListener;

const MAX_PENDING: usize = 320_000;
const PENDING_TTL: Duration = Duration::from_secs(20);

pub(super) fn validate(c: &Config) -> Result<()> {
    if ![
        "netbaiot-json",
        "netbaiot-cbor",
        "netbaiot-msgpack",
        "netbaiot-protobuf",
    ]
    .contains(&c.codec.as_str())
        || !["legacy", "sensor", "telemetry16", "telemetry64"].contains(&c.scenario.as_str())
        || ![
            "none",
            "wrong_hmac",
            "wrong_version",
            "wrong_codec_version",
            "oversize",
            "repeat_sequence",
            "reconnect",
            "half_connection",
        ]
        .contains(&c.fault.as_str())
        || (c.scenario == "legacy" && (c.codec != "netbaiot-json" || c.fault != "none"))
        || (c.scenario != "legacy"
            && (c.connections > 10_000
                || c.subscribe
                || c.command_rate != 0.0
                || c.audit_open_loop))
        || (matches!(c.fault.as_str(), "wrong_hmac" | "repeat_sequence") && c.transport != "udp")
        || (matches!(c.fault.as_str(), "half_connection" | "reconnect") && c.transport == "udp")
        || c.business_sink
            .as_ref()
            .is_some_and(|s| s.len() > 253 || c.scenario == "legacy")
    {
        return Err("invalid production codec/scenario/fault configuration".into());
    }
    Ok(())
}

fn varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 128 {
        out.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn bytes_field(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push((tag << 3) | 2);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}
pub(super) fn body(c: &Config, run: &str, id: usize, sequence: u64) -> Result<Vec<u8>> {
    if c.scenario == "legacy" {
        return Ok(super::body(
            run,
            id,
            sequence,
            c.payload_bytes,
            c.heartbeat_every,
        ));
    }
    if c.fault == "oversize" {
        return Ok(vec![b'x'; MAX_PACKET + 1]);
    }
    let fields = if c.scenario == "sensor" {
        vec![
            ("temperature".to_owned(), 25.3),
            ("humidity".to_owned(), 61.2),
            ("battery".to_owned(), 98.0),
        ]
    } else {
        (0..if c.scenario == "telemetry16" { 16 } else { 64 })
            .map(|i| (format!("f{i}"), i as f64))
            .collect()
    };
    let version = if c.fault == "wrong_version" { 2 } else { 1 };
    let source = format!("{run}:{id}:{sequence}");
    let value = json!({"schema_version":version,"source_message_id":source,"kind":"telemetry","data":fields.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<serde_json::Map<String, Value>>()});
    // Keep one independent specification encoder per wire format. No codec fallback.
    match c.codec.as_str() {
        "netbaiot-json" => Ok(serde_json::to_vec(&value)?),
        "netbaiot-cbor" => {
            let mut out = Vec::new();
            ciborium::into_writer(&value, &mut out)?;
            Ok(out)
        }
        "netbaiot-msgpack" => Ok(rmp_serde::to_vec_named(&value)?),
        "netbaiot-protobuf" => {
            let mut out = vec![8, version];
            bytes_field(&mut out, 2, source.as_bytes());
            let mut telemetry = Vec::new();
            for (name, number) in fields {
                let mut scalar = if c.scenario == "sensor" {
                    vec![9]
                } else {
                    vec![40]
                };
                if c.scenario == "sensor" {
                    scalar.extend_from_slice(&number.to_le_bytes());
                } else {
                    varint(&mut scalar, number as u64);
                }
                let mut field = Vec::new();
                bytes_field(&mut field, 1, name.as_bytes());
                bytes_field(&mut field, 2, &scalar);
                bytes_field(&mut telemetry, 1, &field);
            }
            bytes_field(&mut out, 10, &telemetry);
            Ok(out)
        }
        _ => Err("unknown codec".into()),
    }
}

pub(super) fn sent(c: &Config, stats: &Shared, source: &str, at: Instant) -> Result<()> {
    if c.business_sink.is_some()
        && matches!(c.fault.as_str(), "none" | "reconnect" | "repeat_sequence")
    {
        let mut s = stats.lock().map_err(|_| "statistics lock poisoned")?;
        if s.business_pending.len() >= (c.connections * c.window).min(MAX_PENDING) {
            return Err("business observation window full".into());
        }
        s.business_pending.insert(source.to_owned(), at);
    }
    Ok(())
}
pub(super) fn expire(stats: &Shared) {
    if let Ok(mut s) = stats.lock() {
        let previous = s.business_pending.len();
        s.business_pending
            .retain(|_, at| at.elapsed() < PENDING_TTL);
        let expired = previous - s.business_pending.len();
        *s.counters.entry("business_ack_timeouts").or_default() += expired as u64;
    }
}

async fn sink_connection(mut socket: TcpStream, stats: Shared) -> Result<()> {
    let mut buffer = BytesMut::with_capacity(1024);
    loop {
        let header_end = loop {
            if let Some(at) = buffer.windows(4).position(|b| b == b"\r\n\r\n") {
                break at + 4;
            }
            if buffer.len() >= 8192 {
                return Err("sink header limit".into());
            }
            let mut chunk = [0; 1024];
            let size = timeout(Duration::from_secs(5), socket.read(&mut chunk)).await??;
            if size == 0 {
                return Ok(());
            }
            buffer.extend_from_slice(&chunk[..size]);
        };
        let header = std::str::from_utf8(&buffer[..header_end])?;
        if !header.starts_with("POST /events HTTP/1.1\r\n")
            || header.to_ascii_lowercase().contains("transfer-encoding:")
        {
            return Err("invalid sink request".into());
        }
        let mut lengths = header
            .lines()
            .filter_map(|l| l.split_once(':'))
            .filter(|(n, _)| n.eq_ignore_ascii_case("content-length"));
        let size: usize = lengths
            .next()
            .ok_or("missing content length")?
            .1
            .trim()
            .parse()?;
        if lengths.next().is_some() || size == 0 || size > MAX_PACKET {
            return Err("sink body limit".into());
        }
        let total = header_end.checked_add(size).ok_or("sink body overflow")?;
        while buffer.len() < total {
            let mut chunk = [0; 4096];
            let size = timeout(Duration::from_secs(5), socket.read(&mut chunk)).await??;
            if size == 0 {
                return Err("partial sink request".into());
            }
            buffer.extend_from_slice(&chunk[..size]);
        }
        let event: Value = serde_json::from_slice(&buffer[header_end..total])?;
        let source = event["source_message_id"]
            .as_str()
            .filter(|s| s.len() <= 256)
            .ok_or("invalid source id")?;
        timeout(
            Duration::from_secs(5),
            socket.write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"),
        )
        .await??;
        let at = stats
            .lock()
            .map_err(|_| "statistics lock poisoned")?
            .business_pending
            .remove(source);
        count(&stats, "business_acks_emitted", 1);
        if let Some(at) = at {
            observe(&stats, "device_send_to_business_ack", at.elapsed());
        } else {
            count(&stats, "business_ack_without_pending", 1);
        }
        buffer.advance(total);
    }
}
pub(super) async fn sink(
    listener: TcpListener,
    stats: Shared,
    end: Instant,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = sleep_until(end) => break,
            _ = stop.changed() => break,
            Some(result) = tasks.join_next(), if !tasks.is_empty() => { if !matches!(result, Ok(Ok(()))) { count(&stats, "sink_probe_errors", 1); } },
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                if tasks.len() < 64 { tasks.spawn(sink_connection(socket, stats.clone())); }
                else { count(&stats, "sink_probe_overloads", 1); }
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub(super) async fn udp(
    c: &Config,
    id: usize,
    relative: usize,
    stats: &Shared,
    measure: Instant,
    end: Instant,
    run: &str,
) -> Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(&c.address).await?;
    count(stats, "udp_devices", 1);
    let boot = *Uuid::new_v4().as_bytes();
    let key: [u8; 32] = std::array::from_fn(|i| i as u8);
    let step = Duration::from_secs_f64(c.connections as f64 / c.publish_rate.max(0.001));
    let mut due = measure + Duration::from_secs_f64(relative as f64 / c.publish_rate.max(1.0));
    let stop = end - Duration::from_secs_f64(c.cooldown_secs);
    let mut seq = 0u64;
    let mut previous = Vec::new();
    while Instant::now() < stop {
        sleep_until(due.min(stop)).await;
        if Instant::now() >= stop {
            break;
        }
        let (next, slots) = advance_open_loop(due, Instant::now(), step);
        due = next;
        count(stats, "scheduled_slots", slots);
        count(stats, "schedule_missed", slots - 1);
        seq += 1;
        let payload = body(c, run, id, seq)?;
        let name = format!("a{id}");
        let mut wire = b"NBI1".to_vec();
        wire.push(name.len() as u8);
        wire.extend_from_slice(name.as_bytes());
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&boot);
        wire.extend_from_slice(&seq.to_be_bytes());
        wire.extend_from_slice(&(now_ms() as i64).to_be_bytes());
        if payload.len() <= u16::MAX as usize {
            wire.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            wire.extend_from_slice(&payload);
        } else {
            wire.resize(65_000, b'x');
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(&wire);
        wire.extend_from_slice(&mac.finalize().into_bytes());
        if c.fault == "wrong_hmac"
            && let Some(last) = wire.last_mut()
        {
            *last ^= 1;
        }
        let replay = c.fault == "repeat_sequence" && seq.is_multiple_of(2);
        let at = Instant::now();
        if !replay {
            sent(c, stats, &format!("{run}:{id}:{seq}"), at)?;
        }
        socket.send(if replay { &previous } else { &wire }).await?;
        count(stats, "published", 1);
        let mut ack = [0; 65];
        match timeout(
            Duration::from_secs_f64(c.timeout_secs).min(step),
            socket.recv(&mut ack),
        )
        .await
        {
            Ok(Ok(64))
                if &ack[..4] == b"NBA1"
                    && ack[4..8] == 1u32.to_be_bytes()
                    && ack[8..24] == boot
                    && ack[24..32] == (if replay { seq - 1 } else { seq }).to_be_bytes() =>
            {
                let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
                mac.update(&ack[..32]);
                mac.verify_slice(&ack[32..64])?;
                count(stats, "event_accepted_receipts", 1);
                observe(stats, "nba1", at.elapsed());
                if replay {
                    count(stats, "duplicate_receipts", 1);
                }
            }
            _ => count(stats, "udp_unconfirmed", 1),
        }
        if !replay {
            previous = wire;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::{CodecLimits, DecodeContext, DeviceKey};
    #[test]
    fn all_wire_encoders_preserve_sensor_and_high_frequency_semantics() {
        let device = DeviceKey {
            tenant_id: netbaiot_core::TenantId::new("t0").unwrap(),
            product_id: netbaiot_core::ProductId::new("p").unwrap(),
            device_id: netbaiot_core::DeviceId::new("d0").unwrap(),
        };
        for scenario in ["sensor", "telemetry16", "telemetry64"] {
            let mut events = Vec::new();
            for (id, _, codec) in netbaiot_codecs::builtins(CodecLimits::default()).unwrap() {
                let c = Config {
                    codec: id.to_string(),
                    scenario: scenario.into(),
                    subscribe: false,
                    ..Config::default()
                };
                let decoded = codec
                    .decode(
                        &DecodeContext {
                            device: &device,
                            received_at: 1,
                        },
                        &body(&c, "run", 0, 1).unwrap(),
                    )
                    .unwrap();
                assert!(
                    body(&c, "0123456789abcdef0123456789abcdef", 9999, u64::MAX)
                        .unwrap()
                        .len()
                        + 81
                        <= 1200
                );
                let invalid = Config {
                    fault: "wrong_version".into(),
                    ..c
                };
                assert!(
                    codec
                        .decode(
                            &DecodeContext {
                                device: &device,
                                received_at: 1
                            },
                            &body(&invalid, "run", 0, 1).unwrap()
                        )
                        .is_err()
                );
                events.push(decoded[0].kind.clone());
            }
            assert!(events.windows(2).all(|p| p[0] == p[1]));
        }
    }
}
