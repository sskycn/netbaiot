//! Separate, bounded real-network workload process. Test credentials only.
mod mixed;
use bytes::{Buf, BytesMut};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    sync::watch,
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};
use tokio_rustls::{TlsConnector, rustls};
use uuid::Uuid;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const ADMIN: &str = "abababababababababababababababababababababababababababababababab";
const MAX_PACKET: usize = 65536;
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    audit_open_loop: bool,
    tls_server_name: String,
    transport: String,
    address: String,
    management_url: String,
    tls_ca: Option<String>,
    tls_resumption: bool,
    connections: usize,
    offset: usize,
    tenant_width: usize,
    ramp_per_sec: f64,
    warmup_secs: f64,
    duration_secs: f64,
    cooldown_secs: f64,
    publish_rate: f64,
    payload_bytes: usize,
    qos: u8,
    subscribe: bool,
    mqtt_clean_session: bool,
    slow_fraction: f64,
    reconnect_every_secs: f64,
    reconnect_fraction: f64,
    retry_connections: bool,
    bad_auth: bool,
    clean_disconnect: bool,
    command_rate: f64,
    command_concurrency: usize,
    window: usize,
    timeout_secs: f64,
    phases: Vec<Phase>,
    command_padding: usize,
    report_every_secs: f64,
    heartbeat_every: u64,
    partial_frame: bool,
    udp_replay_every: u64,
}
#[derive(Clone, Deserialize, Serialize)]
struct Phase {
    seconds: f64,
    rate: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            audit_open_loop: false,
            tls_server_name: "localhost".into(),
            transport: "mqtt".into(),
            address: "127.0.0.1:8080".into(),
            management_url: "http://127.0.0.1:9090".into(),
            tls_ca: None,
            tls_resumption: false,
            connections: 100,
            offset: 0,
            tenant_width: 16,
            ramp_per_sec: 100.0,
            warmup_secs: 3.0,
            duration_secs: 30.0,
            cooldown_secs: 3.0,
            publish_rate: 0.0,
            payload_bytes: 256,
            qos: 1,
            subscribe: true,
            mqtt_clean_session: true,
            slow_fraction: 0.0,
            reconnect_every_secs: 0.0,
            reconnect_fraction: 1.0,
            retry_connections: false,
            bad_auth: false,
            clean_disconnect: true,
            command_rate: 0.0,
            command_concurrency: 1,
            window: 4,
            timeout_secs: 5.0,
            phases: vec![],
            command_padding: 0,
            report_every_secs: 1.0,
            heartbeat_every: 0,
            partial_frame: false,
            udp_replay_every: 0,
        }
    }
}
impl Config {
    fn validate(&self) -> Result<()> {
        if self.connections == 0
            || self.connections > 50000
            || self.offset > 50000
            || self.tenant_width == 0
            || self.payload_bytes > 65000
            || self.qos > 2
            || self.window == 0
            || self.window > 32
            || self.command_padding > 256
            || self.command_concurrency == 0
            || self.command_concurrency > 32
            || !self.report_every_secs.is_finite()
            || !(1.0..=60.0).contains(&self.report_every_secs)
        {
            return Err("invalid bounded load configuration".into());
        }
        if self.audit_open_loop
            && (self.transport != "mqtt"
                || !self.phases.is_empty()
                || self.reconnect_every_secs != 0.0
                || self.retry_connections
                || self.bad_auth
                || self.slow_fraction != 0.0
                || self.command_rate != 0.0
                || self.connections > 10000
                || !(0.01..=1_000_000.0).contains(&self.publish_rate))
        {
            return Err("audit mode requires bounded, fixed-rate MQTT connections".into());
        }
        if self.tls_server_name.is_empty() || self.tls_server_name.len() > 253 {
            return Err("invalid TLS server name".into());
        }
        for v in [
            self.ramp_per_sec,
            self.warmup_secs,
            self.duration_secs,
            self.cooldown_secs,
            self.reconnect_every_secs,
            self.command_rate,
            self.timeout_secs,
        ] {
            if !v.is_finite() || !(0.0..=86400.0).contains(&v) {
                return Err("invalid rate/duration".into());
            }
        }
        if !self.publish_rate.is_finite() || !(0.0..=1_000_000.0).contains(&self.publish_rate) {
            return Err("invalid publish rate".into());
        }
        if self.ramp_per_sec == 0.0
            || self.timeout_secs == 0.0
            || !(0.0..=1.0).contains(&self.slow_fraction)
            || !(0.0..=1.0).contains(&self.reconnect_fraction)
        {
            return Err("invalid rate/fraction".into());
        }
        if self.phases.len() > 32
            || self.phases.iter().any(|p| {
                !p.seconds.is_finite()
                    || !p.rate.is_finite()
                    || p.seconds <= 0.0
                    || p.seconds > 86400.0
                    || p.rate < 0.0
                    || p.rate > 86400.0
            })
        {
            return Err("invalid phases".into());
        }
        if !["mqtt", "tcp", "udp"].contains(&self.transport.as_str()) {
            return Err("unknown transport".into());
        }
        Ok(())
    }
    fn seconds(&self) -> f64 {
        if self.phases.is_empty() {
            self.duration_secs
        } else {
            self.phases.iter().map(|p| p.seconds).sum()
        }
    }
    fn phase(&self, elapsed: f64) -> usize {
        let mut at = 0.0;
        for (index, phase) in self.phases.iter().enumerate() {
            at += phase.seconds;
            if elapsed < at {
                return index;
            }
        }
        0
    }
    fn rate(&self, elapsed: f64) -> f64 {
        if elapsed < 0.0 || elapsed >= self.seconds() {
            return 0.0;
        }
        let mut at = 0.0;
        for p in &self.phases {
            at += p.seconds;
            if elapsed < at {
                return p.rate;
            }
        }
        self.publish_rate
    }
}
// Fixed 10us buckets through 100ms, then 1ms through 60s. Shared histograms, not one per socket.
struct Histogram {
    bins: Vec<u64>,
    count: u64,
    max_us: u64,
    sum_us: u128,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            bins: vec![0; 70001],
            count: 0,
            max_us: 0,
            sum_us: 0,
        }
    }
}
impl Histogram {
    fn add(&mut self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let b = if us < 100000 {
            (us / 10) as usize
        } else {
            10000 + (us / 1000) as usize
        };
        let i = b.min(self.bins.len() - 1);
        self.bins[i] += 1;
        self.count += 1;
        self.max_us = self.max_us.max(us);
        self.sum_us += u128::from(us);
    }
    fn percentile(&self, p: u64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (self.count * p).div_ceil(100);
        let mut n = 0;
        for (i, v) in self.bins.iter().enumerate() {
            n += v;
            if n >= target {
                return if i < 10000 {
                    ((i + 1) * 10) as f64 / 1000.0
                } else {
                    (i - 10000 + 1) as f64
                };
            }
        }
        60000.0
    }
    fn value(&self) -> Value {
        json!({"count":self.count,"p50_ms":self.percentile(50),"p95_ms":self.percentile(95),"p99_ms":self.percentile(99),"max_ms":self.max_us as f64/1000.0,"mean_ms":if self.count>0{self.sum_us as f64/self.count as f64/1000.0}else{0.0}})
    }
}
#[derive(Default)]
struct Stats {
    counters: HashMap<&'static str, u64>,
    hist: HashMap<&'static str, Histogram>,
    errors: Vec<String>,
    inflight: usize,
    inflight_peak: usize,
}
type Shared = Arc<Mutex<Stats>>;
fn count(s: &Shared, k: &'static str, n: u64) {
    if let Ok(mut s) = s.lock() {
        *s.counters.entry(k).or_default() += n;
    }
}
fn observe(s: &Shared, k: &'static str, t: Duration) {
    if let Ok(mut s) = s.lock() {
        s.hist.entry(k).or_default().add(t);
    }
}
fn error_sample(s: &Shared, error: &dyn std::fmt::Display) {
    if let Ok(mut s) = s.lock()
        && s.errors.len() < 8
    {
        s.errors.push(error.to_string().chars().take(128).collect());
    }
}
fn phase_ack(s: &Shared, phase: usize, elapsed: Duration) {
    observe(
        s,
        match phase {
            0 => "application_ack_phase0",
            1 => "application_ack_phase1",
            2 => "application_ack_phase2",
            _ => "application_ack_phase_other",
        },
        elapsed,
    );
}
fn snapshot(s: &Shared) -> Value {
    match s.lock() {
        Ok(s) => {
            json!({"counters":s.counters,"error_samples":s.errors,"inflight":s.inflight,"inflight_peak":s.inflight_peak,"latencies":s.hist.iter().map(|(k,h)|(*k,h.value())).collect::<HashMap<_,_>>()})
        }
        Err(_) => json!({"error":"statistics lock poisoned"}),
    }
}
fn read_bounded(path: impl AsRef<std::path::Path>, maximum: usize) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let mut limited = std::io::Read::take(file, (maximum + 1) as u64);
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut limited, &mut bytes)?;
    if bytes.len() > maximum {
        return Err("local input exceeds configured limit".into());
    }
    Ok(bytes)
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Stream = Box<dyn Io>;
fn tls(c: &Config) -> Result<Option<TlsConnector>> {
    let Some(path) = &c.tls_ca else {
        return Ok(None);
    };
    let cert = read_bounded(path, 65536)?;
    let mut roots = rustls::RootCertStore::empty();
    for der in rustls_pemfile::certs(&mut cert.as_slice()) {
        roots.add(der?)?;
    }
    let mut client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    if !c.tls_resumption {
        client.resumption = rustls::client::Resumption::disabled();
    }
    Ok(Some(TlsConnector::from(Arc::new(client))))
}
async fn connect(c: &Config, connector: &Option<TlsConnector>) -> Result<Stream> {
    let socket = TcpStream::connect(&c.address).await?;
    socket.set_nodelay(true)?;
    if let Some(tls) = connector {
        Ok(Box::new(
            tls.connect(
                rustls::pki_types::ServerName::try_from(c.tls_server_name.clone())?,
                socket,
            )
            .await?,
        ))
    } else {
        Ok(Box::new(socket))
    }
}
fn text(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}
fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len() + 5);
    out.push(first);
    let mut n = body.len();
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 128;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}
fn frame(body: &[u8]) -> Vec<u8> {
    let mut v = (body.len() as u32).to_be_bytes().to_vec();
    v.extend_from_slice(body);
    v
}
fn parse(buffer: &mut BytesMut, mqtt: bool) -> Result<Option<(u8, Vec<u8>)>> {
    let (first, len, header) = if mqtt {
        if buffer.len() < 2 {
            return Ok(None);
        }
        let (mut len, mut mul, mut done) = (0, 1, None);
        for i in 1..=4 {
            let Some(b) = buffer.get(i) else {
                return Ok(None);
            };
            len += usize::from(b & 127) * mul;
            if b & 128 == 0 {
                done = Some(i + 1);
                break;
            }
            mul *= 128;
        }
        let h = done.ok_or("invalid remaining length")?;
        (buffer[0], len, h)
    } else {
        if buffer.len() < 4 {
            return Ok(None);
        }
        (0, u32::from_be_bytes(buffer[..4].try_into()?) as usize, 4)
    };
    if len > MAX_PACKET {
        return Err("response too large".into());
    }
    if buffer.len() < header + len {
        return Ok(None);
    }
    buffer.advance(header);
    Ok(Some((first, buffer.split_to(len).to_vec())))
}
async fn next(stream: &mut Stream, buffer: &mut BytesMut, mqtt: bool) -> Result<(u8, Vec<u8>)> {
    loop {
        if let Some(p) = parse(buffer, mqtt)? {
            return Ok(p);
        }
        read_more(stream, buffer).await?;
    }
}
async fn read_more(stream: &mut Stream, buffer: &mut BytesMut) -> Result<()> {
    if buffer.len() >= MAX_PACKET + 5 {
        return Err("client buffer full".into());
    }
    let mut scratch = [0; 4096];
    let n = stream
        .read(&mut scratch[..(MAX_PACKET + 5 - buffer.len()).min(4096)])
        .await?;
    if n == 0 {
        return Err("peer closed".into());
    }
    buffer.extend_from_slice(&scratch[..n]);
    Ok(())
}
async fn write(stream: &mut Stream, bytes: &[u8], s: &Shared) -> Result<()> {
    timeout(Duration::from_secs(5), stream.write_all(bytes)).await??;
    count(s, "wire_bytes_sent", bytes.len() as u64);
    Ok(())
}
fn prefix(c: &Config, id: usize) -> String {
    format!("v1/t/t{}/p/p/d/d{id}/", id / c.tenant_width)
}
fn body(run: &str, id: usize, seq: u64, bytes: usize, heartbeat_every: u64) -> Vec<u8> {
    if heartbeat_every > 0 && seq.is_multiple_of(heartbeat_every) {
        return json!({"schema_version":1,"source_message_id":format!("{run}:{id}:{seq}"),"kind":"heartbeat","data":{"sequence":seq}}).to_string().into_bytes();
    }
    let mut fields = serde_json::Map::new();
    fields.insert("value".into(), json!(seq % 1000));
    let field_count = bytes.saturating_sub(140).div_ceil(266).min(63);
    if let Some(width) = bytes.saturating_sub(140).checked_div(field_count) {
        let width = width.min(256);
        for i in 0..field_count {
            fields.insert(format!("f{i}"), json!("x".repeat(width)));
        }
    }
    let mut v=json!({"schema_version":1,"source_message_id":format!("{run}:{id}:{seq}"),"kind":"telemetry","data":fields}).to_string().into_bytes();
    v.resize(v.len().max(bytes), b' ');
    v
}
struct Pending {
    at: Instant,
    command: bool,
    phase: usize,
}
fn connack_accepted(packet: &(u8, Vec<u8>), clean_session: bool) -> bool {
    packet.0 == 0x20
        && packet.1.len() == 2
        && packet.1[1] == 0
        && packet.1[0] <= 1
        && (!clean_session || packet.1[0] == 0)
}
#[allow(clippy::too_many_arguments)]
async fn mqtt_or_tcp(
    c: &Config,
    id: usize,
    relative: usize,
    s: &Shared,
    connector: &Option<TlsConnector>,
    measure: Instant,
    end: Instant,
    run: &str,
    generation: usize,
    mut audit_start: Option<watch::Receiver<Option<AuditTiming>>>,
) -> Result<()> {
    let mqtt = c.transport == "mqtt";
    let started = Instant::now();
    let mut stream = timeout(
        Duration::from_secs_f64(c.timeout_secs),
        connect(c, connector),
    )
    .await??;
    let mut buffer = BytesMut::with_capacity(4096);
    let auth = if c.bad_auth {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    } else {
        SECRET
    };
    if mqtt {
        let mut b = Vec::new();
        text(&mut b, "MQTT");
        b.extend_from_slice(&[4, 0xc0 | (u8::from(c.mqtt_clean_session) << 1), 0, 30]);
        text(&mut b, &format!("a{id}"));
        text(&mut b, &format!("a{id}"));
        text(&mut b, auth);
        write(&mut stream, &packet(0x10, &b), s).await?;
        let ack = timeout(
            Duration::from_secs_f64(c.timeout_secs),
            next(&mut stream, &mut buffer, true),
        )
        .await??;
        if !connack_accepted(&ack, c.mqtt_clean_session) {
            count(s, "connect_rejected", 1);
            return Err("CONNACK rejection".into());
        }
        if ack.1[0] == 1 {
            count(s, "session_present", 1);
        }
    } else {
        write(
            &mut stream,
            &frame(
                json!({"credential_id":format!("a{id}"),"secret":auth})
                    .to_string()
                    .as_bytes(),
            ),
            s,
        )
        .await?;
        let (_, b) = timeout(
            Duration::from_secs_f64(c.timeout_secs),
            next(&mut stream, &mut buffer, false),
        )
        .await??;
        if !serde_json::from_slice::<Value>(&b)?["authenticated"]
            .as_bool()
            .unwrap_or(false)
        {
            return Err("TCP auth failed".into());
        }
    }
    observe(s, "connect", started.elapsed());
    count(s, "connected", 1);
    if !mqtt && c.partial_frame {
        write(&mut stream, &[0, 0, 0, 20, b'{'], s).await?;
        sleep_until(end).await;
        count(s, "disconnected", 1);
        return Ok(());
    }
    let topics = prefix(c, id);
    if mqtt && c.subscribe {
        let mut b = vec![0, 1];
        text(&mut b, &(topics.clone() + "down"));
        b.push(1);
        text(&mut b, &(topics.clone() + "up_ack"));
        b.push(0);
        write(&mut stream, &packet(0x82, &b), s).await?;
        let sub = timeout(
            Duration::from_secs_f64(c.timeout_secs),
            next(&mut stream, &mut buffer, true),
        )
        .await??;
        if sub != (0x90, vec![0, 1, 1, 0]) {
            return Err("SUBACK rejected".into());
        }
    }
    let (traffic_start, measure, end) = if let Some(rx) = &mut audit_start {
        count(s, "ready", 1);
        let timing = loop {
            if let Some(timing) = *rx.borrow_and_update() {
                break timing;
            }
            rx.changed().await.map_err(|_| "audit start cancelled")?;
        };
        (timing.warmup, timing.measure, timing.end)
    } else {
        (measure, measure, end)
    };
    let mut occupancy = InflightGauge {
        stats: s.clone(),
        current: 0,
    };
    let slow = (relative as f64) < (c.connections as f64 * c.slow_fraction);
    if slow {
        sleep_until(end).await;
        count(s, "disconnected", 1);
        return Ok(());
    }
    let mut pending = HashMap::<String, Pending>::new();
    let mut ids = HashMap::<u16, Pending>::new();
    let mut seq = 0u64;
    let mut pid = 1u16;
    let mut ping = if mqtt {
        Instant::now() + Duration::from_secs(10)
    } else {
        end
    };
    let mut due = traffic_start + Duration::from_secs_f64(relative as f64 / c.rate(0.0).max(1.0));
    // Reconnection must not replay missed schedule slots as an artificial burst.
    let rate_now = c.rate(
        Instant::now()
            .saturating_duration_since(measure)
            .as_secs_f64(),
    );
    if rate_now > 0.0 && Instant::now() > due {
        let step = c.connections as f64 / rate_now;
        due += Duration::from_secs_f64((due.elapsed().as_secs_f64() / step).ceil() * step);
    }
    let mut churn = if c.reconnect_every_secs > 0.0
        && (relative as f64) < c.connections as f64 * c.reconnect_fraction
    {
        measure + Duration::from_secs_f64(c.reconnect_every_secs * (generation + 1) as f64)
    } else {
        end
    };
    churn = churn.min(end);
    let traffic_end = end - Duration::from_secs_f64(c.cooldown_secs);
    loop {
        while let Some((first, b)) = parse(&mut buffer, mqtt)? {
            if c.audit_open_loop {
                let header = 2 + usize::from(b.len() >= 128) + usize::from(b.len() >= 16384);
                count(s, "wire_bytes_received", (header + b.len()) as u64);
            }
            if first == 0x40 && b.len() == 2 {
                let id = u16::from_be_bytes([b[0], b[1]]);
                if let Some(p) = ids.remove(&id) {
                    if c.audit_open_loop && p.at >= measure {
                        count(s, "measurement_pubacks", 1);
                    }
                    if !c.audit_open_loop || p.at >= measure {
                        observe(
                            s,
                            if p.command {
                                "command_puback"
                            } else {
                                "puback"
                            },
                            p.at.elapsed(),
                        );
                    }
                    count(s, "pubacks", 1);
                }
            } else if first == 0x50 && b.len() == 2 {
                let id = u16::from_be_bytes([b[0], b[1]]);
                if let Some(p) = ids.get(&id) {
                    if !c.audit_open_loop || p.at >= measure {
                        observe(s, "pubrec", p.at.elapsed());
                    }
                    count(s, "pubrecs", 1);
                    write(&mut stream, &packet(0x62, &b), s).await?;
                    count(s, "pubrels", 1);
                }
            } else if first == 0x70 && b.len() == 2 {
                let id = u16::from_be_bytes([b[0], b[1]]);
                if let Some(p) = ids.remove(&id) {
                    if c.audit_open_loop && p.at >= measure {
                        count(s, "measurement_pubcomps", 1);
                    }
                    if !c.audit_open_loop || p.at >= measure {
                        observe(s, "pubcomp", p.at.elapsed());
                    }
                    count(s, "pubcomps", 1);
                }
            } else if !mqtt || first >> 4 == 3 {
                let payload = if mqtt {
                    if b.len() < 2 {
                        return Err("short publish".into());
                    }
                    let n = usize::from(u16::from_be_bytes([b[0], b[1]]));
                    let mut at = 2 + n;
                    if first & 6 == 2 {
                        if b.len() < at + 2 {
                            return Err("short packet id".into());
                        }
                        let ack_start = Instant::now();
                        write(&mut stream, &packet(0x40, &b[at..at + 2]), s).await?;
                        observe(s, "downlink_puback_write", ack_start.elapsed());
                        at += 2;
                    }
                    b.get(at..).ok_or("short topic")?
                } else {
                    &b
                };
                let v: Value = serde_json::from_slice(payload)?;
                if let Some(source) = v["source_message_id"].as_str() {
                    if let Some(p) = pending.remove(source) {
                        if !p.command && !c.phases.is_empty() {
                            phase_ack(s, p.phase, p.at.elapsed());
                        }
                        if !c.audit_open_loop || p.at >= measure {
                            observe(
                                s,
                                if p.command {
                                    "command_ack_completion"
                                } else {
                                    "application_ack"
                                },
                                p.at.elapsed(),
                            );
                        }
                        count(
                            s,
                            if p.command {
                                "command_acks"
                            } else {
                                "accepted"
                            },
                            1,
                        );
                        if v["duplicate"] == true {
                            count(s, "duplicates", 1);
                        }
                    }
                } else if let Some(command_id) = v["command_id"].as_str() {
                    count(s, "downlinks", 1);
                    if let Some(issued) = v["payload"]["arguments"]["issued_ms"]
                        .as_f64()
                        .map(|v| v as u64)
                    {
                        observe(
                            s,
                            "command_queue_to_receive",
                            Duration::from_millis(now_ms().saturating_sub(issued)),
                        );
                    }
                    if pending.len() >= 32 || ids.len() >= 32 {
                        count(s, "client_window_full", 1);
                        continue;
                    }
                    let source = format!("ack:{command_id}");
                    let a=json!({"schema_version":1,"source_message_id":source,"kind":"command_ack","data":{"command_id":command_id,"execution":"succeeded"}}).to_string();
                    let at = Instant::now();
                    if c.subscribe {
                        pending.insert(
                            source,
                            Pending {
                                at,
                                command: true,
                                phase: 0,
                            },
                        );
                    }
                    if mqtt {
                        pid = pid.wrapping_add(1).max(1);
                        while ids.contains_key(&pid) {
                            pid = pid.wrapping_add(1).max(1);
                        }
                        ids.insert(
                            pid,
                            Pending {
                                at,
                                command: true,
                                phase: 0,
                            },
                        );
                        let mut b = Vec::new();
                        text(&mut b, &(topics.clone() + "down_ack"));
                        b.extend_from_slice(&pid.to_be_bytes());
                        b.extend_from_slice(a.as_bytes());
                        write(&mut stream, &packet(0x32, &b), s).await?;
                    } else {
                        write(&mut stream, &frame(a.as_bytes()), s).await?;
                    }
                }
            }
        }
        let now = Instant::now();
        if now >= end || now >= churn {
            break;
        }
        if pending
            .values()
            .chain(ids.values())
            .any(|p| p.at.elapsed().as_secs_f64() > c.timeout_secs)
        {
            count(s, "ack_timeouts", 1);
            return Err("ack deadline".into());
        }
        let elapsed = now.saturating_duration_since(measure).as_secs_f64();
        let rate = if c.audit_open_loop && now >= traffic_start && now < measure {
            c.publish_rate
        } else if now < measure {
            0.0
        } else {
            c.rate(elapsed)
        };
        if now >= traffic_end {
            due = end;
        }
        if now >= due && now < traffic_end {
            if rate > 0.0 {
                let step = Duration::from_secs_f64(c.connections as f64 / rate);
                if !c.audit_open_loop || now >= measure {
                    observe(
                        s,
                        "generator_schedule_lag",
                        now.saturating_duration_since(due),
                    );
                }
                if c.audit_open_loop {
                    let (next, slots) = advance_open_loop(due, now, step);
                    due = next;
                    count(s, "scheduled_slots", slots);
                    count(s, "schedule_missed", slots - 1);
                    count(s, "publish_attempted", 1);
                    if now >= measure {
                        count(s, "measurement_scheduled_slots", slots);
                        count(s, "measurement_schedule_missed", slots - 1);
                        count(s, "measurement_attempted", 1);
                    }
                } else {
                    due = (due + step).max(now + step.min(Duration::from_millis(1)));
                }
                if pending.len() < c.window && ids.len() < c.window {
                    seq += 1;
                    let unique = format!("{run}.{generation}");
                    let payload = body(&unique, id, seq, c.payload_bytes, c.heartbeat_every);
                    let source = format!("{unique}:{id}:{seq}");
                    let at = Instant::now();
                    if c.subscribe || !mqtt {
                        pending.insert(
                            source,
                            Pending {
                                at,
                                command: false,
                                phase: c.phase(elapsed),
                            },
                        );
                    }
                    if mqtt {
                        let mut b = Vec::new();
                        text(&mut b, &(topics.clone() + "up"));
                        if c.qos > 0 {
                            pid = pid.wrapping_add(1).max(1);
                            while ids.contains_key(&pid) {
                                pid = pid.wrapping_add(1).max(1);
                            }
                            b.extend_from_slice(&pid.to_be_bytes());
                            ids.insert(
                                pid,
                                Pending {
                                    at,
                                    command: false,
                                    phase: c.phase(elapsed),
                                },
                            );
                        }
                        b.extend_from_slice(&payload);
                        write(&mut stream, &packet(0x30 | (c.qos << 1), &b), s).await?;
                    } else {
                        write(&mut stream, &frame(&payload), s).await?;
                    }
                    count(s, "published", 1);
                    count(s, "payload_bytes", payload.len() as u64);
                    if c.audit_open_loop && now >= measure {
                        count(s, "measurement_published", 1);
                        count(s, "measurement_payload_bytes", payload.len() as u64);
                    }
                } else {
                    count(s, "client_window_full", 1);
                    if c.audit_open_loop && now >= measure {
                        count(s, "measurement_window_full", 1);
                    }
                }
            } else {
                due = now + Duration::from_millis(if c.phases.is_empty() { 10000 } else { 100 });
            }
        }
        if c.audit_open_loop {
            occupancy.set(pending.len() + ids.len());
        }
        if mqtt && now >= ping {
            write(&mut stream, &[0xc0, 0], s).await?;
            ping = now + Duration::from_secs(10);
        }
        let deadline = due
            .min(ping)
            .min(end)
            .min(churn)
            .min(Instant::now() + Duration::from_secs_f64(c.timeout_secs));
        tokio::select! {r=read_more(&mut stream,&mut buffer)=>r?,_=sleep_until(deadline)=>{}}
    }
    count(
        s,
        "pending_at_disconnect",
        (pending.len() + ids.len()) as u64,
    );
    if c.clean_disconnect && mqtt {
        write(&mut stream, &[0xe0, 0], s).await?;
    }
    count(s, "disconnected", 1);
    Ok(())
}
fn http_client(c: &Config) -> Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs_f64(c.timeout_secs))
        .pool_max_idle_per_host(0);
    if let Some(path) = &c.tls_ca {
        b = b.add_root_certificate(reqwest::Certificate::from_pem(&read_bounded(path, 65536)?)?);
    }
    Ok(b.build()?)
}
async fn stateless(
    c: &Config,
    id: usize,
    relative: usize,
    s: &Shared,
    measure: Instant,
    end: Instant,
    run: &str,
) -> Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let mut seq = 0;
    let mut due = measure + Duration::from_secs_f64(relative as f64 / c.rate(0.0).max(1.0));
    let stop = end - Duration::from_secs_f64(c.cooldown_secs);
    let boot = *Uuid::new_v4().as_bytes();
    while Instant::now() < stop {
        sleep_until(due.min(stop)).await;
        if Instant::now() >= stop {
            break;
        }
        let rate = c.rate(
            Instant::now()
                .saturating_duration_since(measure)
                .as_secs_f64(),
        );
        if rate <= 0.0 {
            due = Instant::now() + Duration::from_millis(100);
            continue;
        }
        observe(
            s,
            "generator_schedule_lag",
            Instant::now().saturating_duration_since(due),
        );
        seq += 1;
        let payload = body(run, id, seq, c.payload_bytes, c.heartbeat_every);
        count(s, "published", 1);
        count(s, "payload_bytes", payload.len() as u64);
        let name = format!("a{id}");
        let mut wire = b"NBI1".to_vec();
        wire.push(name.len() as u8);
        wire.extend_from_slice(name.as_bytes());
        wire.extend_from_slice(&1u32.to_be_bytes());
        wire.extend_from_slice(&boot);
        wire.extend_from_slice(&seq.to_be_bytes());
        wire.extend_from_slice(&(now_ms() as i64).to_be_bytes());
        wire.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        wire.extend_from_slice(&payload);
        let key: Vec<u8> = (0..32).collect();
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(&wire);
        wire.extend_from_slice(&mac.finalize().into_bytes());
        socket.send_to(&wire, &c.address).await?;
        count(s, "wire_bytes_sent", wire.len() as u64);
        if c.udp_replay_every > 0 && seq.is_multiple_of(c.udp_replay_every) {
            socket.send_to(&wire, &c.address).await?;
            count(s, "replays_sent", 1);
            count(s, "wire_bytes_sent", wire.len() as u64);
        }
        let step = Duration::from_secs_f64(c.connections as f64 / rate);
        due = (due + step).max(Instant::now() + step.min(Duration::from_millis(1)));
    }
    Ok(())
}
async fn commands(
    c: Arc<Config>,
    s: Shared,
    measure: Instant,
    end: Instant,
    worker: usize,
) -> Result<()> {
    if c.command_rate == 0.0 {
        return Ok(());
    }
    let client = http_client(&c)?;
    let step = Duration::from_secs_f64(c.command_concurrency as f64 / c.command_rate);
    let mut due = measure + Duration::from_secs_f64(1.0 + worker as f64 / c.command_rate);
    let mut seq = worker;
    while Instant::now() < end {
        sleep_until(due.min(end)).await;
        if Instant::now() >= end {
            break;
        }
        let id = c.offset + seq % c.connections;
        seq += c.command_concurrency;
        let at = Instant::now();
        let command = json!({"command_id":Uuid::new_v4().to_string(),"device":{"tenant_id":format!("t{}",id/c.tenant_width),"product_id":"p","device_id":format!("d{id}")},"expires_at":now_ms()+60000,"payload":{"name":"load","arguments":{"issued_ms":now_ms(),"padding":"x".repeat(c.command_padding)}}});
        match client
            .post(format!("{}/api/v1/devices/commands", c.management_url))
            .bearer_auth(ADMIN)
            .json(&command)
            .send()
            .await
        {
            Ok(r) => {
                if r.status() == 202 {
                    count(&s, "commands_queued", 1);
                    observe(&s, "command_submit", at.elapsed());
                } else {
                    count(&s, "commands_rejected", 1);
                }
            }
            Err(_) => count(&s, "command_errors", 1),
        }
        due = (due + step).max(Instant::now() + step.min(Duration::from_millis(1)));
    }
    Ok(())
}
// Each connection owns one bounded pending map and one active write. No send queue.
struct InflightGauge {
    stats: Shared,
    current: usize,
}
impl InflightGauge {
    fn set(&mut self, value: usize) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.inflight = stats.inflight.saturating_sub(self.current) + value;
            stats.inflight_peak = stats.inflight_peak.max(stats.inflight);
        }
        self.current = value;
    }
}
impl Drop for InflightGauge {
    fn drop(&mut self) {
        self.set(0);
    }
}

#[derive(Clone, Copy)]
struct AuditTiming {
    warmup: Instant,
    measure: Instant,
    end: Instant,
}

// Absolute schedule: missed slots are explicitly discarded, never shifted into
// an unbounded catch-up burst or silently turned into an ACK-paced workload.
fn advance_open_loop(due: Instant, now: Instant, step: Duration) -> (Instant, u64) {
    let step_ns = step.as_nanos().max(1);
    let lag = now.saturating_duration_since(due).as_nanos();
    let slots = (lag / step_ns + 1).min(u128::from(u64::MAX)) as u64;
    let remaining = (step_ns - lag % step_ns).min(u128::from(u64::MAX)) as u64;
    (now + Duration::from_nanos(remaining), slots)
}

async fn run_audit(c: Config) -> Result<()> {
    let connector = tls(&c)?;
    let c = Arc::new(c);
    let stats: Shared = Arc::new(Mutex::new(Stats::default()));
    let (start_tx, start_rx) = watch::channel(None);
    let began = Instant::now();
    let readiness_deadline = began
        + Duration::from_secs_f64(c.connections as f64 / c.ramp_per_sec + c.timeout_secs + 10.0);
    let run = Uuid::new_v4().simple().to_string();
    let mut tasks = JoinSet::new();
    println!(
        "{}",
        json!({"event":"start","pid":std::process::id(),"config":&*c,"epoch_ms":now_ms(),"pacing":"absolute_schedule_drop_missed"})
    );
    for relative in 0..c.connections {
        let (c, stats, connector, rx, run) = (
            c.clone(),
            stats.clone(),
            connector.clone(),
            start_rx.clone(),
            run.clone(),
        );
        tasks.spawn(async move {
            sleep_until(began + Duration::from_secs_f64(relative as f64 / c.ramp_per_sec)).await;
            count(&stats, "connect_attempts", 1);
            if let Err(error) = mqtt_or_tcp(
                &c,
                c.offset + relative,
                relative,
                &stats,
                &connector,
                readiness_deadline,
                readiness_deadline,
                &run,
                0,
                Some(rx),
            )
            .await
            {
                error_sample(&stats, &error);
                count(&stats, "client_errors", 1);
            }
        });
    }
    // A failed connection cannot leave an unbounded barrier wait.
    loop {
        let state = snapshot(&stats);
        if state["counters"]["ready"].as_u64().unwrap_or(0) == c.connections as u64 {
            break;
        }
        if state["counters"]["client_errors"].as_u64().unwrap_or(0) > 0
            || Instant::now() >= readiness_deadline
        {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            println!(
                "{}",
                json!({"event":"setup_failed","stats":snapshot(&stats)})
            );
            return Err("not all publishers became ready".into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let warmup = Instant::now() + Duration::from_millis(100);
    let measure = warmup + Duration::from_secs_f64(c.warmup_secs);
    let traffic_end = measure + Duration::from_secs_f64(c.duration_secs);
    let end = traffic_end + Duration::from_secs_f64(c.cooldown_secs);
    start_tx
        .send(Some(AuditTiming {
            warmup,
            measure,
            end,
        }))
        .map_err(|_| "no audit workers")?;
    let mut tick = tokio::time::interval(Duration::from_secs_f64(c.report_every_secs));
    let mut stage = 0;
    let mut boundary = warmup;
    while !tasks.is_empty() {
        tokio::select! {
            result = tasks.join_next() => { if result.is_some_and(|r|r.is_err()) { return Err("audit worker panicked".into()); } },
            _ = sleep_until(boundary), if stage < 3 => {
                let event = ["warmup_start", "measurement_start", "measurement_end"][stage];
                if stage == 1 && let Ok(mut s) = stats.lock() { s.inflight_peak=s.inflight; }
                println!("{}",json!({"event":event,"epoch_ms":now_ms(),"elapsed_s":began.elapsed().as_secs_f64(),"stats":snapshot(&stats)}));
                stage += 1; boundary = if stage == 1 { measure } else { traffic_end };
            },
            _ = tick.tick() => println!("{}",json!({"event":"sample","epoch_ms":now_ms(),"elapsed_s":began.elapsed().as_secs_f64(),"measurement_s":Instant::now().saturating_duration_since(measure).as_secs_f64(),"phase":if Instant::now()<measure {"warmup"} else if Instant::now()<traffic_end {"measurement"} else {"cooldown"},"tasks":tokio::runtime::Handle::current().metrics().num_alive_tasks(),"stats":snapshot(&stats)})),
            _ = sleep_until(end + Duration::from_secs(6)) => { tasks.abort_all(); while tasks.join_next().await.is_some() {} count(&stats,"forced_tasks",1); break; }
        }
    }
    println!(
        "{}",
        json!({"event":"final","epoch_ms":now_ms(),"elapsed_s":began.elapsed().as_secs_f64(),"stats":snapshot(&stats)})
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--mixed") {
        return mixed::run(
            &std::env::args()
                .nth(2)
                .ok_or("mixed config path required")?,
        )
        .await;
    }
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: netbaiot-loadgen CONFIG.json")?;
    let bytes = read_bounded(path, 65536)?;
    if bytes.len() > 65536 {
        return Err("load configuration too large".into());
    }
    let c: Config = serde_json::from_slice(&bytes)?;
    c.validate()?;
    if c.audit_open_loop {
        return run_audit(c).await;
    }
    let c = Arc::new(c);
    let connector = tls(&c)?;
    let s: Shared = Arc::new(Mutex::new(Stats::default()));
    let start = Instant::now();
    let measure =
        start + Duration::from_secs_f64(c.connections as f64 / c.ramp_per_sec + c.warmup_secs);
    let end = measure + Duration::from_secs_f64(c.seconds() + c.cooldown_secs);
    let run = Uuid::new_v4().simple().to_string();
    let mut tasks = JoinSet::new();
    println!(
        "{}",
        json!({"event":"start","pid":std::process::id(),"config":&*c,"run":run,"epoch_ms":now_ms()})
    );
    for relative in 0..c.connections {
        let (c, s, connector, run) = (c.clone(), s.clone(), connector.clone(), run.clone());
        tasks.spawn(async move {
            let due = start + Duration::from_secs_f64(relative as f64 / c.ramp_per_sec);
            sleep_until(due).await;
            observe(
                &s,
                "ramp_schedule_lag",
                Instant::now().saturating_duration_since(due),
            );
            if c.transport == "udp" {
                if stateless(&c, c.offset + relative, relative, &s, measure, end, &run)
                    .await
                    .is_err()
                {
                    count(&s, "client_errors", 1);
                }
                return;
            }
            let mut generation = 0;
            while Instant::now() < end && generation < 10000 {
                count(&s, "connect_attempts", 1);
                match mqtt_or_tcp(
                    &c,
                    c.offset + relative,
                    relative,
                    &s,
                    &connector,
                    measure,
                    end,
                    &run,
                    generation,
                    None,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        error_sample(&s, &error);
                        count(&s, "client_errors", 1);
                        if !c.retry_connections {
                            break;
                        }
                    }
                }
                generation += 1;
                if c.reconnect_every_secs == 0.0 && !c.retry_connections {
                    break;
                }
                sleep_until((Instant::now() + Duration::from_millis(100)).min(end)).await;
            }
        });
    }
    for worker in 0..c.command_concurrency {
        let (c2, s2) = (c.clone(), s.clone());
        tasks.spawn(async move {
            if commands(
                c2.clone(),
                s2.clone(),
                measure,
                end - Duration::from_secs_f64(c2.cooldown_secs),
                worker,
            )
            .await
            .is_err()
            {
                count(&s2, "command_task_errors", 1);
            }
        });
    }
    // The root owns all tasks; the absolute deadline aborts and joins any stragglers.
    let mut tick = tokio::time::interval(Duration::from_secs_f64(c.report_every_secs));
    let final_deadline = end + Duration::from_secs(6);
    while !tasks.is_empty() {
        tokio::select! {r=tasks.join_next()=>{if r.is_some_and(|r|r.is_err()){return Err("load task panicked".into());}},_=tick.tick()=>println!("{}",json!({"event":"sample","elapsed_s":start.elapsed().as_secs_f64(),"measurement_s":Instant::now().saturating_duration_since(measure).as_secs_f64(),"epoch_ms":now_ms(),"tasks":tokio::runtime::Handle::current().metrics().num_alive_tasks(),"stats":snapshot(&s)})),_=sleep_until(final_deadline)=>{tasks.abort_all();while tasks.join_next().await.is_some(){} count(&s,"forced_tasks",1);break;}}
    }
    println!(
        "{}",
        json!({"event":"final","elapsed_s":start.elapsed().as_secs_f64(),"epoch_ms":now_ms(),"stats":snapshot(&s)})
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn open_loop_schedule_counts_skips_without_drift() {
        let due = Instant::now();
        let step = Duration::from_millis(10);
        let (next, slots) = advance_open_loop(due, due + Duration::from_millis(35), step);
        assert_eq!(slots, 4);
        assert_eq!(next, due + Duration::from_millis(40));
        let (next, slots) = advance_open_loop(next, next, step);
        assert_eq!(slots, 1);
        assert_eq!(next, due + Duration::from_millis(50));
    }
    #[test]
    fn audit_rejects_unbounded_or_incompatible_workloads() {
        let mut c = Config {
            audit_open_loop: true,
            publish_rate: 100_000.0,
            ..Config::default()
        };
        assert!(c.validate().is_ok());
        c.connections = 10001;
        assert!(c.validate().is_err());
        c.connections = 100;
        c.retry_connections = true;
        assert!(c.validate().is_err());
    }
    #[test]
    fn frames_handle_all_splits() {
        for mqtt in [true, false] {
            let wire = if mqtt {
                packet(0x30, &[1; 256])
            } else {
                frame(&[1; 256])
            };
            for n in 0..wire.len() {
                let mut b = BytesMut::from(&wire[..n]);
                assert!(parse(&mut b, mqtt).unwrap().is_none());
                b.extend_from_slice(&wire[n..]);
                assert_eq!(parse(&mut b, mqtt).unwrap().unwrap().1.len(), 256);
                assert!(b.is_empty());
            }
        }
    }
    #[test]
    fn histogram_counts_and_quantiles() {
        let mut h = Histogram::default();
        for _ in 0..100 {
            h.add(Duration::from_micros(125));
        }
        assert_eq!(h.percentile(50), 0.13);
        h.add(Duration::from_secs(100));
        assert_eq!(h.count, 101);
        assert!(h.percentile(99) >= 0.13);
    }
    #[test]
    fn bad_config_and_oversize_response() {
        assert!(
            Config {
                qos: 2,
                ..Config::default()
            }
            .validate()
            .is_ok()
        );
        assert!(
            Config {
                connections: 50001,
                ..Config::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Config {
                qos: 3,
                ..Config::default()
            }
            .validate()
            .is_err()
        );
        assert!(parse(&mut BytesMut::from(&[0x30, 0xff, 0xff, 0x7f][..]), true).is_err());
        assert!(connack_accepted(&(0x20, vec![0, 0]), true));
        assert!(!connack_accepted(&(0x20, vec![1, 0]), true));
        assert!(connack_accepted(&(0x20, vec![1, 0]), false));
        assert!(!connack_accepted(&(0x20, vec![2, 0]), false));
    }
}
