//! Bounded mixed-ingress audit driver. Owns one task and finite inflight state per worker.
//! Uses the existing load generator's wire encoders, TLS validation and histograms.
use super::*;
use std::{collections::VecDeque, io};

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Audit {
    address: String,
    tls_ca: Option<String>,
    start_ms: u64,
    duration_secs: f64,
    warmup_secs: f64,
    ramp_secs: f64,
    timeout_ms: u64,
    payload_bytes: usize,
    groups: Vec<Group>,
}
impl Default for Audit {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:24640".into(),
            tls_ca: None,
            start_ms: 0,
            duration_secs: 60.0,
            warmup_secs: 5.0,
            ramp_secs: 1.0,
            timeout_ms: 1000,
            payload_bytes: 256,
            groups: Vec::new(),
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct Group {
    label: String,
    protocol: String,
    workers: usize,
    offset: usize,
    rate: f64,
    reuse: bool,
    mode: String,
    delay_secs: f64,
    window: usize,
}
impl Default for Group {
    fn default() -> Self {
        Self {
            label: "mqtt".into(),
            protocol: "mqtt".into(),
            workers: 16,
            offset: 0,
            rate: 1000.0,
            reuse: true,
            mode: "normal".into(),
            delay_secs: 0.0,
            window: 32,
        }
    }
}
impl Audit {
    fn validate(&self) -> Result<()> {
        if self.groups.is_empty()
            || self.groups.len() > 16
            || self.payload_bytes > 1024
            || !(100..=10_000).contains(&self.timeout_ms)
            || !self.duration_secs.is_finite()
            || !(0.1..=3600.0).contains(&self.duration_secs)
            || !self.warmup_secs.is_finite()
            || !(0.0..=60.0).contains(&self.warmup_secs)
            || !self.ramp_secs.is_finite()
            || !(0.0..=60.0).contains(&self.ramp_secs)
            || self
                .groups
                .iter()
                .try_fold(0usize, |n, g| n.checked_add(g.workers))
                .is_none_or(|n| n > 2048)
        {
            return Err("invalid bounded mixed audit configuration".into());
        }
        let mut labels = std::collections::HashSet::new();
        let mut devices = std::collections::HashSet::new();
        for g in &self.groups {
            if !labels.insert(&g.label)
                || g.label.is_empty()
                || (g.mode == "normal" && g.rate == 0.0)
                || (g.mode == "idle" && !["mqtt", "tcp"].contains(&g.protocol.as_str()))
                || g.label.len() > 48
                || g.workers == 0
                || g.workers > 1024
                || g.offset > 50000
                || !(1..=256).contains(&g.window)
                || !g.rate.is_finite()
                || !(0.0..=500_000.0).contains(&g.rate)
                || !g.delay_secs.is_finite()
                || !(0.0..=3600.0).contains(&g.delay_secs)
                || !["http", "mqtt", "tcp", "udp"].contains(&g.protocol.as_str())
                || ![
                    "normal",
                    "idle",
                    "tls_storm",
                    "tls_pending",
                    "unclassified",
                    "slow_http",
                    "slow_tcp",
                ]
                .contains(&g.mode.as_str())
            {
                return Err("invalid mixed audit group".into());
            }
            for id in g.offset..g.offset + g.workers {
                if !devices.insert(id) {
                    return Err("overlapping audit device identities".into());
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone)]
struct Worker {
    config: Arc<Audit>,
    group: Group,
    id: usize,
    stats: Shared,
    measure: Instant,
    end: Instant,
    traffic: Instant,
    run: String,
}
impl Worker {
    fn measured(&self) -> bool {
        let now = Instant::now();
        now >= self.measure && now < self.end
    }
    fn counter(&self, key: &'static str, n: u64) {
        if self.measured() {
            count(&self.stats, key, n);
        }
    }
    fn ack(&self, at: Instant) {
        if at >= self.measure && at < self.end {
            count(&self.stats, "accepted", 1);
            observe(&self.stats, "acceptance", at.elapsed());
        }
    }
    fn failure(&self, key: &'static str) {
        self.counter(key, 1);
    }
    fn timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout_ms)
    }
    fn interval(&self) -> Duration {
        Duration::from_secs_f64(self.group.workers as f64 / self.group.rate.max(0.001))
    }
    fn due(&self) -> Instant {
        self.traffic
            + Duration::from_secs_f64(
                (self.id - self.group.offset) as f64 / self.group.rate.max(1.0),
            )
    }
    fn scheduled(&self, due: &mut Instant, maximum: usize) -> usize {
        let now = Instant::now();
        if now < *due || now >= self.end || self.group.rate == 0.0 {
            return 0;
        }
        let step = self.interval();
        let slots = (now.duration_since(*due).as_nanos() / step.as_nanos()).min(u64::MAX as u128)
            as u64
            + 1;
        *due += step.mul_f64(slots as f64);
        let send = (slots as usize).min(maximum);
        self.counter("scheduled", slots);
        self.counter("schedule_missed", slots - send as u64);
        send
    }
}
fn io_error(e: &io::Error) -> &'static str {
    match e.kind() {
        io::ErrorKind::ConnectionRefused => "connect_refused",
        io::ErrorKind::TimedOut => "connect_timeout",
        _ => "remote_close",
    }
}
async fn dial(w: &Worker, connector: &Option<TlsConnector>) -> Result<Stream> {
    count(&w.stats, "connect_attempts_total", 1);
    w.counter("connect_attempts", 1);
    let at = Instant::now();
    let socket = match timeout(w.timeout(), TcpStream::connect(&w.config.address)).await {
        Err(_) => {
            w.failure("connect_timeout");
            return Err("connect timeout".into());
        }
        Ok(Err(e)) => {
            w.failure(io_error(&e));
            return Err("connect failed".into());
        }
        Ok(Ok(s)) => s,
    };
    socket.set_nodelay(true)?;
    let stream: Stream = if let Some(tls) = connector {
        match timeout(
            w.timeout(),
            tls.connect(
                rustls::pki_types::ServerName::try_from("localhost")?,
                socket,
            ),
        )
        .await
        {
            Ok(Ok(s)) => Box::new(s),
            _ => {
                w.failure("tls_failure");
                return Err("TLS failed".into());
            }
        }
    } else {
        Box::new(socket)
    };
    count(&w.stats, "connected_total", 1);
    w.counter("connected", 1);
    if w.measured() {
        observe(&w.stats, "connect", at.elapsed());
    }
    Ok(stream)
}
async fn send(w: &Worker, stream: &mut Stream, bytes: &[u8]) -> Result<()> {
    match timeout(w.timeout(), stream.write_all(bytes)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => {
            w.failure("remote_close");
            Err("write close".into())
        }
        Err(_) => {
            w.failure("write_timeout");
            Err("write timeout".into())
        }
    }
}
async fn receive(
    w: &Worker,
    stream: &mut Stream,
    buffer: &mut BytesMut,
    mqtt: bool,
) -> Result<(u8, Vec<u8>)> {
    match timeout(w.timeout(), async {
        loop {
            if let Some(p) = parse(buffer, mqtt)? {
                return Ok(p);
            }
            read_more(stream, buffer).await?;
        }
    })
    .await
    {
        Ok(Ok(p)) => Ok(p),
        Ok(Err(e)) => {
            w.failure("remote_close");
            Err(e)
        }
        Err(_) => {
            w.failure("read_timeout");
            Err("read timeout".into())
        }
    }
}
async fn authenticate(w: &Worker, stream: &mut Stream, buffer: &mut BytesMut) -> Result<()> {
    let mqtt = w.group.protocol == "mqtt";
    let wire = if mqtt {
        let mut b = Vec::new();
        text(&mut b, "MQTT");
        b.extend_from_slice(&[4, 0xc2, 0, 30]);
        text(&mut b, &format!("mixed-{}", w.id));
        text(&mut b, &format!("a{}", w.id));
        text(&mut b, SECRET);
        packet(0x10, &b)
    } else {
        frame(&serde_json::to_vec(
            &json!({"credential_id":format!("a{}",w.id),"secret":SECRET}),
        )?)
    };
    send(w, stream, &wire).await?;
    let (first, bytes) = receive(w, stream, buffer, mqtt).await?;
    let valid = if mqtt {
        first == 0x20 && bytes == [0, 0]
    } else {
        serde_json::from_slice::<Value>(&bytes)?["authenticated"] == true
    };
    if !valid {
        w.failure("auth_failure");
        return Err("auth failure".into());
    }
    count(&w.stats, "authenticated_total", 1);
    w.counter("authenticated", 1);
    Ok(())
}
async fn stream_session(
    w: &Worker,
    connector: &Option<TlsConnector>,
    sequence: &mut u64,
    due: &mut Instant,
) -> Result<()> {
    let mqtt = w.group.protocol == "mqtt";
    let mut stream = dial(w, connector).await?;
    let mut buffer = BytesMut::with_capacity(4096);
    authenticate(w, &mut stream, &mut buffer).await?;
    let mut pending: VecDeque<(u16, Instant)> = VecDeque::new();
    let mut pid = 0u16;
    let mut ping = Instant::now() + Duration::from_secs(10);
    loop {
        while let Some((first, b)) = parse(&mut buffer, mqtt)? {
            if mqtt && first == 0xd0 {
                continue;
            }
            if let Some((expected, at)) = pending.pop_front() {
                let valid = if mqtt {
                    first == 0x40 && b == expected.to_be_bytes()
                } else {
                    serde_json::from_slice::<Value>(&b).is_ok_and(|v| {
                        v["event_id"].is_string()
                            && v["accepted_at"].is_number()
                            && v["required_deliveries"].as_u64().is_some_and(|n| n > 0)
                    })
                };
                if !valid {
                    w.failure("protocol_error");
                    return Err("receipt mismatch".into());
                }
                w.ack(at);
                if !w.group.reuse {
                    if mqtt {
                        send(w, &mut stream, &[0xe0, 0]).await?;
                    }
                    return Ok(());
                }
            } else {
                w.failure("unexpected_receipt");
            }
        }
        let now = Instant::now();
        if now >= w.end && pending.is_empty() {
            break;
        }
        if now >= w.end + w.timeout() {
            count(&w.stats, "unconfirmed", pending.len() as u64);
            break;
        }
        if pending
            .front()
            .is_some_and(|(_, at)| at.elapsed() > w.timeout())
        {
            w.failure("read_timeout");
            w.counter("unconfirmed", pending.len() as u64);
            return Err("receipt timeout".into());
        }
        let slots = if w.group.mode == "idle" {
            0
        } else {
            w.scheduled(due, if w.group.reuse { 32 } else { 1 })
        };
        for _ in 0..slots {
            if pending.len() >= w.group.window {
                w.counter("client_window_full", 1);
                continue;
            }
            *sequence += 1;
            pid = pid.wrapping_add(1).max(1);
            let payload = body(&w.run, w.id, *sequence, w.config.payload_bytes, 0);
            let wire = if mqtt {
                let mut b = Vec::new();
                text(&mut b, &format!("v1/t/t{}/p/p/d/d{}/up", w.id / 4096, w.id));
                b.extend_from_slice(&pid.to_be_bytes());
                b.extend_from_slice(&payload);
                packet(0x32, &b)
            } else {
                frame(&payload)
            };
            let at = Instant::now();
            w.counter("attempted", 1);
            send(w, &mut stream, &wire).await?;
            pending.push_back((pid, at));
        }
        if mqtt && Instant::now() >= ping {
            send(w, &mut stream, &[0xc0, 0]).await?;
            ping = Instant::now() + Duration::from_secs(10);
        }
        let wake = if now >= w.end {
            w.end + w.timeout()
        } else {
            if w.group.mode == "idle" {
                Instant::now() + Duration::from_secs(1)
            } else {
                *due
            }
            .min(w.end)
        }
        .min(Instant::now() + w.timeout());
        tokio::select! {r=read_more(&mut stream,&mut buffer)=>{
            if r.is_err(){w.failure("remote_close");w.counter("unconfirmed",pending.len() as u64);return r;}
        },_=sleep_until(wake)=>{}}
    }
    if mqtt {
        let _ = send(w, &mut stream, &[0xe0, 0]).await;
    }
    Ok(())
}
async fn http_response(w: &Worker, stream: &mut Stream) -> Result<(u16, bool)> {
    let result = timeout(w.timeout(), async {
        let mut input = Vec::with_capacity(1024);
        let mut header = None;
        loop {
            if let Some((end, size)) = header {
                if input.len() >= end + size {
                    break;
                }
            }
            if input.len() > 65536 {
                return Err("HTTP response too large".into());
            }
            let mut chunk = [0; 4096];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err("HTTP remote close".into());
            }
            input.extend_from_slice(&chunk[..n]);
            if header.is_none() {
                if let Some(at) = input.windows(4).position(|b| b == b"\r\n\r\n") {
                    let head = std::str::from_utf8(&input[..at])?;
                    let size = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::trim)
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .ok_or("missing content length")?;
                    if size > 65536 {
                        return Err("HTTP body limit".into());
                    }
                    header = Some((at + 4, size));
                }
            }
        }
        let closes = std::str::from_utf8(&input[..header.ok_or("HTTP header")?.0])?
            .lines()
            .any(|line| line.eq_ignore_ascii_case("connection: close"));
        let status = std::str::from_utf8(
            &input[..input.iter().position(|b| *b == b'\r').ok_or("HTTP line")?],
        )?
        .split_whitespace()
        .nth(1)
        .ok_or("HTTP status")?
        .parse::<u16>()?;
        Ok((status, closes))
    })
    .await;
    match result {
        Ok(Ok(status)) => Ok(status),
        Ok(Err(e)) => {
            w.failure("remote_close");
            Err(e)
        }
        Err(_) => {
            w.failure("read_timeout");
            Err("HTTP timeout".into())
        }
    }
}
async fn http_worker(w: Worker, connector: Option<TlsConnector>) {
    let mut stream = None;
    let mut seq = 0;
    let mut due = w.due();
    while Instant::now() < w.end {
        sleep_until(due.min(w.end)).await;
        if w.scheduled(&mut due, 1) == 0 {
            continue;
        }
        let at = Instant::now();
        w.counter("attempted", 1);
        if stream.is_none() {
            match dial(&w, &connector).await {
                Ok(s) => stream = Some(s),
                Err(_) => continue,
            }
        }
        let Some(socket) = stream.as_mut() else {
            continue;
        };
        seq += 1;
        let payload = body(&w.run, w.id, seq, w.config.payload_bytes, 0);
        let mut request=format!("POST /v1/device/data HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a{}:{}\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n",w.id,SECRET,payload.len(),if w.group.reuse {"keep-alive"}else{"close"}).into_bytes();
        request.extend_from_slice(&payload);
        if send(&w, socket, &request).await.is_err() {
            stream = None;
            continue;
        }
        let (status, closes) = match http_response(&w, socket).await {
            Ok(result) => result,
            Err(_) => {
                stream = None;
                continue;
            }
        };
        match status {
            202 => w.ack(at),
            401 | 403 => w.failure("auth_failure"),
            429 | 503 => w.failure("http_overloaded"),
            _ => w.failure("http_status_error"),
        }
        if closes {
            w.counter("http_server_close", 1);
        }
        if !w.group.reuse || closes {
            let _ = timeout(Duration::from_millis(100), socket.read(&mut [0; 1])).await;
            stream = None;
        }
    }
}
async fn udp_worker(w: Worker) -> Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.connect(&w.config.address).await?;
    let boot = *Uuid::new_v4().as_bytes();
    let key: [u8; 32] = std::array::from_fn(|i| i as u8);
    let mut pending: HashMap<u64, Instant> = HashMap::new();
    let mut seq = 0u64;
    let mut due = w.due();
    let mut input = [0; 65];
    while Instant::now() < w.end {
        let slots = w.scheduled(&mut due, 256);
        for _ in 0..slots {
            if pending.len() >= w.group.window {
                w.counter("client_window_full", 1);
                continue;
            }
            seq += 1;
            let payload = body(&w.run, w.id, seq, w.config.payload_bytes, 0);
            let id = format!("a{}", w.id);
            let mut wire = b"NBI1".to_vec();
            wire.push(id.len() as u8);
            wire.extend_from_slice(id.as_bytes());
            wire.extend_from_slice(&1u32.to_be_bytes());
            wire.extend_from_slice(&boot);
            wire.extend_from_slice(&seq.to_be_bytes());
            wire.extend_from_slice(&(now_ms() as i64).to_be_bytes());
            wire.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            wire.extend_from_slice(&payload);
            let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
            mac.update(&wire);
            wire.extend_from_slice(&mac.finalize().into_bytes());
            let at = Instant::now();
            w.counter("attempted", 1);
            match socket.try_send(&wire) {
                Ok(_) => {
                    pending.insert(seq, at);
                }
                Err(_) => w.failure("udp_send_error"),
            }
        }
        let expired = pending
            .values()
            .filter(|at| at.elapsed() > w.timeout())
            .count();
        if expired > 0 {
            w.counter("udp_no_ack", expired as u64);
            pending.retain(|_, at| at.elapsed() <= w.timeout());
        }
        tokio::select! {
            r=socket.recv(&mut input)=>{
                match r {
                    Ok(64) if &input[..4]==b"NBA1" && input[4..8]==1u32.to_be_bytes() && input[8..24]==boot=>{
                        let mut mac=Hmac::<Sha256>::new_from_slice(&key)?;mac.update(&input[..32]);
                        if mac.verify_slice(&input[32..64]).is_err(){w.failure("udp_invalid_ack");continue;}
                        let id=u64::from_be_bytes(input[24..32].try_into()?);
                        if let Some(at)=pending.remove(&id){w.ack(at);}else{w.failure("udp_late_ack");}
                    },Ok(_)=>w.failure("udp_invalid_ack"),Err(_)=>w.failure("remote_close"),
                }
            },_=sleep_until(due.min(w.end))=>{}
        }
    }
    // Bounded receipt drain; no new transmissions after measurement ends.
    let drain = Instant::now() + w.timeout();
    while !pending.is_empty() && Instant::now() < drain {
        match timeout_at(drain, socket.recv(&mut input)).await {
            Ok(Ok(64))
                if &input[..4] == b"NBA1"
                    && input[4..8] == 1u32.to_be_bytes()
                    && input[8..24] == boot =>
            {
                let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
                mac.update(&input[..32]);
                if mac.verify_slice(&input[32..64]).is_ok() {
                    let id = u64::from_be_bytes(input[24..32].try_into()?);
                    if let Some(at) = pending.remove(&id) {
                        w.ack(at);
                    }
                }
            }
            _ => break,
        }
    }
    count(
        &w.stats,
        "udp_no_ack",
        pending.values().filter(|at| **at >= w.measure).count() as u64,
    );
    Ok(())
}
async fn interference(w: Worker, connector: Option<TlsConnector>) -> Result<()> {
    let mut due = w.due();
    while Instant::now() < w.end {
        if w.group.rate > 0.0 {
            sleep_until(due.min(w.end)).await;
            if w.scheduled(&mut due, 1) == 0 {
                continue;
            }
        }
        if w.group.mode == "tls_pending" {
            w.counter("connect_attempts", 1);
            match timeout(w.timeout(), TcpStream::connect(&w.config.address)).await {
                Ok(Ok(mut s)) => {
                    w.counter("connected", 1);
                    let _ = timeout_at(w.end, s.read(&mut [0; 16])).await;
                }
                Ok(Err(error)) => w.failure(io_error(&error)),
                Err(_) => w.failure("connect_timeout"),
            }
        } else {
            match dial(&w, &connector).await {
                Ok(mut s) => {
                    let sent = match w.group.mode.as_str() {
                        "slow_http" => {
                            send(
                                &w,
                                &mut s,
                                b"POST /v1/device/data HTTP/1.1\r\nHost: localhost\r\n",
                            )
                            .await
                        }
                        "slow_tcp" => send(&w, &mut s, &[0, 0, 0, 128, b'{']).await,
                        _ => Ok(()),
                    };
                    if sent.is_err() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        continue;
                    }
                    if w.group.mode != "tls_storm" {
                        let _ = timeout_at(w.end, s.read(&mut [0; 16])).await;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        if w.group.rate == 0.0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    Ok(())
}

pub(super) async fn run(path: &str) -> Result<()> {
    let mut config: Audit = serde_json::from_slice(&read_bounded(path, 65536)?)?;
    config.validate()?;
    if config.start_ms == 0 {
        config.start_ms =
            now_ms() + ((config.warmup_secs + config.ramp_secs + 1.0) * 1000.0) as u64;
    }
    if config.start_ms > now_ms().saturating_add(120_000) {
        return Err("start time more than two minutes ahead".into());
    }
    let config = Arc::new(config);
    let connector = tls(&super::Config {
        tls_ca: config.tls_ca.clone(),
        ..super::Config::default()
    })?;
    let until = config.start_ms.saturating_sub(now_ms());
    let measure = Instant::now() + Duration::from_millis(until);
    let end = measure + Duration::from_secs_f64(config.duration_secs);
    let base = Instant::now();
    let run = Uuid::new_v4().simple().to_string();
    let mut tasks = JoinSet::new();
    let mut groups = Vec::new();
    println!(
        "{}",
        json!({"event":"start","pid":std::process::id(),"config":&*config,"epoch_ms":now_ms()})
    );
    for group in &config.groups {
        let stats: Shared = Arc::new(Mutex::new(Stats::default()));
        groups.push((group.label.clone(), stats.clone()));
        for relative in 0..group.workers {
            let w = Worker {
                config: config.clone(),
                group: group.clone(),
                id: group.offset + relative,
                stats: stats.clone(),
                measure,
                end,
                traffic: measure - Duration::from_secs_f64(config.warmup_secs)
                    + Duration::from_secs_f64(group.delay_secs),
                run: run.clone(),
            };
            let connector = connector.clone();
            let ramp =
                Duration::from_secs_f64(config.ramp_secs * relative as f64 / group.workers as f64);
            tasks.spawn(async move {
                sleep_until(base + ramp + Duration::from_secs_f64(w.group.delay_secs)).await;
                if w.group.mode != "normal" && w.group.mode != "idle" {
                    return interference(w, connector).await;
                }
                match w.group.protocol.as_str() {
                    "http" => {
                        http_worker(w, connector).await;
                        Ok(())
                    }
                    "udp" => udp_worker(w).await,
                    _ => {
                        let mut sequence = 0;
                        let mut due = w.due();
                        while Instant::now() < w.end {
                            if stream_session(&w, &connector, &mut sequence, &mut due)
                                .await
                                .is_err()
                            {
                                w.counter("disconnects", 1);
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        Ok(())
                    }
                }
            });
        }
    }
    let snapshot_all = || {
        groups
            .iter()
            .map(|(label, s)| (label.clone(), snapshot(s)))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let deadline = end + Duration::from_millis(config.timeout_ms + 3000);
    while !tasks.is_empty() {
        tokio::select! {r=tasks.join_next()=>{if let Some(r)=r{r??;}},_=tick.tick()=>{
            println!("{}",json!({"event":"sample","epoch_ms":now_ms(),"measurement_secs":Instant::now().saturating_duration_since(measure).as_secs_f64(),"groups":snapshot_all()}));
        },_=sleep_until(deadline)=>{tasks.abort_all();while tasks.join_next().await.is_some(){}return Err("audit workers exceeded shutdown deadline".into());}}
    }
    println!(
        "{}",
        json!({"event":"final","epoch_ms":now_ms(),"duration_secs":config.duration_secs,"groups":snapshot_all()})
    );
    Ok(())
}
use tokio::time::timeout_at;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_and_group_names_are_validated() {
        let mut c = Audit {
            groups: vec![Group::default()],
            ..Audit::default()
        };
        assert!(c.validate().is_ok());
        c.groups.push(Group::default());
        assert!(c.validate().is_err());
        c.groups.pop();
        c.groups[0].window = 257;
        assert!(c.validate().is_err());
        c.groups[0].window = 32;
        c.groups[0].rate = f64::NAN;
        assert!(c.validate().is_err());
        c.groups[0].rate = 0.0;
        assert!(c.validate().is_err());
        c.groups[0].mode = "idle".into();
        assert!(c.validate().is_ok());
        c.groups[0].workers = usize::MAX;
        assert!(c.validate().is_err());
        c.groups[0] = Group::default();
        let overlapping = Group {
            label: "another".into(),
            ..Group::default()
        };
        c.groups.push(overlapping);
        assert!(c.validate().is_err());
    }
}
