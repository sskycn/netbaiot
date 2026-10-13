//! Separate low-frequency connection-density workload. One owned task per held
//! socket, one root ramp waiter, and bounded handshake concurrency. No server API.
use super::*;
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpSocket;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Density {
    address: SocketAddr,
    source_ips: Vec<IpAddr>,
    connections: usize,
    offset: usize,
    tenant_width: usize,
    ramp_per_sec: u32,
    handshake_concurrency: usize,
    hold_secs: u32,
    timeout_secs: u32,
    ping_secs: u16,
    transport: String,
    mqtt_version: u8,
    persistent: bool,
    subscribe: bool,
    tls_ca: Option<String>,
    tls_server_name: String,
}
impl Default for Density {
    fn default() -> Self {
        Self {
            address: SocketAddr::from(([127, 0, 0, 1], 8080)),
            source_ips: Vec::new(),
            connections: 10_000,
            offset: 0,
            tenant_width: 1_000_001,
            ramp_per_sec: 1000,
            handshake_concurrency: 64,
            hold_secs: 60,
            timeout_secs: 10,
            ping_secs: 0,
            transport: "mqtt".into(),
            mqtt_version: 4,
            persistent: false,
            subscribe: false,
            tls_ca: None,
            tls_server_name: "localhost".into(),
        }
    }
}
impl Density {
    fn validate(&self) -> Result<()> {
        if !(1..=1_000_000).contains(&self.connections)
            || self
                .offset
                .checked_add(self.connections)
                .is_none_or(|n| n > 100_000_000)
            || self.tenant_width == 0
            || !(1..=100_000).contains(&self.ramp_per_sec)
            || !(1..=4096).contains(&self.handshake_concurrency)
            || !(5..=86400).contains(&self.hold_secs)
            || !(1..=120).contains(&self.timeout_secs)
            || ![4, 5].contains(&self.mqtt_version)
            || !["mqtt", "tcp"].contains(&self.transport.as_str())
            || self.source_ips.len() > 64
            || self.tls_server_name.is_empty()
            || self.tls_server_name.len() > 253
            || self
                .source_ips
                .iter()
                .any(|ip| ip.is_ipv4() != self.address.is_ipv4())
        {
            return Err("invalid bounded density configuration".into());
        }
        Ok(())
    }
}

async fn read_packet(
    stream: &mut (impl AsyncRead + Unpin + ?Sized),
    mqtt: bool,
    maximum: usize,
) -> Result<(u8, Vec<u8>)> {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header[..1]).await?;
    // Idle reads have no packet deadline. Once a byte arrives, the complete
    // hostile frame must arrive within a bounded read budget.
    timeout(Duration::from_secs(10), async {
        // Header is at most five bytes; no large scratch buffer survives a read.
        let (first, length) = if mqtt {
            stream.read_exact(&mut header[1..2]).await?;
            let (mut length, mut multiplier) = (usize::from(header[1] & 127), 128usize);
            let mut at = 1;
            while header[at] & 128 != 0 {
                at += 1;
                if at >= 5 {
                    return Err("invalid remaining length".into());
                }
                stream.read_exact(&mut header[at..at + 1]).await?;
                length = length
                    .checked_add(usize::from(header[at] & 127) * multiplier)
                    .ok_or("length overflow")?;
                multiplier *= 128;
            }
            (header[0], length)
        } else {
            stream.read_exact(&mut header[1..4]).await?;
            (0, u32::from_be_bytes(header[..4].try_into()?) as usize)
        };
        if length > maximum {
            return Err("density peer packet exceeds bound".into());
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await?;
        Ok((first, body))
    })
    .await?
}

async fn establish(
    c: &Density,
    id: usize,
    relative: usize,
    connector: &Option<TlsConnector>,
    s: &Shared,
) -> Result<Stream> {
    let began = Instant::now();
    let socket = if c.address.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if !c.source_ips.is_empty() {
        socket.bind(SocketAddr::new(
            c.source_ips[relative % c.source_ips.len()],
            0,
        ))?;
    }
    let socket = socket.connect(c.address).await?;
    socket.set_nodelay(true)?;
    let mut stream: Stream = if let Some(connector) = connector {
        Box::new(
            connector
                .connect(
                    rustls::pki_types::ServerName::try_from(c.tls_server_name.clone())?,
                    socket,
                )
                .await?,
        )
    } else {
        Box::new(socket)
    };
    let mqtt = c.transport == "mqtt";
    if mqtt {
        let mut body = Vec::with_capacity(128);
        text(&mut body, "MQTT");
        let keepalive = if c.ping_secs == 0 {
            0
        } else {
            c.ping_secs.saturating_mul(3)
        };
        body.extend_from_slice(&[c.mqtt_version, 0xc0 | (u8::from(!c.persistent) << 1)]);
        body.extend_from_slice(&keepalive.to_be_bytes());
        if c.mqtt_version == 5 {
            if c.persistent {
                body.extend_from_slice(&[5, 0x11, 0, 0, 14, 16]);
            } else {
                body.push(0);
            }
        }
        let identity = format!("a{id}");
        text(&mut body, &identity);
        text(&mut body, &identity);
        text(&mut body, SECRET);
        stream.write_all(&packet(0x10, &body)).await?;
        let ack = read_packet(&mut stream, true, MAX_PACKET).await?;
        let accepted = if c.mqtt_version == 4 {
            connack_accepted(&ack, !c.persistent)
        } else {
            ack.0 == 0x20
                && ack.1.len() >= 3
                && ack.1[1] == 0
                && ack.1[0] <= 1
                && (c.persistent || ack.1[0] == 0)
                && property_end(&ack.1, 2)? == ack.1.len()
        };
        if !accepted {
            count(s, "connect_rejected", 1);
            return Err("CONNACK rejection".into());
        }
        if c.subscribe {
            let mut body = vec![0, 1];
            if c.mqtt_version == 5 {
                body.push(0);
            }
            text(
                &mut body,
                &format!("v1/t/t{}/p/p/d/d{id}/down", id / c.tenant_width),
            );
            body.push(1);
            stream.write_all(&packet(0x82, &body)).await?;
            let ack = read_packet(&mut stream, true, MAX_PACKET).await?;
            let expected = if c.mqtt_version == 5 {
                vec![0, 1, 0, 1]
            } else {
                vec![0, 1, 1]
            };
            if ack != (0x90, expected) {
                return Err("SUBACK rejection".into());
            }
        }
    } else {
        let body = json!({"credential_id":format!("a{id}"),"secret":SECRET}).to_string();
        stream.write_all(&frame(body.as_bytes())).await?;
        let (_, body) = read_packet(&mut stream, false, MAX_PACKET).await?;
        if serde_json::from_slice::<Value>(&body)?["authenticated"] != true {
            return Err("TCP auth rejection".into());
        }
    }
    observe(s, "connect", began.elapsed());
    count(s, "connected", 1);
    Ok(stream)
}

async fn hold(c: &Density, stream: Stream, end: Instant, s: &Shared) -> Result<()> {
    let mqtt = c.transport == "mqtt";
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut ping = if mqtt && c.ping_secs != 0 {
        Instant::now() + Duration::from_secs(u64::from(c.ping_secs))
    } else {
        end
    };
    let mut ping_sent = None;
    let mut qos2 = std::collections::HashSet::new();
    // One retained packet read survives ping selections, so partial read_exact
    // bytes are never lost by cancellation. Only owned shutdown drops the read.
    while Instant::now() < end {
        let mut packet = Box::pin(read_packet(&mut reader, mqtt, MAX_PACKET));
        let received = loop {
            let ack_deadline = ping_sent.map_or(end, |sent: Instant| {
                sent + Duration::from_secs(u64::from(c.timeout_secs))
            });
            tokio::select! {
                result = &mut packet => break Some(result?),
                _ = sleep_until(end) => break None,
                _ = sleep_until(ack_deadline), if ping_sent.is_some() => return Err("PINGRESP timeout".into()),
                _ = sleep_until(ping), if mqtt && c.ping_secs != 0 && ping_sent.is_none() => {
                    if end.saturating_duration_since(Instant::now()) <= Duration::from_secs(u64::from(c.timeout_secs.min(u32::from(c.ping_secs)))) {
                        ping = end;
                        continue;
                    }
                    timeout(Duration::from_secs(u64::from(c.timeout_secs)), writer.write_all(&[0xc0, 0])).await??;
                    ping_sent = Some(Instant::now());
                    ping = Instant::now() + Duration::from_secs(u64::from(c.ping_secs));
                    count(s, "pings_sent", 1);
                }
            }
        };
        drop(packet);
        let Some((first, body)) = received else {
            break;
        };
        let ack = if mqtt && first == 0xd0 && body.is_empty() {
            count(s, "ping_responses", 1);
            if let Some(sent) = ping_sent.take() {
                observe(s, "ping", Instant::now() - sent);
            }
            None
        } else if mqtt && first == 0xe0 {
            return Err("server disconnect".into());
        } else if mqtt && first >> 4 == 3 {
            let topic_len = usize::from(u16::from_be_bytes(
                body.get(..2).ok_or("short topic")?.try_into()?,
            ));
            let at = 2usize.checked_add(topic_len).ok_or("topic overflow")?;
            if at > body.len() {
                return Err("short publish topic".into());
            }
            let qos = (first >> 1) & 3;
            count(s, "downlink_packets", 1);
            match qos {
                0 => None,
                1 | 2 => {
                    let id = u16::from_be_bytes(
                        body.get(at..at + 2).ok_or("short packet id")?.try_into()?,
                    );
                    if id == 0 {
                        return Err("invalid packet id".into());
                    }
                    if qos == 2 && !qos2.contains(&id) {
                        if qos2.len() >= 32 {
                            return Err("client QoS2 window exceeded".into());
                        }
                        qos2.insert(id);
                    }
                    Some(vec![
                        if qos == 1 { 0x40 } else { 0x50 },
                        2,
                        (id >> 8) as u8,
                        id as u8,
                    ])
                }
                _ => return Err("invalid publish qos".into()),
            }
        } else if mqtt && first == 0x62 {
            let id = u16::from_be_bytes(body.get(..2).ok_or("short pubrel")?.try_into()?);
            if !qos2.remove(&id) {
                return Err("unexpected PUBREL".into());
            }
            Some(vec![0x70, 2, (id >> 8) as u8, id as u8])
        } else {
            count(s, "received_packets", 1);
            None
        };
        if let Some(ack) = ack {
            timeout(
                Duration::from_secs(u64::from(c.timeout_secs)),
                writer.write_all(&ack),
            )
            .await??;
        }
    }
    if mqtt {
        timeout(
            Duration::from_secs(u64::from(c.timeout_secs)),
            writer.write_all(&[0xe0, 0]),
        )
        .await??;
    }
    count(s, "disconnected", 1);
    Ok(())
}

async fn device(
    c: Arc<Density>,
    s: Shared,
    connector: Option<TlsConnector>,
    permit: tokio::sync::OwnedSemaphorePermit,
    relative: usize,
    end: Instant,
) {
    count(&s, "connect_attempts", 1);
    let connection = timeout(
        Duration::from_secs(u64::from(c.timeout_secs)),
        Box::pin(establish(&c, c.offset + relative, relative, &connector, &s)),
    )
    .await;
    drop(permit);
    let result = match connection {
        Ok(Ok(stream)) => hold(&c, stream, end, &s).await,
        Ok(Err(error)) => Err(error),
        Err(error) => Err(error.into()),
    };
    if let Err(error) = result {
        error_sample(&s, &error);
        count(&s, "client_errors", 1);
    }
}

pub(super) async fn run(path: &str) -> Result<()> {
    let c: Density = serde_json::from_slice(&read_bounded(path, 65536)?)?;
    c.validate()?;
    let connector = tls(&Config {
        tls_ca: c.tls_ca.clone(),
        tls_server_name: c.tls_server_name.clone(),
        ..Config::default()
    })?;
    let c = Arc::new(c);
    let s: Shared = Arc::new(Mutex::new(Stats::default()));
    let slots = Arc::new(tokio::sync::Semaphore::new(c.handshake_concurrency));
    let start = Instant::now();
    let end = start
        + Duration::from_secs_f64(
            c.connections as f64 / f64::from(c.ramp_per_sec) + f64::from(c.hold_secs),
        );
    let mut tasks = JoinSet::new();
    for relative in 0..c.connections {
        sleep_until(start + Duration::from_secs_f64(relative as f64 / f64::from(c.ramp_per_sec)))
            .await;
        let permit = slots.clone().acquire_owned().await?; // only root waits, before spawning
        while let Some(result) = tasks.try_join_next() {
            if result.is_err() {
                count(&s, "panicked_tasks", 1);
            }
        }
        let (c, s, connector) = (c.clone(), s.clone(), connector.clone());
        tasks.spawn(device(c, s, connector, permit, relative, end));
    }
    println!(
        "{}",
        json!({"event":"held","pid":std::process::id(),"stats":snapshot(&s)})
    );
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let deadline = end + Duration::from_secs(u64::from(c.timeout_secs) + 5);
    while !tasks.is_empty() {
        tokio::select! {
            result = tasks.join_next() => { if result.is_some_and(|r| r.is_err()) { count(&s, "panicked_tasks", 1); } },
            _ = tick.tick() => println!("{}", json!({"event":"sample","elapsed_s":start.elapsed().as_secs_f64(),"stats":snapshot(&s)})),
            _ = sleep_until(deadline) => { tasks.abort_all(); while tasks.join_next().await.is_some() {} count(&s,"forced_tasks",1); break; }
        }
    }
    let final_stats = snapshot(&s);
    println!(
        "{}",
        json!({"event":"final","elapsed_s":start.elapsed().as_secs_f64(),"stats":final_stats})
    );
    if final_stats["counters"]["connected"].as_u64() != Some(c.connections as u64)
        || final_stats["counters"]["disconnected"].as_u64() != Some(c.connections as u64)
        || final_stats["counters"]["client_errors"]
            .as_u64()
            .unwrap_or(0)
            != 0
    {
        return Err("density population validation failed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "isolated release future-layout measurement"]
    async fn density_compact_future_sizes() {
        let c = Arc::new(Density::default());
        let s: Shared = Arc::new(Mutex::new(Stats::default()));
        let connector = None;
        let (socket, _peer) = tokio::io::duplex(64);
        let mut stream: Stream = Box::new(socket);
        println!(
            "LOADGEN_SIZE,compact_packet_read,{}",
            std::mem::size_of_val(&read_packet(&mut stream, true, MAX_PACKET))
        );
        println!(
            "LOADGEN_SIZE,compact_establish_boxed_at_call,{}",
            std::mem::size_of_val(&establish(&c, 0, 0, &connector, &s))
        );
        let end = Instant::now() + Duration::from_secs(60);
        println!(
            "LOADGEN_SIZE,compact_hold,{}",
            std::mem::size_of_val(&hold(&c, stream, end, &s))
        );
        let slot = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap();
        println!(
            "LOADGEN_SIZE,compact_device_task,{}",
            std::mem::size_of_val(&device(c, s, connector, slot, 0, end))
        );
    }

    #[test]
    fn million_count_is_a_bound_not_a_capacity_claim() {
        let mut config = Density {
            connections: 1_000_000,
            ..Density::default()
        };
        assert!(config.validate().is_ok());
        config.connections += 1;
        assert!(config.validate().is_err());
        config.connections = 1;
        config.handshake_concurrency = 0;
        assert!(config.validate().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_read_survives_timer_selections_and_owned_shutdown() {
        let c = Density {
            ping_secs: 1,
            ..Density::default()
        };
        let s: Shared = Arc::new(Mutex::new(Stats::default()));
        let (client, mut server) = tokio::io::duplex(64);
        let peer = tokio::spawn(async move {
            loop {
                let mut control = [0; 2];
                server.read_exact(&mut control).await.unwrap();
                if control == [0xe0, 0] {
                    break;
                }
                assert_eq!(control, [0xc0, 0]);
                // Splitting the two-byte PINGRESP exercises the retained read.
                server.write_all(&[0xd0]).await.unwrap();
                tokio::task::yield_now().await;
                server.write_all(&[0]).await.unwrap();
            }
        });
        hold(
            &c,
            Box::new(client),
            Instant::now() + Duration::from_secs(5),
            &s,
        )
        .await
        .unwrap();
        peer.await.unwrap();
        let stats = snapshot(&s);
        assert!(stats["counters"]["pings_sent"].as_u64().unwrap() >= 3);
        assert_eq!(
            stats["counters"]["pings_sent"],
            stats["counters"]["ping_responses"]
        );
        assert_eq!(stats["counters"]["disconnected"], 1);
    }

    #[tokio::test]
    async fn density_packets_split_coalesce_and_reject_hostile_lengths() {
        let mut input = &[0xd0, 0, 0x40, 2, 0, 7][..];
        assert_eq!(
            read_packet(&mut input, true, MAX_PACKET).await.unwrap(),
            (0xd0, vec![])
        );
        assert_eq!(
            read_packet(&mut input, true, MAX_PACKET).await.unwrap(),
            (0x40, vec![0, 7])
        );
        for wire in [
            &[0x30, 0xff, 0xff, 0xff, 0xff][..],
            &[0x30, 0x80][..],
            &[0x30, 127][..],
        ] {
            assert!(read_packet(&mut &wire[..], true, 64).await.is_err());
        }
        assert!(
            read_packet(&mut &[0xff, 0xff, 0xff, 0xff][..], false, 64)
                .await
                .is_err()
        );
    }
}
