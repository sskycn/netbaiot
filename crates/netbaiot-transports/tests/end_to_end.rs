use bytes::BytesMut;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_storage::MemoryStore;
use netbaiot_transports::{
    mqtt::{
        packet,
        topics::{TopicKind, topic},
    },
    tcp::{LengthPrefixFramer, TcpFramer},
    *,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
fn auth(name: &str) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new(name).unwrap(),
        },
        credential_version: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}
fn payload(source: &str) -> Vec<u8> {
    format!(r#"{{"schema_version":1,"source_message_id":"{source}","kind":"telemetry","data":{{"temperature":25.3}}}}"#).into_bytes()
}
struct Fixture {
    s: Arc<Services>,
    store: Arc<MemoryStore>,
    addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
}
impl Fixture {
    async fn new(transport: Transport, limits: Limits) -> Self {
        Self::with_gate(transport, limits, None).await
    }
    async fn with_gate(
        transport: Transport,
        limits: Limits,
        gate: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Self {
        Self::with_auth(transport, limits, gate, None).await
    }
    async fn with_auth(
        transport: Transport,
        limits: Limits,
        gate: Option<Arc<tokio::sync::Semaphore>>,
        authenticator: Option<Arc<dyn DeviceAuthenticator>>,
    ) -> Self {
        let l = Arc::new(limits);
        let store = MemoryStore::new(l.clone());
        let credentials = ["a", "b"]
            .map(|name| Credential {
                credential_id: name.into(),
                secret_hex: SECRET.into(),
                identity: auth(name),
            })
            .to_vec();
        let authenticator =
            authenticator.unwrap_or_else(|| StaticAuthenticator::new(credentials, &l).unwrap());
        let codecs = CodecRegistry::new(vec![(
            CodecId::new("netbaiot-json").unwrap(),
            1,
            Arc::new(JsonV1::default()),
        )])
        .unwrap();
        let metrics = Arc::new(Metrics::default());
        let sessions = Sessions::new(l.clone());
        let selected: Arc<dyn Store> = if let Some(gate) = gate {
            Arc::new(GatedStore {
                inner: store.clone(),
                gate,
                after_commit: None,
            })
        } else {
            store.clone()
        };
        let ingress = Arc::new(Ingress::new(
            l,
            authenticator,
            codecs,
            selected,
            metrics,
            sessions,
        ));
        let mut s = Services::new(ingress);
        let admin = AdminAccess::new(
            &"ab".repeat(32),
            [auth("a"), auth("b")]
                .into_iter()
                .map(|a| (a.device_key.clone(), a))
                .collect(),
            &s.ingress.limits,
        )
        .unwrap();
        Arc::get_mut(&mut s).unwrap().admin = Some(admin);
        let stop = CancellationToken::new();
        let (addr, task) = if transport == Transport::Udp {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = socket.local_addr().unwrap();
            let task = tokio::spawn(udp::serve(socket, s.clone(), stop.clone()));
            (addr, task)
        } else {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let task = tokio::spawn(serve_stream(
                listener,
                transport,
                s.clone(),
                None,
                stop.clone(),
            ));
            (addr, task)
        };
        Self {
            s,
            store,
            addr,
            stop,
            task,
        }
    }
    async fn shutdown(self) {
        self.s.ingress.drain();
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(self.s.connections.active().unwrap(), [0; 4]);
        assert_eq!(self.s.ingress.sessions.queued_bytes(), 0);
    }
    async fn mqtt(&self, name: &str, keepalive: u16) -> TcpStream {
        let mut stream = TcpStream::connect(self.addr).await.unwrap();
        stream
            .write_all(&connect(name, SECRET, keepalive, true))
            .await
            .unwrap();
        assert_eq!(read_mqtt(&mut stream).await, vec![0x20, 2, 0, 0]);
        stream
    }
}
fn string(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
}
fn connect(name: &str, secret: &str, keepalive: u16, clean: bool) -> Vec<u8> {
    let mut body = Vec::new();
    string(&mut body, b"MQTT");
    body.push(4);
    body.push(if clean { 0xc2 } else { 0xc0 });
    body.extend_from_slice(&keepalive.to_be_bytes());
    string(&mut body, name.as_bytes());
    string(&mut body, name.as_bytes());
    string(&mut body, secret.as_bytes());
    packet::encode(0x10, &body, 65536).unwrap()
}
async fn read_mqtt(stream: &mut TcpStream) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let first = stream.read_u8().await.unwrap();
        let mut out = vec![first];
        let mut length = 0usize;
        let mut mul = 1;
        loop {
            let byte = stream.read_u8().await.unwrap();
            out.push(byte);
            length += usize::from(byte & 127) * mul;
            if byte & 128 == 0 {
                break;
            }
            mul *= 128;
        }
        let start = out.len();
        out.resize(start + length, 0);
        stream.read_exact(&mut out[start..]).await.unwrap();
        out
    })
    .await
    .unwrap()
}
async fn closed(stream: &mut TcpStream) {
    let mut one = [0];
    let result = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut one))
        .await
        .unwrap();
    assert!(matches!(result, Ok(0) | Err(_)));
}
fn subscribe(id: u16, topics: &[(&str, u8)]) -> Vec<u8> {
    let mut body = id.to_be_bytes().to_vec();
    for (topic, qos) in topics {
        string(&mut body, topic.as_bytes());
        body.push(*qos);
    }
    packet::encode(0x82, &body, 65536).unwrap()
}
fn command(a: &AuthenticatedDevice) -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId::generate(),
        device: a.device_key.clone(),
        expires_at: now_ms() + 60000,
        payload: DeviceCommandPayload {
            name: "set_led".into(),
            arguments: Default::default(),
        },
    }
}

#[tokio::test]
async fn mqtt_ingress_application_ack_dedup_and_command_execution() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let a = auth("a");
    let mut stream = f.mqtt("a", 30).await;
    let down = topic(&a.device_key, TopicKind::Down);
    let up = topic(&a.device_key, TopicKind::Up);
    let up_ack = topic(&a.device_key, TopicKind::UpAck);
    stream
        .write_all(&subscribe(1, &[(&down, 1), (&up_ack, 1)]))
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut stream).await, vec![0x90, 4, 0, 1, 1, 1]);
    let uplink = packet::publish(&up, &payload("boot:1"), Some(7), &f.s.ingress.limits).unwrap();
    stream.write_all(&uplink).await.unwrap();
    assert_eq!(read_mqtt(&mut stream).await, packet::ack(0x40, 7));
    assert_eq!(f.store.message_count().unwrap(), 1);
    let mut frame = BytesMut::from(read_mqtt(&mut stream).await.as_slice());
    let packet::Packet::Publish {
        payload, packet_id, ..
    } = packet::decode(&mut frame, &f.s.ingress.limits)
        .unwrap()
        .unwrap()
    else {
        panic!("expected application receipt")
    };
    let receipt: IngressReceipt = serde_json::from_slice(&payload).unwrap();
    assert_eq!(receipt.boundary, ReceiptBoundary::Volatile);
    assert!(!receipt.duplicate);
    stream
        .write_all(&packet::ack(0x40, packet_id.unwrap()))
        .await
        .unwrap();
    // Same application identity with a different packet ID returns its original receipt.
    let mut retry = uplink.clone();
    retry[0] |= 8;
    stream.write_all(&retry).await.unwrap();
    assert_eq!(read_mqtt(&mut stream).await, packet::ack(0x40, 7));
    let raw = read_mqtt(&mut stream).await;
    let packet::Packet::Publish {
        payload, packet_id, ..
    } = packet::decode(&mut BytesMut::from(raw.as_slice()), &f.s.ingress.limits)
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    let duplicate: IngressReceipt = serde_json::from_slice(&payload).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(receipt.message_id, duplicate.message_id);
    stream
        .write_all(&packet::ack(0x40, packet_id.unwrap()))
        .await
        .unwrap();
    assert_eq!(f.store.message_count().unwrap(), 1);
    let c = command(&a);
    f.s.router.queue(&a, c.clone()).await.unwrap();
    let claimed = f
        .store
        .claim_commands(Some(&a.device_key), now_ms(), 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    f.s.router.dispatch(claimed[0].clone(), &a).await.unwrap();
    let raw = read_mqtt(&mut stream).await;
    let packet::Packet::Publish {
        topic,
        payload,
        packet_id,
        ..
    } = packet::decode(&mut BytesMut::from(raw.as_slice()), &f.s.ingress.limits)
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(topic, down);
    let received: DeviceCommand = serde_json::from_slice(&payload).unwrap();
    assert_eq!(received.command_id, c.command_id);
    stream
        .write_all(&packet::ack(0x40, packet_id.unwrap()))
        .await
        .unwrap();
    stream.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(read_mqtt(&mut stream).await, vec![0xd0, 0]);
    let record = f
        .store
        .get_command(&a.device_key, c.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.delivery, DeliveryState::Received);
    assert_eq!(record.execution, ExecutionState::Unknown);
    let ack = format!(
        r#"{{"schema_version":1,"source_message_id":"ack:1","kind":"command_ack","data":{{"command_id":"{}","execution":"succeeded"}}}}"#,
        c.command_id.0
    );
    let down_ack = netbaiot_transports::mqtt::topics::topic(&a.device_key, TopicKind::DownAck);
    stream
        .write_all(
            &packet::publish(&down_ack, ack.as_bytes(), Some(9), &f.s.ingress.limits).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut stream).await, packet::ack(0x40, 9));
    let _receipt = read_mqtt(&mut stream).await;
    assert_eq!(
        f.store
            .get_command(&a.device_key, c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Succeeded
    );
    stream.write_all(&[0xe0, 0]).await.unwrap();
    closed(&mut stream).await;
    f.shutdown().await;
}
#[tokio::test]
async fn mqtt_acl_subscribe_unsubscribe_qos0_and_ping() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let a = auth("a");
    let mut b = f.mqtt("b", 30).await;
    let down = topic(&a.device_key, TopicKind::Down);
    b.write_all(&subscribe(1, &[(&down, 1), ("#", 1)]))
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut b).await, vec![0x90, 4, 0, 1, 0x80, 0x80]);
    let own = topic(&auth("b").device_key, TopicKind::Down);
    b.write_all(&subscribe(2, &[(&own, 0)])).await.unwrap();
    assert_eq!(read_mqtt(&mut b).await, vec![0x90, 3, 0, 2, 0]);
    let mut body = 3u16.to_be_bytes().to_vec();
    string(&mut body, own.as_bytes());
    b.write_all(&packet::encode(0xa2, &body, 65536).unwrap())
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut b).await, packet::ack(0xb0, 3));
    let up = topic(&auth("b").device_key, TopicKind::Up);
    b.write_all(&packet::publish(&up, &payload("qos0"), None, &f.s.ingress.limits).unwrap())
        .await
        .unwrap();
    b.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(read_mqtt(&mut b).await, vec![0xd0, 0]);
    assert_eq!(f.store.message_count().unwrap(), 1);
    let forbidden = topic(&a.device_key, TopicKind::Up);
    b.write_all(
        &packet::publish(&forbidden, &payload("bad"), Some(1), &f.s.ingress.limits).unwrap(),
    )
    .await
    .unwrap();
    closed(&mut b).await;
    assert_eq!(f.store.message_count().unwrap(), 1);
    f.shutdown().await;
}
#[tokio::test]
async fn mqtt_protocol_rejections() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    for before in [
        vec![0xc0, 0],
        vec![0x40, 2, 0, 1],
        vec![0x30, 3, 0, 1, b'x'],
    ] {
        let mut c = TcpStream::connect(f.addr).await.unwrap();
        c.write_all(&before).await.unwrap();
        closed(&mut c).await;
    }
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    c.write_all(&connect("a", "wrong", 30, true)).await.unwrap();
    assert_eq!(read_mqtt(&mut c).await, packet::connack(4));
    closed(&mut c).await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    c.write_all(&connect("a", SECRET, 30, false)).await.unwrap();
    assert_eq!(read_mqtt(&mut c).await, packet::connack(5));
    closed(&mut c).await;
    let mut c = f.mqtt("a", 30).await;
    c.write_all(&connect("a", SECRET, 30, true)).await.unwrap();
    closed(&mut c).await;
    let mut c = f.mqtt("a", 30).await;
    c.write_all(&[0x34, 5, 0, 1, b'x', 0, 1]).await.unwrap();
    closed(&mut c).await;
    let mut c = f.mqtt("a", 30).await;
    c.write_all(&[0x40, 2, 0, 0]).await.unwrap();
    closed(&mut c).await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    let mut bytes = connect("a", SECRET, 30, true);
    bytes[8] = 5;
    c.write_all(&bytes).await.unwrap();
    assert_eq!(read_mqtt(&mut c).await, vec![0x20, 3, 0, 0x84, 0]);
    closed(&mut c).await;
    f.shutdown().await;
}
#[tokio::test]
async fn mqtt_reconnect_keeps_new_session_and_cleans_subscriptions() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let a = auth("a");
    let mut old = f.mqtt("a", 30).await;
    let down = topic(&a.device_key, TopicKind::Down);
    old.write_all(&subscribe(1, &[(&down, 1)])).await.unwrap();
    read_mqtt(&mut old).await;
    let generation =
        f.s.ingress
            .sessions
            .lookup(&a.device_key)
            .unwrap()
            .unwrap()
            .generation;
    let mut new = f.mqtt("a", 30).await;
    closed(&mut old).await;
    let active = f.s.ingress.sessions.lookup(&a.device_key).unwrap().unwrap();
    assert!(active.generation > generation);
    assert!(
        f.s.subscriptions
            .lookup(&down, active.generation)
            .unwrap()
            .is_none()
    );
    new.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(read_mqtt(&mut new).await, vec![0xd0, 0]);
    f.shutdown().await;
}
#[tokio::test]
async fn mqtt_keepalive_and_partial_frame_deadlines() {
    let f = Fixture::new(
        Transport::Mqtt,
        Limits {
            packet_read_timeout_ms: 80,
            ..Limits::default()
        },
    )
    .await;
    let mut c = f.mqtt("a", 1).await;
    closed(&mut c).await;
    assert_eq!(f.s.ingress.metrics.get(Metric::MqttKeepaliveDisconnects), 1);
    let mut c = f.mqtt("a", 0).await;
    c.write_all(&[0x30, 127, 0]).await.unwrap();
    closed(&mut c).await;
    f.shutdown().await;
}
async fn http(f: &Fixture, method: &str, path: &str, body: &[u8]) -> Vec<u8> {
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    let header = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:{SECRET}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    c.write_all(header.as_bytes()).await.unwrap();
    if body.len() <= f.s.ingress.limits.max_http_body_size {
        c.write_all(body).await.unwrap();
    }
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), c.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    response
}
#[tokio::test]
async fn http_acceptance_dedup_conflict_pull_ack_and_limits() {
    let f = Fixture::new(Transport::Http, Limits::default()).await;
    let response = http(&f, "POST", "/v1/device/messages", &payload("http:1")).await;
    assert!(response.starts_with(b"HTTP/1.1 202"));
    assert_eq!(f.store.message_count().unwrap(), 1);
    assert!(
        String::from_utf8(http(&f, "POST", "/v1/device/messages", &payload("http:1")).await)
            .unwrap()
            .contains("\"duplicate\":true")
    );
    let changed = String::from_utf8(payload("http:1"))
        .unwrap()
        .replace("25.3", "26.0");
    assert!(
        http(&f, "POST", "/v1/device/messages", changed.as_bytes())
            .await
            .starts_with(b"HTTP/1.1 409")
    );
    let a = auth("a");
    let c = command(&a);
    f.s.router.queue(&a, c.clone()).await.unwrap();
    let pulled = http(&f, "GET", "/v1/device/commands", b"").await;
    assert!(pulled.starts_with(b"HTTP/1.1 200"));
    assert!(
        String::from_utf8(pulled)
            .unwrap()
            .contains(&c.command_id.0.to_string())
    );
    assert_eq!(
        f.store
            .get_command(&a.device_key, c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Unknown
    );
    let ack = format!(
        r#"{{"schema_version":1,"source_message_id":"http:ack","kind":"command_ack","data":{{"command_id":"{}","execution":"succeeded"}}}}"#,
        c.command_id.0
    );
    assert!(
        http(&f, "POST", "/v1/device/commands/ack", ack.as_bytes())
            .await
            .starts_with(b"HTTP/1.1 202")
    );
    assert_eq!(
        f.store
            .get_command(&a.device_key, c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Succeeded
    );
    assert!(
        http(&f, "POST", "/v1/device/messages", &vec![0; 65537])
            .await
            .starts_with(b"HTTP/1.1 413")
    );
    assert!(
        !f.s.ingress
            .sessions
            .presence(&a.device_key)
            .unwrap()
            .unwrap()
            .connected
    );
    let metrics = String::from_utf8(http(&f, "GET", "/metrics", b"").await).unwrap();
    assert!(metrics.starts_with("HTTP/1.1 200"));
    assert!(metrics.contains("netbaiot_ingress_inflight 0\n"));
    // The metrics request itself owns one protocol permit.
    assert!(metrics.contains("netbaiot_protocol_inflight 1\n"));
    assert!(metrics.contains("netbaiot_registered_sessions 0\n"));
    assert!(metrics.contains("netbaiot_subscription_entries 0\n"));
    assert!(metrics.contains("netbaiot_runtime_alive_tasks "));
    f.shutdown().await;
}
async fn tcp_read(c: &mut TcpStream) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let n = c.read_u32().await.unwrap();
        assert!(n <= 65536);
        let mut b = vec![0; n as usize];
        c.read_exact(&mut b).await.unwrap();
        b
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn tcp_split_frames_downlink_and_shutdown() {
    let f = Fixture::new(Transport::Tcp, Limits::default()).await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    let framer = LengthPrefixFramer { maximum: 65536 };
    let hello = framer
        .encode(format!(r#"{{"credential_id":"a","secret":"{SECRET}"}}"#).as_bytes())
        .unwrap();
    for part in hello.chunks(3) {
        c.write_all(part).await.unwrap();
    }
    assert!(
        String::from_utf8(tcp_read(&mut c).await)
            .unwrap()
            .contains("authenticated")
    );
    let mut frames = framer.encode(&payload("tcp:1")).unwrap();
    frames.extend_from_slice(&framer.encode(&payload("tcp:2")).unwrap());
    c.write_all(&frames).await.unwrap();
    tcp_read(&mut c).await;
    tcp_read(&mut c).await;
    assert_eq!(f.store.message_count().unwrap(), 2);
    let a = auth("a");
    let command = command(&a);
    f.s.router.queue(&a, command.clone()).await.unwrap();
    f.s.router
        .dispatch(
            f.store
                .claim_commands(Some(&a.device_key), now_ms(), 1)
                .await
                .unwrap()
                .remove(0),
            &a,
        )
        .await
        .unwrap();
    let down: DeviceCommand = serde_json::from_slice(&tcp_read(&mut c).await).unwrap();
    assert_eq!(down.command_id, command.command_id);
    f.shutdown().await;
    closed(&mut c).await;
}
fn datagram(sequence: u64, timestamp: i64, payload: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut d = b"NBI1".to_vec();
    d.push(1);
    d.push(b'a');
    d.extend_from_slice(&1u32.to_be_bytes());
    d.extend_from_slice(&[1; 16]);
    d.extend_from_slice(&sequence.to_be_bytes());
    d.extend_from_slice(&timestamp.to_be_bytes());
    d.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    d.extend_from_slice(payload);
    let mut mac = Hmac::<Sha256>::new_from_slice(&decode_hex(SECRET).unwrap()).unwrap();
    mac.update(&d);
    d.extend_from_slice(&mac.finalize().into_bytes());
    d
}
#[tokio::test]
async fn udp_authentication_replay_and_no_fake_session() {
    let f = Fixture::new(Transport::Udp, Limits::default()).await;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let d = datagram(1, now_ms(), &payload("udp:1"));
    socket.send_to(&d, f.addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.store.message_count().unwrap() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    socket.send_to(&d, f.addr).await.unwrap();
    let mut forged = datagram(2, now_ms(), &payload("udp:2"));
    forged[30] ^= 1;
    socket.send_to(&forged, f.addr).await.unwrap();
    socket
        .send_to(&datagram(3, now_ms() - 200000, &payload("udp:3")), f.addr)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(f.store.message_count().unwrap(), 1);
    assert!(
        !f.s.ingress
            .sessions
            .presence(&auth("a").device_key)
            .unwrap()
            .unwrap()
            .connected
    );
    let mut response = [0; 1200];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), socket.recv(&mut response))
            .await
            .is_err()
    );
    f.shutdown().await;
}
#[tokio::test]
async fn slow_writer_is_bounded_and_disconnect_mid_frame_releases_connection() {
    let (mut writer, _reader) = tokio::io::duplex(1);
    assert!(matches!(
        write(&mut writer, &[0; 128], 20).await,
        Err(Error::Timeout)
    ));
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    c.write_all(&[0x10, 100, 0]).await.unwrap();
    drop(c);
    f.shutdown().await;
}

struct GatedStore {
    inner: Arc<dyn Store>,
    gate: Arc<tokio::sync::Semaphore>,
    after_commit: Option<std::path::PathBuf>,
}
#[async_trait::async_trait]
impl Store for GatedStore {
    async fn accept(&self, input: StoredIngress) -> Result<IngressReceipt> {
        let permit = self.gate.acquire().await.map_err(|_| Error::Storage)?;
        permit.forget();
        let receipt = self.inner.accept(input).await?;
        if let Some(path) = &self.after_commit {
            tokio::fs::write(path, serde_json::to_vec(&receipt).unwrap())
                .await
                .unwrap();
            std::future::pending::<()>().await;
        }
        Ok(receipt)
    }
    async fn claim_jobs(&self, o: uuid::Uuid, n: i64, l: usize) -> Result<Vec<DeliveryJob>> {
        self.inner.claim_jobs(o, n, l).await
    }
    async fn finish_job(&self, j: &DeliveryJob, s: bool, r: bool, n: i64, next: i64) -> Result<()> {
        self.inner.finish_job(j, s, r, n, next).await
    }
    async fn insert_command(&self, c: DeviceCommand) -> Result<CommandRecord> {
        self.inner.insert_command(c).await
    }
    async fn claim_commands(
        &self,
        d: Option<&DeviceKey>,
        n: i64,
        l: usize,
    ) -> Result<Vec<CommandRecord>> {
        self.inner.claim_commands(d, n, l).await
    }
    async fn claim_command_batch(
        &self,
        devices: &[DeviceKey],
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>> {
        self.inner.claim_command_batch(devices, now, limit).await
    }
    async fn command_state(
        &self,
        d: &DeviceKey,
        id: CommandId,
        attempt: u32,
        s: DeliveryState,
    ) -> Result<bool> {
        self.inner.command_state(d, id, attempt, s).await
    }
    async fn get_command(&self, d: &DeviceKey, id: CommandId) -> Result<Option<CommandRecord>> {
        self.inner.get_command(d, id).await
    }
    async fn maintain(&self, n: i64, b: usize) -> Result<()> {
        self.inner.maintain(n, b).await
    }
}
#[tokio::test]
async fn mqtt_never_acknowledges_before_acceptance_and_closes_on_storage_timeout() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let f = Fixture::with_gate(Transport::Mqtt, Limits::default(), Some(gate.clone())).await;
    let mut c = f.mqtt("a", 30).await;
    let a = auth("a");
    let up = topic(&a.device_key, TopicKind::Up);
    let ack = topic(&a.device_key, TopicKind::UpAck);
    c.write_all(&subscribe(1, &[(&ack, 0)])).await.unwrap();
    read_mqtt(&mut c).await;
    c.write_all(&packet::publish(&up, &payload("gated"), Some(1), &f.s.ingress.limits).unwrap())
        .await
        .unwrap();
    let mut b = [0; 1];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), c.read(&mut b))
            .await
            .is_err()
    );
    assert_eq!(f.store.message_count().unwrap(), 0);
    gate.add_permits(1);
    assert_eq!(read_mqtt(&mut c).await, packet::ack(0x40, 1));
    let _receipt = read_mqtt(&mut c).await;
    assert_eq!(f.store.message_count().unwrap(), 1);
    f.shutdown().await;
    let f = Fixture::with_gate(
        Transport::Mqtt,
        Limits {
            external_timeout_ms: 30,
            ..Limits::default()
        },
        Some(Arc::new(tokio::sync::Semaphore::new(0))),
    )
    .await;
    let mut c = f.mqtt("a", 30).await;
    c.write_all(&packet::publish(&up, &payload("timeout"), Some(1), &f.s.ingress.limits).unwrap())
        .await
        .unwrap();
    closed(&mut c).await;
    assert_eq!(f.store.message_count().unwrap(), 0);
    f.shutdown().await;
}
#[tokio::test]
async fn shutdown_drains_accepted_ingress_without_leaking_owners() {
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let f = Fixture::with_gate(Transport::Mqtt, Limits::default(), Some(gate.clone())).await;
    let mut c = f.mqtt("a", 30).await;
    let up = topic(&auth("a").device_key, TopicKind::Up);
    c.write_all(&packet::publish(&up, &payload("shutdown"), Some(1), &f.s.ingress.limits).unwrap())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.s.ingress.metrics.get(Metric::MqttPublishes) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.s.ingress.drain();
    f.stop.cancel();
    gate.add_permits(1);
    assert_eq!(read_mqtt(&mut c).await, packet::ack(0x40, 1));
    closed(&mut c).await;
    assert_eq!(f.store.message_count().unwrap(), 1);
    f.shutdown().await;
}
#[tokio::test]
async fn mqtt_control_packet_flood_is_rate_limited() {
    let f = Fixture::new(
        Transport::Mqtt,
        Limits {
            messages_per_device_second: 2,
            ..Limits::default()
        },
    )
    .await;
    let mut c = f.mqtt("a", 30).await;
    for _ in 0..2 {
        c.write_all(&[0xc0, 0]).await.unwrap();
        assert_eq!(read_mqtt(&mut c).await, vec![0xd0, 0]);
    }
    c.write_all(&[0xc0, 0]).await.unwrap();
    closed(&mut c).await;
    f.shutdown().await;
}
struct SlowSink;
#[async_trait::async_trait]
impl netbaiot_runtime::worker::DeliverySink for SlowSink {
    async fn deliver(
        &self,
        _: &DeviceMessage,
    ) -> std::result::Result<(), netbaiot_runtime::worker::DeliveryError> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn delivery_worker_times_out_retries_with_bound_and_shuts_down() {
    let f = Fixture::new(
        Transport::Http,
        Limits {
            external_timeout_ms: 10,
            max_attempts: 2,
            retry_base_ms: 1,
            retry_max_ms: 2,
            ..Limits::default()
        },
    )
    .await;
    http(&f, "POST", "/v1/device/messages", &payload("worker")).await;
    let stop = CancellationToken::new();
    let worker = tokio::spawn(netbaiot_runtime::worker::delivery_worker(
        f.s.ingress.clone(),
        Arc::new(SlowSink),
        stop.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.s.ingress.metrics.get(Metric::DeliveryFailed) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(f.store.job_count().unwrap(), 0);
    assert_eq!(f.s.ingress.metrics.get(Metric::DeliveryFailed), 2);
    f.shutdown().await;
}

#[tokio::test]
async fn business_api_rejects_device_credentials_and_persists_authorized_commands() {
    let f = Fixture::new(Transport::Http, Limits::default()).await;
    let command = command(&auth("a"));
    let body = serde_json::to_vec(&command).unwrap();
    assert!(
        http(&f, "POST", "/v1/admin/commands", &body)
            .await
            .starts_with(b"HTTP/1.1 401")
    );
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    let header = format!(
        "POST /v1/admin/commands HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        "ab".repeat(32),
        body.len()
    );
    c.write_all(header.as_bytes()).await.unwrap();
    c.write_all(&body).await.unwrap();
    let mut response = Vec::new();
    c.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 202"));
    assert_eq!(
        f.store
            .get_command(&command.device, command.command_id)
            .await
            .unwrap()
            .unwrap()
            .delivery,
        DeliveryState::Queued
    );
    f.shutdown().await;
}
#[tokio::test]
async fn command_worker_routes_durable_claim_to_active_session() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let a = auth("a");
    let mut c = f.mqtt("a", 30).await;
    let down = topic(&a.device_key, TopicKind::Down);
    c.write_all(&subscribe(1, &[(&down, 0)])).await.unwrap();
    read_mqtt(&mut c).await;
    let command = command(&a);
    f.s.router.queue(&a, command.clone()).await.unwrap();
    let stop = CancellationToken::new();
    let worker = tokio::spawn(netbaiot_runtime::worker::command_worker(
        f.s.router.clone(),
        [(a.device_key.clone(), a)].into_iter().collect(),
        stop.clone(),
    ));
    let raw = read_mqtt(&mut c).await;
    let packet::Packet::Publish { payload, .. } =
        packet::decode(&mut BytesMut::from(raw.as_slice()), &f.s.ingress.limits)
            .unwrap()
            .unwrap()
    else {
        panic!()
    };
    let received: DeviceCommand = serde_json::from_slice(&payload).unwrap();
    assert_eq!(received.command_id, command.command_id);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        f.store
            .get_command(&command.device, command.command_id)
            .await
            .unwrap()
            .unwrap()
            .attempts,
        1
    );
    f.shutdown().await;
}

#[tokio::test]
async fn audit_http_rejects_unsupported_content_encoding() {
    let f = Fixture::new(Transport::Http, Limits::default()).await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    let body = payload("encoding");
    let request = format!(
        "POST /v1/device/messages HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:{SECRET}\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    c.write_all(request.as_bytes()).await.unwrap();
    c.write_all(&body).await.unwrap();
    let mut response = Vec::new();
    c.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 415"));
    assert_eq!(f.store.message_count().unwrap(), 0);
    f.shutdown().await;
}

#[tokio::test]
async fn audit_http_body_admission_respects_tenant_capacity() {
    let f = Fixture::new(
        Transport::Http,
        Limits {
            max_ingress_per_tenant: 1,
            ..Limits::default()
        },
    )
    .await;
    // Hold the tenant's request-stage permit, as an authenticated slow body does.
    let held =
        f.s.protocol_admission
            .acquire(&auth("b").device_key, 0)
            .unwrap();
    let response = http(&f, "POST", "/v1/device/messages", &payload("tenant-full")).await;
    assert!(response.starts_with(b"HTTP/1.1 429"));
    assert_eq!(f.store.message_count().unwrap(), 0);
    drop(held);
    assert!(
        http(&f, "POST", "/v1/device/messages", &payload("tenant-free"))
            .await
            .starts_with(b"HTTP/1.1 202")
    );
    f.shutdown().await;
}

struct DelayedAuth {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl DeviceAuthenticator for DelayedAuth {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        self.entered.notify_one();
        self.release
            .acquire()
            .await
            .map_err(|_| Error::Authentication)?
            .forget();
        Ok(auth("a"))
    }
}
#[tokio::test]
async fn audit_shutdown_during_auth_cannot_register_session() {
    for transport in [Transport::Mqtt, Transport::Tcp] {
        let delayed = Arc::new(DelayedAuth {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let f = Fixture::with_auth(transport, Limits::default(), None, Some(delayed.clone())).await;
        let mut c = TcpStream::connect(f.addr).await.unwrap();
        let hello = if transport == Transport::Mqtt {
            connect("a", SECRET, 30, true)
        } else {
            LengthPrefixFramer { maximum: 65536 }
                .encode(format!(r#"{{"credential_id":"a","secret":"{SECRET}"}}"#).as_bytes())
                .unwrap()
        };
        c.write_all(&hello).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), delayed.entered.notified())
            .await
            .unwrap();
        f.s.ingress.drain();
        f.stop.cancel();
        delayed.release.add_permits(1);
        let mut received = Vec::new();
        c.read_to_end(&mut received).await.unwrap();
        assert!(
            received.is_empty(),
            "shutdown must not send successful auth reply"
        );
        assert!(
            f.s.ingress
                .sessions
                .presence(&auth("a").device_key)
                .unwrap()
                .is_none()
        );
        f.shutdown().await;
    }
}
#[tokio::test]
async fn audit_disconnected_auth_cannot_replace_new_session() {
    let delayed = Arc::new(DelayedAuth {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let f = Fixture::with_auth(
        Transport::Mqtt,
        Limits::default(),
        None,
        Some(delayed.clone()),
    )
    .await;
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    c.write_all(&connect("a", SECRET, 30, true)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), delayed.entered.notified())
        .await
        .unwrap();
    c.shutdown().await.unwrap();
    drop(c);
    // Wait for server-side EOF observation, independent of OS FIN scheduling.
    // Before the fix this times out: the task only observes authentication.
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.s.connections.active().unwrap()[Transport::Mqtt as usize] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (new, _rx) =
        f.s.ingress
            .sessions
            .register(&auth("a").device_key, Transport::Mqtt)
            .unwrap();
    delayed.release.add_permits(1);
    assert!(
        !new.cancel.is_cancelled(),
        "disconnected authentication evicted current owner"
    );
    assert_eq!(
        f.s.ingress
            .sessions
            .lookup(&new.device)
            .unwrap()
            .unwrap()
            .generation,
        new.generation
    );
    drop(new);
    f.shutdown().await;
}

// Crash-only fixtures wrap production I/O/storage; no failpoints in production code.
struct ReceiptFence {
    socket: TcpStream,
    reached: Arc<tokio::sync::Notify>,
}
impl tokio::io::AsyncRead for ReceiptFence {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for ReceiptFence {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if buf.first() == Some(&0x30) {
            self.reached.notify_one();
            return std::task::Poll::Pending;
        }
        std::pin::Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
#[tokio::test]
#[ignore = "subprocess helper, invoked only by audit_postgres_process_crash_boundaries"]
async fn audit_crash_child() {
    let phase = std::env::var("NETBAIOT_CRASH_PHASE").unwrap();
    let path = std::path::PathBuf::from(std::env::var("NETBAIOT_CRASH_PATH").unwrap());
    let l = Arc::new(Limits::default());
    let pg = netbaiot_storage::PgStore::connect(
        &std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap(),
        l.clone(),
    )
    .await
    .unwrap();
    let store: Arc<dyn Store> = if phase == "b" {
        Arc::new(GatedStore {
            inner: pg,
            gate: Arc::new(tokio::sync::Semaphore::new(1)),
            after_commit: Some(path.with_extension("fence")),
        })
    } else {
        pg
    };
    let authenticator = StaticAuthenticator::new(
        vec![Credential {
            credential_id: "a".into(),
            secret_hex: SECRET.into(),
            identity: auth("a"),
        }],
        &l,
    )
    .unwrap();
    let ingress = Arc::new(Ingress::new(
        l.clone(),
        authenticator,
        CodecRegistry::new(vec![(
            CodecId::new("netbaiot-json").unwrap(),
            1,
            Arc::new(JsonV1::default()),
        )])
        .unwrap(),
        store,
        Arc::new(Metrics::default()),
        Sessions::new(l),
    ));
    let services = Services::new(ingress);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    tokio::fs::write(
        path.with_extension("ready"),
        listener.local_addr().unwrap().to_string(),
    )
    .await
    .unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let lease = services
        .connections
        .acquire(peer.ip(), Transport::Mqtt)
        .unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let stream: BoxStream = if phase == "c" {
        Box::new(ReceiptFence {
            socket,
            reached: reached.clone(),
        })
    } else {
        Box::new(socket)
    };
    let connection = mqtt::connection(stream, services, lease, CancellationToken::new());
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => { result.unwrap(); },
        _ = reached.notified() => {
            tokio::fs::write(path.with_extension("fence"), b"PUBACK written; application receipt blocked").await.unwrap();
            std::future::pending::<()>().await;
        }
    }
}
async fn await_file(path: &std::path::Path) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(bytes) = tokio::fs::read(path).await {
                return bytes;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
async fn crash_child(phase: &str, path: &std::path::Path) -> (tokio::process::Child, TcpStream) {
    let child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "audit_crash_child", "--nocapture"])
        .env("NETBAIOT_CRASH_PHASE", phase)
        .env("NETBAIOT_CRASH_PATH", path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .unwrap();
    let address = String::from_utf8(await_file(&path.with_extension("ready")).await).unwrap();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(&connect("a", SECRET, 30, true))
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut socket).await, packet::connack(0));
    let up_ack = topic(&auth("a").device_key, TopicKind::UpAck);
    socket
        .write_all(&subscribe(1, &[(&up_ack, 0)]))
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut socket).await, vec![0x90, 3, 0, 1, 0]);
    (child, socket)
}
#[tokio::test]
#[ignore = "requires fresh NETBAIOT_TEST_DATABASE_URL; kills only owned test child processes"]
async fn audit_postgres_process_crash_boundaries() {
    let url = std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap();
    let pg = netbaiot_storage::PgStore::connect(&url, Arc::new(Limits::default()))
        .await
        .unwrap();
    pg.migrate().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION audit_commit_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM ingress_messages WHERE message_id=NEW.message_id AND source_message_id='crash-a') THEN PERFORM pg_advisory_xact_lock(739281); END IF; RETURN NEW; END $$; CREATE TRIGGER audit_commit_gate BEFORE INSERT ON delivery_jobs FOR EACH ROW EXECUTE FUNCTION audit_commit_gate();").execute(&pool).await.unwrap();
    let directory = std::env::temp_dir().join(format!("netbaiot-crash-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir(&directory).await.unwrap();
    for phase in ["a", "b", "c"] {
        eprintln!("testing process crash phase {phase}");
        let mut gate = pool.acquire().await.unwrap();
        if phase == "a" {
            sqlx::query("SELECT pg_advisory_lock(739281)")
                .execute(&mut *gate)
                .await
                .unwrap();
        }
        let path = directory.join(phase);
        let (mut child, mut socket) = crash_child(phase, &path).await;
        let source = format!("crash-{phase}");
        let wire = packet::publish(
            &topic(&auth("a").device_key, TopicKind::Up),
            &payload(&source),
            Some(7),
            &Limits::default(),
        )
        .unwrap();
        socket.write_all(&wire).await.unwrap();
        if phase == "a" {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=739281 AND NOT granted").fetch_one(&pool).await.unwrap();
                    if waiting == 1 { break; }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.unwrap();
        } else {
            await_file(&path.with_extension("fence")).await;
        }
        if phase == "c" {
            assert_eq!(read_mqtt(&mut socket).await, packet::ack(0x40, 7));
        }
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(30), socket.read(&mut byte))
                .await
                .is_err()
        );
        let before: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT message_id FROM ingress_messages WHERE source_message_id=$1",
        )
        .bind(&source)
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(before.is_some(), phase != "a");
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        drop(socket);
        if phase == "a" {
            sqlx::query("SELECT pg_advisory_unlock(739281)")
                .execute(&mut *gate)
                .await
                .unwrap();
        }
        drop(gate);
        let recovery_path = directory.join(format!("recover-{phase}"));
        let (mut recovery, mut socket) = crash_child("recover", &recovery_path).await;
        socket.write_all(&wire).await.unwrap();
        assert_eq!(read_mqtt(&mut socket).await, packet::ack(0x40, 7));
        let packet::Packet::Publish { payload, .. } = packet::decode(
            &mut BytesMut::from(read_mqtt(&mut socket).await.as_slice()),
            &Limits::default(),
        )
        .unwrap()
        .unwrap() else {
            panic!()
        };
        let receipt: IngressReceipt = serde_json::from_slice(&payload).unwrap();
        assert_eq!(receipt.boundary, ReceiptBoundary::Durable);
        assert_eq!(receipt.duplicate, phase != "a");
        if let Some(id) = before {
            assert_eq!(receipt.message_id.0, id);
        }
        let counts: (i64,i64) = sqlx::query_as("SELECT count(*),count(j.message_id) FROM ingress_messages m LEFT JOIN delivery_jobs j USING(message_id) WHERE m.source_message_id=$1").bind(&source).fetch_one(&pool).await.unwrap();
        assert_eq!(counts, (1, 1));
        socket.write_all(&[0xe0, 0]).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(3), recovery.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
    sqlx::raw_sql(
        "DROP TRIGGER audit_commit_gate ON delivery_jobs; DROP FUNCTION audit_commit_gate();",
    )
    .execute(&pool)
    .await
    .unwrap();
    tokio::fs::remove_dir_all(directory).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn audit_http_chunked_limits_malformed_auth_and_slow_body() {
    let f = Fixture::new(
        Transport::Http,
        Limits {
            max_http_body_size: 1024,
            request_timeout_ms: 80,
            ..Limits::default()
        },
    )
    .await;
    for (headers, body, status) in [
        (
            "Transfer-Encoding: chunked\r\n".to_owned(),
            format!("401\r\n{}\r\n0\r\n\r\n", "x".repeat(1025)).into_bytes(),
            "413",
        ),
        ("Content-Length: 1\r\n".to_owned(), b"{".to_vec(), "400"),
        ("".to_owned(), Vec::new(), "400"),
    ] {
        let mut c = TcpStream::connect(f.addr).await.unwrap();
        let request = format!(
            "POST /v1/device/messages HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:{SECRET}\r\n{headers}\r\n"
        );
        c.write_all(request.as_bytes()).await.unwrap();
        c.write_all(&body).await.unwrap();
        let mut response = Vec::new();
        c.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(format!("HTTP/1.1 {status}").as_bytes()));
    }
    let mut slow = TcpStream::connect(f.addr).await.unwrap();
    slow.write_all(format!("POST /v1/device/messages HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:{SECRET}\r\nTransfer-Encoding: chunked\r\n\r\n20\r\nx").as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), slow.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 504"));
    let mut c = TcpStream::connect(f.addr).await.unwrap();
    c.write_all(b"POST /v1/device/messages HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:bad\r\nContent-Length: 0\r\n\r\n").await.unwrap();
    let mut response = Vec::new();
    c.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 401"));
    assert_eq!(f.store.message_count().unwrap(), 0);
    f.shutdown().await;
}

#[tokio::test]
async fn audit_udp_oversize_and_failed_ingress_do_not_consume_sequence() {
    let f = Fixture::new(Transport::Udp, Limits::default()).await;
    let c = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let valid = datagram(7, now_ms(), &payload("udp-after-oversize"));
    let mut oversize = valid.clone();
    oversize.resize(1201, 0);
    c.send_to(&oversize, f.addr).await.unwrap();
    c.send_to(&datagram(7, now_ms(), b"{"), f.addr)
        .await
        .unwrap();
    c.send_to(&valid, f.addr).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.s.ingress.metrics.get(Metric::IngressAccepted) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.store.message_count().unwrap(), 1);
    assert_eq!(f.s.ingress.metrics.get(Metric::IngressRejected), 1);
    f.shutdown().await;
}

#[tokio::test]
async fn audit_mqtt_acl_path_tricks_will_and_client_id_ownership() {
    let f = Fixture::new(Transport::Mqtt, Limits::default()).await;
    let mut b = f.mqtt("b", 30).await;
    let attacks = [
        "v1/t/t/p/p/d/a/up",
        "v1/t/t2/p/p/d/b/up",
        "v1/t/t/p/p/d/b2/up",
        "v1/t/t/p/p/d/b/up/extra",
        "v1/t/t/p/p/d/b//up",
        "v1/t/t/p/p/d/%62/up",
        "v1/t/t/p/p/d/b%2Fa/up",
        "v1/t/t/p/p/d/b/down",
        "v1/t/t/p/p/d/b/up_ack",
    ];
    let filters: Vec<_> = attacks
        .iter()
        .filter(|x| packet::valid_topic(x, &f.s.ingress.limits, true))
        .map(|x| (*x, 1))
        .collect();
    b.write_all(&subscribe(1, &filters)).await.unwrap();
    let reply = read_mqtt(&mut b).await;
    let expected: Vec<_> = filters
        .iter()
        .map(|(topic, _)| {
            if matches!(*topic, "v1/t/t/p/p/d/b/down" | "v1/t/t/p/p/d/b/up_ack") {
                1
            } else {
                0x80
            }
        })
        .collect();
    assert_eq!(&reply[4..], expected.as_slice());
    b.write_all(&[0xe0, 0]).await.unwrap();
    closed(&mut b).await;
    drop(b);
    for attack in attacks {
        let mut c = f.mqtt("b", 30).await;
        let mut body = Vec::new();
        string(&mut body, attack.as_bytes());
        body.extend_from_slice(&[0, 1]);
        body.extend_from_slice(&payload("forbidden"));
        c.write_all(&packet::encode(0x32, &body, 65536).unwrap())
            .await
            .unwrap();
        closed(&mut c).await;
    }
    assert_eq!(f.store.message_count().unwrap(), 0);
    let mut a = f.mqtt("a", 30).await;
    let mut alias = TcpStream::connect(f.addr).await.unwrap();
    let mut bytes = connect("b", SECRET, 30, true);
    bytes[14] = b'a'; // client ID a, username b
    alias.write_all(&bytes).await.unwrap();
    assert_eq!(read_mqtt(&mut alias).await, packet::connack(2));
    a.write_all(&[0xc0, 0]).await.unwrap();
    assert_eq!(read_mqtt(&mut a).await, vec![0xd0, 0]);
    let mut will = TcpStream::connect(f.addr).await.unwrap();
    let mut body = Vec::new();
    string(&mut body, b"MQTT");
    body.extend_from_slice(&[4, 0xc6, 0, 30]);
    for value in [
        b"b".as_slice(),
        b"will",
        b"payload",
        b"b",
        SECRET.as_bytes(),
    ] {
        string(&mut body, value);
    }
    will.write_all(&packet::encode(0x10, &body, 65536).unwrap())
        .await
        .unwrap();
    assert_eq!(read_mqtt(&mut will).await, packet::connack(5));
    let mut malformed = f.mqtt("b", 30).await;
    malformed
        .write_all(&subscribe(3, &[("a/#/b", 1)]))
        .await
        .unwrap();
    closed(&mut malformed).await;
    f.shutdown().await;
}

#[tokio::test]
async fn audit_shutdown_during_http_body_tcp_frame_and_udp_ingress() {
    for transport in [Transport::Http, Transport::Tcp] {
        let f = Fixture::new(
            transport,
            Limits {
                shutdown_timeout_ms: 20,
                request_timeout_ms: 500,
                ..Limits::default()
            },
        )
        .await;
        let mut c = TcpStream::connect(f.addr).await.unwrap();
        if transport == Transport::Http {
            c.write_all(format!("POST /v1/device/messages HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer a:{SECRET}\r\nContent-Length: 100\r\n\r\nx").as_bytes()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(1), async {
                while f.s.ingress.metrics.get(Metric::HttpRequests) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            c.write_all(&[0, 0, 1]).await.unwrap();
        }
        f.shutdown().await;
        closed(&mut c).await;
    }
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let f = Fixture::with_gate(Transport::Udp, Limits::default(), Some(gate.clone())).await;
    let c = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    c.send_to(&datagram(1, now_ms(), &payload("udp-drain")), f.addr)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.s.ingress.metrics.get(Metric::IngressBytes) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.s.ingress.drain();
    f.stop.cancel();
    gate.add_permits(1);
    let store = f.store.clone();
    f.shutdown().await;
    assert_eq!(store.message_count().unwrap(), 1);
}
