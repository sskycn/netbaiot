use async_trait::async_trait;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{Services, serve_stream};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

struct CountingProvider {
    calls: AtomicUsize,
    auth: AuthenticatedDevice,
    delay: bool,
}

#[async_trait]
impl DeviceAuthenticator for CountingProvider {
    async fn authenticate(&self, _: AuthenticationRequest<'_>) -> Result<AuthenticatedDevice> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok(self.auth.clone())
    }

    async fn resolve_verifier(&self, _: &str) -> Result<DeviceVerifier> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.delay {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Ok(DeviceVerifier::new(self.auth.clone(), [7; 32]))
    }
}

struct AckSink;
#[async_trait]
impl EventSink for AckSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        Ok(SinkAck)
    }
}

fn auth() -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("a").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}

fn payload(sequence: usize) -> Vec<u8> {
    format!(
        r#"{{"schema_version":1,"source_message_id":"s:{sequence}","kind":"heartbeat","data":{{"sequence":{sequence}}}}}"#
    )
    .into_bytes()
}

fn runtime(
    limits: Limits,
    provider: Arc<CountingProvider>,
) -> (Arc<Ingress>, Arc<Services>, CancellationToken) {
    runtime_with_sink(limits, provider, Arc::new(AckSink))
}

fn runtime_with_sink(
    limits: Limits,
    provider: Arc<CountingProvider>,
    sink: Arc<dyn EventSink>,
) -> (Arc<Ingress>, Arc<Services>, CancellationToken) {
    let limits = Arc::new(limits);
    let metrics = Arc::new(Metrics::default());
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let sink_id = SinkId::new("sink").unwrap();
    let events = EventBus::new(
        limits.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sink_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink,
            &limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sink_id],
        }],
        1,
    )
    .unwrap();
    let config = ConfigCache::empty(limits.clone());
    let ingress = Arc::new(Ingress::new(
        limits.clone(),
        AuthCache::new(provider, limits.clone(), metrics.clone()),
        CodecRegistry::new(vec![(
            CodecId::new("netbaiot-json").unwrap(),
            1,
            Arc::new(JsonV1::default()),
        )])
        .unwrap(),
        events,
        config,
        metrics,
        Sessions::new(limits),
        lifecycle,
    ));
    let stop = CancellationToken::new();
    let services = Services::new(ingress.clone(), stop.clone());
    (ingress, services, stop)
}

struct CountSink(AtomicUsize);

#[async_trait]
impl EventSink for CountSink {
    async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(SinkAck)
    }
}

#[tokio::test]
async fn one_authentication_then_ten_thousand_messages_calls_provider_once() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        requests_per_second: 20_000,
        messages_per_device_second: 20_000,
        messages_per_tenant_second: 20_000,
        sink_queue_max_count: 20_000,
        sink_queue_max_bytes: 32 * 1024 * 1024,
        global_event_max_count: 20_000,
        global_event_max_bytes: 32 * 1024 * 1024,
        ..Limits::default()
    };
    let (ingress, _, _) = runtime(limits, provider.clone());
    let bound = ingress
        .authenticate(AuthenticationRequest::Secret {
            credential_id: "a",
            secret: b"secret",
        })
        .await
        .unwrap();
    for sequence in 0..10_000 {
        let bytes = payload(sequence);
        ingress
            .ingest(
                &bound,
                IngressEnvelope {
                    transport: Transport::Mqtt,
                    payload: &bytes,
                    require_command_ack: false,
                    require_config_ack: false,
                    validated_at: std::time::Instant::now(),
                    validation_us: 0,
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn identical_cache_misses_are_single_flight() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: true,
    });
    let limits = Arc::new(Limits::default());
    let cache = AuthCache::new(provider.clone(), limits, Arc::new(Metrics::default()));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(async move {
            cache
                .authenticate(AuthenticationRequest::Secret {
                    credential_id: "same",
                    secret: b"same-secret",
                })
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
}

fn mqtt_string(value: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value);
}

fn connect_packet() -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_string(b"MQTT", &mut body);
    body.push(4);
    body.push(0xc2);
    body.extend_from_slice(&30u16.to_be_bytes());
    mqtt_string(b"a", &mut body);
    mqtt_string(b"a", &mut body);
    mqtt_string(b"secret", &mut body);
    let mut packet = vec![0x10];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining != 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

fn connect_packet_options(clean: bool, will: Option<(&str, &[u8], u8, bool)>) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_string(b"MQTT", &mut body);
    body.push(4);
    let mut flags = 0xc0 | if clean { 2 } else { 0 };
    if let Some((_, _, qos, retain)) = will {
        flags |= 4 | (qos << 3) | (u8::from(retain) * 0x20);
    }
    body.push(flags);
    body.extend_from_slice(&30u16.to_be_bytes());
    mqtt_string(b"client-a", &mut body);
    if let Some((topic, payload, _, _)) = will {
        mqtt_string(topic.as_bytes(), &mut body);
        mqtt_string(payload, &mut body);
    }
    mqtt_string(b"a", &mut body);
    mqtt_string(b"secret", &mut body);
    let mut packet = vec![0x10];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining != 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

fn publish_packet(body: &[u8]) -> Vec<u8> {
    let topic = "v1/t/t/p/p/d/a/up";
    let remaining = 2 + topic.len() + 2 + body.len();
    assert!(remaining < 128);
    let mut packet = vec![0x32, remaining as u8];
    mqtt_string(topic.as_bytes(), &mut packet);
    packet.extend_from_slice(&7u16.to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

fn publish_packet_with_id(body: &[u8], packet_id: u16) -> Vec<u8> {
    let topic = "v1/t/t/p/p/d/a/up";
    let remaining = 2 + topic.len() + 2 + body.len();
    let mut packet = vec![0x32];
    let mut value = remaining;
    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            encoded |= 0x80;
        }
        packet.push(encoded);
        if value == 0 {
            break;
        }
    }
    mqtt_string(topic.as_bytes(), &mut packet);
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

fn qos2_publish(body: &[u8], packet_id: u16, dup: bool) -> Vec<u8> {
    let topic = "v1/t/t/p/p/d/a/up";
    let mut packet = Vec::new();
    mqtt_string(topic.as_bytes(), &mut packet);
    packet.extend_from_slice(&packet_id.to_be_bytes());
    packet.extend_from_slice(body);
    let first = if dup { 0x3c } else { 0x34 };
    let mut wire = vec![first, packet.len() as u8];
    wire.extend_from_slice(&packet);
    wire
}

#[tokio::test]
async fn inbound_qos2_duplicate_sequence_emits_one_device_event() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let sink = Arc::new(CountSink(AtomicUsize::new(0)));
    let (_, services, stop) = runtime_with_sink(Limits::default(), provider.clone(), sink.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        None,
        stop.clone(),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&connect_packet_options(false, None))
        .await
        .unwrap();
    let mut response = [0u8; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x20, 2, 0, 0]);
    let body = payload(17);
    stream
        .write_all(&qos2_publish(&body, 9, false))
        .await
        .unwrap();
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x50, 2, 0, 9]);
    stream
        .write_all(&qos2_publish(&body, 9, true))
        .await
        .unwrap();
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x50, 2, 0, 9]);
    stream.write_all(&[0x62, 2, 0, 9]).await.unwrap();
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x70, 2, 0, 9]);
    stream.write_all(&[0x62, 2, 0, 9]).await.unwrap();
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response, [0x70, 2, 0, 9]);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while sink.0.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn disconnect_without_mqtt_disconnect_publishes_will_including_server_stop() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let sink = Arc::new(CountSink(AtomicUsize::new(0)));
    let (_, services, stop) = runtime_with_sink(Limits::default(), provider, sink.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        None,
        stop.clone(),
    ));
    let will = payload(88);
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(&connect_packet_options(
            true,
            Some(("v1/t/t/p/p/d/a/up", &will, 1, true)),
        ))
        .await
        .unwrap();
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).await.unwrap();
    drop(stream);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while sink.0.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut planned = TcpStream::connect(address).await.unwrap();
    planned
        .write_all(&connect_packet_options(
            true,
            Some(("v1/t/t/p/p/d/a/up", &will, 1, false)),
        ))
        .await
        .unwrap();
    planned.read_exact(&mut connack).await.unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
    assert_eq!(sink.0.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn unauthorized_publish_is_rejected_before_broker_side_effects() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (_, services, stop) = runtime(Limits::default(), provider);
    let broker = services.mqtt.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        None,
        stop.clone(),
    ));
    let unauthorized_topic = "v1/t/t/p/p/d/b/up";

    for qos in [1, 2] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(&connect_packet()).await.unwrap();
        let mut connack = [0u8; 4];
        stream.read_exact(&mut connack).await.unwrap();
        assert_eq!(connack, [0x20, 2, 0, 0]);

        let mut body = Vec::new();
        mqtt_string(unauthorized_topic.as_bytes(), &mut body);
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(&payload(700 + usize::from(qos)));
        let first = 0x30 | (qos << 1) | 1;
        let mut wire = vec![first, u8::try_from(body.len()).unwrap()];
        wire.extend_from_slice(&body);
        stream.write_all(&wire).await.unwrap();

        let mut byte = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap();
        assert!(matches!(closed, Ok(0) | Err(_)));
        assert!(!broker.has_retained_topic(unauthorized_topic).unwrap());
    }

    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn active_mqtt_authenticates_once_for_ten_thousand_real_publishes() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        requests_per_second: 20_000,
        messages_per_device_second: 20_000,
        messages_per_tenant_second: 20_000,
        sink_queue_max_count: 20_000,
        sink_queue_max_bytes: 32 * 1024 * 1024,
        global_event_max_count: 20_000,
        global_event_max_bytes: 32 * 1024 * 1024,
        ..Limits::default()
    };
    let (_, services, stop) = runtime(limits, provider.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        None,
        stop.clone(),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(&connect_packet()).await.unwrap();
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 2, 0, 0]);
    for sequence in 0..10_000 {
        let packet_id = u16::try_from(sequence + 1).unwrap();
        stream
            .write_all(&publish_packet_with_id(&payload(sequence), packet_id))
            .await
            .unwrap();
        let mut puback = [0u8; 4];
        stream.read_exact(&mut puback).await.unwrap();
        assert_eq!(&puback[..2], &[0x40, 2]);
        assert_eq!(u16::from_be_bytes([puback[2], puback[3]]), packet_id);
    }
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn active_tcp_connection_reuses_bound_auth_context_for_frames() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        requests_per_second: 1_000,
        messages_per_device_second: 1_000,
        messages_per_tenant_second: 1_000,
        ..Limits::default()
    };
    let (_, services, stop) = runtime(limits, provider.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Tcp,
        services,
        None,
        stop.clone(),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    let hello = br#"{"credential_id":"a","secret":"secret"}"#;
    stream
        .write_all(&(hello.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(hello).await.unwrap();
    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await.unwrap();
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    stream.read_exact(&mut reply).await.unwrap();
    for sequence in 0..100 {
        let body = payload(sequence);
        stream
            .write_all(&(body.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&body).await.unwrap();
        stream.read_exact(&mut length).await.unwrap();
        let mut receipt = vec![0; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut receipt).await.unwrap();
    }
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn fragmented_mqtt_qos1_puback_follows_event_acceptance() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (_, services, stop) = runtime(Limits::default(), provider.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Mqtt,
        services,
        None,
        stop.clone(),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    for byte in connect_packet() {
        stream.write_all(&[byte]).await.unwrap();
    }
    let mut connack = [0u8; 4];
    stream.read_exact(&mut connack).await.unwrap();
    assert_eq!(connack, [0x20, 2, 0, 0]);
    let packet = publish_packet(&payload(1));
    for chunk in packet.chunks(3) {
        stream.write_all(chunk).await.unwrap();
    }
    let mut puback = [0u8; 4];
    stream.read_exact(&mut puback).await.unwrap();
    assert_eq!(puback, [0x40, 2, 0, 7]);
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    stop.cancel();
    task.await.unwrap().unwrap();
}
