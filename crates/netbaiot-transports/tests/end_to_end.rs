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
    let config = GatewayControl::empty(limits.clone());
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

#[tokio::test]
async fn auth_mqtt_attach_revoke_race_001() {
    let identity = auth();
    let invalidations = [
        AuthInvalidation::Device {
            device: identity.device_key.clone(),
        },
        AuthInvalidation::CredentialVersion { version: 1 },
        AuthInvalidation::AuthGeneration { generation: 1 },
        AuthInvalidation::All,
    ];
    for (index, invalidation) in invalidations.into_iter().enumerate() {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: identity.clone(),
            delay: false,
        });
        let (ingress, services, _) = runtime(Limits::default(), provider);
        let candidate = ingress
            .authenticate_session(AuthenticationRequest::Secret {
                credential_id: "a",
                secret: b"secret",
            })
            .await
            .unwrap();
        let stale_candidate = ingress
            .authenticate_session(AuthenticationRequest::Secret {
                credential_id: "a",
                secret: b"secret",
            })
            .await
            .unwrap();
        let register_ingress = ingress.clone();
        let register_broker = services.mqtt.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let register = std::thread::spawn(move || {
            register_ingress.register_session_with(candidate, Transport::Mqtt, move |bound, _| {
                entered_tx.send(()).unwrap();
                resume_rx.recv().unwrap();
                register_broker.attach(bound, format!("race-{index}"), false)
            })
        });
        entered_rx.recv().unwrap();

        let invalidate_ingress = ingress.clone();
        let invalidate_broker = services.mqtt.clone();
        let invalidation_for_thread = invalidation.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let invalidate = std::thread::spawn(move || {
            let result = invalidate_ingress.invalidate_auth_with(&invalidation_for_thread, || {
                invalidate_broker.invalidate_sessions(&invalidation_for_thread)
            });
            done_tx.send(()).unwrap();
            result
        });
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(25))
                .is_err(),
            "revocation must wait for the fenced MQTT attach"
        );
        resume_tx.send(()).unwrap();
        let registered = register.join().unwrap().unwrap();
        let (_, disconnected, invalidated_mqtt) = invalidate.join().unwrap().unwrap();
        assert_eq!(disconnected, 1);
        assert_eq!(invalidated_mqtt, 1);
        drop(registered);
        assert!(matches!(
            ingress.register_session_with(stale_candidate, Transport::Mqtt, |bound, _| {
                services.mqtt.attach(bound, format!("stale-{index}"), false)
            }),
            Err(Error::Unavailable)
        ));

        let fresh = ingress
            .authenticate_session(AuthenticationRequest::Secret {
                credential_id: "a",
                secret: b"secret",
            })
            .await
            .unwrap();
        let (_, _, probe) = ingress
            .register_session_with(fresh, Transport::Mqtt, |bound, _| {
                services.mqtt.attach(bound, format!("race-{index}"), false)
            })
            .unwrap();
        assert!(!probe.session_present);
        ingress.events.stop_workers().await.unwrap();
    }
}

fn mqtt_string(value: &[u8], output: &mut Vec<u8>) {
    output.extend_from_slice(&(value.len() as u16).to_be_bytes());
    output.extend_from_slice(value);
}

fn connect_packet() -> Vec<u8> {
    connect_packet_for(b"a", true)
}

fn connect_packet_for(client_id: &[u8], clean_session: bool) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_string(b"MQTT", &mut body);
    body.push(4);
    body.push(if clean_session { 0xc2 } else { 0xc0 });
    body.extend_from_slice(&30u16.to_be_bytes());
    mqtt_string(client_id, &mut body);
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

#[tokio::test]
async fn mqtt_connack_write_fail_cleanup() {
    async fn failed_handshake_session_present(clean_session: bool, client_id: &str) -> bool {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        let (ingress, services, stop) = runtime(Limits::default(), provider);
        // One-byte server-to-client capacity makes the four-byte CONNACK block after attachment.
        let (mut client, server) = tokio::io::duplex(1);
        let lease = services
            .connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let task = tokio::spawn(netbaiot_transports::mqtt::connection(
            Box::new(server),
            services.clone(),
            lease,
            stop.child_token(),
        ));
        client
            .write_all(&connect_packet_for(client_id.as_bytes(), clean_session))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if services
                    .ingress
                    .sessions
                    .lookup(&auth().device_key)
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(client);
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_err(),
            "closed peer must make CONNACK write fail"
        );
        let probe = services
            .mqtt
            .attach(&auth(), client_id.to_owned(), false)
            .unwrap();
        let present = probe.session_present;
        drop(probe);
        ingress.events.stop_workers().await.unwrap();
        present
    }

    assert!(!failed_handshake_session_present(true, "connack-clean").await);
    assert!(failed_handshake_session_present(false, "connack-persistent").await);
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

fn retained_delete(qos: u8, packet_id: u16, v5: bool) -> Vec<u8> {
    let topic = "v1/t/t/p/p/d/a/up";
    let mut body = Vec::new();
    mqtt_string(topic.as_bytes(), &mut body);
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    if v5 {
        body.push(0); // zero publish properties
    }
    let mut frame = vec![0x31 | (qos << 1), body.len() as u8];
    frame.extend_from_slice(&body);
    frame
}

fn connect_packet_v5() -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_string(b"MQTT", &mut body);
    body.extend_from_slice(&[5, 0xc2, 0, 30, 0]); // version, flags, keepalive, properties
    mqtt_string(b"client-v5", &mut body);
    mqtt_string(b"a", &mut body);
    mqtt_string(b"secret", &mut body);
    let mut frame = vec![0x10, body.len() as u8];
    frame.extend_from_slice(&body);
    frame
}

async fn read_mqtt_packet(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[1] & 0x80, 0, "test expects a short reply");
    let mut packet = header.to_vec();
    let mut body = vec![0; usize::from(header[1])];
    stream.read_exact(&mut body).await.unwrap();
    packet.extend_from_slice(&body);
    packet
}

async fn read_mqtt_frame(stream: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
    let mut first = [0u8; 1];
    stream.read_exact(&mut first).await.unwrap();
    let mut remaining = 0usize;
    let mut multiplier = 1usize;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        remaining += usize::from(byte[0] & 0x7f) * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        assert!(multiplier <= 128 * 128 * 128);
    }
    assert!(remaining <= 4096);
    let mut body = vec![0; remaining];
    stream.read_exact(&mut body).await.unwrap();
    (first[0], body)
}

fn packet_id_from_publish(body: &[u8]) -> u16 {
    let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
    u16::from_be_bytes([body[topic_len + 2], body[topic_len + 3]])
}

#[tokio::test]
async fn retained_delete_qos_matrix_skips_business_json_decode() {
    use netbaiot_transports::mqtt::broker::BrokerMessage;

    for v5 in [false, true] {
        for qos in 0..=2 {
            let provider = Arc::new(CountingProvider {
                calls: AtomicUsize::new(0),
                auth: auth(),
                delay: false,
            });
            let sink = Arc::new(CountSink(AtomicUsize::new(0)));
            let (ingress, services, stop) =
                runtime_with_sink(Limits::default(), provider, sink.clone());
            let topic = "v1/t/t/p/p/d/a/up";
            services
                .mqtt
                .route(
                    &auth().device_key,
                    BrokerMessage {
                        topic: topic.into(),
                        payload: b"old".to_vec(),
                        qos: 0,
                        retain: true,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert!(services.mqtt.has_retained_topic(topic).unwrap());
            let (mut client, server) = tokio::io::duplex(4096);
            let lease = services
                .connections
                .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
                .unwrap();
            let task = tokio::spawn(netbaiot_transports::mqtt::connection(
                Box::new(server),
                services.clone(),
                lease,
                stop.child_token(),
            ));
            let connect = if v5 {
                connect_packet_v5()
            } else {
                connect_packet()
            };
            client.write_all(&connect).await.unwrap();
            let connack = read_mqtt_packet(&mut client).await;
            assert_eq!(connack[0], 0x20);
            assert_eq!(connack[3], 0);
            let id = 7;
            client
                .write_all(&retained_delete(qos, id, v5))
                .await
                .unwrap();
            if qos > 0 {
                let response = read_mqtt_packet(&mut client).await;
                assert_eq!(response[0], if qos == 1 { 0x40 } else { 0x50 });
                assert_eq!(&response[2..4], &id.to_be_bytes());
            }
            if qos == 2 {
                client.write_all(&[0x62, 2, 0, id as u8]).await.unwrap();
                let response = read_mqtt_packet(&mut client).await;
                assert_eq!(response[0], 0x70);
            }
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while services.mqtt.has_retained_topic(topic).unwrap() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(sink.0.load(Ordering::Relaxed), 0);
            drop(client);
            stop.cancel();
            task.await.unwrap().unwrap();
            ingress.events.stop_workers().await.unwrap();
        }
    }
}

#[tokio::test]
async fn mqtt_publish_fast_paths_consume_protocol_budget() {
    for case in 0..3 {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        let limits = Limits {
            requests_per_second: 100,
            messages_per_device_second: 2,
            messages_per_tenant_second: 100,
            ..Limits::default()
        };
        let (ingress, services, stop) = runtime(limits, provider);
        let (mut client, server) = tokio::io::duplex(4096);
        let lease = services
            .connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let task = tokio::spawn(netbaiot_transports::mqtt::connection(
            Box::new(server),
            services,
            lease,
            stop.child_token(),
        ));
        let connect = if case == 2 {
            connect_packet_v5()
        } else {
            connect_packet()
        };
        client.write_all(&connect).await.unwrap();
        assert_eq!(read_mqtt_packet(&mut client).await[0], 0x20);
        for iteration in 0..3 {
            let frame = match case {
                0 => retained_delete(0, 0, false),
                1 => qos2_publish(&payload(1), 7, iteration > 0),
                _ => {
                    let topic = "v1/t/t/p/p/d/a/up";
                    let mut body = Vec::new();
                    mqtt_string(topic.as_bytes(), &mut body);
                    body.extend_from_slice(&(iteration + 1u16).to_be_bytes());
                    body.push(0); // zero MQTT 5 PUBLISH properties
                    body.extend_from_slice(b"invalid-json");
                    let mut frame = vec![0x34, body.len() as u8];
                    frame.extend_from_slice(&body);
                    frame
                }
            };
            client.write_all(&frame).await.unwrap();
            if iteration < 2 && case != 0 {
                let response = read_mqtt_packet(&mut client).await;
                assert_eq!(response[0], 0x50);
                if case == 2 {
                    assert!(response[4] >= 0x80);
                }
            }
        }
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap(),
            Err(Error::Overloaded)
        ));
        assert_eq!(ingress.metrics.get(Metric::MqttPublishes), 2);
        assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 0);
        drop(client);
        stop.cancel();
        ingress.events.stop_workers().await.unwrap();
    }
}

#[tokio::test]
async fn mqtt_v5_command_metrics_distinguish_ordinary_and_negative_puback() {
    use netbaiot_transports::mqtt::broker::BrokerMessage;

    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, services, stop) = runtime(Limits::default(), provider);
    let (mut client, server) = tokio::io::duplex(4096);
    let lease = services
        .connections
        .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
        .unwrap();
    let task = tokio::spawn(netbaiot_transports::mqtt::connection(
        Box::new(server),
        services.clone(),
        lease,
        stop.child_token(),
    ));
    client.write_all(&connect_packet_v5()).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0x20);
    let down = "v1/t/t/p/p/d/a/down";
    let up = "v1/t/t/p/p/d/a/up";
    let mut subscribe = vec![0, 1, 0]; // packet ID and zero properties
    for filter in [down, up] {
        mqtt_string(filter.as_bytes(), &mut subscribe);
        subscribe.push(1);
    }
    let mut wire = vec![0x82, subscribe.len() as u8];
    wire.extend_from_slice(&subscribe);
    client.write_all(&wire).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0x90);
    services
        .mqtt
        .route(
            &auth().device_key,
            BrokerMessage {
                topic: up.into(),
                payload: b"ordinary".to_vec(),
                qos: 1,
                retain: false,
                properties: Default::default(),
            },
        )
        .unwrap();
    let (first, body) = read_mqtt_frame(&mut client).await;
    assert_eq!(first & 0xf0, 0x30);
    let ordinary_id = packet_id_from_publish(&body);
    client
        .write_all(&[0x40, 2, (ordinary_id >> 8) as u8, ordinary_id as u8])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while ingress.metrics.get(Metric::MqttPubacks) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(ingress.metrics.get(Metric::CommandReceived), 0);

    for (reason, expected_received, expected_failed) in [(0x80, 0, 1), (0, 1, 1)] {
        services
            .router
            .send(DeviceCommand {
                command_id: CommandId::generate(),
                device: auth().device_key,
                expires_at: None,
                payload: DeviceCommandPayload {
                    name: "test".into(),
                    arguments: Default::default(),
                },
            })
            .unwrap();
        let (first, body) = read_mqtt_frame(&mut client).await;
        assert_eq!(first & 0xf0, 0x30);
        let command_id = packet_id_from_publish(&body);
        let ack = if reason == 0 {
            vec![0x40, 2, (command_id >> 8) as u8, command_id as u8]
        } else {
            vec![0x40, 3, (command_id >> 8) as u8, command_id as u8, reason]
        };
        client.write_all(&ack).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while ingress.metrics.get(Metric::CommandReceived) != expected_received
                || ingress.metrics.get(Metric::CommandFailed) != expected_failed
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(ingress.metrics.get(Metric::CommandSent), 2);
    let mut subscribe = vec![0, 2, 0];
    mqtt_string(down.as_bytes(), &mut subscribe);
    subscribe.push(2);
    let mut wire = vec![0x82, subscribe.len() as u8];
    wire.extend_from_slice(&subscribe);
    client.write_all(&wire).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0x90);
    services
        .router
        .send(DeviceCommand {
            command_id: CommandId::generate(),
            device: auth().device_key,
            expires_at: None,
            payload: DeviceCommandPayload {
                name: "test".into(),
                arguments: Default::default(),
            },
        })
        .unwrap();
    let (first, body) = read_mqtt_frame(&mut client).await;
    assert_eq!(first & 0x06, 0x04);
    let packet_id = packet_id_from_publish(&body);
    client
        .write_all(&[0x50, 3, (packet_id >> 8) as u8, packet_id as u8, 0x80])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while ingress.metrics.get(Metric::CommandFailed) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(ingress.metrics.get(Metric::CommandReceived), 1);
    drop(client);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn mqtt_command_requires_live_subscription_and_unsubscribe_preserves_inflight_ack() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, services, stop) = runtime(Limits::default(), provider);
    let (mut client, server) = tokio::io::duplex(4096);
    let lease = services
        .connections
        .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
        .unwrap();
    let task = tokio::spawn(netbaiot_transports::mqtt::connection(
        Box::new(server),
        services.clone(),
        lease,
        stop.child_token(),
    ));
    client.write_all(&connect_packet()).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0x20);
    let command = || DeviceCommand {
        command_id: CommandId::generate(),
        device: auth().device_key,
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "test".into(),
            arguments: Default::default(),
        },
    };
    assert!(matches!(
        services.router.send(command()),
        Err(Error::Unavailable)
    ));
    let down = "v1/t/t/p/p/d/a/down";
    let mut subscribe = vec![0, 1];
    mqtt_string(down.as_bytes(), &mut subscribe);
    subscribe.push(1);
    let mut wire = vec![0x82, subscribe.len() as u8];
    wire.extend_from_slice(&subscribe);
    client.write_all(&wire).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0x90);
    services.router.send(command()).unwrap();
    let (first, body) = read_mqtt_frame(&mut client).await;
    assert_eq!(first & 0xf0, 0x30);
    let packet_id = packet_id_from_publish(&body);
    let mut unsubscribe = vec![0, 2];
    mqtt_string(down.as_bytes(), &mut unsubscribe);
    let mut wire = vec![0xa2, unsubscribe.len() as u8];
    wire.extend_from_slice(&unsubscribe);
    client.write_all(&wire).await.unwrap();
    assert_eq!(read_mqtt_frame(&mut client).await.0, 0xb0);
    assert!(matches!(
        services.router.send(command()),
        Err(Error::Unavailable)
    ));
    client
        .write_all(&[0x40, 2, (packet_id >> 8) as u8, packet_id as u8])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while ingress.metrics.get(Metric::CommandReceived) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(client);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
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
