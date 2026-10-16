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

fn live_command() -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId::generate(),
        device: auth().device_key,
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: Default::default(),
        },
    }
}

#[tokio::test]
async fn command_service_retries_conflicts_and_drain_share_one_dispatch() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, services, _) = runtime(Limits::default(), provider);
    let mut command = live_command();
    command
        .payload
        .arguments
        .insert("first".into(), Scalar::Number(1.0));
    command
        .payload
        .arguments
        .insert("second".into(), Scalar::Number(2.0));
    assert!(matches!(
        services.commands.send(command.clone()).await,
        Err(Error::Unavailable)
    ));
    let (_lease, mut device_rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();

    // A None expiry is fingerprinted before the Gateway fills in its actual TTL.
    let first = services.commands.send(command.clone()).await.unwrap();
    let mut reordered = command.clone();
    reordered.payload.arguments.clear();
    reordered
        .payload
        .arguments
        .insert("second".into(), Scalar::Number(2.0));
    reordered
        .payload
        .arguments
        .insert("first".into(), Scalar::Number(1.0));
    let repeated = services.commands.send(reordered).await.unwrap();
    assert_eq!(first, repeated);
    assert_eq!(first.state, DeliveryState::Queued);
    assert_eq!(device_rx.try_recv().unwrap().command_id, command.command_id);
    assert!(device_rx.try_recv().is_err());

    let mut changed = command.clone();
    changed.payload.name = "different".into();
    assert!(matches!(
        services.commands.send(changed).await,
        Err(Error::Conflict)
    ));
    let mut changed = command.clone();
    changed.device.device_id = DeviceId::new("other").unwrap();
    assert!(matches!(
        services.commands.send(changed).await,
        Err(Error::Conflict)
    ));
    let mut changed = command.clone();
    changed.expires_at = Some(now_ms() + 1_000);
    assert!(matches!(
        services.commands.send(changed).await,
        Err(Error::Conflict)
    ));
    assert!(device_rx.try_recv().is_err());

    ingress.lifecycle.begin_quiesce().await.unwrap();
    assert!(matches!(
        services.commands.send(command.clone()).await,
        Err(Error::Draining)
    ));
    command.command_id = CommandId::generate();
    assert!(matches!(
        services.commands.send(command).await,
        Err(Error::Draining)
    ));
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
}

#[tokio::test]
async fn command_service_concurrent_same_id_reaches_device_once() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, services, _) = runtime(Limits::default(), provider);
    let (_lease, mut device_rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let command = live_command();
    let mut jobs = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let service = services.commands.clone();
        let command = command.clone();
        jobs.spawn(async move { service.send(command).await });
    }
    while let Some(result) = jobs.join_next().await {
        assert_eq!(result.unwrap().unwrap().command_id, command.command_id);
    }
    assert_eq!(device_rx.try_recv().unwrap().command_id, command.command_id);
    assert!(device_rx.try_recv().is_err());
    assert_eq!(ingress.metrics.get(Metric::CommandQueued), 1);
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
}

#[tokio::test]
async fn command_service_capacity_expiry_and_invalid_expiry_cleanup() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        command_dedup_max_entries: 1,
        command_dedup_ttl_ms: 10,
        ..Limits::default()
    };
    let (ingress, services, _) = runtime(limits, provider);
    let (_lease, mut device_rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let first = live_command();
    let mut invalid = live_command();
    invalid.expires_at = Some(now_ms() - 1);
    assert!(matches!(
        services.commands.send(invalid.clone()).await,
        Err(Error::Invalid)
    ));
    invalid.expires_at = Some(now_ms() + 1_000);
    services.commands.send(invalid.clone()).await.unwrap();
    assert_eq!(device_rx.try_recv().unwrap().command_id, invalid.command_id);
    assert_eq!(
        services
            .commands
            .send(invalid.clone())
            .await
            .unwrap()
            .command_id,
        invalid.command_id
    );
    assert!(matches!(
        services.commands.send(first.clone()).await,
        Err(Error::Overloaded)
    ));
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    services.commands.send(invalid).await.unwrap();
    assert!(device_rx.try_recv().is_ok());
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    services.commands.send(first).await.unwrap();
    assert!(device_rx.try_recv().is_ok());
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
    assert_eq!(ingress.metrics.get(Metric::CommandDedupEvictions), 2);
}

#[tokio::test]
async fn command_dedup_expiry_is_ordered_and_lazily_reclaimed() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        command_dedup_max_entries: 2,
        command_dedup_ttl_ms: 500,
        ..Limits::default()
    };
    let (ingress, services, _) = runtime(limits, provider);
    let (_lease, mut device_rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let first = live_command();
    let second = live_command();
    services.commands.send(first.clone()).await.unwrap();
    assert_eq!(device_rx.try_recv().unwrap().command_id, first.command_id);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    services.commands.send(second.clone()).await.unwrap();
    assert_eq!(device_rx.try_recv().unwrap().command_id, second.command_id);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // The physical registry retains expired entries until the next send.
    assert_eq!(services.commands.usage().unwrap(), (2, 0));
    services.commands.send(second.clone()).await.unwrap();
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
    assert!(device_rx.try_recv().is_err());
    services.commands.send(first.clone()).await.unwrap();
    assert_eq!(device_rx.try_recv().unwrap().command_id, first.command_id);
    assert_eq!(services.commands.usage().unwrap(), (2, 0));
    assert_eq!(ingress.metrics.get(Metric::CommandDedupEvictions), 1);

    tokio::time::sleep(std::time::Duration::from_millis(550)).await;
    assert_eq!(services.commands.usage().unwrap(), (2, 0));
    let third = live_command();
    services.commands.send(third.clone()).await.unwrap();
    assert_eq!(device_rx.try_recv().unwrap().command_id, third.command_id);
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
    assert_eq!(ingress.metrics.get(Metric::CommandDedupEvictions), 3);
}

#[tokio::test]
async fn accepted_receipt_survives_command_ttl_within_dedup_window() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        command_ttl_ms: 100,
        command_dedup_ttl_ms: 1_000,
        ..Limits::default()
    };
    let (ingress, services, _) = runtime(limits, provider);
    let (_lease, mut device_rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let command = live_command();
    services.commands.send(command.clone()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(
        services.commands.send(command).await.unwrap().state,
        DeliveryState::Expired
    );
    drop(device_rx.try_recv().unwrap());
    assert_eq!(ingress.metrics.get(Metric::CommandFailed), 1);
    assert!(device_rx.try_recv().is_err());
    assert_eq!(services.commands.usage().unwrap(), (1, 0));
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
    runtime_with_codec(limits, provider, sink, Arc::new(JsonV1::default()))
}

fn runtime_with_codec(
    limits: Limits,
    provider: Arc<CountingProvider>,
    sink: Arc<dyn EventSink>,
    codec: Arc<dyn DeviceCodec>,
) -> (Arc<Ingress>, Arc<Services>, CancellationToken) {
    let codec_id = provider.auth.codec_id.clone();
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
        CodecRegistry::new(vec![(codec_id, 1, codec)]).unwrap(),
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

#[tokio::test]
async fn continuously_ready_outbound_cannot_starve_ping_for_either_mqtt_version() {
    use netbaiot_transports::mqtt::broker::{BrokerMessage, MqttBroker};
    use std::{
        collections::VecDeque,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    struct FloodStream {
        input: VecDeque<u8>,
        qos: u8,
        acked_id: Option<u16>,
        command_traffic: bool,
        router: Arc<CommandRouter>,
        broker: Arc<MqttBroker>,
        stop: CancellationToken,
        publishes: Arc<AtomicUsize>,
        ping_at: Arc<AtomicUsize>,
    }
    impl FloodStream {
        fn refill(&self) {
            if self.command_traffic {
                self.router.send(live_command()).unwrap();
                return;
            }
            let routed = self
                .broker
                .route(
                    &auth().device_key,
                    BrokerMessage {
                        topic: "v1/t/t/p/p/d/a/down".into(),
                        payload: vec![1].into(),
                        qos: self.qos,
                        retain: false,
                        properties: Default::default(),
                    },
                )
                .unwrap();
            assert_eq!(
                routed, 1,
                "flood must continuously refill a real subscription"
            );
        }
    }
    impl AsyncRead for FloodStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.input.is_empty() {
                return Poll::Pending;
            }
            while buffer.remaining() > 0 {
                match self.input.pop_front() {
                    Some(byte) => buffer.put_slice(&[byte]),
                    None => break,
                }
            }
            Poll::Ready(Ok(()))
        }
    }
    impl AsyncWrite for FloodStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            match bytes[0] & 0xf0 {
                0x90 => self.refill(), // SUBACK ensures the outbound channel is ready
                0x30 => {
                    let count = self.publishes.fetch_add(1, Ordering::Relaxed) + 1;
                    if count == 1 && self.qos > 0 {
                        let topic_bytes = usize::from(u16::from_be_bytes([bytes[2], bytes[3]]));
                        let at = 4 + topic_bytes;
                        let id = [bytes[at], bytes[at + 1]];
                        self.acked_id = Some(u16::from_be_bytes(id));
                        let kind = if self.qos == 1 { 0x40 } else { 0x50 };
                        self.input.extend([kind, 2, id[0], id[1]]);
                        if self.qos == 1 {
                            self.input.extend([0xc0, 0]);
                        }
                    }
                    if count >= 8 {
                        self.stop.cancel();
                    } else {
                        self.refill();
                    }
                }
                0x60 => {
                    self.input.extend([0x70, 2, bytes[2], bytes[3], 0xc0, 0]);
                }
                0xd0 => {
                    if let Some(id) = self.acked_id {
                        let snapshot =
                            serde_json::to_value(self.broker.snapshot().unwrap()).unwrap();
                        assert!(
                            snapshot["sessions"][0]["outbound"]
                                .get(id.to_string())
                                .is_none(),
                            "control ACK must release the original inflight packet ID before the later PINGRESP"
                        );
                    }
                    self.ping_at
                        .store(self.publishes.load(Ordering::Relaxed), Ordering::Relaxed);
                    self.stop.cancel();
                }
                _ => (),
            }
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let mut failures = Vec::new();
    for (v5, qos, command_traffic) in [false, true].into_iter().flat_map(|v5| {
        [
            (v5, 0, false),
            (v5, 1, false),
            (v5, 2, false),
            (v5, 0, true),
        ]
    }) {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        let (ingress, services, stop) = runtime(Limits::default(), provider);
        let mut input = if v5 {
            connect_packet_v5()
        } else {
            connect_packet()
        };
        let mut body = vec![0, 1];
        if v5 {
            body.push(0);
        }
        mqtt_string(b"v1/t/t/p/p/d/a/down", &mut body);
        body.push(qos);
        input.extend([0x82, body.len() as u8]);
        input.extend(body);
        if qos == 0 {
            input.extend([0xc0, 0]);
        }
        let publishes = Arc::new(AtomicUsize::new(0));
        let ping_at = Arc::new(AtomicUsize::new(usize::MAX));
        let stream = FloodStream {
            input: input.into(),
            qos,
            acked_id: None,
            command_traffic,
            router: services.router.clone(),
            broker: services.mqtt.clone(),
            stop: stop.clone(),
            publishes,
            ping_at: ping_at.clone(),
        };
        let lease = services
            .connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            netbaiot_transports::mqtt::connection(Box::new(stream), services, lease, stop),
        )
        .await
        .unwrap()
        .unwrap();
        ingress.events.stop_workers().await.unwrap();
        if qos == 1 {
            assert!(ingress.metrics.get(Metric::MqttPubacks) >= 1);
        }
        let bound = [2, 3, 5][usize::from(qos)];
        if ping_at.load(Ordering::Relaxed) > bound {
            failures.push((v5, qos, command_traffic));
        }
    }
    assert!(
        failures.is_empty(),
        "PINGREQ exceeded two ordinary work selections: MQTT5 flags {failures:?}"
    );
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

async fn read_mqtt_frame(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> (u8, Vec<u8>) {
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
                        payload: b"old".to_vec().into(),
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
                payload: b"ordinary".to_vec().into(),
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
    for sequence in 0..101 {
        let mut body = payload(sequence);
        if sequence == 100 {
            // A retained authentication frame must not consume the read quota
            // needed by the next maximum-size legal TCP frame. JSON permits
            // trailing whitespace; the event still traverses normal admission.
            body.resize(Limits::default().max_tcp_frame_size, b' ');
        }
        stream
            .write_all(&(body.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&body).await.unwrap();
        stream.read_exact(&mut length).await.unwrap();
        assert_eq!(length, tcp_ack::FRAME_HEADER);
        let mut receipt = [0; tcp_ack::BODY_LEN];
        stream.read_exact(&mut receipt).await.unwrap();
        assert_eq!(tcp_ack::decode(&receipt).unwrap().required_deliveries, 1);
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

#[tokio::test]
async fn delayed_authentication_preserves_a_full_pipelined_maximum_tcp_frame() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: true,
    });
    let limits = Limits::default();
    let (ingress, services, stop) = runtime(limits.clone(), provider.clone());
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
    let mut body = payload(1);
    body.resize(limits.max_tcp_frame_size, b' ');
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&(hello.len() as u32).to_be_bytes());
    pipeline.extend_from_slice(hello);
    pipeline.extend_from_slice(&(body.len() as u32).to_be_bytes());
    pipeline.extend_from_slice(&body);
    stream.write_all(&pipeline).await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        for index in 0..2 {
            let mut length = [0; 4];
            stream.read_exact(&mut length).await?;
            let (kind, size) = tcp_frame::decode_header(length, 1024).unwrap();
            assert_eq!(
                kind,
                if index == 0 {
                    tcp_frame::FrameKind::Payload
                } else {
                    tcp_frame::FrameKind::EventAccepted
                }
            );
            let mut reply = vec![0; size];
            stream.read_exact(&mut reply).await?;
            if index == 0 {
                assert_eq!(reply, br#"{"authenticated":true}"#);
            } else {
                assert_eq!(tcp_ack::decode(&reply).unwrap().required_deliveries, 1);
            }
        }
        std::io::Result::Ok(())
    })
    .await;
    stop.cancel();
    task.await.unwrap().unwrap();
    assert!(matches!(result, Ok(Ok(()))), "pipeline failed: {result:?}");
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
}

#[tokio::test]
async fn delayed_authentication_preserves_maximum_mqtt_publish_for_both_versions() {
    use netbaiot_transports::mqtt::codec::v5;
    for mqtt5 in [false, true] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: true,
        });
        let limits = Limits::default();
        let (ingress, services, stop) = runtime(limits.clone(), provider.clone());
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
        let topic = "v1/t/t/p/p/d/a/up";
        let overhead = 4 + 2 + topic.len() + 2 + usize::from(mqtt5);
        let mut body = payload(1);
        body.resize(limits.max_mqtt_packet_size - overhead, b' ');
        let publish = if mqtt5 {
            v5::publish(
                v5::OutboundPublish {
                    topic,
                    payload: &body,
                    qos: 1,
                    packet_id: Some(7),
                    retain: false,
                    dup: false,
                    properties: &v5::Properties::default(),
                },
                limits.max_mqtt_packet_size,
            )
            .unwrap()
        } else {
            publish_packet_with_id(&body, 7)
        };
        assert_eq!(publish.len(), limits.max_mqtt_packet_size);
        let mut pipeline = if mqtt5 {
            connect_packet_v5()
        } else {
            connect_packet()
        };
        pipeline.extend_from_slice(&publish);
        stream.write_all(&pipeline).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let (kind, body) = read_mqtt_frame(&mut stream).await;
            assert_eq!(kind, 0x20);
            assert_eq!(&body[..2], &[0, 0]);
            let (kind, body) = read_mqtt_frame(&mut stream).await;
            assert_eq!(kind, 0x40);
            assert_eq!(&body[..2], &[0, 7]);
        })
        .await;
        stop.cancel();
        task.await.unwrap().unwrap();
        assert!(result.is_ok(), "MQTT5={mqtt5} pipeline timed out");
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn full_preauth_reader_observes_eof_overload_shutdown_and_packet_deadline() {
    use netbaiot_transports::{BoxStream, Reader, authenticate_stream};
    use std::{future::Future, task::Poll};
    for case in [0, 1, 2, 3, 4, 5] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: true,
        });
        let (_, services, stop) = runtime(Limits::default(), provider);
        let mut lease = services
            .connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
            .unwrap();
        let tracking = lease.read_budget();
        let mut reader = Reader::with_budget(8, 5, lease.read_budget());
        let mut source = &[7; 8][..];
        reader
            .read_more_to(
                &mut source,
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
                8,
            )
            .await
            .unwrap();
        let (mut peer, stream) = tokio::io::duplex(8);
        let mut stream: BoxStream = Box::new(stream);
        match case {
            0 => {
                peer.shutdown().await.unwrap();
            }
            1 => {
                peer.write_all(&[7]).await.unwrap();
            }
            2 => {
                stop.cancel();
            }
            4 => {
                tokio::time::advance(std::time::Duration::from_millis(6)).await;
            }
            _ => {}
        }
        let mut authenticate = Box::pin(authenticate_stream(
            &services,
            AuthenticationRequest::Secret {
                credential_id: "a",
                secret: b"secret",
            },
            &mut reader,
            &mut stream,
            &stop,
        ));
        if case == 5 {
            std::future::poll_fn(|context| {
                assert!(authenticate.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
            tokio::time::advance(std::time::Duration::from_millis(25)).await;
        }
        let result = authenticate.await;
        assert!(
            matches!(
                (&result, case),
                (Err(Error::Unavailable), 0)
                    | (Err(Error::Overloaded), 1)
                    | (Err(Error::Draining), 2)
                    | (Err(Error::Timeout), 3..=5)
            ),
            "case {case}"
        );
        assert_eq!(&reader.buffer[..], &[7; 8]);
        assert_eq!(tracking.capacity().unwrap(), 8);
        drop((reader, lease));
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
}

#[tokio::test]
async fn authenticated_candidates_cannot_register_after_quiesce() {
    for (transport, v5) in [
        (Transport::Mqtt, false),
        (Transport::Mqtt, true),
        (Transport::Tcp, false),
    ] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
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
        ingress.lifecycle.begin_quiesce().await.unwrap();
        assert!(matches!(
            ingress.register_session_with(candidate, transport, |auth, _| {
                match (transport, v5) {
                    (Transport::Mqtt, true) => services
                        .mqtt
                        .attach_v5(auth, "late".into(), false, 60, 8)
                        .map(Some),
                    (Transport::Mqtt, false) => {
                        services.mqtt.attach(auth, "late".into(), false).map(Some)
                    }
                    _ => Ok(None),
                }
            }),
            Err(Error::Draining)
        ));
        assert!(ingress.sessions.list(0, 10).unwrap().is_empty());
        assert_eq!(
            serde_json::to_value(services.mqtt.snapshot().unwrap()).unwrap()["sessions"],
            serde_json::json!([])
        );
        ingress.events.stop_workers().await.unwrap();
    }
}

#[tokio::test]
async fn quiesce_waits_for_invalidation_before_committing_mqtt_recovery() {
    for admitted_before in [true, false] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        let (ingress, services, _) = runtime(Limits::default(), provider);
        let attachment = services
            .mqtt
            .attach(&auth(), "persistent-drain".into(), false)
            .unwrap();
        drop(attachment);
        let directory =
            std::env::temp_dir().join(format!("netbaiot-fence-{}", uuid::Uuid::new_v4()));
        if admitted_before {
            let (entered, wait_entered) = tokio::sync::oneshot::channel();
            let (resume, wait_resume) = std::sync::mpsc::channel();
            let owner = ingress.clone();
            let broker = services.mqtt.clone();
            let mutation = std::thread::spawn(move || {
                owner.invalidate_auth_with(&AuthInvalidation::All, || {
                    entered.send(()).unwrap();
                    wait_resume.recv().unwrap();
                    broker.invalidate_sessions(&AuthInvalidation::All)
                })
            });
            wait_entered.await.unwrap();
            let lifecycle = ingress.lifecycle.clone();
            let quiesce = tokio::spawn(async move { lifecycle.begin_quiesce().await });
            tokio::task::yield_now().await;
            assert_eq!(ingress.lifecycle.state(), LifecycleState::Quiescing);
            assert!(!quiesce.is_finished());
            resume.send(()).unwrap();
            assert_eq!(mutation.join().unwrap().unwrap().2, 1);
            quiesce.await.unwrap().unwrap();
        } else {
            ingress.lifecycle.begin_quiesce().await.unwrap();
            assert!(matches!(
                ingress.invalidate_auth_with(&AuthInvalidation::All, || services
                    .mqtt
                    .invalidate_sessions(&AuthInvalidation::All)),
                Err(Error::Draining)
            ));
        }
        services.mqtt.commit_to(&directory).await.unwrap();
        // The management listener remains available after this point. A successful
        // late invalidation must never coexist with an older committed session.
        assert!(matches!(
            ingress.invalidate_auth_with(&AuthInvalidation::All, || services
                .mqtt
                .invalidate_sessions(&AuthInvalidation::All)),
            Err(Error::Draining)
        ));
        let restored = netbaiot_transports::mqtt::broker::MqttBroker::new(ingress.limits.clone());
        assert!(restored.recover_from(&directory).await.unwrap());
        let attachment = restored
            .attach(&auth(), "persistent-drain".into(), false)
            .unwrap();
        assert_eq!(attachment.session_present, !admitted_before);
        drop(attachment);
        std::fs::remove_dir_all(directory).unwrap();
        ingress.events.stop_workers().await.unwrap();
    }
}

#[tokio::test]
async fn management_mutations_close_but_diagnostics_and_drain_remain_available() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, mut services, _) = runtime(Limits::default(), provider);
    let secret = "ab".repeat(32);
    Arc::get_mut(&mut services).unwrap().admin = Some(Arc::new(
        AdminAccess::new(&secret, Default::default(), &ingress.limits).unwrap(),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let server = tokio::spawn(netbaiot_transports::serve_management_http(
        listener,
        services,
        None,
        stop.clone(),
    ));
    async fn request(
        address: std::net::SocketAddr,
        secret: &str,
        method: &str,
        path: &str,
        body: &str,
    ) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(format!("{method} /api/v1/{path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {secret}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }
    let requests = [
        (
            "POST",
            "auth/invalidate",
            serde_json::to_string(&AuthInvalidation::All).unwrap(),
        ),
        (
            "PUT",
            "control/snapshot",
            r#"{"revision":2,"products":[],"routes":[{"tenant":null,"sinks":["sink"]}]}"#.into(),
        ),
        (
            "PUT",
            "routes",
            r#"{"revision":3,"routes":[{"tenant":null,"sinks":["sink"]}]}"#.into(),
        ),
        (
            "POST",
            "devices/commands",
            serde_json::to_string(&live_command()).unwrap(),
        ),
    ];
    let (_session, _rx) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let mut live = None;
    for (method, path, body) in &requests {
        if *path == "devices/commands" {
            live = Some(
                ingress
                    .sessions
                    .register(Arc::new(auth()), Transport::Tcp)
                    .unwrap(),
            );
        }
        let response = request(address, &secret, method, path, body).await;
        assert!(response.starts_with("HTTP/1.1 2"), "{path}: {response}");
    }
    drop(live);
    // Keep a guard to expose Quiescing, not only the later Draining state.
    let guard = ingress.lifecycle.begin_admission().unwrap();
    let lifecycle = ingress.lifecycle.clone();
    let quiesce = tokio::spawn(async move { lifecycle.begin_quiesce().await });
    tokio::task::yield_now().await;
    for (method, path, body) in &requests {
        let response = request(address, &secret, method, path, body).await;
        assert!(response.starts_with("HTTP/1.1 503"), "{path}: {response}");
        assert!(response.contains("draining"), "{response}");
    }
    for path in ["health", "status", "metrics", "connections"] {
        assert!(
            request(address, &secret, "GET", path, "")
                .await
                .starts_with("HTTP/1.1 200")
        );
    }
    assert!(
        request(address, &secret, "GET", "ready", "")
            .await
            .starts_with("HTTP/1.1 503")
    );
    for _ in 0..2 {
        assert!(
            request(address, &secret, "POST", "drain", "")
                .await
                .starts_with("HTTP/1.1 202")
        );
    }
    drop(guard);
    quiesce.await.unwrap().unwrap();
    stop.cancel();
    server.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn command_dedup_tracks_sent_receipts_and_fences_evicted_dispatch_updates() {
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let (ingress, services, _) = runtime(
        Limits {
            command_dedup_ttl_ms: 50,
            ..Limits::default()
        },
        provider,
    );
    let (_lease, mut receiver) = ingress
        .sessions
        .register(Arc::new(auth()), Transport::Tcp)
        .unwrap();
    let command = live_command();
    services.commands.send(command.clone()).await.unwrap();
    let old = receiver.try_recv().unwrap();
    let progress = old.progress.as_ref().unwrap().clone();
    assert!(progress.begin_transfer());
    progress.update(DeliveryState::Sent);
    assert!(!progress.expire(i64::MAX));
    drop(old);
    assert_eq!(
        services.commands.send(command.clone()).await.unwrap().state,
        DeliveryState::Sent
    );
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert_eq!(
        services.commands.send(command.clone()).await.unwrap().state,
        DeliveryState::Queued
    );
    let new = receiver.try_recv().unwrap();
    progress.update(DeliveryState::Received);
    assert_eq!(
        services.commands.send(command.clone()).await.unwrap().state,
        DeliveryState::Queued
    );
    drop(new);
    assert_eq!(
        services.commands.send(command).await.unwrap().state,
        DeliveryState::Failed
    );
    assert!(receiver.try_recv().is_err());
    ingress.events.stop_workers().await.unwrap();
}

struct TcpReceiptSink {
    events: std::sync::Mutex<Vec<Arc<DeviceEvent>>>,
    hold: bool,
}
#[async_trait]
impl EventSink for TcpReceiptSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        self.events.lock().unwrap().push(delivery.event.clone());
        if self.hold {
            std::future::pending::<()>().await;
        }
        Ok(SinkAck)
    }
}

#[tokio::test]
async fn tcp_ack_frame_limit_is_checked_before_authentication_and_acceptance() {
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};

    struct TinyCodec;
    impl DeviceCodec for TinyCodec {
        fn decode(
            &self,
            ctx: &DecodeContext<'_>,
            bytes: &[u8],
        ) -> std::result::Result<Vec<DeviceEvent>, CodecError> {
            assert_eq!(bytes, &[1]);
            JsonV1::default().decode(ctx, &payload(0))
        }
        fn encode(
            &self,
            ctx: &EncodeContext<'_>,
            command: &DeviceCommand,
        ) -> std::result::Result<Vec<u8>, CodecError> {
            JsonV1::default().encode(ctx, command)
        }
    }

    for maximum in [41, 42, Limits::default().max_tcp_frame_size] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        // Deliberately bypass Config/Limits validation, as an embedded host can.
        let (ingress, services, stop) = runtime_with_codec(
            Limits {
                max_tcp_frame_size: maximum,
                ..Limits::default()
            },
            provider.clone(),
            Arc::new(AckSink),
            Arc::new(TinyCodec),
        );
        let lease = services
            .connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
            .unwrap();
        let (mut client, server) = tokio::io::duplex(256);
        let framer = LengthPrefixFramer { maximum };
        let mut pipeline = framer
            .encode(br#"{"credential_id":"a","secret":"secret"}"#)
            .unwrap();
        pipeline.extend(framer.encode(&[1]).unwrap());
        client.write_all(&pipeline).await.unwrap();
        let (result, reply) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                netbaiot_transports::tcp::connection(
                    Box::new(server),
                    services.clone(),
                    lease,
                    stop
                ),
                async {
                    let mut reply = Vec::new();
                    if maximum < tcp_ack::BODY_LEN {
                        client.read_to_end(&mut reply).await.unwrap();
                    } else {
                        reply.resize(26 + tcp_ack::FRAME_LEN, 0);
                        client.read_exact(&mut reply).await.unwrap();
                        client.shutdown().await.unwrap();
                    }
                    reply
                }
            )
        })
        .await
        .unwrap();
        let accepted = ingress.metrics.get(Metric::EventsAccepted);
        let authenticated = provider.calls.load(Ordering::Relaxed);
        assert_eq!(services.connections.active().unwrap(), [0; 3]);
        ingress.events.stop_workers().await.unwrap();
        if maximum < tcp_ack::BODY_LEN {
            assert_eq!((accepted, authenticated, reply.len()), (0, 0, 0));
            assert!(matches!(result, Err(Error::Configuration)));
        } else {
            assert_eq!((accepted, authenticated), (1, 1));
            assert_eq!(&reply[4..26], br#"{"authenticated":true}"#);
            assert_eq!(reply.len(), 26 + tcp_ack::FRAME_LEN);
            assert_eq!(&reply[26..30], &tcp_ack::FRAME_HEADER);
            assert_eq!(
                tcp_ack::decode_frame(&reply[26..])
                    .unwrap()
                    .required_deliveries,
                1
            );
        }
    }
}

#[tokio::test]
async fn tcp_vendor_command_cannot_forge_acceptance_between_real_receipts() {
    struct VendorCodec(Vec<u8>);
    impl DeviceCodec for VendorCodec {
        fn decode(
            &self,
            ctx: &DecodeContext<'_>,
            p: &[u8],
        ) -> std::result::Result<Vec<DeviceEvent>, CodecError> {
            JsonV1::default().decode(ctx, p)
        }
        fn encode(
            &self,
            _: &EncodeContext<'_>,
            _: &DeviceCommand,
        ) -> std::result::Result<Vec<u8>, CodecError> {
            Ok(self.0.clone())
        }
    }
    let forged = tcp_ack::encode(&EventAccepted {
        event_id: EventId::generate(),
        accepted_at: -2,
        required_deliveries: 1,
        best_effort_deliveries: 0,
    })
    .unwrap();
    let mut identity = auth();
    identity.codec_id = CodecId::new("vendor-binary").unwrap();
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: identity,
        delay: false,
    });
    let (ingress, services, stop) = runtime_with_codec(
        Limits::default(),
        provider.clone(),
        Arc::new(AckSink),
        Arc::new(VendorCodec(forged[4..].to_vec())),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_stream(
        listener,
        Transport::Tcp,
        services.clone(),
        None,
        stop.clone(),
    ));
    let mut client = TcpStream::connect(address).await.unwrap();
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};
    let framer = LengthPrefixFramer { maximum: 65536 };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        client
            .write_all(
                &framer
                    .encode(br#"{"credential_id":"a","secret":"secret"}"#)
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut hello = [0; 26];
        client.read_exact(&mut hello).await.unwrap();
        assert_eq!(&hello[4..], br#"{"authenticated":true}"#);
        // One stream: command, ACK, ACK, command, ACK, ACK. Every downlink
        // body has the ACK shape; only the transport header tells them apart.
        for round in 0..2 {
            services.router.send(live_command()).unwrap();
            let mut command = [0; tcp_ack::FRAME_LEN];
            for part in command.chunks_mut(3) {
                client.read_exact(part).await.unwrap();
            }
            assert_eq!(
                tcp_frame::decode_header(command[..4].try_into().unwrap(), 65536).unwrap(),
                (tcp_frame::FrameKind::Payload, tcp_ack::BODY_LEN)
            );
            assert_eq!(&command[4..], &forged[4..]);
            assert!(tcp_ack::decode_frame(&command).is_err());
            assert_eq!(ingress.metrics.get(Metric::EventsAccepted), round * 2);
            let mut pipeline = framer.encode(&payload(round as usize * 2)).unwrap();
            pipeline.extend(framer.encode(&payload(round as usize * 2 + 1)).unwrap());
            for part in pipeline.chunks(3) {
                client.write_all(part).await.unwrap();
            }
            for _ in 0..2 {
                let mut ack = [0; tcp_ack::FRAME_LEN];
                for part in ack.chunks_mut(2) {
                    client.read_exact(part).await.unwrap();
                }
                assert_eq!(ack[..4], tcp_ack::FRAME_HEADER);
                assert_ne!(
                    tcp_ack::decode_frame(&ack).unwrap().event_id,
                    tcp_ack::decode_frame(&forged).unwrap().event_id
                );
            }
        }
        // A device may not upload a server-only control frame. The connection
        // closes on its header, without waiting for a body or admitting an event.
        client.write_all(&forged[..4]).await.unwrap();
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    })
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 4);
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn tcp_binary_receipts_preserve_pipeline_order_and_reject_without_ack() {
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};
    for overload in [false, true] {
        let provider = Arc::new(CountingProvider {
            calls: AtomicUsize::new(0),
            auth: auth(),
            delay: false,
        });
        let sink = Arc::new(TcpReceiptSink {
            events: Default::default(),
            hold: overload,
        });
        let limits = Limits {
            global_event_max_count: if overload { 1 } else { 64 },
            ..Limits::default()
        };
        let (ingress, services, stop) = runtime_with_sink(limits, provider.clone(), sink.clone());
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
        let framer = LengthPrefixFramer { maximum: 65536 };
        let mut pipeline = framer
            .encode(br#"{"credential_id":"a","secret":"secret"}"#)
            .unwrap();
        let count = if overload { 1 } else { 8 };
        for sequence in 0..count {
            pipeline.extend(framer.encode(&payload(sequence)).unwrap());
        }
        // The next frame fails either resource admission or codec validation.
        pipeline.extend(
            framer
                .encode(&if overload {
                    payload(count)
                } else {
                    b"invalid".to_vec()
                })
                .unwrap(),
        );
        for chunk in pipeline.chunks(7) {
            stream.write_all(chunk).await.unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut hello = [0; 26];
            stream.read_exact(&mut hello).await.unwrap();
            assert_eq!(&hello[..4], &22u32.to_be_bytes());
            assert_eq!(&hello[4..], br#"{"authenticated":true}"#);
            let mut ids = Vec::new();
            for _ in 0..count {
                // A client must tolerate split ACK reads independently of server writes.
                let mut frame = [0; tcp_ack::FRAME_LEN];
                for part in frame.chunks_mut(3) {
                    stream.read_exact(part).await.unwrap();
                }
                assert_eq!(&frame[..4], &tcp_ack::FRAME_HEADER);
                let receipt = tcp_ack::decode(&frame[4..]).unwrap();
                assert_eq!(receipt.required_deliveries, 1);
                assert_eq!(receipt.best_effort_deliveries, 0);
                ids.push(receipt.event_id);
            }
            assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
            loop {
                if sink.events.lock().unwrap().len() == count {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let events = sink.events.lock().unwrap();
            for (sequence, id) in ids.into_iter().enumerate() {
                let event = events.iter().find(|event| event.event_id == id).unwrap();
                assert_eq!(event.source_message_id.as_str(), format!("s:{sequence}"));
            }
        })
        .await
        .unwrap();
        assert_eq!(ingress.metrics.get(Metric::EventsAccepted), count as u64);
        assert_eq!(provider.calls.load(Ordering::Relaxed), 1);
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn tcp_binary_ack_write_timeout_keeps_acceptance_and_bounds_read_ahead() {
    use netbaiot_transports::tcp::{LengthPrefixFramer, TcpFramer};
    let provider = Arc::new(CountingProvider {
        calls: AtomicUsize::new(0),
        auth: auth(),
        delay: false,
    });
    let limits = Limits {
        write_timeout_ms: 100,
        ..Limits::default()
    };
    let (ingress, services, stop) = runtime(limits, provider);
    let mut lease = services
        .connections
        .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
        .unwrap();
    let tracking = lease.read_budget();
    // Exactly one complete ACK plus a partial second ACK fits, after reading auth.
    let (mut client, server) = tokio::io::duplex(64);
    let task = tokio::spawn(netbaiot_transports::tcp::connection(
        Box::new(server),
        services.clone(),
        lease,
        stop.clone(),
    ));
    let framer = LengthPrefixFramer { maximum: 65536 };
    client
        .write_all(
            &framer
                .encode(br#"{"credential_id":"a","secret":"secret"}"#)
                .unwrap(),
        )
        .await
        .unwrap();
    let mut hello = [0; 26];
    client.read_exact(&mut hello).await.unwrap();
    assert_eq!(&hello[4..], br#"{"authenticated":true}"#);
    let (mut reader, mut writer) = tokio::io::split(client);
    let sending = tokio::spawn(async move {
        for sequence in 0..3 {
            writer
                .write_all(&framer.encode(&payload(sequence)).unwrap())
                .await?;
        }
        std::io::Result::Ok(())
    });
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(Error::Timeout)));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes.len(), 64);
    assert_eq!(&bytes[..4], &tcp_ack::FRAME_HEADER);
    assert_eq!(
        tcp_ack::decode(&bytes[4..46]).unwrap().required_deliveries,
        1
    );
    assert_eq!(ingress.metrics.get(Metric::EventsAccepted), 2);
    assert_eq!(services.connections.active().unwrap(), [0; 3]);
    assert_eq!(tracking.capacity().unwrap(), 0);
    // The blocked third uplink cannot be admitted after ACK writing times out.
    let _ = sending.await.unwrap();
    stop.cancel();
}
