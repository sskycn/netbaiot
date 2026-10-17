use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    Services,
    mqtt::topics::{TopicKind, topic},
    serve_device_ingress, udp,
};
use sha2::Sha256;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use tokio_util::sync::CancellationToken;
const FORMATS: [&str; 4] = ["json", "cbor", "msgpack", "protobuf"];

fn control_snapshot(ingress: &Ingress, revision: u64) -> ControlSnapshot {
    ControlSnapshot {
        revision,
        products: FORMATS
            .iter()
            .map(|f| {
                let a = identity(f, "d");
                ProductRuntimeConfig {
                    tenant_id: a.device_key.tenant_id,
                    product_id: a.device_key.product_id,
                    codec_id: a.codec_id,
                    codec_version: 1,
                    revision,
                }
            })
            .collect(),
        routes: ingress.control.routes().unwrap().as_ref().clone(),
    }
}

#[tokio::test]
async fn control_commit_publishes_event_revision_and_rejects_invalid_routes_atomically() {
    let (ingress, _, _, _) = fixture(None);
    ingress.events.stop_workers().await.unwrap();
    ingress
        .apply_control_snapshot(control_snapshot(&ingress, 2))
        .unwrap();
    ingress
        .events
        .publish(DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("control:2").unwrap(),
            device: identity("json", "d").device_key,
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
        })
        .unwrap();
    assert_eq!(
        ingress.events.spool_records().unwrap()[0].routing_revision,
        2
    );
    let mut invalid = control_snapshot(&ingress, 3);
    invalid.routes[0].sinks = vec![SinkId::new("missing").unwrap()];
    assert!(ingress.apply_control_snapshot(invalid).is_err());
    assert_eq!(ingress.control.revision().unwrap(), 2);
    assert_eq!(
        ingress.events.spool_records().unwrap()[0].routing_revision,
        2
    );
}
fn identity(format: &str, device: &str) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new(format).unwrap(),
            device_id: DeviceId::new(device).unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new(if format == "acme" {
            "vendor-acme".into()
        } else {
            format!("netbaiot-{format}")
        })
        .unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}

#[tokio::test]
async fn admitted_control_and_routes_finish_before_quiesce_and_new_updates_are_fenced() {
    let (ingress, _, _, _) = fixture(None);
    let admitted = ingress.lifecycle.begin_admission().unwrap();
    let owner = ingress.lifecycle.clone();
    let quiesce = tokio::spawn(async move { owner.begin_quiesce().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while ingress.lifecycle.state() == LifecycleState::Running {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!quiesce.is_finished());
    ingress
        .apply_control_snapshot_admitted(&admitted, control_snapshot(&ingress, 2))
        .unwrap();
    ingress
        .replace_routes_admitted(
            &admitted,
            3,
            ingress.control.routes().unwrap().as_ref().clone(),
        )
        .unwrap();
    assert_eq!(ingress.control.revision().unwrap(), 3);
    assert!(matches!(
        ingress.apply_control_snapshot(control_snapshot(&ingress, 4)),
        Err(Error::Draining)
    ));
    drop(admitted);
    quiesce.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn unconfigured_dynamic_product_allows_distinct_bound_profiles_until_provisioned() {
    let (ingress, _, _, _) = fixture(None);
    let mut leases = Vec::new();
    let mut candidates = Vec::new();
    for f in ["json", "cbor"] {
        let id = format!("{f}-tcp");
        let mut candidate = ingress
            .authenticate_session(AuthenticationRequest::Secret {
                credential_id: &id,
                secret: b"0707070707070707070707070707070707070707070707070707070707070707",
            })
            .await
            .unwrap();
        candidate.auth.device_key.product_id = ProductId::new("dynamic").unwrap();
        candidate.auth.device_key.device_id = DeviceId::new(f).unwrap();
        candidates.push(candidate.clone());
        leases.push(
            ingress
                .register_session(candidate, Transport::Tcp)
                .unwrap()
                .0,
        );
    }
    let mut snapshot = control_snapshot(&ingress, 2);
    snapshot.products.push(ProductRuntimeConfig {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("dynamic").unwrap(),
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        revision: 2,
    });
    assert!(matches!(
        ingress.apply_control_snapshot(snapshot.clone()),
        Err(Error::Conflict)
    ));
    assert_eq!(ingress.control.revision().unwrap(), 1);
    drop(leases);
    ingress.apply_control_snapshot(snapshot).unwrap();
    assert!(
        ingress
            .register_session(candidates[0].clone(), Transport::Tcp)
            .is_ok()
    );
    assert!(matches!(
        ingress.register_session(candidates[1].clone(), Transport::Tcp),
        Err(Error::Forbidden)
    ));
    // A positive cache entry also undergoes the current product-profile check.
    let stale = identity("cbor", "d");
    let mut stale = stale;
    stale.device_key.product_id = ProductId::new("dynamic").unwrap();
    assert!(matches!(
        ingress.validate_auth_profile(&stale),
        Err(Error::Forbidden)
    ));
    ingress.events.stop_workers().await.unwrap();
}
fn payload(format: &str) -> Vec<u8> {
    if format == "acme" {
        let mut bytes = vec![
            165, 90, 0, 19, 1, 42, 0, 0, 0, 0x85, 0xff, 0x02, 0x8e, 1, b'A', b'1', b'0', b'0',
        ];
        bytes.push(bytes.iter().fold(0u8, |sum, b| sum.wrapping_add(*b)));
        return bytes;
    }
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "../netbaiot-codecs/tests/fixtures/telemetry.{format}"
        )),
    )
    .unwrap()
}
#[derive(Default)]
struct Capture(Mutex<Vec<DeviceEvent>>);
#[async_trait]
impl EventSink for Capture {
    async fn deliver(&self, e: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        self.0.lock().unwrap().push((*e.event).clone());
        Ok(SinkAck)
    }
}
fn fixture(
    provider: Option<Arc<dyn DeviceAuthenticator>>,
) -> (Arc<Ingress>, Arc<Services>, Arc<Capture>, CancellationToken) {
    fixture_with_sink(provider, None, None)
}
fn fixture_with_sink(
    provider: Option<Arc<dyn DeviceAuthenticator>>,
    delivery: Option<Arc<dyn EventSink>>,
    limits: Option<Limits>,
) -> (Arc<Ingress>, Arc<Services>, Arc<Capture>, CancellationToken) {
    fixture_with_vendor(provider, delivery, limits, None)
}
fn fixture_with_vendor(
    provider: Option<Arc<dyn DeviceAuthenticator>>,
    delivery: Option<Arc<dyn EventSink>>,
    limits: Option<Limits>,
    vendor_config: Option<netbaiot_codecs::vendor::declarative::DeclarativeConfig>,
) -> (Arc<Ingress>, Arc<Services>, Arc<Capture>, CancellationToken) {
    let l = Arc::new(limits.unwrap_or(Limits {
        requests_per_second: 10000,
        messages_per_device_second: 10000,
        messages_per_tenant_second: 10000,
        ..Limits::default()
    }));
    let metrics = Arc::new(Metrics::default());
    let sink = Arc::new(Capture::default());
    let sid = SinkId::new("capture").unwrap();
    let routes = vec![RouteDefinition {
        tenant: None,
        sinks: vec![sid.clone()],
    }];
    let events = EventBus::new(
        l.clone(),
        metrics.clone(),
        vec![SinkDefinition::bounded(
            sid,
            SinkDeliveryMode::ConfirmedRequired,
            delivery.unwrap_or_else(|| sink.clone()),
            &l,
        )],
        routes.clone(),
        1,
    )
    .unwrap();
    let mut entries = netbaiot_codecs::builtins(CodecLimits::default()).unwrap();
    let vendor_config = vendor_config.unwrap_or_else(|| {
        serde_json::from_slice(include_bytes!("../../../configs/codecs/acme-v1.json")).unwrap()
    });
    let vendor = netbaiot_codecs::vendor::declarative::DeclarativeCodec::compile(
        &vendor_config,
        CodecLimits::default(),
    )
    .unwrap();
    entries
        .push(netbaiot_codecs::vendor::registration("vendor-acme", 1, Arc::new(vendor)).unwrap());
    let codecs = CodecRegistry::new(entries).unwrap();
    let control = GatewayControl::empty(l.clone());
    control
        .apply(
            ControlSnapshot {
                revision: 1,
                products: FORMATS
                    .iter()
                    .map(|f| {
                        let a = identity(f, "d");
                        ProductRuntimeConfig {
                            tenant_id: a.device_key.tenant_id,
                            product_id: a.device_key.product_id,
                            codec_id: a.codec_id,
                            codec_version: 1,
                            revision: 1,
                        }
                    })
                    .collect(),
                routes,
            },
            &codecs,
        )
        .unwrap();
    let static_provider = StaticAuthenticator::new(
        FORMATS
            .iter()
            .chain([&"acme"])
            .flat_map(|f| {
                ["mqtt", "tcp", "udp"].map(|d| Credential {
                    credential_id: format!("{f}-{d}"),
                    secret_hex: "07".repeat(32),
                    identity: identity(f, d),
                })
            })
            .collect(),
        &l,
    )
    .unwrap();
    let lifecycle = Arc::new(Lifecycle::starting());
    lifecycle.mark_running().unwrap();
    let ingress = Arc::new(Ingress::new(
        l.clone(),
        AuthCache::new(
            provider.unwrap_or(static_provider),
            l.clone(),
            metrics.clone(),
        ),
        codecs,
        events,
        control,
        metrics,
        Sessions::new(l),
        lifecycle,
    ));
    let stop = CancellationToken::new();
    let services = Services::new(ingress.clone(), stop.clone());
    (ingress, services, sink, stop)
}

#[tokio::test]
async fn mixed_codec_required_backpressure_control_update_and_capacity_cleanup() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct SlowSink {
        blocked: AtomicBool,
        release: tokio::sync::Notify,
        acked: AtomicUsize,
    }
    #[async_trait]
    impl EventSink for SlowSink {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            let released = self.release.notified();
            if self.blocked.load(Ordering::SeqCst) {
                released.await;
            }
            self.acked.fetch_add(1, Ordering::SeqCst);
            Ok(SinkAck)
        }
    }
    let slow = Arc::new(SlowSink {
        blocked: AtomicBool::new(true),
        release: tokio::sync::Notify::new(),
        acked: AtomicUsize::new(0),
    });
    let (ingress, _, _, _) = fixture_with_sink(
        None,
        Some(slow.clone()),
        Some(Limits {
            global_event_max_count: 16,
            sink_queue_max_count: 16,
            requests_per_second: 10000,
            messages_per_device_second: 10000,
            messages_per_tenant_second: 10000,
            ..Limits::default()
        }),
    );
    for _ in 0..4 {
        for f in FORMATS {
            ingress
                .ingest(
                    &identity(f, "tcp"),
                    IngressEnvelope {
                        transport: Transport::Tcp,
                        payload: &payload(f),
                        require_command_ack: false,
                        validated_at: std::time::Instant::now(),
                        validation_us: 0,
                    },
                )
                .await
                .unwrap();
        }
    }
    let before = ingress.events.usage().unwrap();
    assert_eq!(before.pending_required, 16);
    assert!(matches!(
        ingress
            .ingest(
                &identity("json", "tcp"),
                IngressEnvelope {
                    transport: Transport::Tcp,
                    payload: &payload("json"),
                    require_command_ack: false,
                    validated_at: std::time::Instant::now(),
                    validation_us: 0,
                }
            )
            .await,
        Err(Error::Overloaded)
    ));
    assert_eq!(ingress.events.usage().unwrap(), before);
    ingress
        .apply_control_snapshot(control_snapshot(&ingress, 2))
        .unwrap();
    let retained = ingress.events.spool_records().unwrap();
    assert_eq!(retained.len(), 16);
    assert!(retained.iter().all(|r| r.routing_revision == 1));
    slow.blocked.store(false, Ordering::SeqCst);
    slow.release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(3), async {
        while ingress.events.usage().unwrap().events != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(ingress.events.usage().unwrap(), EventBusUsage::default());
    assert_eq!(slow.acked.load(Ordering::SeqCst), 16);
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn management_snapshot_and_route_endpoints_use_joint_commit() {
    let (ingress, mut services, _, _) = fixture(None);
    let secret = "ab".repeat(32);
    Arc::get_mut(&mut services).unwrap().admin = Some(Arc::new(
        AdminAccess::new(&secret, Default::default(), &ingress.limits).unwrap(),
    ));
    ingress.events.stop_workers().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let owner = tokio::spawn(netbaiot_transports::serve_management_http(
        listener,
        services,
        None,
        stop.clone(),
    ));
    async fn put(address: std::net::SocketAddr, secret: &str, path: &str, body: Vec<u8>) -> bool {
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut stream = TcpStream::connect(address).await.unwrap();
            let header = format!("PUT /api/v1/{path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {secret}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
            stream.write_all(header.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            let mut bytes = Vec::new();
            stream.take(65537).read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.len() <= 65536);
            bytes.starts_with(b"HTTP/1.1 204")
        }).await.unwrap()
    }
    let snapshot = control_snapshot(&ingress, 2);
    assert!(
        put(
            address,
            &secret,
            "control/snapshot",
            serde_json::to_vec(&snapshot).unwrap()
        )
        .await
    );
    let routes = RoutesUpdate {
        revision: 3,
        routes: snapshot.routes,
    };
    assert!(
        put(
            address,
            &secret,
            "routes",
            serde_json::to_vec(&routes).unwrap()
        )
        .await
    );
    let mut invalid = control_snapshot(&ingress, 4);
    invalid.routes[0].sinks = vec![SinkId::new("unknown").unwrap()];
    assert!(
        !put(
            address,
            &secret,
            "control/snapshot",
            serde_json::to_vec(&invalid).unwrap()
        )
        .await
    );
    assert_eq!(ingress.control.revision().unwrap(), 3);
    ingress
        .events
        .publish(DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("http:3").unwrap(),
            device: identity("json", "d").device_key,
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
        })
        .unwrap();
    assert_eq!(
        ingress.events.spool_records().unwrap()[0].routing_revision,
        3
    );
    stop.cancel();
    owner.await.unwrap().unwrap();
}
fn mqtt_string(v: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(v.len() as u16).to_be_bytes());
    out.extend_from_slice(v.as_bytes());
}
fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    let mut n = body.len();
    loop {
        let b = (n % 128) as u8;
        n /= 128;
        out.push(b | if n > 0 { 128 } else { 0 });
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}
async fn read_mqtt(s: &mut TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let h = s.read_u8().await.unwrap();
        let mut length = 0;
        let mut shift = 0;
        loop {
            let b = s.read_u8().await.unwrap();
            length |= usize::from(b & 127) << shift;
            if b & 128 == 0 {
                break;
            }
            shift += 7;
            assert!(shift <= 21);
        }
        assert!(length <= 65536);
        let mut body = vec![0; length];
        s.read_exact(&mut body).await.unwrap();
        (h, body)
    })
    .await
    .unwrap()
}
async fn mqtt_connect(
    address: std::net::SocketAddr,
    a: &AuthenticatedDevice,
    v5: bool,
) -> TcpStream {
    mqtt_connect_with_maximum(address, a, v5, None).await
}
async fn mqtt_connect_with_maximum(
    address: std::net::SocketAddr,
    a: &AuthenticatedDevice,
    v5: bool,
    maximum: Option<u32>,
) -> TcpStream {
    let mut s = TcpStream::connect(address).await.unwrap();
    let mut body = Vec::new();
    mqtt_string("MQTT", &mut body);
    body.extend_from_slice(&[if v5 { 5 } else { 4 }, 0xc2, 0, 30]);
    if v5 {
        if let Some(maximum) = maximum {
            body.extend_from_slice(&[5, 0x27]);
            body.extend_from_slice(&maximum.to_be_bytes());
        } else {
            body.push(0);
        }
    }
    let id = format!("{}-mqtt", a.device_key.product_id.as_str());
    mqtt_string(&id, &mut body);
    mqtt_string(&id, &mut body);
    mqtt_string(&"07".repeat(32), &mut body);
    for bytes in packet(0x10, &body).chunks(3) {
        s.write_all(bytes).await.unwrap();
    }
    let (h, reply) = read_mqtt(&mut s).await;
    assert_eq!(h, 0x20);
    assert_eq!(reply[1], 0);
    let mut sub = vec![0, 1];
    if v5 {
        sub.push(0);
    }
    mqtt_string(&topic(&a.device_key, TopicKind::Down), &mut sub);
    sub.push(1);
    s.write_all(&packet(0x82, &sub)).await.unwrap();
    assert_eq!(read_mqtt(&mut s).await.0, 0x90);
    s
}
fn publish(a: &AuthenticatedDevice, p: &[u8], qos: u8, v5: bool) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_string(&topic(&a.device_key, TopicKind::Up), &mut body);
    if qos > 0 {
        body.extend_from_slice(&[0, qos]);
    }
    if v5 {
        body.push(0);
    }
    body.extend_from_slice(p);
    packet(0x30 | (qos << 1), &body)
}
async fn read_tcp(s: &mut TcpStream, expected: tcp_frame::FrameKind) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let header = s.read_u32().await.unwrap().to_be_bytes();
        let (kind, n) = tcp_frame::decode_header(header, 65536).unwrap();
        assert_eq!(kind, expected);
        let mut v = vec![0; n];
        s.read_exact(&mut v).await.unwrap();
        v
    })
    .await
    .unwrap()
}
fn frame(p: &[u8]) -> Vec<u8> {
    let mut v = (p.len() as u32).to_be_bytes().to_vec();
    v.extend_from_slice(p);
    v
}
fn command(a: &AuthenticatedDevice) -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId::generate(),
        device: a.device_key.clone(),
        expires_at: Some(now_ms() + 5000),
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: std::collections::BTreeMap::from([(
                "temperature".into(),
                Scalar::Number(25.3),
            )]),
        },
    }
}
fn datagram(f: &str, p: &[u8]) -> Vec<u8> {
    datagram_sequence(f, p, 1)
}
fn datagram_sequence(f: &str, p: &[u8], sequence: u64) -> Vec<u8> {
    let id = format!("{f}-udp");
    let mut v = b"NBI1".to_vec();
    v.push(id.len() as u8);
    v.extend_from_slice(id.as_bytes());
    v.extend_from_slice(&1u32.to_be_bytes());
    v.extend_from_slice(&[3; 16]);
    v.extend_from_slice(&sequence.to_be_bytes());
    v.extend_from_slice(&now_ms().to_be_bytes());
    v.extend_from_slice(&(p.len() as u16).to_be_bytes());
    v.extend_from_slice(p);
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&v);
    v.extend_from_slice(&mac.finalize().into_bytes());
    v
}

#[tokio::test]
#[ignore = "bounded 60-second real-network mixed-codec reconnect soak"]
async fn mixed_codec_reconnect_and_control_soak() {
    let (ingress, services, sink, stop) = fixture_with_sink(
        None,
        None,
        Some(Limits {
            requests_per_second: 10000,
            requests_per_ip_second: 10000,
            messages_per_device_second: 10000,
            messages_per_tenant_second: 10000,
            ..Limits::default()
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let udp_socket = UdpSocket::bind(address).await.unwrap();
    let tcp_owner = tokio::spawn(serve_device_ingress(
        listener,
        services.clone(),
        None,
        stop.clone(),
    ));
    let udp_owner = tokio::spawn(udp::serve(udp_socket, services.clone(), stop.clone()));
    let started = tokio::time::Instant::now();
    // The gateway's persistent churn is under test, rather than exhaustion of
    // the load host's ephemeral ports. Bound reconnect cycles to four per second.
    let mut pace = tokio::time::interval(Duration::from_millis(250));
    pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cycle = 0u64;
    while started.elapsed() < Duration::from_secs(60) {
        pace.tick().await;
        cycle += 1;
        for f in FORMATS.into_iter().chain(["acme"]) {
            let auth = identity(f, "mqtt");
            let v5 = cycle.is_multiple_of(2);
            let payload = payload(f);
            let mut mqtt = mqtt_connect(address, &auth, v5).await;
            for qos in [1, 2] {
                mqtt.write_all(&publish(&auth, &payload, qos, v5))
                    .await
                    .unwrap();
                assert_eq!(
                    read_mqtt(&mut mqtt).await.0,
                    if qos == 1 { 0x40 } else { 0x50 }
                );
                if qos == 2 {
                    mqtt.write_all(&[0x62, 2, 0, 2]).await.unwrap();
                    assert_eq!(read_mqtt(&mut mqtt).await.0, 0x70);
                }
            }
            mqtt.write_all(&[0xe0, 0]).await.unwrap();
            let mut tcp = TcpStream::connect(address).await.unwrap();
            let hello = serde_json::to_vec(
                &serde_json::json!({"credential_id":format!("{f}-tcp"),"secret":"07".repeat(32)}),
            )
            .unwrap();
            for part in frame(&hello).chunks(3) {
                tcp.write_all(part).await.unwrap();
            }
            assert_eq!(
                read_tcp(&mut tcp, tcp_frame::FrameKind::Payload).await,
                br#"{"authenticated":true}"#
            );
            for part in frame(&payload).chunks(3) {
                tcp.write_all(part).await.unwrap();
            }
            let _: EventAcceptance =
                tcp_ack::decode(&read_tcp(&mut tcp, tcp_frame::FrameKind::EventAccepted).await)
                    .unwrap();
            let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let data = datagram_sequence(f, &payload, cycle);
            udp.send_to(&data, address).await.unwrap();
            let mut ack = [0; 65];
            let (n, _) = tokio::time::timeout(Duration::from_secs(3), udp.recv_from(&mut ack))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(n, 64);
            assert_eq!(&ack[24..32], &cycle.to_be_bytes());
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while ingress.events.usage().unwrap().events != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(ingress.events.usage().unwrap(), EventBusUsage::default());
        let mut captured = sink.0.lock().unwrap();
        assert_eq!(captured.len(), 20);
        captured.clear(); // Test evidence itself never grows with soak duration.
        drop(captured);
        ingress
            .apply_control_snapshot(control_snapshot(&ingress, cycle + 1))
            .unwrap();
        assert!(ingress.sessions.list(0, 32).unwrap().len() <= 10);
    }
    stop.cancel();
    tcp_owner.await.unwrap().unwrap();
    udp_owner.await.unwrap().unwrap();
    assert!(ingress.sessions.list(0, 32).unwrap().is_empty());
    ingress.events.stop_workers().await.unwrap();
    println!(
        "cycles={cycle}, accepted_events={}, duration_seconds={:.2}",
        cycle * 20,
        started.elapsed().as_secs_f64()
    );
}
#[tokio::test]
async fn four_codecs_coexist_on_real_mqtt_tcp_udp_with_binary_commands() {
    let (ingress, services, sink, stop) = fixture(None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let socket = UdpSocket::bind(address).await.unwrap();
    let tcp_task = tokio::spawn(serve_device_ingress(
        listener,
        services.clone(),
        None,
        stop.clone(),
    ));
    let udp_task = tokio::spawn(udp::serve(socket, services.clone(), stop.clone()));
    let mut clients = Vec::new();
    for f in FORMATS {
        let a = identity(f, "mqtt");
        let p = payload(f);
        let mut client = mqtt_connect(address, &a, false).await;
        for qos in 0..=2 {
            client
                .write_all(&publish(&a, &p, qos, false))
                .await
                .unwrap();
            if qos > 0 {
                let (h, body) = read_mqtt(&mut client).await;
                assert_eq!(h, if qos == 1 { 0x40 } else { 0x50 });
                assert_eq!(&body[..2], &[0, qos]);
            }
            if qos == 2 {
                // DUP before PUBREL must validate identically and own only one event.
                let mut dup = publish(&a, &p, qos, false);
                dup[0] |= 8;
                client.write_all(&dup).await.unwrap();
                assert_eq!(read_mqtt(&mut client).await.0, 0x50);
                client.write_all(&[0x62, 2, 0, 2]).await.unwrap();
                assert_eq!(read_mqtt(&mut client).await.0, 0x70);
                client.write_all(&[0x62, 2, 0, 2]).await.unwrap();
                assert_eq!(read_mqtt(&mut client).await.0, 0x70);
            }
        }
        let c = command(&a);
        let expected = ingress
            .codecs
            .get(&a)
            .unwrap()
            .encode(
                &EncodeContext {
                    device: &a.device_key,
                },
                &c,
            )
            .unwrap();
        services.router.send(c).unwrap();
        let (h, body) = read_mqtt(&mut client).await;
        assert_eq!(h & 0xf0, 0x30);
        let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let offset = 2 + topic_len;
        let pid = &body[offset..offset + 2];
        assert_eq!(&body[offset + 2..], expected);
        client.write_all(&[0x40, 2, pid[0], pid[1]]).await.unwrap();
        clients.push(client);
        let a = identity(f, "tcp");
        let mut client = TcpStream::connect(address).await.unwrap();
        let hello = serde_json::to_vec(
            &serde_json::json!({"credential_id":format!("{f}-tcp"),"secret":"07".repeat(32)}),
        )
        .unwrap();
        for bytes in frame(&hello).chunks(2) {
            client.write_all(bytes).await.unwrap();
        }
        assert_eq!(
            read_tcp(&mut client, tcp_frame::FrameKind::Payload).await,
            br#"{"authenticated":true}"#
        );
        // Split binary payload, then two coalesced frames.
        for bytes in frame(&p).chunks(3) {
            client.write_all(bytes).await.unwrap();
        }
        let _: EventAcceptance =
            tcp_ack::decode(&read_tcp(&mut client, tcp_frame::FrameKind::EventAccepted).await)
                .unwrap();
        let mut frames = frame(&p);
        frames.extend_from_slice(&frame(&p));
        client.write_all(&frames).await.unwrap();
        for _ in 0..2 {
            let _: EventAcceptance =
                tcp_ack::decode(&read_tcp(&mut client, tcp_frame::FrameKind::EventAccepted).await)
                    .unwrap();
        }
        let c = command(&a);
        let expected = ingress
            .codecs
            .get(&a)
            .unwrap()
            .encode(
                &EncodeContext {
                    device: &a.device_key,
                },
                &c,
            )
            .unwrap();
        services.router.send(c).unwrap();
        assert_eq!(
            read_tcp(&mut client, tcp_frame::FrameKind::Payload).await,
            expected
        );
        // Resume receipts after the command on the same connection for each codec.
        client.write_all(&frame(&p)).await.unwrap();
        let receipt =
            tcp_ack::decode(&read_tcp(&mut client, tcp_frame::FrameKind::EventAccepted).await)
                .unwrap();
        assert_eq!(receipt.required_deliveries, 1);
        clients.push(client);
        let udp_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request = datagram(f, &p);
        let mut ack = [0; 128];
        for _ in 0..2 {
            udp_client.send_to(&request, address).await.unwrap();
            let (n, _) =
                tokio::time::timeout(Duration::from_secs(3), udp_client.recv_from(&mut ack))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(n, 64);
            assert_eq!(&ack[..4], b"NBA1");
            assert_eq!(&ack[4..8], &1u32.to_be_bytes());
            assert_eq!(&ack[8..24], &[3; 16]);
            assert_eq!(&ack[24..32], &1u64.to_be_bytes());
            let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
            mac.update(&ack[..32]);
            mac.verify_slice(&ack[32..64]).unwrap();
        }
        let mut bad = request.clone();
        *bad.last_mut().unwrap() ^= 1;
        udp_client.send_to(&bad, address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), udp_client.recv_from(&mut ack))
                .await
                .is_err()
        );
    }
    assert_eq!(ingress.sessions.list(0, 32).unwrap().len(), 8);
    tokio::time::timeout(Duration::from_secs(3), async {
        while sink.0.lock().unwrap().len() != 32 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let captured = sink.0.lock().unwrap();
        let expected = &captured[0].kind;
        for e in captured.iter() {
            assert_eq!(&e.kind, expected);
            assert_eq!(e.source_message_id.as_str(), "sample:1");
        }
    }
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 4);
    assert_eq!(ingress.metrics.get(Metric::UdpAcceptedDuplicates), 4);
    stop.cancel();
    tcp_task.await.unwrap().unwrap();
    udp_task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
    drop(clients);
}
#[tokio::test]
async fn mqtt5_binary_payloads_and_qos2_prevalidation_agree() {
    let (ingress, services, sink, stop) = fixture(None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(serve_device_ingress(listener, services, None, stop.clone()));
    for f in FORMATS {
        let a = identity(f, "mqtt");
        let mut c = mqtt_connect(addr, &a, true).await;
        let p = payload(f);
        let truncated = &p[..p.len() / 2];
        c.write_all(&publish(&a, truncated, 2, true)).await.unwrap();
        let (header, reason) = read_mqtt(&mut c).await;
        assert_eq!(header, 0x50);
        assert!(reason.len() >= 3 && reason[2] >= 0x80);
        // The rejected packet ID is free for the next valid QoS2 exchange.
        for qos in 0..=2 {
            c.write_all(&publish(&a, &p, qos, true)).await.unwrap();
            if qos > 0 {
                assert_eq!(
                    read_mqtt(&mut c).await.0,
                    if qos == 1 { 0x40 } else { 0x50 }
                );
            }
            if qos == 2 {
                c.write_all(&[0x62, 2, 0, 2]).await.unwrap();
                assert_eq!(read_mqtt(&mut c).await.0, 0x70);
            }
        }
        let mut bad = p.clone();
        bad.truncate(bad.len() / 2);
        assert!(ingress.validate_mqtt_qos2_payload(&a, &bad, false).is_err());
        assert!(ingress.validate_mqtt_qos2_payload(&a, &p, true).is_err());
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while sink.0.lock().unwrap().len() != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    task.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn registry_profiles_control_rollback_and_stale_candidates() {
    let (ingress, _, _, _) = fixture(None);
    let id = CodecId::new("test").unwrap();
    let c: Arc<dyn DeviceCodec> = Arc::new(netbaiot_codecs::JsonV1::default());
    assert!(CodecRegistry::new(vec![(id.clone(), 0, c.clone())]).is_err());
    assert!(
        CodecRegistry::new(vec![(id.clone(), 1, c.clone()), (id.clone(), 1, c.clone())]).is_err()
    );
    assert!(
        CodecRegistry::new(vec![(id.clone(), 1, c.clone()), (id.clone(), 2, c.clone())]).is_ok()
    );
    assert!(CodecRegistry::new(vec![(id, 1, c); 65]).is_err());
    let req = || AuthenticationRequest::Secret {
        credential_id: "cbor-tcp",
        secret: b"0707070707070707070707070707070707070707070707070707070707070707",
    };
    let candidate = ingress.authenticate_session(req()).await.unwrap();
    let mut unknown = candidate.clone();
    unknown.auth.codec_id = CodecId::new("unknown").unwrap();
    assert!(matches!(
        ingress.register_session(unknown, Transport::Tcp),
        Err(Error::Codec)
    ));
    let mut version = candidate.clone();
    version.auth.codec_version = 2;
    assert!(matches!(
        ingress.register_session(version, Transport::Tcp),
        Err(Error::Codec)
    ));
    let mut conflict = candidate.clone();
    conflict.auth.codec_id = CodecId::new("netbaiot-json").unwrap();
    assert!(matches!(
        ingress.register_session(conflict, Transport::Tcp),
        Err(Error::Forbidden)
    ));
    assert_eq!(ingress.metrics.get(Metric::CodecProfileUnsupported), 2);
    assert_eq!(ingress.metrics.get(Metric::CodecProfileMismatch), 1);
    let product = |a: AuthenticatedDevice| ProductRuntimeConfig {
        tenant_id: a.device_key.tenant_id,
        product_id: a.device_key.product_id,
        codec_id: a.codec_id,
        codec_version: a.codec_version,
        revision: 2,
    };
    for change in ["unknown", "version", "switch", "remove"] {
        let mut products: Vec<_> = FORMATS.iter().map(|f| product(identity(f, "d"))).collect();
        match change {
            "unknown" => products[0].codec_id = CodecId::new("unknown").unwrap(),
            "version" => products[0].codec_version = 0,
            "switch" => products[0].codec_id = CodecId::new("netbaiot-cbor").unwrap(),
            _ => {
                products.remove(0);
            }
        }
        assert!(
            ingress
                .apply_control_snapshot(ControlSnapshot {
                    revision: 2,
                    products,
                    routes: ingress.control.routes().unwrap().as_ref().clone()
                })
                .is_err()
        );
        assert_eq!(ingress.control.revision().unwrap(), 1);
    }
    ingress
        .invalidate_auth(&AuthInvalidation::Product {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("cbor").unwrap(),
        })
        .unwrap();
    assert!(matches!(
        ingress.register_session(candidate, Transport::Tcp),
        Err(Error::Unavailable)
    ));
    ingress.events.stop_workers().await.unwrap();
}
#[tokio::test]
async fn business_rpc_auth_selects_all_codecs_before_registration() {
    let registry = BusinessRpcRegistry::new(4, 65536, Duration::from_secs(2)).unwrap();
    let (send, mut requests) = tokio::sync::mpsc::channel(4);
    let lease = registry
        .register(
            send,
            BusinessProviderScope {
                global: true,
                tenants: vec![],
            },
        )
        .unwrap();
    lease.mark_serving(1).unwrap();
    let (ingress, _, _, _) = fixture(Some(BusinessRpcAuthProvider::new(registry.clone())));
    for format in FORMATS.into_iter().chain(["unknown"]) {
        let auth = identity(format, "tcp");
        let pending = tokio::spawn({
            let ingress = ingress.clone();
            let id = format.to_owned();
            async move {
                ingress
                    .authenticate_session(AuthenticationRequest::Secret {
                        credential_id: &id,
                        secret: b"secret",
                    })
                    .await
            }
        });
        let outbound = requests.recv().await.unwrap();
        let BusinessRpcCall::Request {
            request_id, method, ..
        } = outbound.call
        else {
            panic!("expected auth request")
        };
        let value = serde_json::json!({"device_key":auth.device_key,"credential_version":1,"auth_generation":1,"codec_id":auth.codec_id,"codec_version":1,"publish":true,"commands":true,"auth_revision":1});
        assert!(registry.complete(lease.epoch(), request_id, method, Ok(value)));
        let result = pending.await.unwrap();
        if format == "unknown" {
            assert!(matches!(result, Err(Error::Codec)));
        } else {
            let (session, _) = ingress
                .register_session(result.unwrap(), Transport::Tcp)
                .unwrap();
            assert_eq!(session.device.product_id.as_str(), format);
        }
    }
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn command_ack_permissions_and_spool_replay_are_codec_independent() {
    let (ingress, _, _, _) = fixture(None);
    ingress.events.stop_workers().await.unwrap();
    let mut accepted = std::collections::HashSet::new();
    for f in FORMATS {
        let mut auth = identity(f, "tcp");
        let p = std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("../netbaiot-codecs/tests/fixtures/command_ack.{f}")),
        )
        .unwrap();
        let env = || IngressEnvelope {
            transport: Transport::Tcp,
            payload: &p,
            require_command_ack: true,
            validated_at: std::time::Instant::now(),
            validation_us: 0,
        };
        auth.permissions.commands = false;
        assert!(matches!(
            ingress.ingest(&auth, env()).await,
            Err(Error::Forbidden)
        ));
        assert!(matches!(
            ingress.validate_mqtt_qos2_payload(&auth, &p, true),
            Err(Error::Forbidden)
        ));
        auth.permissions.commands = true;
        auth.permissions.publish = false;
        assert!(matches!(
            ingress.ingest(&auth, env()).await,
            Err(Error::Forbidden)
        ));
        auth.permissions.publish = true;
        let receipt = ingress.ingest(&auth, env()).await.unwrap().receipt;
        accepted.insert(receipt.event_id);
        assert!(matches!(
            CommandRouter::new(ingress.clone()).send(command(&auth)),
            Err(Error::Unavailable)
        ));
    }
    let root = std::env::temp_dir().join(format!(
        "netbaiot-multi-codec-spool-{}",
        uuid::Uuid::new_v4()
    ));
    let spool = RestartSpool::new(root.clone(), ingress.limits.clone());
    ingress.lifecycle.begin_quiesce().await.unwrap();
    assert!(
        ingress
            .events
            .commit_required_to_spool(&spool)
            .await
            .unwrap()
            .is_some()
    );
    let batch = spool.recover().await.unwrap();
    assert_eq!(batch.records.len(), 4);
    let ids: std::collections::HashSet<_> =
        batch.records.iter().map(|r| r.event.event_id).collect();
    assert_eq!(ids, accepted);
    let sid = SinkId::new("capture").unwrap();
    let sink = Arc::new(Capture::default());
    let restored = EventBus::new(
        ingress.limits.clone(),
        Arc::new(Metrics::default()),
        vec![SinkDefinition::bounded(
            sid.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink.clone(),
            &ingress.limits,
        )],
        vec![RouteDefinition {
            tenant: None,
            sinks: vec![sid],
        }],
        1,
    )
    .unwrap();
    assert_eq!(restored.restore(batch.records.clone()).unwrap(), 4);
    assert!(
        restored
            .wait_required_drained(Duration::from_secs(3))
            .await
            .unwrap()
    );
    let observed: std::collections::HashSet<_> =
        sink.0.lock().unwrap().iter().map(|e| e.event_id).collect();
    assert_eq!(observed, accepted);
    // Explicit replay duplicates keep the same event identity even after prior ACK.
    assert_eq!(restored.restore(batch.records).unwrap(), 4);
    assert!(
        restored
            .wait_required_drained(Duration::from_secs(3))
            .await
            .unwrap()
    );
    assert_eq!(sink.0.lock().unwrap().len(), 8);
    spool.remove_committed(batch.committed_files).await.unwrap();
    restored.stop_workers().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn new_product_profiles_cannot_switch_live_or_pending_authentication() {
    let (ingress, _, _, _) = fixture(None);
    let mut candidate = ingress
        .authenticate_session(AuthenticationRequest::Secret {
            credential_id: "cbor-tcp",
            secret: b"0707070707070707070707070707070707070707070707070707070707070707",
        })
        .await
        .unwrap();
    candidate.auth.device_key.product_id = ProductId::new("dynamic").unwrap();
    let pending = candidate.clone();
    let (session, _) = ingress.register_session(candidate, Transport::Tcp).unwrap();
    let original: Vec<_> = FORMATS
        .iter()
        .map(|f| {
            let a = identity(f, "d");
            ProductRuntimeConfig {
                tenant_id: a.device_key.tenant_id,
                product_id: a.device_key.product_id,
                codec_id: a.codec_id,
                codec_version: 1,
                revision: 1,
            }
        })
        .collect();
    let mut products = original.clone();
    products.push(ProductRuntimeConfig {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("dynamic").unwrap(),
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        revision: 2,
    });
    let snapshot = ControlSnapshot {
        revision: 2,
        products,
        routes: ingress.control.routes().unwrap().as_ref().clone(),
    };
    assert!(matches!(
        ingress.apply_control_snapshot(snapshot.clone()),
        Err(Error::Conflict)
    ));
    assert_eq!(ingress.control.revision().unwrap(), 1);
    drop(session);
    ingress.apply_control_snapshot(snapshot).unwrap();
    assert!(matches!(
        ingress.register_session(pending, Transport::Tcp),
        Err(Error::Forbidden)
    ));
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn vendor_payloads_use_authenticated_codec_on_real_mqtt_tcp_udp_and_commands() {
    let (ingress, services, sink, stop) = fixture(None);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let udp_socket = UdpSocket::bind(address).await.unwrap();
    let tcp_owner = tokio::spawn(serve_device_ingress(
        listener,
        services.clone(),
        None,
        stop.clone(),
    ));
    let udp_owner = tokio::spawn(udp::serve(udp_socket, services.clone(), stop.clone()));
    let json_payload = payload("json");
    let payload = payload("acme");
    let mut clients = Vec::new();
    for v5 in [false, true] {
        tokio::time::timeout(Duration::from_secs(3), async {
            while sink.0.lock().unwrap().len() != usize::from(v5) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let auth = identity("acme", "mqtt");
        let mut client = mqtt_connect(address, &auth, v5).await;
        client
            .write_all(&publish(&auth, &payload, 2, v5))
            .await
            .unwrap();
        assert_eq!(read_mqtt(&mut client).await.0, 0x50);
        // Prevalidation owns no business event. Only PUBREL crosses EventAccepted.
        assert_eq!(sink.0.lock().unwrap().len(), usize::from(v5));
        client.write_all(&[0x62, 2, 0, 2]).await.unwrap();
        assert_eq!(read_mqtt(&mut client).await.0, 0x70);
        let mut cmd = command(&auth);
        cmd.payload.name = "set_temperature".into();
        let expected = ingress
            .codecs
            .get(&auth)
            .unwrap()
            .encode(
                &EncodeContext {
                    device: &auth.device_key,
                },
                &cmd,
            )
            .unwrap();
        services.router.send(cmd).unwrap();
        let (_, body) = read_mqtt(&mut client).await;
        let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let offset = 2 + topic_len;
        let pid = &body[offset..offset + 2];
        let payload_offset = if v5 {
            // The command TTL is carried by MQTT 5 Message Expiry Interval.
            let properties = usize::from(body[offset + 2]);
            assert!(properties < 128);
            offset + 3 + properties
        } else {
            offset + 2
        };
        assert_eq!(&body[payload_offset..], &expected);
        client.write_all(&[0x40, 2, pid[0], pid[1]]).await.unwrap();
        client.write_all(&[0xe0, 0]).await.unwrap();
        clients.push(client);
    }
    let auth = identity("acme", "tcp");
    let mut tcp = TcpStream::connect(address).await.unwrap();
    let hello = serde_json::to_vec(
        &serde_json::json!({"credential_id":"acme-tcp","secret":"07".repeat(32)}),
    )
    .unwrap();
    tcp.write_all(&frame(&hello)).await.unwrap();
    assert_eq!(
        read_tcp(&mut tcp, tcp_frame::FrameKind::Payload).await,
        br#"{"authenticated":true}"#
    );
    for part in frame(&payload).chunks(2) {
        tcp.write_all(part).await.unwrap();
    }
    let receipt =
        tcp_ack::decode(&read_tcp(&mut tcp, tcp_frame::FrameKind::EventAccepted).await).unwrap();
    assert_eq!(receipt.required_deliveries, 1);
    let mut cmd = command(&auth);
    cmd.payload.name = "set_temperature".into();
    let id = cmd.command_id;
    services.router.send(cmd).unwrap();
    let wire = read_tcp(&mut tcp, tcp_frame::FrameKind::Payload).await;
    assert_eq!(&wire[5..21], id.0.as_bytes());
    let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let request = datagram("acme", &payload);
    let mut ack = [0; 128];
    for _ in 0..2 {
        udp.send_to(&request, address).await.unwrap();
        let (n, _) = tokio::time::timeout(Duration::from_secs(3), udp.recv_from(&mut ack))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 64);
        assert_eq!(&ack[..4], b"NBA1");
        let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
        mac.update(&ack[..32]);
        mac.verify_slice(&ack[32..64]).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while sink.0.lock().unwrap().len() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(ingress.metrics.get(Metric::UdpAccepted), 1);
    assert_eq!(ingress.metrics.get(Metric::UdpAcceptedDuplicates), 1);
    for e in sink.0.lock().unwrap().iter() {
        assert_eq!(e.source_message_id.as_str(), "42");
        assert_eq!(e.device.product_id.as_str(), "acme");
    }
    // No payload-selected fallback to JSON, including before QoS2 PUBREC.
    let auth = identity("acme", "mqtt");
    let mut mqtt = mqtt_connect(address, &auth, false).await;
    mqtt.write_all(&publish(&auth, &json_payload, 2, false))
        .await
        .unwrap();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), mqtt.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(sink.0.lock().unwrap().len(), 4);
    stop.cancel();
    tcp_owner.await.unwrap().unwrap();
    udp_owner.await.unwrap().unwrap();
    ingress.events.stop_workers().await.unwrap();
}

#[tokio::test]
async fn vendor_command_transport_limits_reject_before_queue_and_keep_connections_alive() {
    use netbaiot_codecs::vendor::declarative::DeclarativeConfig;
    // Include the MQTT Remaining Length expansion and MQTT 5 expiry property.
    for (transport, maximum) in [
        ("mqtt", 8192),
        ("tcp", 256),
        ("mqtt5", 8192),
        ("mqtt5-client", 128),
        ("write", 256),
        ("global", 256),
    ] {
        let mqtt = transport.starts_with("mqtt");
        let v5 = transport.starts_with("mqtt5");
        let auth = identity("acme", if mqtt { "mqtt" } else { "tcp" });
        let overhead = if mqtt {
            2 + topic(&auth.device_key, TopicKind::Down).len() + 2 + if v5 { 6 } else { 0 }
        } else {
            0
        };
        let capacity = if mqtt {
            maximum - if maximum <= 128 { 2 } else { 3 } - overhead
        } else if transport == "write" {
            maximum - 4
        } else {
            maximum
        };
        let mut config: DeclarativeConfig =
            serde_json::from_slice(include_bytes!("../../../configs/codecs/acme-v1.json")).unwrap();
        config.commands[0].checksum = None;
        config.commands[0].size = capacity;
        let mut large = config.commands[0].clone();
        large.name = "oversized".into();
        large.tag += 1;
        large.size += 1;
        config.commands.push(large);
        let limits = Limits {
            max_tcp_frame_size: if transport == "tcp" { maximum } else { 65536 },
            max_mqtt_packet_size: if mqtt && transport != "mqtt5-client" {
                maximum
            } else {
                65536
            },
            max_retained_message_bytes: if mqtt && transport != "mqtt5-client" {
                maximum
            } else {
                65536
            },
            max_will_payload_bytes: if mqtt && transport != "mqtt5-client" {
                maximum
            } else {
                65536
            },
            max_write_buffer_per_connection: if transport == "write" { maximum } else { 65536 },
            max_command_bytes: if transport == "global" || transport == "write" {
                maximum
            } else {
                16384
            },
            ..Default::default()
        };
        limits.validate().unwrap();
        let (ingress, services, _, stop) =
            fixture_with_vendor(None, None, Some(limits), Some(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let owner = tokio::spawn(serve_device_ingress(
            listener,
            services.clone(),
            None,
            stop.clone(),
        ));
        let mut client = if mqtt {
            mqtt_connect_with_maximum(
                address,
                &auth,
                v5,
                (transport == "mqtt5-client").then_some(maximum as u32),
            )
            .await
        } else {
            let mut stream = TcpStream::connect(address).await.unwrap();
            let hello = serde_json::to_vec(
                &serde_json::json!({"credential_id":"acme-tcp","secret":"07".repeat(32)}),
            )
            .unwrap();
            stream.write_all(&frame(&hello)).await.unwrap();
            assert_eq!(
                read_tcp(&mut stream, tcp_frame::FrameKind::Payload).await,
                br#"{"authenticated":true}"#
            );
            stream
        };
        let mut cmd = command(&auth);
        cmd.payload.name = "oversized".into();
        // CommandService must also roll back its dedup reservation on rejection.
        assert!(
            matches!(
                services.commands.send(cmd.clone()).await,
                Err(Error::Invalid)
            ),
            "{transport}"
        );
        assert_eq!(services.commands.usage().unwrap(), (0, 0));
        assert_eq!(ingress.metrics.get(Metric::CommandQueued), 0);
        assert!(
            !ingress
                .sessions
                .lookup(&auth.device_key)
                .unwrap()
                .unwrap()
                .cancel
                .is_cancelled()
        );
        if mqtt {
            client.write_all(&[0xc0, 0]).await.unwrap();
            assert_eq!(read_mqtt(&mut client).await.0, 0xd0);
        } else {
            client.write_all(&frame(&payload("acme"))).await.unwrap();
            assert!(
                tcp_ack::decode(&read_tcp(&mut client, tcp_frame::FrameKind::EventAccepted).await)
                    .is_ok()
            );
        }
        cmd.payload.name = "set_temperature".into();
        services.router.send(cmd).unwrap();
        if mqtt {
            let (header, body) = read_mqtt(&mut client).await;
            assert_eq!(header & 0xf0, 0x30);
            assert_eq!(body.len(), overhead + capacity);
            let offset = 2 + topic(&auth.device_key, TopicKind::Down).len();
            client
                .write_all(&[0x40, 2, body[offset], body[offset + 1]])
                .await
                .unwrap();
            client.write_all(&[0xe0, 0]).await.unwrap();
        } else {
            assert_eq!(
                read_tcp(&mut client, tcp_frame::FrameKind::Payload)
                    .await
                    .len(),
                capacity
            );
        }
        stop.cancel();
        owner.await.unwrap().unwrap();
        ingress.events.stop_workers().await.unwrap();
    }
}
