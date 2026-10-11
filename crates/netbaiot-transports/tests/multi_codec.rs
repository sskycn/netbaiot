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
fn identity(format: &str, device: &str) -> AuthenticatedDevice {
    AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new(format).unwrap(),
            device_id: DeviceId::new(device).unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new(format!("netbaiot-{format}")).unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    }
}
fn payload(format: &str) -> Vec<u8> {
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
    let l = Arc::new(Limits {
        requests_per_second: 10000,
        messages_per_device_second: 10000,
        messages_per_tenant_second: 10000,
        ..Limits::default()
    });
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
            sink.clone(),
            &l,
        )],
        routes.clone(),
        1,
    )
    .unwrap();
    let codecs =
        CodecRegistry::new(netbaiot_codecs::builtins(CodecLimits::default()).unwrap()).unwrap();
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
    let mut s = TcpStream::connect(address).await.unwrap();
    let mut body = Vec::new();
    mqtt_string("MQTT", &mut body);
    body.extend_from_slice(&[if v5 { 5 } else { 4 }, 0xc2, 0, 30]);
    if v5 {
        body.push(0);
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
async fn read_tcp(s: &mut TcpStream) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let n = s.read_u32().await.unwrap() as usize;
        assert!(n <= 65536);
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
    let id = format!("{f}-udp");
    let mut v = b"NBI1".to_vec();
    v.push(id.len() as u8);
    v.extend_from_slice(id.as_bytes());
    v.extend_from_slice(&1u32.to_be_bytes());
    v.extend_from_slice(&[3; 16]);
    v.extend_from_slice(&1u64.to_be_bytes());
    v.extend_from_slice(&now_ms().to_be_bytes());
    v.extend_from_slice(&(p.len() as u16).to_be_bytes());
    v.extend_from_slice(p);
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&v);
    v.extend_from_slice(&mac.finalize().into_bytes());
    v
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
        assert_eq!(read_tcp(&mut client).await, br#"{"authenticated":true}"#);
        // Split binary payload, then two coalesced frames.
        for bytes in frame(&p).chunks(3) {
            client.write_all(bytes).await.unwrap();
        }
        let _: EventAcceptance = serde_json::from_slice(&read_tcp(&mut client).await).unwrap();
        let mut frames = frame(&p);
        frames.extend_from_slice(&frame(&p));
        client.write_all(&frames).await.unwrap();
        for _ in 0..2 {
            let _: EventAcceptance = serde_json::from_slice(&read_tcp(&mut client).await).unwrap();
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
        assert_eq!(read_tcp(&mut client).await, expected);
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
        while sink.0.lock().unwrap().len() != 28 {
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
