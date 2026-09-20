use async_trait::async_trait;
use bytes::BytesMut;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    mqtt::{
        broker::{BrokerMessage, MqttBroker},
        packet,
        topics::{TopicKind, publish_acl, topic},
    },
    tcp::{LengthPrefixFramer, TcpFramer},
};
use std::{hint::black_box, sync::Arc, time::Instant};

struct BenchAuthProvider {
    auth: AuthenticatedDevice,
}

#[async_trait]
impl DeviceAuthenticator for BenchAuthProvider {
    async fn authenticate(
        &self,
        _: AuthenticationRequest<'_>,
    ) -> netbaiot_runtime::Result<AuthenticatedDevice> {
        Ok(self.auth.clone())
    }
}

fn measure(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..1000 {
        f();
    }
    let start = Instant::now();
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        f();
        samples.push(t.elapsed().as_nanos());
    }
    let elapsed = start.elapsed();
    samples.sort_unstable();
    println!(
        "{name}: {iterations} iterations, {:.0} ops/s, p50={} ns p95={} ns p99={} ns",
        iterations as f64 / elapsed.as_secs_f64(),
        samples[iterations / 2],
        samples[iterations * 95 / 100],
        samples[iterations * 99 / 100]
    );
}
fn main() {
    let l = Arc::new(Limits {
        requests_per_second: 1_000_000,
        messages_per_device_second: 1_000_000,
        messages_per_tenant_second: 1_000_000,
        ..Limits::default()
    });
    let auth = AuthenticatedDevice {
        device_key: DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let auth_limits = Arc::new(Limits::default());
    let auth_metrics = Arc::new(Metrics::default());
    let hit_cache = AuthCache::new(
        Arc::new(BenchAuthProvider { auth: auth.clone() }),
        auth_limits.clone(),
        auth_metrics.clone(),
    );
    runtime
        .block_on(hit_cache.authenticate(AuthenticationRequest::Secret {
            credential_id: "bench",
            secret: b"bench-secret",
        }))
        .unwrap();
    measure("auth_cache_hit", 20_000, || {
        black_box(
            runtime
                .block_on(hit_cache.authenticate(AuthenticationRequest::Secret {
                    credential_id: "bench",
                    secret: b"bench-secret",
                }))
                .unwrap(),
        );
    });
    let miss_cache = AuthCache::new(
        Arc::new(BenchAuthProvider { auth: auth.clone() }),
        auth_limits.clone(),
        auth_metrics,
    );
    let mut miss_sequence = 0u64;
    measure("auth_cache_miss_local_provider", 2_000, || {
        miss_sequence += 1;
        let secret = miss_sequence.to_be_bytes();
        black_box(
            runtime
                .block_on(miss_cache.authenticate(AuthenticationRequest::Secret {
                    credential_id: "bench",
                    secret: &secret,
                }))
                .unwrap(),
        );
    });
    let config_cache = ConfigCache::empty(auth_limits);
    config_cache
        .apply(ControlSnapshot {
            revision: 1,
            products: vec![ProductRuntimeConfig {
                tenant_id: auth.device_key.tenant_id.clone(),
                product_id: auth.device_key.product_id.clone(),
                codec_id: auth.codec_id.clone(),
                codec_version: auth.codec_version,
                revision: 1,
            }],
            devices: vec![DeviceConfigSnapshot {
                device: auth.device_key.clone(),
                revision: ConfigRevision::new(1).unwrap(),
                payload: Arc::new(serde_json::json!({"sample_interval_seconds": 30})),
            }],
            routes: Vec::new(),
        })
        .unwrap();
    measure("config_cache_hit", 20_000, || {
        black_box(config_cache.device(&auth.device_key).unwrap());
    });
    let missing_device = DeviceKey {
        tenant_id: auth.device_key.tenant_id.clone(),
        product_id: auth.device_key.product_id.clone(),
        device_id: DeviceId::new("missing").unwrap(),
    };
    measure("config_cache_miss", 20_000, || {
        black_box(config_cache.device(&missing_device).unwrap());
    });
    let up = topic(&auth.device_key, TopicKind::Up);
    let down = topic(&auth.device_key, TopicKind::Down);
    let payload=br#"{"schema_version":1,"source_message_id":"boot:1","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}"#;
    let wire = packet::publish(&up, payload, 1, Some(1), false, false, &l).unwrap();
    measure("mqtt_decode", 20000, || {
        black_box(packet::decode(&mut BytesMut::from(wire.as_slice()), &l).unwrap());
    });
    measure("mqtt_encode", 20000, || {
        black_box(packet::publish(&up, payload, 1, Some(1), false, false, &l).unwrap());
    });
    measure("topic_acl", 20000, || {
        black_box(publish_acl(&auth, &up).unwrap());
    });
    let subscriptions = MqttBroker::new(l.clone());
    let attachment = subscriptions.attach(&auth, "bench".into(), false).unwrap();
    subscriptions
        .subscribe(&attachment.key, attachment.generation, &down, 1)
        .unwrap();
    measure("subscription_lookup", 20000, || {
        black_box(
            subscriptions
                .subscription_qos(&attachment.key, &down)
                .unwrap(),
        );
    });
    let codec = JsonV1::default();
    let ctx = DecodeContext {
        device: &auth.device_key,
        received_at: 0,
    };
    measure("json_codec", 20000, || {
        black_box(codec.decode(&ctx, payload).unwrap());
    });
    let framer = LengthPrefixFramer { maximum: 65536 };
    let wire = framer.encode(payload).unwrap();
    measure("tcp_frame", 20000, || {
        black_box(framer.decode(&mut BytesMut::from(wire.as_slice())).unwrap());
    });
    let admission = Admission::new(l);
    measure("ingress_admission", 20000, || {
        black_box(admission.acquire(&auth.device_key, payload.len()).unwrap());
    });
    // Scale-sensitive paths: fixed protocol bytes, valid codec fields, registry size.
    for (name, size) in [("small", 16), ("medium", 4096), ("maximum", 65504)] {
        let payload = vec![b'x'; size];
        let wire =
            packet::publish(&up, &payload, 1, Some(1), false, false, &Limits::default()).unwrap();
        measure(&format!("mqtt_decode_{name}"), 10000, || {
            black_box(
                packet::decode(&mut BytesMut::from(wire.as_slice()), &Limits::default()).unwrap(),
            );
        });
    }
    for (name, fields, text_bytes) in [("small", 1, 16), ("medium", 32, 128), ("maximum", 64, 256)]
    {
        let fields = (0..fields)
            .map(|i| format!("\"f{i}\":\"{}\"", "x".repeat(text_bytes)))
            .collect::<Vec<_>>()
            .join(",");
        let mut wire = format!(r#"{{"schema_version":1,"source_message_id":"bench","kind":"telemetry","data":{{{fields}}}}}"#).into_bytes();
        if name == "maximum" {
            wire.resize(65536, b' ');
        }
        measure(&format!("json_codec_{name}"), 10000, || {
            black_box(codec.decode(&ctx, &wire).unwrap());
        });
    }
    let limits = Arc::new(Limits {
        requests_per_second: 1_000_000,
        messages_per_device_second: 1_000_000,
        messages_per_tenant_second: 1_000_000,
        ..Limits::default()
    });
    for size in [1, 64, 256] {
        let sessions = Sessions::new(limits.clone());
        let subscriptions = MqttBroker::new(limits.clone());
        let bookkeeping = Admission::new(limits.clone());
        let mut owners = Vec::new();
        let mut selected = auth.clone();
        for i in 0..size {
            let mut identity = auth.clone();
            identity.device_key.tenant_id = TenantId::new(format!("t{}", i / 32)).unwrap();
            identity.device_key.device_id = DeviceId::new(format!("d{i}")).unwrap();
            let pair = sessions
                .register(Arc::new(identity.clone()), Transport::Mqtt)
                .unwrap();
            let mqtt = subscriptions
                .attach(&identity, format!("bench-{i}"), false)
                .unwrap();
            subscriptions
                .subscribe(
                    &mqtt.key,
                    mqtt.generation,
                    &topic(&identity.device_key, TopicKind::Down),
                    1,
                )
                .unwrap();
            black_box(bookkeeping.acquire(&identity.device_key, 1).unwrap());
            owners.push((pair, mqtt));
            selected = identity;
        }
        let selected_key = &owners.last().unwrap().1.key;
        let down = topic(&selected.device_key, TopicKind::Down);
        measure(&format!("session_lookup_{size}"), 10000, || {
            black_box(sessions.lookup(&selected.device_key).unwrap());
        });
        measure(&format!("subscription_lookup_{size}"), 10000, || {
            black_box(subscriptions.subscription_qos(selected_key, &down).unwrap());
        });
        measure(&format!("topic_acl_{size}"), 10000, || {
            black_box(publish_acl(&selected, &topic(&selected.device_key, TopicKind::Up)).unwrap());
        });
        measure(&format!("ingress_bookkeeping_{size}"), 10000, || {
            black_box(bookkeeping.acquire(&selected.device_key, 128).unwrap());
        });
    }
    for entries in [10, 100, 1_000, 10_000] {
        let limits = Arc::new(Limits {
            max_devices: 10_001,
            max_devices_per_tenant: 128,
            requests_per_second: 1_000_000,
            messages_per_device_second: 1_000_000,
            messages_per_tenant_second: 1_000_000,
            ..Limits::default()
        });
        let admission = Admission::new(limits);
        let mut selected = auth.device_key.clone();
        for i in 0..entries {
            let device = DeviceKey {
                tenant_id: TenantId::new(format!("quota_t{i}")).unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new(format!("quota_d{i}")).unwrap(),
            };
            admission.check_rate(&device).unwrap();
            selected = device;
        }
        measure(&format!("admission_entries_{entries}"), 10_000, || {
            black_box(admission.check_rate(&selected).unwrap());
        });
    }
    for entries in [100, 1_000, 10_000] {
        let limits = Arc::new(Limits {
            max_persistent_sessions: 10_001,
            max_persistent_sessions_per_tenant: 10_001,
            max_subscriptions_per_session: 4,
            max_subscriptions_per_device: 4,
            max_subscriptions_per_tenant: 10_001,
            max_subscriptions: 10_001,
            global_mqtt_session_bytes: 536_870_912,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        let mut selected_topic = String::new();
        for i in 0..entries {
            let mut identity = auth.clone();
            identity.device_key.tenant_id = TenantId::new(format!("router-t{i}")).unwrap();
            identity.device_key.device_id = DeviceId::new(format!("router-d{i}")).unwrap();
            let attachment = broker
                .attach(&identity, format!("router-c{i}"), false)
                .unwrap();
            let prefix = format!("bench/router-t{i}");
            let filter = match i % 3 {
                0 => format!("{prefix}/value"),
                1 => format!("{prefix}/+"),
                _ => format!("{prefix}/#"),
            };
            broker
                .subscribe(&attachment.key, attachment.generation, &filter, 1)
                .unwrap();
            broker
                .detach(&attachment.key, attachment.generation, false)
                .unwrap();
            selected_topic = format!("{prefix}/value");
        }
        let (sessions, logical_bytes, _, _, _) = broker.usage().unwrap();
        println!(
            "disconnected_sessions_{entries}: logical_bytes={} bytes/session={:.1}",
            logical_bytes,
            logical_bytes as f64 / sessions as f64
        );
        measure(&format!("subscription_router_{entries}"), 20_000, || {
            black_box(broker.matching_subscription_count(&selected_topic).unwrap());
        });
    }
    for entries in [1_000, 4_000] {
        let limits = Arc::new(Limits {
            max_retained_messages: 4_096,
            max_retained_messages_per_tenant: 4_096,
            max_retained_bytes: 67_108_864,
            max_retained_bytes_per_tenant: 67_108_864,
            ..Limits::default()
        });
        let broker = MqttBroker::new(limits);
        for i in 0..entries {
            broker
                .route(
                    &auth.device_key,
                    BrokerMessage {
                        topic: format!("bench/retained/{i}"),
                        payload: vec![b'x'; 64],
                        qos: 1,
                        retain: true,
                    },
                )
                .unwrap();
        }
        let (_, _, _, retained, retained_bytes) = broker.usage().unwrap();
        println!(
            "retained_store_{entries}: logical_bytes={} bytes/message={:.1}",
            retained_bytes,
            retained_bytes as f64 / retained as f64
        );
        let exact = format!("bench/retained/{}", entries - 1);
        measure(&format!("retained_exact_{entries}"), 10_000, || {
            black_box(broker.has_retained_topic(&exact).unwrap());
        });
        measure(&format!("retained_wildcard_{entries}"), 1_000, || {
            black_box(broker.matching_retained_count("bench/retained/#").unwrap());
        });
    }
    println!(
        "layout_bytes: session_endpoint={} queued_command={} device_event={}",
        std::mem::size_of::<SessionEndpoint>(),
        std::mem::size_of::<QueuedCommand>(),
        std::mem::size_of::<DeviceEvent>()
    );
}
