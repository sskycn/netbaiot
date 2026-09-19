use bytes::BytesMut;
use netbaiot_codecs::JsonV1;
use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_transports::{
    mqtt::{
        packet,
        topics::{Subscriptions, TopicKind, publish_acl, topic},
    },
    tcp::{LengthPrefixFramer, TcpFramer},
};
use std::{hint::black_box, sync::Arc, time::Instant};
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
        codec_id: CodecId::new("netbaiot-json").unwrap(),
        codec_version: 1,
        permissions: Permissions {
            publish: true,
            commands: true,
        },
    };
    let up = topic(&auth.device_key, TopicKind::Up);
    let down = topic(&auth.device_key, TopicKind::Down);
    let payload=br#"{"schema_version":1,"source_message_id":"boot:1","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}"#;
    let wire = packet::publish(&up, payload, Some(1), &l).unwrap();
    measure("mqtt_decode", 20000, || {
        black_box(packet::decode(&mut BytesMut::from(wire.as_slice()), &l).unwrap());
    });
    measure("mqtt_encode", 20000, || {
        black_box(packet::publish(&up, payload, Some(1), &l).unwrap());
    });
    measure("topic_acl", 20000, || {
        black_box(publish_acl(&auth, &up).unwrap());
    });
    let subscriptions = Subscriptions::new(l.clone());
    subscriptions.subscribe(&auth, 1, &down, 1).unwrap();
    measure("subscription_lookup", 20000, || {
        black_box(subscriptions.lookup(&down, 1).unwrap());
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
        let wire = packet::publish(&up, &payload, Some(1), &Limits::default()).unwrap();
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
        let subscriptions = Subscriptions::new(limits.clone());
        let bookkeeping = Admission::new(limits.clone());
        let mut owners = Vec::new();
        let mut selected = auth.clone();
        for i in 0..size {
            let mut identity = auth.clone();
            identity.device_key.tenant_id = TenantId::new(format!("t{}", i / 32)).unwrap();
            identity.device_key.device_id = DeviceId::new(format!("d{i}")).unwrap();
            let pair = sessions
                .register(&identity.device_key, Transport::Mqtt)
                .unwrap();
            subscriptions
                .subscribe(
                    &identity,
                    pair.0.generation,
                    &topic(&identity.device_key, TopicKind::Down),
                    1,
                )
                .unwrap();
            black_box(bookkeeping.acquire(&identity.device_key, 1).unwrap());
            owners.push(pair);
            selected = identity;
        }
        let endpoint = sessions.lookup(&selected.device_key).unwrap().unwrap();
        let down = topic(&selected.device_key, TopicKind::Down);
        measure(&format!("session_lookup_{size}"), 10000, || {
            black_box(sessions.lookup(&selected.device_key).unwrap());
        });
        measure(&format!("subscription_lookup_{size}"), 10000, || {
            black_box(subscriptions.lookup(&down, endpoint.generation).unwrap());
        });
        measure(&format!("topic_acl_{size}"), 10000, || {
            black_box(publish_acl(&selected, &topic(&selected.device_key, TopicKind::Up)).unwrap());
        });
        measure(&format!("ingress_bookkeeping_{size}"), 10000, || {
            black_box(bookkeeping.acquire(&selected.device_key, 128).unwrap());
        });
    }
    println!(
        "layout_bytes: session_endpoint={} queued_command={} device_message={} command_record={}",
        std::mem::size_of::<SessionEndpoint>(),
        std::mem::size_of::<QueuedCommand>(),
        std::mem::size_of::<DeviceMessage>(),
        std::mem::size_of::<CommandRecord>()
    );
}
