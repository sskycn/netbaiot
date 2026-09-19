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
}
