use super::*;
use std::{collections::BTreeMap, hint::black_box};

// Isolated unit-test binary only; no allocator changes in the server runtime.
#[global_allocator]
static DELIVERY_ALLOCATOR: &stats_alloc::StatsAlloc<std::alloc::System> =
    &stats_alloc::INSTRUMENTED_SYSTEM;

fn event(kind: DeviceEventKind) -> DeviceEvent {
    DeviceEvent {
        event_id: EventId(uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap()),
        source_message_id: SourceMessageId::new("source:1").unwrap(),
        device: DeviceKey {
            tenant_id: TenantId::new("tenant").unwrap(),
            product_id: ProductId::new("product").unwrap(),
            device_id: DeviceId::new("device").unwrap(),
        },
        received_at: 1_700_000_000_001,
        occurred_at: None,
        kind,
    }
}

fn legacy_envelope(event: &DeviceEvent) -> serde_json::Value {
    serde_json::json!({
        "event_id": event.event_id, "source_message_id": event.source_message_id,
        "tenant_id": event.device.tenant_id, "product_id": event.device.product_id,
        "device_id": event.device.device_id, "event_type": event.kind.event_type(),
        "received_at": event.received_at, "occurred_at": event.occurred_at,
        "payload": event.kind,
    })
}

#[test]
fn borrowed_webhook_preserves_exact_json_values_and_nulls() {
    for kind in [
        DeviceEventKind::Heartbeat(Heartbeat { sequence: u64::MAX }),
        DeviceEventKind::DeviceEvent(DeviceEventPayload {
            name: "text \\\"中".into(),
            value: None,
        }),
        DeviceEventKind::Telemetry(BTreeMap::from([
            ("temperature".into(), Scalar::Number(23.5)),
            ("online".into(), Scalar::Boolean(true)),
            ("label".into(), Scalar::Text("a\nb".into())),
        ])),
        DeviceEventKind::CommandAck(CommandAck {
            command_id: CommandId(
                uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000002").unwrap(),
            ),
            execution: ExecutionState::Succeeded,
        }),
    ] {
        for occurred_at in [None, Some(1_699_999_999_999)] {
            let mut event = event(kind.clone());
            event.occurred_at = occurred_at;
            let bytes = serde_json::to_vec(&WebhookEnvelope::from(&event)).unwrap();
            let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(actual, legacy_envelope(&event));
            assert_eq!(actual.as_object().unwrap().len(), 9);
            assert_eq!(actual["event_id"], "00000000-0000-4000-8000-000000000001");
            assert_eq!(actual["source_message_id"], "source:1");
            assert_eq!(actual["tenant_id"], "tenant");
            assert_eq!(actual["product_id"], "product");
            assert_eq!(actual["device_id"], "device");
            assert_eq!(actual["received_at"], 1_700_000_000_001i64);
            assert_eq!(actual["occurred_at"], serde_json::json!(occurred_at));
            assert_eq!(actual["payload"], serde_json::to_value(&kind).unwrap());
            assert_eq!(
                actual["event_type"],
                serde_json::to_value(kind.event_type()).unwrap()
            );
        }
    }
}

#[test]
#[ignore = "serial release HTTP serializer allocation measurement"]
fn engineering_webhook_serialization_benchmark() {
    if cfg!(debug_assertions) {
        panic!("release measurements only");
    }
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for size in [32, 1024, 16384, 65536] {
        let event = event(DeviceEventKind::DeviceEvent(DeviceEventPayload {
            name: "x".repeat(size),
            value: None,
        }));
        for borrowed in [false, true] {
            for repeat in 1..=3 {
                let mut samples = Vec::with_capacity(512);
                let (mut allocations, mut bytes) = (0, 0);
                for _ in 0..512 {
                    let region = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
                    let start = std::time::Instant::now();
                    let request = if borrowed {
                        client
                            .post("http://127.0.0.1/events")
                            .json(&WebhookEnvelope::from(&event))
                            .build()
                            .unwrap()
                    } else {
                        client
                            .post("http://127.0.0.1/events")
                            .json(&legacy_envelope(&event))
                            .build()
                            .unwrap()
                    };
                    black_box(request.body().unwrap().as_bytes().unwrap());
                    let elapsed = start.elapsed().as_nanos();
                    let stats = region.change();
                    allocations += stats.allocations + stats.reallocations;
                    bytes += stats.bytes_allocated;
                    samples.push(elapsed);
                }
                samples.sort_unstable();
                println!(
                    "WEBHOOK,{borrowed},{size},{repeat},{},{},{},{:.3},{:.3}",
                    samples[256],
                    samples[486],
                    samples[506],
                    allocations as f64 / 512.,
                    bytes as f64 / 512.
                );
            }
        }
    }
}
