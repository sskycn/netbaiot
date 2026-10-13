//! Isolated experiment for the V3 event serialization expression. This compares
//! owned and borrowed DTOs; it does not establish network throughput or RSS.
use netbaiot_core::*;
use serde::Serialize;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, hint::black_box, time::Instant};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

// Preserve the public EventDelivery field order and wire representation while
// borrowing the event that the delivery owner already retains.
#[derive(Serialize)]
struct BorrowedEventDelivery<'a> {
    delivery_id: DeliveryId,
    subscription_id: SubscriptionId,
    event: &'a DeviceEvent,
    attempt: u32,
}

fn serialize(event: &DeviceEvent, borrowed: bool) -> Vec<u8> {
    let delivery_id = DeliveryId(uuid::Uuid::from_u128(1));
    let subscription_id = SubscriptionId(uuid::Uuid::from_u128(2));
    if borrowed {
        serde_json::to_vec(&BorrowedEventDelivery {
            delivery_id,
            subscription_id,
            event,
            attempt: 7,
        })
        .unwrap()
    } else {
        // Exact expression used by the current V3 server delivery path.
        serde_json::to_vec(&EventDelivery {
            delivery_id,
            subscription_id,
            event: event.clone(),
            attempt: 7,
        })
        .unwrap()
    }
}

fn verify_wire(event: &DeviceEvent) {
    let owned = serialize(event, false);
    let borrowed = serialize(event, true);
    assert_eq!(
        owned, borrowed,
        "private DTO must preserve exact JSON bytes"
    );
    let decoded: EventDelivery = serde_json::from_slice(&borrowed).unwrap();
    assert_eq!(decoded.event, *event);
    assert_eq!(decoded.delivery_id.0, uuid::Uuid::from_u128(1));
    assert_eq!(decoded.subscription_id.0, uuid::Uuid::from_u128(2));
    assert_eq!(decoded.attempt, 7);
}

fn measure(case: &str, event: &DeviceEvent, borrowed: bool, run: usize) {
    const SAMPLES: usize = 512;
    const BATCH: usize = 8;
    let variant = if borrowed { "borrowed" } else { "owned" };
    let wire_bytes = serialize(event, borrowed).len();
    for _ in 0..256 {
        black_box(serialize(black_box(event), borrowed));
    }
    // Sample storage, input construction and warmup are outside allocation regions.
    let mut samples = Vec::with_capacity(SAMPLES);
    let (mut allocations, mut reallocations, mut allocated_bytes) = (0usize, 0usize, 0usize);
    for _ in 0..SAMPLES {
        let region = Region::new(ALLOCATOR);
        let began = Instant::now();
        for _ in 0..BATCH {
            black_box(serialize(black_box(event), borrowed));
        }
        let elapsed = began.elapsed().as_nanos() / BATCH as u128;
        let stats = region.change();
        allocations += stats.allocations;
        reallocations += stats.reallocations;
        allocated_bytes += stats.bytes_allocated;
        samples.push(elapsed);
    }
    let mean = samples.iter().sum::<u128>() as f64 / SAMPLES as f64;
    samples.sort_unstable();
    let operations = (SAMPLES * BATCH) as f64;
    println!(
        "BUSINESS_EVENT_SERIALIZE,{case},{variant},{run},{wire_bytes},{mean:.3},{},{},{},{:.3},{:.3},{:.3}",
        samples[SAMPLES / 2],
        samples[SAMPLES * 95 / 100],
        samples[SAMPLES * 99 / 100],
        allocations as f64 / operations,
        reallocations as f64 / operations,
        allocated_bytes as f64 / operations,
    );
}

fn main() {
    assert!(!cfg!(debug_assertions), "release measurements only");
    let device = DeviceKey {
        tenant_id: TenantId::new("tenant").unwrap(),
        product_id: ProductId::new("product").unwrap(),
        device_id: DeviceId::new("device").unwrap(),
    };
    let kinds = [
        (
            "telemetry2",
            DeviceEventKind::Telemetry(
                (0..2)
                    .map(|index| (format!("field{index}"), Scalar::Number(index as f64 + 0.5)))
                    .collect(),
            ),
        ),
        (
            "telemetry16",
            DeviceEventKind::Telemetry(
                (0..16)
                    .map(|index| (format!("field{index}"), Scalar::Number(index as f64 + 0.5)))
                    .collect(),
            ),
        ),
        (
            "telemetry64_text",
            DeviceEventKind::Telemetry(
                (0..64)
                    .map(|index| (format!("field{index}"), Scalar::Text("x".repeat(128))))
                    .collect(),
            ),
        ),
        (
            "event",
            DeviceEventKind::DeviceEvent(DeviceEventPayload {
                name: "boot".into(),
                value: Some(Scalar::Text("quoted \" text\\\n设备".into())),
            }),
        ),
        (
            "heartbeat",
            DeviceEventKind::Heartbeat(Heartbeat { sequence: u64::MAX }),
        ),
        (
            "command_ack",
            DeviceEventKind::CommandAck(CommandAck {
                command_id: CommandId(uuid::Uuid::from_u128(3)),
                execution: ExecutionState::Succeeded,
            }),
        ),
    ];
    println!(
        "marker,case,variant,run,wire_bytes,mean_ns,p50_ns,p95_ns,p99_ns,allocations_per_op,reallocations_per_op,allocated_bytes_per_op"
    );
    for (case, kind) in kinds {
        let event = DeviceEvent {
            event_id: EventId(uuid::Uuid::from_u128(4)),
            source_message_id: SourceMessageId::new("bench:1").unwrap(),
            device: device.clone(),
            received_at: 1_000,
            occurred_at: Some(999),
            kind,
        };
        verify_wire(&event);
        let mut without_occurred = event.clone();
        without_occurred.occurred_at = None;
        verify_wire(&without_occurred);
        // Five paired repeats, alternating order to expose drift and warmup bias.
        for run in 1..=5 {
            for borrowed in if run % 2 == 1 {
                [false, true]
            } else {
                [true, false]
            } {
                measure(case, &event, borrowed, run);
            }
        }
    }
}
