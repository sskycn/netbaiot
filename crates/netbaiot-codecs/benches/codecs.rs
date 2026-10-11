use netbaiot_core::*;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, hint::black_box, time::Instant};
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
fn measure(format: &str, fields: usize, operation: &str, bytes: usize, mut f: impl FnMut()) {
    for _ in 0..1000 {
        f();
    }
    for run in 1..=3 {
        let region = Region::new(ALLOCATOR);
        let start = Instant::now();
        for _ in 0..20000 {
            f();
        }
        let ns = start.elapsed().as_nanos() as f64 / 20000.;
        let stats = region.change();
        println!(
            "{format},{fields},{operation},{run},{bytes},{ns:.1},{:.2},{:.1}",
            stats.allocations as f64 / 20000.,
            stats.bytes_allocated as f64 / 20000.
        );
    }
}
fn main() {
    let key = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext {
        device: &key,
        received_at: 1000,
    };
    println!(
        "codec,fields,operation,run,wire_bytes,mean_ns,allocations_per_op,allocated_bytes_per_op"
    );
    for count in [2, 16, 64] {
        let command = DeviceCommand {
            command_id: CommandId(uuid::Uuid::from_u128(1)),
            device: key.clone(),
            expires_at: Some(1000),
            payload: DeviceCommandPayload {
                name: "set".into(),
                arguments: (0..count)
                    .map(|i| (format!("field{i}"), Scalar::Number(i as f64 + 0.5)))
                    .collect(),
            },
        };
        for (id, _, codec) in netbaiot_codecs::builtins(CodecLimits::default()).unwrap() {
            let format = id.as_str().strip_prefix("netbaiot-").unwrap();
            let p = std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join(format!("tests/fixtures/bench{count}.{format}")),
            )
            .unwrap();
            codec.decode(&ctx, &p).unwrap();
            measure(format, count, "decode", p.len(), || {
                black_box(codec.decode(black_box(&ctx), black_box(&p)).unwrap());
            });
            let ectx = EncodeContext { device: &key };
            let size = codec.encode(&ectx, &command).unwrap().len();
            measure(format, count, "encode", size, || {
                black_box(codec.encode(black_box(&ectx), black_box(&command)).unwrap());
            });
        }
    }
}
