//! Fixed native/declarative vectors; compilation is outside hot-path measurements.
use netbaiot_codecs::vendor::{declarative::DeclarativeCodec, demo::AcmeV1};
use netbaiot_core::*;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{alloc::System, hint::black_box, time::Instant};
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
fn measure(name: &str, operation: &str, mut f: impl FnMut()) {
    for _ in 0..1000 {
        f();
    }
    for round in 1..=3 {
        let region = Region::new(ALLOCATOR);
        let started = Instant::now();
        for _ in 0..20000 {
            f();
        }
        let elapsed = started.elapsed().as_nanos() as f64 / 20000.;
        let stats = region.change();
        println!(
            "{name},{operation},{round},{elapsed:.1},{:.2},{:.1}",
            stats.allocations as f64 / 20000.,
            stats.bytes_allocated as f64 / 20000.
        );
    }
}
fn main() {
    let limits = CodecLimits::default();
    let config = include_bytes!("../../../configs/codecs/acme-v1.json");
    let declarative = DeclarativeCodec::from_json(config, limits.clone()).unwrap();
    let native = AcmeV1::new(limits).unwrap();
    let device = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext {
        device: &device,
        received_at: 1234,
    };
    let mut payload = vec![
        165, 90, 0, 19, 1, 42, 0, 0, 0, 0x85, 0xff, 0x02, 0x8e, 1, b'A', b'1', b'0', b'0',
    ];
    payload.push(payload.iter().fold(0u8, |s, b| s.wrapping_add(*b)));
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: device.clone(),
        expires_at: Some(1700000000000),
        payload: DeviceCommandPayload {
            name: "set_temperature".into(),
            arguments: [("temperature".into(), Scalar::Number(12.3))].into(),
        },
    };
    println!("codec,operation,round,mean_ns,allocations_per_op,allocated_bytes_per_op");
    for (name, codec) in [
        ("native", &native as &dyn DeviceCodec),
        ("declarative", &declarative),
    ] {
        measure(name, "decode", || {
            black_box(codec.decode(black_box(&ctx), black_box(&payload)).unwrap());
        });
        measure(name, "prevalidate", || {
            black_box(
                codec
                    .validate_payload(black_box(&ctx), black_box(&payload))
                    .unwrap(),
            );
        });
        measure(name, "encode", || {
            black_box(
                codec
                    .encode(&EncodeContext { device: &device }, black_box(&command))
                    .unwrap(),
            );
        });
        measure(name, "reject", || {
            assert!(black_box(codec.decode(&ctx, black_box(&payload[..18]))).is_err());
        });
    }
    measure("declarative", "compile_json", || {
        black_box(DeclarativeCodec::from_json(black_box(config), CodecLimits::default()).unwrap());
    });
}
