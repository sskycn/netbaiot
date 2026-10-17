//! Isolated allocation checks: compiled plans allocate only normalized owned output.
use netbaiot_codecs::vendor::{declarative::DeclarativeCodec, demo::AcmeV1};
use netbaiot_core::*;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
#[test]
fn compiled_codecs_have_bounded_output_and_allocation_free_structural_rejection() {
    let d = DeclarativeCodec::from_json(
        include_bytes!("../../../configs/codecs/acme-v1.json"),
        CodecLimits::default(),
    )
    .unwrap();
    let n = AcmeV1::new(CodecLimits::default()).unwrap();
    let key = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext {
        device: &key,
        received_at: 1,
    };
    let mut bytes = vec![
        165, 90, 0, 19, 1, 42, 0, 0, 0, 0x85, 0xff, 0x02, 0x8e, 1, b'A', b'1', b'0', b'0',
    ];
    bytes.push(bytes.iter().fold(0u8, |s, b| s.wrapping_add(*b)));
    let command = DeviceCommand {
        command_id: CommandId::generate(),
        device: key.clone(),
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set_temperature".into(),
            arguments: [("temperature".into(), Scalar::Number(25.3))].into(),
        },
    };
    for codec in [&d as &dyn DeviceCodec, &n] {
        codec.decode(&ctx, &bytes).unwrap(); // Warm UUID entropy initialization outside measurement.
        let region = Region::new(ALLOCATOR);
        let event = codec.decode(&ctx, &bytes).unwrap();
        let stats = region.change();
        assert!(stats.bytes_allocated <= 2048);
        // The two exact integer fields each own one bounded DecimalScalar allocation.
        assert!(stats.allocations <= 10);
        drop(event);
        let region = Region::new(ALLOCATOR);
        assert!(codec.decode(&ctx, &bytes[..18]).is_err());
        assert_eq!(region.change().allocations, 0);
        let region = Region::new(ALLOCATOR);
        let output = codec
            .encode(&EncodeContext { device: &key }, &command)
            .unwrap();
        let stats = region.change();
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.reallocations, 0);
        assert_eq!(stats.bytes_allocated, 32);
        assert_eq!(output.len(), 32);
    }
}
