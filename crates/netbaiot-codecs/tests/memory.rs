use netbaiot_core::*;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::alloc::System;
#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;
#[test]
fn hostile_length_declarations_fail_before_heap_allocation() {
    let key = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let ctx = DecodeContext {
        device: &key,
        received_at: 0,
    };
    let cases: Vec<(Box<dyn DeviceCodec>, Vec<u8>)> = vec![
        (
            Box::new(netbaiot_codecs::CborV1::default()),
            vec![0xbb, 255, 255, 255, 255, 255, 255, 255, 255],
        ),
        (
            Box::new(netbaiot_codecs::MsgpackV1::default()),
            vec![0xdf, 255, 255, 255, 255],
        ),
        (
            Box::new(netbaiot_codecs::ProtobufV1::default()),
            vec![0x12, 255, 255, 255, 255, 15],
        ),
    ];
    for (codec, payload) in cases {
        let region = Region::new(ALLOCATOR);
        assert!(codec.decode(&ctx, &payload).is_err());
        let stats = region.change();
        assert_eq!(stats.allocations, 0);
        assert_eq!(stats.bytes_allocated, 0);
    }
}
