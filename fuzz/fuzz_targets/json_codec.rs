#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len()>65540 { return; }
    use netbaiot_core::*;
    let key=DeviceKey{tenant_id:TenantId::new("t").unwrap(),product_id:ProductId::new("p").unwrap(),device_id:DeviceId::new("d").unwrap()};
    let codec=netbaiot_codecs::JsonV1::default();
    if let Ok(messages)=codec.decode(&DecodeContext{device:&key,received_at:0},data) { assert_eq!(messages.len(), 1); assert_eq!(messages[0].device, key); if let DevicePayload::Telemetry(fields) = &messages[0].payload { assert!(fields.len() <= 64); } }
});
