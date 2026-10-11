#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_core::*;
fuzz_target!(|data: &[u8]| {
    if data.len() > 65536 { return; }
    let key = DeviceKey{tenant_id:TenantId::new("t").unwrap(),product_id:ProductId::new("p").unwrap(),device_id:DeviceId::new("d").unwrap()};
    let codec = netbaiot_codecs::MsgpackV1::default();
    let ctx = DecodeContext{device:&key,received_at:0};
    let decoded=codec.decode(&ctx,data);
    assert_eq!(decoded.is_ok(),codec.validate_payload(&ctx,data).is_ok());
    if let Ok(events)=decoded { assert_eq!(events.len(),1); assert_eq!(events[0].device,key); if let DeviceEventKind::Telemetry(fields)=&events[0].kind{assert!(fields.len()<=64);} }
});
