#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|data: &[u8]| {
    if data.len()>65540 { return; }
    let l=netbaiot_runtime::Limits::default();
    let mut input=bytes::BytesMut::new();
    for part in data.chunks(7) {
        if input.len()+part.len()>l.max_mqtt_packet_size { break; }
        input.extend_from_slice(part);
        loop { match netbaiot_transports::mqtt::packet::decode(&mut input,&l) { Ok(Some(_)) => {}, Ok(None) => break, Err(_) => return } }
    }
});
