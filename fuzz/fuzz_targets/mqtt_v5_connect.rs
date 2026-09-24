#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::codec::{common::encode, v5::decode};

fuzz_target!(|data: &[u8]| {
    let limits = netbaiot_runtime::Limits::default();
    if data.len() > limits.max_mqtt_property_bytes { return; }
    let mut body = b"\0\x04MQTT\x05\xc2\0\x1e".to_vec();
    let mut length = data.len();
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 { byte |= 0x80; }
        body.push(byte);
        if length == 0 { break; }
    }
    body.extend_from_slice(data);
    body.extend_from_slice(b"\0\x01a\0\x01a\0\x01b");
    if let Ok(wire) = encode(0x10, &body, limits.max_mqtt_packet_size) {
        let _ = decode(&mut bytes::BytesMut::from(wire.as_slice()), &limits);
    }
});
