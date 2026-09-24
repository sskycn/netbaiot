#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::codec::{common::encode, v5::decode};

fuzz_target!(|data: &[u8]| {
    let limits = netbaiot_runtime::Limits::default();
    if data.len() > limits.max_mqtt_property_bytes { return; }
    let topic = b"v1/t/t/p/p/d/d/up";
    let mut body = Vec::with_capacity(2 + topic.len() + 3 + data.len());
    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    body.extend_from_slice(topic);
    body.extend_from_slice(&1u16.to_be_bytes());
    let mut length = data.len();
    loop {
        let mut byte = (length % 128) as u8;
        length /= 128;
        if length > 0 { byte |= 0x80; }
        body.push(byte);
        if length == 0 { break; }
    }
    body.extend_from_slice(data);
    if let Ok(wire) = encode(0x32, &body, limits.max_mqtt_packet_size) {
        let _ = decode(&mut bytes::BytesMut::from(wire.as_slice()), &limits);
    }
});
