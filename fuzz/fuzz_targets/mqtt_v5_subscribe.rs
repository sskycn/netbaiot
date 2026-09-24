#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::codec::{common::encode, v5::decode};

fuzz_target!(|data: &[u8]| {
    let limits = netbaiot_runtime::Limits::default();
    if data.is_empty() || data.len() > limits.max_topic_bytes { return; }
    let mut body = vec![0, 1, 0];
    body.extend_from_slice(&((data.len() - 1) as u16).to_be_bytes());
    body.extend_from_slice(&data[..data.len() - 1]);
    body.push(data[data.len() - 1]);
    if let Ok(wire) = encode(0x82, &body, limits.max_mqtt_packet_size) {
        let _ = decode(&mut bytes::BytesMut::from(wire.as_slice()), &limits);
    }
});
