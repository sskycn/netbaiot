#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::packet::{decode, Packet};
fuzz_target!(|data: &[u8]| {
    if data.len() > 65540 { return; }
    let l = netbaiot_runtime::Limits::default();
    let mut input = bytes::BytesMut::new();
    for part in data.chunks(7) {
        if input.len() + part.len() > l.max_mqtt_packet_size { break; }
        input.extend_from_slice(part);
        loop {
            let before = input.len();
            match decode(&mut input, &l) {
                Ok(Some(packet)) => {
                    assert!(input.len() < before);
                    match packet {
                        Packet::Connect(c) => {
                            assert!(c.client_id.len() <= l.max_client_id_bytes);
                            assert!(c.username.len() <= l.max_username_bytes);
                            assert!(c.password.len() <= l.max_password_bytes);
                        }
                        Packet::Publish { topic, payload, .. } => {
                            assert!(topic.len() <= l.max_topic_bytes);
                            assert!(payload.len() <= l.max_mqtt_packet_size);
                        }
                        Packet::Subscribe { filters, .. } => assert!(filters.len() <= l.max_subscription_filters_per_packet),
                        Packet::Unsubscribe { filters, .. } => assert!(filters.len() <= l.max_subscription_filters_per_packet),
                        _ => {}
                    }
                }
                Ok(None) => { assert_eq!(input.len(), before); break; }
                Err(_) => return,
            }
        }
    }
});
