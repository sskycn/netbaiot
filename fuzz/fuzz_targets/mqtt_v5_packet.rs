#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::codec::v5::{decode, Packet};

fuzz_target!(|data: &[u8]| {
    let limits = netbaiot_runtime::Limits::default();
    if data.len() > limits.max_mqtt_packet_size { return; }
    let mut input = bytes::BytesMut::from(data);
    let before = input.len();
    if let Ok(Some(packet)) = decode(&mut input, &limits) {
        assert!(input.len() < before);
        match packet {
            Packet::Connect(connect) => {
                assert!(connect.client_id.len() <= limits.max_client_id_bytes);
                assert!(connect.password.as_ref().is_none_or(|value| value.len() <= limits.max_password_bytes));
            }
            Packet::Publish { topic, payload, .. } => {
                assert!(topic.len() <= limits.max_topic_bytes);
                assert!(payload.len() <= limits.max_mqtt_packet_size);
            }
            Packet::Subscribe { filters, .. } => assert!(filters.len() <= limits.max_subscription_filters_per_packet),
            Packet::Unsubscribe { filters, .. } => assert!(filters.len() <= limits.max_subscription_filters_per_packet),
            _ => {}
        }
    }
});
