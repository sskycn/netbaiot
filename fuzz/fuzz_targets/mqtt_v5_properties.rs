#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_transports::mqtt::codec::v5::{decode_properties, PropertyContext};

fuzz_target!(|data: &[u8]| {
    let limits = netbaiot_runtime::Limits::default();
    for context in [PropertyContext::Connect, PropertyContext::Will, PropertyContext::Publish,
        PropertyContext::Subscribe, PropertyContext::Unsubscribe, PropertyContext::Disconnect,
        PropertyContext::Ack] {
        if let Ok(properties) = decode_properties(data, context, &limits) {
            assert!(properties.user_properties.len() <= limits.max_mqtt_user_properties);
            assert!(properties.retained_bytes() <= limits.max_mqtt_property_bytes);
        }
    }
});
