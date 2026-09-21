#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_runtime::Limits;
use netbaiot_transports::mqtt::broker::{MqttBroker, MqttRecoverySnapshot};
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    // The recovery file reader rejects images above its configured bound before allocation. Keep
    // this decoder-focused target small enough for high iteration throughput.
    if data.len() > 1_048_576 {
        return;
    }
    if let Ok(snapshot) = serde_json::from_slice::<MqttRecoverySnapshot>(data) {
        let broker = MqttBroker::new(Arc::new(Limits::default()));
        let _ = broker.restore(snapshot);
    }
});
