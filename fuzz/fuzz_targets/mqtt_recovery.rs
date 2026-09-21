#![no_main]
use libfuzzer_sys::fuzz_target;
use netbaiot_runtime::Limits;
use netbaiot_transports::mqtt::broker::{MqttBroker, decode_mqtt_recovery};
use sha2::{Digest, Sha256};
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    // The recovery file reader rejects images above its configured bound before allocation. Keep
    // this decoder-focused target small enough for high iteration throughput.
    if data.len() > 1_048_576 {
        return;
    }
    let limits = Arc::new(Limits::default());
    if let Ok(snapshot) = decode_mqtt_recovery(data, &limits) {
        let broker = MqttBroker::new(limits);
        let _ = broker.restore(snapshot);
    }
    // Also guarantee that mutations reach the v2 record parser instead of spending most runs on
    // the four-byte magic/version gate. `data` remains the exact untrusted record byte stream.
    let mut framed = Vec::with_capacity(48 + data.len());
    framed.extend_from_slice(b"NBMQ");
    framed.extend_from_slice(&2u32.to_be_bytes());
    framed.extend_from_slice(&1u64.to_be_bytes());
    framed.extend_from_slice(&Sha256::digest(&framed));
    framed.extend_from_slice(data);
    let limits = Limits::default();
    let _ = decode_mqtt_recovery(&framed, &limits);
});
