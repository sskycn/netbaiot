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
    // Reach both v3 and v4 record decoders behind valid framing and whole-image integrity.
    let kind = data.first().copied().unwrap_or(1);
    let payload = data.get(1..).unwrap_or_default();
    let limits = Limits::default();
    for version in [3u32, 4] {
        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(b"NBMQ");
        header.extend_from_slice(&version.to_be_bytes());
        header.extend_from_slice(&1u64.to_be_bytes());
        let mut record = Vec::with_capacity(37 + payload.len());
        record.push(kind);
        record.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        record.extend_from_slice(payload);
        record.extend_from_slice(&Sha256::digest(payload));
        let mut whole = Sha256::new();
        whole.update(&header);
        whole.update(&record);
        let mut framed = Vec::with_capacity(48 + record.len() + 52);
        framed.extend_from_slice(&header);
        framed.extend_from_slice(&Sha256::digest(&header));
        framed.extend_from_slice(&record);
        framed.extend_from_slice(b"NEND");
        framed.extend_from_slice(&1u64.to_be_bytes());
        framed.extend_from_slice(&(record.len() as u64).to_be_bytes());
        framed.extend_from_slice(&whole.finalize());
        let _ = decode_mqtt_recovery(&framed, &limits);

        let trailer_at = framed.len() - 52;
        let mut deleted = framed[..48].to_vec();
        deleted.extend_from_slice(&framed[trailer_at..]);
        let _ = decode_mqtt_recovery(&deleted, &limits);
        let _ = decode_mqtt_recovery(&framed[..framed.len() - 1], &limits);
        let mut bad_count = framed.clone();
        bad_count[trailer_at + 11] ^= 1;
        let _ = decode_mqtt_recovery(&bad_count, &limits);
        let mut bad_digest = framed;
        let last = bad_digest.len() - 1;
        bad_digest[last] ^= 1;
        let _ = decode_mqtt_recovery(&bad_digest, &limits);
    }

    // Exercise the immediately previous release's bounded NBMQ v1 JSON envelope as well.
    let mut legacy = Vec::with_capacity(52 + data.len());
    legacy.extend_from_slice(b"NBMQ");
    legacy.extend_from_slice(&1u32.to_be_bytes());
    legacy.extend_from_slice(&1u64.to_be_bytes());
    legacy.extend_from_slice(&(data.len() as u32).to_be_bytes());
    legacy.extend_from_slice(data);
    legacy.extend_from_slice(&Sha256::digest(data));
    let _ = decode_mqtt_recovery(&legacy, &limits);
});
