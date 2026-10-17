#![no_main]
mod codec_invariants;
mod vendor_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_codecs::vendor::declarative::{DeclarativeCodec, DeclarativeConfig, MAX_CONFIG_BYTES};
use netbaiot_core::CodecLimits;
fuzz_target!(|data: &[u8]| {
    let limits = CodecLimits::default();
    if data.len() > MAX_CONFIG_BYTES {
        assert!(DeclarativeCodec::from_json(data, limits).is_err());
        return;
    }
    if let Ok(codec) = DeclarativeCodec::from_json(data, limits.clone()) {
        codec_invariants::check(&codec, &limits, &[0; 64]);
        codec_invariants::check(&codec, &limits, data);
        let config: DeclarativeConfig = serde_json::from_slice(data).unwrap();
        vendor_invariants::commands(&config, &codec, data);
    }
});
