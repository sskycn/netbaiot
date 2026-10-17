#![no_main]
mod codec_invariants;
mod vendor_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_codecs::vendor::declarative::{DeclarativeCodec, DeclarativeConfig, MAX_CONFIG_BYTES};
use netbaiot_core::CodecLimits;
fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_CONFIG_BYTES {
        assert!(DeclarativeCodec::from_json(data, CodecLimits::default()).is_err());
        return;
    }
    if let Ok(config) = serde_json::from_slice::<DeclarativeConfig>(data) {
        for field_bytes in [4, 19, 20, 64, CodecLimits::default().field_bytes] {
            let limits = CodecLimits {
                field_bytes,
                ..CodecLimits::default()
            };
            if let Ok(codec) = DeclarativeCodec::compile(&config, limits.clone()) {
                codec_invariants::check(&codec, &limits, &[0; 64]);
                codec_invariants::check(&codec, &limits, data);
                vendor_invariants::commands(&config, &codec, data);
            }
        }
    }
});
