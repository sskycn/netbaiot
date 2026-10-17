#![no_main]
mod codec_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_codecs::vendor::declarative::DeclarativeCodec;
use netbaiot_core::CodecLimits;
fuzz_target!(|data: &[u8]| {
    let limits = CodecLimits::default();
    if let Ok(codec) = DeclarativeCodec::from_json(data, limits.clone()) {
        codec_invariants::check(&codec, &limits, &[0; 64]);
        codec_invariants::check(&codec, &limits, data);
    }
});
