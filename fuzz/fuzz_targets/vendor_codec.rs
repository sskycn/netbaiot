#![no_main]
mod codec_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_codecs::vendor::{declarative::DeclarativeCodec, demo::AcmeV1};
use netbaiot_core::*;
use std::sync::OnceLock;
fuzz_target!(|data: &[u8]| {
    static CODEC: OnceLock<DeclarativeCodec> = OnceLock::new();
    static NATIVE: OnceLock<AcmeV1> = OnceLock::new();
    let limits = CodecLimits::default();
    let codec = CODEC.get_or_init(|| {
        DeclarativeCodec::from_json(
            include_bytes!("../../configs/codecs/acme-v1.json"),
            CodecLimits::default(),
        )
        .unwrap()
    });
    codec_invariants::check(codec, &limits, data);
    codec_invariants::check(
        NATIVE.get_or_init(|| AcmeV1::new(CodecLimits::default()).unwrap()),
        &limits,
        data,
    );
});
