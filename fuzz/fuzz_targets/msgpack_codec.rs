#![no_main]
mod codec_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_core::*;
fuzz_target!(|data: &[u8]| {
    if data.len() > 65536 { return; }
    let limits = CodecLimits::default();
    codec_invariants::check(&netbaiot_codecs::MsgpackV1::new(limits.clone()), &limits, data);
    // Independently vary every resource bound while retaining the raw input.
    let small = CodecLimits {
        input_bytes: 1024,
        decoded_bytes: 4096,
        fields: 4,
        field_bytes: 16,
        nesting_depth: 2,
        output_messages: 1,
    };
    codec_invariants::check(&netbaiot_codecs::MsgpackV1::new(small.clone()), &small, data);
});
