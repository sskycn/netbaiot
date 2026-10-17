#![no_main]
mod codec_invariants;
mod vendor_invariants;
use libfuzzer_sys::fuzz_target;
use netbaiot_codecs::vendor::{declarative::*, demo::AcmeV1};
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
    // Keep deep valid telemetry paths reachable despite the checksum gate.
    let mut valid = [
        165, 90, 0, 19, 1, 42, 0, 0, 0, 0, 0, 0, 0, 0, b'A', b'1', b'0', b'0', 0,
    ];
    for (slot, byte) in valid[5..14].iter_mut().zip(data) {
        *slot = *byte;
    }
    valid[13] &= 1;
    valid[18] = valid[..18].iter().fold(0u8, |sum, b| sum.wrapping_add(*b));
    codec_invariants::check(codec, &limits, &valid);
    let mut config: DeclarativeConfig =
        serde_json::from_slice(include_bytes!("../../configs/codecs/acme-v1.json")).unwrap();
    vendor_invariants::commands(&config, codec, data);
    vendor_invariants::commands(&config, NATIVE.get().unwrap(), data);
    // Exercise arbitrary inverse transforms, narrowing and both byte orders.
    let selector = data.first().copied().unwrap_or(0);
    let field = &mut config.commands[0].fields[0].field;
    field.data_type = [
        DataType::U8,
        DataType::I8,
        DataType::U16,
        DataType::I16,
        DataType::U32,
        DataType::I32,
        DataType::U64,
        DataType::I64,
        DataType::F32,
        DataType::F64,
    ][usize::from(selector) % 10];
    field.endian = if selector & 1 == 0 {
        Endian::Big
    } else {
        Endian::Little
    };
    if data.len() >= 24 {
        field.scale = f64::from_le_bytes(data[8..16].try_into().unwrap());
        field.bias = f64::from_le_bytes(data[16..24].try_into().unwrap());
    }
    config.commands[0].expires_at = None;
    config.commands[0].checksum = None;
    if let Ok(custom) = DeclarativeCodec::compile(&config, limits) {
        vendor_invariants::commands(&config, &custom, data);
    }
});
