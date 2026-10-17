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
    if matches!(field.data_type, DataType::F32 | DataType::F64) {
        field.conversion = None;
        if data.len() >= 24 {
            field.scale = Some(f64::from_le_bytes(data[8..16].try_into().unwrap()));
            field.bias = Some(f64::from_le_bytes(data[16..24].try_into().unwrap()));
        } else {
            field.scale = None;
            field.bias = None;
        }
    } else {
        let scales = ["0.1", "-0.001", "1", "0", "1e39", "1e-39", "invalid"];
        let biases = ["0", "0.3", "-1", "1e38", "1e-38", "invalid"];
        field.scale = None;
        field.bias = None;
        field.conversion = (selector & 2 != 0).then(|| DecimalConversion {
            mode: ConversionMode::DecimalExact,
            scale: scales[usize::from(selector) % scales.len()].into(),
            bias: biases[usize::from(selector.rotate_right(3)) % biases.len()].into(),
        });
    }
    config.commands[0].expires_at = None;
    config.commands[0].checksum = None;
    if let Ok(custom) = DeclarativeCodec::compile(&config, limits) {
        vendor_invariants::commands(&config, &custom, data);
    }

    // Keep exact-decimal raw -> normalized value -> raw round trips reachable for
    // every i16 bit pattern. The command invariant independently verifies the
    // accepted wire value with BigRational rather than the production arithmetic.
    static EXACT: OnceLock<DeclarativeCodec> = OnceLock::new();
    let exact = EXACT.get_or_init(|| {
        DeclarativeCodec::from_json(
            br#"{"id":"vendor-fuzz-exact","version":1,"header":[17],
                "message_type":{"offset":1,"type":"u8"},
                "messages":[{"tag":1,"size":5,
                    "source_message_id":{"offset":2,"type":"u8"},
                    "payload":{"kind":"telemetry","fields":[{"name":"value","field":{
                        "offset":3,"type":"i16","conversion":{"mode":"decimal_exact",
                        "scale":"0.1","bias":"0.3"}}}]}}],
                "commands":[{"name":"set","tag":2,"size":20,"command_id_offset":2,
                    "fields":[{"name":"value","field":{"offset":18,"type":"i16",
                    "conversion":{"mode":"decimal_exact","scale":"0.1","bias":"0.3"}}}]}]}"#,
            CodecLimits::default(),
        )
        .unwrap()
    });
    let raw_bytes = [
        data.first().copied().unwrap_or(0),
        data.get(1).copied().unwrap_or(0),
    ];
    let device = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let payload = [17, 1, 1, raw_bytes[0], raw_bytes[1]];
    let event = exact
        .decode(
            &DecodeContext {
                device: &device,
                received_at: 0,
            },
            &payload,
        )
        .unwrap()
        .remove(0);
    let DeviceEventKind::Telemetry(fields) = event.kind else {
        unreachable!()
    };
    let command = DeviceCommand {
        command_id: serde_json::from_str("\"00000000-0000-0000-0000-000000000000\"").unwrap(),
        device: device.clone(),
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: [("value".into(), fields["value"].clone())].into(),
        },
    };
    let encoded = exact
        .encode(&EncodeContext { device: &device }, &command)
        .unwrap();
    assert_eq!(&encoded[18..20], &raw_bytes);

    // Exercise exact round trips for every integer width, signedness and byte order.
    // Configuration compilation happens once; arbitrary raw bytes cover the runtime
    // property while vendor_invariants checks successful command math independently.
    static INTEGER_ROUND_TRIPS: OnceLock<Vec<(DeclarativeCodec, usize)>> = OnceLock::new();
    let integer_round_trips = INTEGER_ROUND_TRIPS.get_or_init(|| {
        let mut codecs = Vec::new();
        for (kind, width) in [
            ("u8", 1),
            ("i8", 1),
            ("u16", 2),
            ("i16", 2),
            ("u32", 4),
            ("i32", 4),
            ("u64", 8),
            ("i64", 8),
        ] {
            for endian in ["big", "little"] {
                let config: DeclarativeConfig = serde_json::from_value(serde_json::json!({
                    "id":"vendor-fuzz-round-trip","version":1,"header":[17],
                    "message_type":{"offset":1,"type":"u8"},
                    "messages":[{"tag":1,"size":3+width,
                        "source_message_id":{"offset":2,"type":"u8"},
                        "payload":{"kind":"telemetry","fields":[{"name":"value","field":{
                            "offset":3,"type":kind,"endian":endian,
                            "conversion":{"mode":"decimal_exact","scale":"-0.1","bias":"0.3"}
                        }}]}}],
                    "commands":[{"name":"set","tag":2,"size":18+width,"command_id_offset":2,
                        "fields":[{"name":"value","field":{"offset":18,"type":kind,
                            "endian":endian,"conversion":{"mode":"decimal_exact",
                            "scale":"-0.1","bias":"0.3"}}}]}]
                }))
                .unwrap();
                codecs.push((
                    DeclarativeCodec::compile(&config, CodecLimits::default()).unwrap(),
                    width,
                ));
            }
        }
        codecs
    });
    let (round_trip, width) =
        &integer_round_trips[usize::from(selector) % integer_round_trips.len()];
    let mut raw = [0u8; 8];
    for (slot, byte) in raw[..*width].iter_mut().zip(data) {
        *slot = *byte;
    }
    let mut payload = vec![17, 1, 1];
    payload.extend_from_slice(&raw[..*width]);
    let event = round_trip
        .decode(
            &DecodeContext {
                device: &device,
                received_at: 0,
            },
            &payload,
        )
        .unwrap()
        .remove(0);
    let DeviceEventKind::Telemetry(fields) = event.kind else {
        unreachable!()
    };
    let command = DeviceCommand {
        command_id: serde_json::from_str("\"00000000-0000-0000-0000-000000000000\"").unwrap(),
        device: device.clone(),
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: [("value".into(), fields["value"].clone())].into(),
        },
    };
    let encoded = round_trip
        .encode(&EncodeContext { device: &device }, &command)
        .unwrap();
    assert_eq!(&encoded[18..18 + *width], &raw[..*width]);

    // Metadata has an independent identifier bound, even with four-byte Scalars.
    static SOURCE_IDS: OnceLock<Vec<(DeclarativeCodec, usize)>> = OnceLock::new();
    let source_ids = SOURCE_IDS.get_or_init(|| {
        [("u32", 4), ("u64", 8), ("utf8", 64)]
            .into_iter()
            .map(|(kind, width)| {
                let mut source = serde_json::json!({"offset":2,"type":kind});
                if kind == "utf8" {
                    source["length"] = width.into();
                }
                let config = serde_json::from_value(serde_json::json!({
                    "id":"vendor-fuzz-source","version":1,"header":[17],
                    "message_type":{"offset":1,"type":"u8"},
                    "messages":[{"tag":1,"size":3+width,"source_message_id":source,
                        "payload":{"kind":"telemetry","fields":[{"name":"v",
                            "field":{"offset":2+width,"type":"i8"}}]}}]
                }))
                .unwrap();
                (
                    DeclarativeCodec::compile(
                        &config,
                        CodecLimits {
                            field_bytes: 4,
                            ..CodecLimits::default()
                        },
                    )
                    .unwrap(),
                    width,
                )
            })
            .collect()
    });
    let (source_codec, width) = &source_ids[usize::from(selector) % source_ids.len()];
    let mut payload = vec![b'x'; 3 + width];
    payload[..2].copy_from_slice(&[17, 1]);
    for (slot, byte) in payload[2..2 + width].iter_mut().zip(data) {
        *slot = *byte;
    }
    payload[2 + width] = selector;
    codec_invariants::check(
        source_codec,
        &CodecLimits {
            field_bytes: 4,
            ..CodecLimits::default()
        },
        &payload,
    );
});
