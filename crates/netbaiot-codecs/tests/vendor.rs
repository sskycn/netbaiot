use netbaiot_codecs::vendor::{declarative::*, demo::AcmeV1};
use netbaiot_core::*;
use num_bigint::BigInt;
use num_rational::BigRational;
use serde_json::{Value, json};

const CONFIG: &[u8] = include_bytes!("../../../configs/codecs/acme-v1.json");
fn config() -> DeclarativeConfig {
    serde_json::from_slice(CONFIG).unwrap()
}
fn codec() -> DeclarativeCodec {
    DeclarativeCodec::from_json(CONFIG, CodecLimits::default()).unwrap()
}
fn key() -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("tenant").unwrap(),
        product_id: ProductId::new("acme").unwrap(),
        device_id: DeviceId::new("device").unwrap(),
    }
}
fn sum(p: &mut [u8]) {
    let last = p.len() - 1;
    p[last] = p[..last].iter().fold(0u8, |s, b| s.wrapping_add(*b));
}
fn packet(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut p = vec![165, 90, 0, (body.len() + 6) as u8, tag];
    p.extend_from_slice(body);
    p.push(0);
    sum(&mut p);
    p
}
fn telemetry() -> Vec<u8> {
    // Independent wire vector: source 42, -12.3 C, 65.4 %, alarm true, model A100.
    packet(
        1,
        &[
            42, 0, 0, 0, 0x85, 0xff, 0x02, 0x8e, 1, b'A', b'1', b'0', b'0',
        ],
    )
}
fn command() -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId(uuid::Uuid::from_u128(1)),
        device: key(),
        expires_at: Some(1_700_000_000_000),
        payload: DeviceCommandPayload {
            name: "set_temperature".into(),
            arguments: [("temperature".into(), Scalar::Number(-12.3))].into(),
        },
    }
}
fn parity(codec: &dyn DeviceCodec, p: &[u8]) -> bool {
    let device = key();
    let ctx = DecodeContext {
        device: &device,
        received_at: 123,
    };
    let d = codec.decode(&ctx, p);
    let v = codec.validate_payload(&ctx, p);
    assert_eq!(d.is_ok(), v.is_ok());
    if let Ok(events) = d {
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, v.unwrap()[0]);
        assert_eq!(events[0].device, device);
        assert_eq!(events[0].received_at, 123);
        assert_eq!(events[0].source_message_id.as_str(), "42");
        true
    } else {
        false
    }
}
#[test]
fn independent_native_and_declarative_vectors_and_commands_match() {
    let p = telemetry();
    let d = codec();
    let n = AcmeV1::new(CodecLimits::default()).unwrap();
    assert!(parity(&d, &p));
    assert!(parity(&n, &p));
    let device = key();
    let ctx = DecodeContext {
        device: &device,
        received_at: 123,
    };
    let event = d.decode(&ctx, &p).unwrap().remove(0);
    assert_eq!(event.kind, n.decode(&ctx, &p).unwrap()[0].kind);
    let DeviceEventKind::Telemetry(fields) = event.kind else {
        panic!()
    };
    assert_eq!(fields["temperature"], Scalar::Number(-123.0 * 0.1));
    assert_eq!(fields["humidity"], Scalar::Number(654.0 * 0.1));
    assert_eq!(fields["alarm"], Scalar::Boolean(true));
    assert_eq!(fields["model"], Scalar::Text("A100".into()));
    let c = command();
    let ectx = EncodeContext { device: &device };
    let out = d.encode(&ectx, &c).unwrap();
    assert_eq!(out, n.encode(&ectx, &c).unwrap());
    assert_eq!(&out[..5], &[165, 90, 0, 32, 129]);
    assert_eq!(&out[5..21], c.command_id.0.as_bytes());
    assert_eq!(&out[21..23], &(-123i16).to_le_bytes());
    assert_eq!(&out[23..31], &1_700_000_000_000u64.to_be_bytes());
    assert_eq!(
        out[31],
        out[..31].iter().fold(0u8, |s, b| s.wrapping_add(*b))
    );
    assert_ne!(event.event_id, d.decode(&ctx, &p).unwrap()[0].event_id);
}
#[test]
fn heartbeat_event_and_command_execution_are_distinct() {
    let codec = codec();
    let mut body = vec![42, 0, 0, 0];
    body.extend_from_slice(&u64::MAX.to_be_bytes());
    assert!(parity(&codec, &packet(2, &body)));
    assert!(parity(&codec, &packet(3, &[42, 0, 0, 0, 0, 7])));
    let device = key();
    let ctx = DecodeContext {
        device: &device,
        received_at: 1,
    };
    for (value, state) in [
        (1, ExecutionState::Running),
        (2, ExecutionState::Succeeded),
        (3, ExecutionState::Failed),
    ] {
        let mut body = vec![42, 0, 0, 0];
        body.extend_from_slice(command().command_id.0.as_bytes());
        body.push(value);
        let p = packet(4, &body);
        assert!(parity(&codec, &p));
        assert_eq!(
            codec.decode(&ctx, &p).unwrap()[0].kind,
            DeviceEventKind::CommandAck(CommandAck {
                command_id: command().command_id,
                execution: state
            })
        );
        *body.last_mut().unwrap() = 0;
        assert!(!parity(&codec, &packet(4, &body)));
    }
}
#[test]
fn malformed_frames_and_qos2_prevalidation_have_identical_rules() {
    let codec = codec();
    let p = telemetry();
    for len in 0..p.len() {
        assert!(!parity(&codec, &p[..len]));
    }
    let mut oversized = p.clone();
    oversized.push(0);
    assert!(!parity(&codec, &oversized));
    for index in 0..p.len() {
        let mut bad = p.clone();
        bad[index] ^= 0x80;
        assert!(!parity(&codec, &bad));
    }
    for (index, value) in [
        (0, 0),
        (2, 1),
        (3, 18),
        (4, 99),
        (13, 2),
        (14, 0xff),
        (14, 0),
    ] {
        let mut bad = p.clone();
        bad[index] = value;
        sum(&mut bad);
        assert!(!parity(&codec, &bad));
    }
    assert!(!parity(&codec, br#"{"kind":"heartbeat"}"#));
    assert!(!parity(&codec, &vec![0; MAX_FRAME_BYTES + 1]));
}
#[test]
fn invalid_layouts_fail_at_startup() {
    let original: Value = serde_json::from_slice(CONFIG).unwrap();
    let cases = [
        ("/id", json!("netbaiot-json")),
        ("/version", json!(0)),
        ("/header", json!([])),
        ("/header", json!(vec![0; 65])),
        ("/messages", json!([])),
        ("/messages/0/size", json!(65537)),
        ("/messages/0/source_message_id/offset", json!(usize::MAX)),
        ("/messages/0/source_message_id/scale", json!(2)),
        ("/messages/0/payload/fields/0/field/offset", json!(4)),
        ("/messages/0/payload/fields/0/field/scale", json!(0)),
        ("/messages/0/payload/fields/0/name", json!("alarm")),
        ("/messages/0/payload/fields/3/field/length", json!(0)),
        ("/messages/0/checksum/end", json!(19)),
        ("/messages/0/checksum/start", json!(18)),
        ("/messages/0/checksum/offset", json!(17)),
        ("/messages/0/tag", json!(256)),
        ("/messages/1/tag", json!(1)),
        ("/length/adjustment", json!(i32::MAX)),
        ("/messages/3/payload/running", json!(2)),
        ("/commands/0/command_id_offset", json!(20)),
    ];
    for (pointer, value) in cases {
        let mut c = original.clone();
        let (parent, name) = pointer.rsplit_once('/').unwrap();
        c.pointer_mut(parent).unwrap()[name] = value;
        let c: DeclarativeConfig = serde_json::from_value(c).unwrap();
        assert!(
            DeclarativeCodec::compile(&c, CodecLimits::default()).is_err(),
            "{pointer}"
        );
    }
    let mut c = config();
    c.messages[0].source_message_id.bias = Some(f64::NAN);
    assert!(DeclarativeCodec::compile(&c, CodecLimits::default()).is_err());
    let mut c = config();
    c.messages = vec![c.messages[0].clone(); MAX_LAYOUTS + 1];
    assert!(DeclarativeCodec::compile(&c, CodecLimits::default()).is_err());
    assert!(
        DeclarativeCodec::from_json(&vec![b' '; MAX_CONFIG_BYTES + 1], CodecLimits::default())
            .is_err()
    );
    let mut c = original;
    c["unknown"] = json!(1);
    assert!(serde_json::from_value::<DeclarativeConfig>(c).is_err());
}
#[test]
fn command_identity_types_quantization_limits_and_arguments_are_strict() {
    let d = codec();
    let n = AcmeV1::new(CodecLimits::default()).unwrap();
    let device = key();
    let ctx = EncodeContext { device: &device };
    for codec in [&d as &dyn DeviceCodec, &n] {
        for value in [
            Scalar::Number(3276.8),
            Scalar::Number(-3276.9),
            Scalar::Number(12.35),
            Scalar::Number(f64::NAN),
            Scalar::Number(f64::INFINITY),
            Scalar::Text("12".into()),
        ] {
            let mut c = command();
            c.payload.arguments.insert("temperature".into(), value);
            assert!(codec.encode(&ctx, &c).is_err());
        }
        for value in [-3276.8, 3276.7, 0.0, 12.3] {
            let mut c = command();
            c.payload
                .arguments
                .insert("temperature".into(), Scalar::Number(value));
            assert!(codec.encode(&ctx, &c).is_ok());
        }
        let mut c = command();
        c.device.device_id = DeviceId::new("other").unwrap();
        assert!(codec.encode(&ctx, &c).is_err());
        let mut c = command();
        c.expires_at = Some(-1);
        assert!(codec.encode(&ctx, &c).is_err());
        let mut c = command();
        c.payload.name = "unknown".into();
        assert!(codec.encode(&ctx, &c).is_err());
        let mut c = command();
        c.payload.arguments.clear();
        assert!(codec.encode(&ctx, &c).is_err());
        let mut c = command();
        c.payload
            .arguments
            .insert("extra".into(), Scalar::Boolean(true));
        assert!(codec.encode(&ctx, &c).is_err());
    }
}

#[test]
fn ordinary_decimal_commands_encode_consistently() {
    let declarative = codec();
    let native = AcmeV1::new(CodecLimits::default()).unwrap();
    let device = key();
    let context = EncodeContext { device: &device };
    for (value, raw) in [
        (0.1, 1i16),
        (0.2, 2),
        (5.1, 51i16),
        (0.3, 3),
        (0.7, 7),
        (1.1, 11),
        (1.7, 17),
        (2.3, 23),
        (3.3, 33),
        (12.3, 123),
        (25.3, 253),
        (-0.1, -1),
        (-0.2, -2),
        (-5.1, -51),
        (-0.3, -3),
        (-0.7, -7),
        (-1.1, -11),
        (-1.7, -17),
        (-2.3, -23),
        (-3.3, -33),
        (-12.3, -123),
        (-25.3, -253),
    ] {
        let mut command = command();
        command
            .payload
            .arguments
            .insert("temperature".into(), Scalar::Number(value));
        let declared = declarative.encode(&context, &command).unwrap();
        let native = native.encode(&context, &command).unwrap();
        assert_eq!(declared, native, "{value}");
        assert_eq!(&declared[21..23], &raw.to_le_bytes(), "{value}");
    }
}

fn exact_i16_config(scale: &str, bias: &str, endian: &str) -> DeclarativeConfig {
    serde_json::from_value(json!({
        "id":"vendor-exact","version":1,"header":[0x11],
        "message_type":{"offset":1,"type":"u8"},
        "messages":[{
            "tag":1,"size":5,"source_message_id":{"offset":2,"type":"u8"},
            "payload":{"kind":"telemetry","fields":[{
                "name":"value","field":{"offset":3,"type":"i16","endian":endian,
                    "conversion":{"mode":"decimal_exact","scale":scale,"bias":bias}}
            }]}
        }],
        "commands":[{
            "name":"set","tag":2,"size":20,"command_id_offset":2,
            "fields":[{"name":"value","field":{"offset":18,"type":"i16","endian":endian,
                "conversion":{"mode":"decimal_exact","scale":scale,"bias":bias}}}]
        }]
    }))
    .unwrap()
}

fn exact_command(value: Scalar) -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId(uuid::Uuid::from_u128(1)),
        device: key(),
        expires_at: None,
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: [("value".into(), value)].into(),
        },
    }
}

#[test]
fn integer_decimal_regressions_use_exact_inverse_math() {
    for (scale, bias, value, raw) in [
        ("0.1", "0", 5.1, 51i16),
        ("0.001", "1", 1.001, 1),
        ("0.1", "0.3", 0.0, -3),
        ("-0.1", "0", -5.1, 51),
    ] {
        for endian in ["big", "little"] {
            let codec = DeclarativeCodec::compile(
                &exact_i16_config(scale, bias, endian),
                CodecLimits::default(),
            )
            .unwrap();
            let command = exact_command(Scalar::Number(value));
            let bytes = codec
                .encode(
                    &EncodeContext {
                        device: &command.device,
                    },
                    &command,
                )
                .unwrap();
            let expected = if endian == "big" {
                raw.to_be_bytes()
            } else {
                raw.to_le_bytes()
            };
            assert_eq!(&bytes[18..20], &expected, "{scale} {bias} {value}");
        }
    }

    for (scale, bias, value) in [("0.1", "0", 5.15), ("-1", "0", 1.0000000000000002)] {
        let codec = DeclarativeCodec::compile(
            &exact_i16_config(scale, bias, "big"),
            CodecLimits::default(),
        )
        .unwrap();
        let command = exact_command(Scalar::Number(value));
        assert!(
            codec
                .encode(
                    &EncodeContext {
                        device: &command.device,
                    },
                    &command
                )
                .is_err(),
            "{scale} {bias} {value}"
        );
    }

    let codec = DeclarativeCodec::compile(
        &exact_i16_config("0.001", "1", "big"),
        CodecLimits::default(),
    )
    .unwrap();
    let command = exact_command(Scalar::Decimal(DecimalScalar::new("1.001").unwrap()));
    assert_eq!(
        &codec
            .encode(
                &EncodeContext {
                    device: &command.device,
                },
                &command
            )
            .unwrap()[18..20],
        &1i16.to_be_bytes()
    );
}

fn reference_decimal(text: &str) -> BigRational {
    let (negative, text) = text
        .strip_prefix('-')
        .map_or((false, text), |rest| (true, rest));
    let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = format!("{integer}{fraction}");
    let mut numerator = digits.parse::<BigInt>().unwrap();
    if negative {
        numerator = -numerator;
    }
    BigRational::new(numerator, BigInt::from(10u8).pow(fraction.len() as u32))
}

#[test]
fn exact_i16_round_trip_matches_independent_big_rational_reference() {
    let device = key();
    let decode_context = DecodeContext {
        device: &device,
        received_at: 1,
    };
    for endian in ["big", "little"] {
        let codec = DeclarativeCodec::compile(
            &exact_i16_config("0.1", "0.3", endian),
            CodecLimits::default(),
        )
        .unwrap();
        for raw in i16::MIN..=i16::MAX {
            let wire = if endian == "big" {
                raw.to_be_bytes()
            } else {
                raw.to_le_bytes()
            };
            let payload = [0x11, 1, 42, wire[0], wire[1]];
            let event = codec.decode(&decode_context, &payload).unwrap().remove(0);
            let DeviceEventKind::Telemetry(fields) = event.kind else {
                panic!()
            };
            let Scalar::Decimal(decimal) = &fields["value"] else {
                panic!()
            };
            let reference = BigRational::new(BigInt::from(raw) + 3, BigInt::from(10));
            assert_eq!(reference_decimal(decimal.as_str()), reference, "{raw}");

            let command = exact_command(fields["value"].clone());
            let encoded = codec
                .encode(
                    &EncodeContext {
                        device: &command.device,
                    },
                    &command,
                )
                .unwrap();
            assert_eq!(&encoded[18..20], &wire, "{raw}");
        }
    }
}

#[test]
fn exact_config_conflicts_bounds_and_legacy_collisions_are_rejected() {
    let mut conflict: Value = serde_json::to_value(exact_i16_config("0.1", "0", "big")).unwrap();
    conflict["commands"][0]["fields"][0]["field"]["scale"] = json!(0.1);
    let conflict: DeclarativeConfig = serde_json::from_value(conflict).unwrap();
    let error = match DeclarativeCodec::compile_diagnostic(&conflict, CodecLimits::default()) {
        Ok(_) => panic!("conflicting conversion unexpectedly compiled"),
        Err(error) => error,
    };
    assert_eq!(error.path, "commands[0].fields[0].field.conversion");

    for scale in ["1e39", "1e-39", "123456789012345678901234567890123456789"] {
        let config = exact_i16_config(scale, "0", "big");
        assert!(DeclarativeCodec::compile(&config, CodecLimits::default()).is_err());
    }
    let extreme = single_field("i16", json!({"scale":1e300,"bias":1e20}), 2);
    assert!(DeclarativeCodec::compile(&extreme, CodecLimits::default()).is_err());

    let collision = DeclarativeCodec::compile(
        &single_field("u64", json!({"scale":0.5,"bias":2.0}), 8),
        CodecLimits::default(),
    )
    .unwrap();
    let mut packet = vec![0x11, 1, 42];
    packet.extend_from_slice(&(1u64 << 53).to_be_bytes());
    assert!(!parity(&collision, &packet));
}

#[test]
fn native_and_declarative_reject_material_command_quantization() {
    let declarative = codec();
    let native = AcmeV1::new(CodecLimits::default()).unwrap();
    let device = key();
    let context = EncodeContext { device: &device };
    for value in [
        5.15,
        -5.15,
        12.30000000001,
        -12.30000000001,
        3276.8,
        -3276.9,
        1e-300,
        -1e-300,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let mut command = command();
        command
            .payload
            .arguments
            .insert("temperature".into(), Scalar::Number(value));
        for codec in [&declarative as &dyn DeviceCodec, &native] {
            assert!(codec.encode(&context, &command).is_err(), "{value}");
        }
    }
}
#[test]
fn resource_limits_are_checked_before_runtime_allocations() {
    for limits in [
        CodecLimits {
            input_bytes: 18,
            ..Default::default()
        },
        CodecLimits {
            output_messages: 0,
            ..Default::default()
        },
        CodecLimits {
            decoded_bytes: 512,
            ..Default::default()
        },
        CodecLimits {
            fields: 3,
            ..Default::default()
        },
        CodecLimits {
            field_bytes: 3,
            ..Default::default()
        },
    ] {
        assert!(DeclarativeCodec::compile(&config(), limits).is_err());
    }
    let mut c = config();
    c.messages[0].size = 20;
    c.messages[0].checksum = None;
    let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
    let mut p = telemetry();
    p[3] = 20;
    p[18] = 0;
    p.push(0);
    assert!(parity(&d, &p));
    p[19] = 1;
    assert!(!parity(&d, &p));
}

fn single_field(kind: &str, extra: Value, width: usize) -> DeclarativeConfig {
    let mut field = json!({"offset":3,"type":kind});
    field
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    serde_json::from_value(json!({
        "id":"vendor-types","version":1,"header":[0x11],"message_type":{"offset":1,"type":"u8"},
        "messages":[{"tag":1,"size":3+width,"source_message_id":{"offset":2,"type":"u8"},
          "payload":{"kind":"telemetry","fields":[{"name":"value","field":field}]}}]
    }))
    .unwrap()
}
#[test]
fn all_numeric_widths_endianness_and_nonfinite_or_lossy_values() {
    let values: &[(&str, &[u8], f64)] = &[
        ("u8", &[255], 255.),
        ("i8", &[128], -128.),
        ("u16", &[0xff, 0xff], 65535.),
        ("i16", &[0x80, 0], -32768.),
        ("u32", &[0xff; 4], 4294967295.),
        ("i32", &[0x80, 0, 0, 0], -2147483648.),
        ("u64", &[0, 0x10, 0, 0, 0, 0, 0, 0], 4503599627370496.),
        ("i64", &[0xff, 0xf0, 0, 0, 0, 0, 0, 0], -4503599627370496.),
        ("f32", &[0x3f, 0xa0, 0, 0], 1.25),
        ("f64", &[0x3f, 0xf4, 0, 0, 0, 0, 0, 0], 1.25),
    ];
    let device = key();
    let ctx = DecodeContext {
        device: &device,
        received_at: 1,
    };
    for (kind, wire, expected) in values {
        for endian in ["big", "little"] {
            let c = single_field(
                kind,
                json!({"endian":endian,"scale":0.5,"bias":2.0}),
                wire.len(),
            );
            let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
            let mut p = vec![0x11, 1, 42];
            let mut bytes = wire.to_vec();
            if endian == "little" {
                bytes.reverse();
            }
            p.extend(bytes);
            assert!(parity(&d, &p));
            let DeviceEventKind::Telemetry(fields) = d.decode(&ctx, &p).unwrap().remove(0).kind
            else {
                panic!()
            };
            assert_eq!(
                fields["value"],
                Scalar::Number(expected * 0.5 + 2.0),
                "{kind} {endian}"
            );
        }
    }
    for (kind, bytes) in [
        ("u64", u64::MAX.to_be_bytes().to_vec()),
        ("i64", i64::MIN.to_be_bytes().to_vec()),
        ("f32", f32::NAN.to_be_bytes().to_vec()),
        ("f32", f32::INFINITY.to_be_bytes().to_vec()),
        ("f64", f64::INFINITY.to_be_bytes().to_vec()),
    ] {
        let d = DeclarativeCodec::compile(
            &single_field(kind, json!({}), bytes.len()),
            CodecLimits::default(),
        )
        .unwrap();
        let mut p = vec![0x11, 1, 42];
        p.extend(bytes);
        assert!(!parity(&d, &p));
    }
    let d = DeclarativeCodec::compile(
        &single_field("f64", json!({"scale":2.0}), 8),
        CodecLimits::default(),
    )
    .unwrap();
    let mut p = vec![0x11, 1, 42];
    p.extend(f64::MAX.to_be_bytes());
    assert!(!parity(&d, &p));
}
#[test]
fn source_ids_timestamps_length_adjustments_and_checksums() {
    let mut c = config();
    c.messages.truncate(1);
    c.commands.clear();
    c.messages[0].checksum = None;
    c.messages[0].source_message_id.data_type = DataType::Utf8;
    c.messages[0].source_message_id.length = Some(4);
    let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
    let device = key();
    let ctx = DecodeContext {
        device: &device,
        received_at: 1,
    };
    let mut p = telemetry();
    p[5..9].copy_from_slice(b"id:1");
    p[18] = 0;
    assert_eq!(
        d.decode(&ctx, &p).unwrap()[0].source_message_id.as_str(),
        "id:1"
    );
    p[5] = b'/';
    assert!(d.validate_payload(&ctx, &p).is_err());
    c.messages[0].source_message_id = config().messages.remove(0).source_message_id;
    c.messages[0].size = 27;
    c.messages[0].occurred_at = Some(IntegerField {
        offset: 19,
        data_type: IntegerType::U64,
        endian: Endian::Little,
    });
    c.length.as_mut().unwrap().adjustment = 4;
    let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
    let mut p = telemetry();
    p[3] = 23;
    p[18] = 0;
    p.extend(i64::MAX.to_le_bytes());
    assert!(parity(&d, &p));
    p[26] = 0x80;
    assert!(!parity(&d, &p));
    for algorithm in [ChecksumAlgorithm::Xor8, ChecksumAlgorithm::Crc16Modbus] {
        let mut c = config();
        c.messages.truncate(1);
        c.commands.clear();
        let check = c.messages[0].checksum.as_mut().unwrap();
        check.algorithm = algorithm;
        check.endian = Endian::Little;
        let mut p = telemetry();
        match algorithm {
            ChecksumAlgorithm::Xor8 => {
                p[18] = p[..18].iter().fold(0, |s, b| s ^ b);
            }
            _ => {
                c.messages[0].size = 20;
                p[3] = 20;
                p.pop();
                let mut crc = 0xffffu16;
                for byte in &p {
                    crc ^= u16::from(*byte);
                    for _ in 0..8 {
                        let odd = crc & 1 != 0;
                        crc >>= 1;
                        if odd {
                            crc ^= 0xa001;
                        }
                    }
                }
                p.extend(crc.to_le_bytes());
            }
        }
        let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
        assert!(parity(&d, &p));
        p[9] ^= 1;
        assert!(!parity(&d, &p));
    }
}
#[test]
fn downlink_fields_reject_lossy_integers_and_enforce_text_width() {
    for (kind, width, value, invalid) in [
        (
            "u64",
            8,
            Scalar::Number(9007199254740992.),
            Scalar::Number(4503599627370495.5),
        ),
        (
            "i32",
            4,
            Scalar::Number(-123.),
            Scalar::Number(-2147483649.),
        ),
        ("f32", 4, Scalar::Number(1.25), Scalar::Number(f64::MAX)),
        (
            "f64",
            8,
            Scalar::Number(1.25),
            Scalar::Number(f64::INFINITY),
        ),
        ("bool", 1, Scalar::Boolean(true), Scalar::Number(1.)),
        (
            "utf8",
            4,
            Scalar::Text("A100".into()),
            Scalar::Text("A10".into()),
        ),
    ] {
        let mut c = config();
        c.commands[0].fields[0].field = serde_json::from_value(
            json!({"offset":21,"type":kind,"length":if kind=="utf8"{Some(4)}else{None::<u8>}}),
        )
        .unwrap();
        c.commands[0].expires_at = None;
        c.commands[0].checksum = None;
        c.commands[0].size = 21 + width;
        let d = DeclarativeCodec::compile(&c, CodecLimits::default()).unwrap();
        let mut cmd = command();
        cmd.payload.arguments.insert("temperature".into(), value);
        let device = key();
        let ctx = EncodeContext { device: &device };
        assert_eq!(d.encode(&ctx, &cmd).unwrap().len(), 21 + width);
        cmd.payload.arguments.insert("temperature".into(), invalid);
        assert!(d.encode(&ctx, &cmd).is_err(), "{kind}");
    }
}

#[test]
fn command_expiry_requires_the_complete_timestamp_range_at_compile_time() {
    for kind in [IntegerType::U8, IntegerType::U16, IntegerType::U32] {
        let mut c = config();
        c.commands[0].expires_at.as_mut().unwrap().data_type = kind;
        assert!(
            DeclarativeCodec::compile(&c, CodecLimits::default()).is_err(),
            "{kind:?}"
        );
    }
}

#[test]
fn native_command_respects_codec_byte_limit() {
    let codec = AcmeV1::new(CodecLimits {
        input_bytes: 31,
        ..Default::default()
    })
    .unwrap();
    assert!(
        codec
            .encode(&EncodeContext { device: &key() }, &command())
            .is_err()
    );
}

#[test]
fn expiry_encodes_full_nonnegative_i64_range_without_float_conversion() {
    let d = codec();
    let n = AcmeV1::new(CodecLimits::default()).unwrap();
    for codec in [&d as &dyn DeviceCodec, &n] {
        for endian in [Endian::Big, Endian::Little] {
            let mut cfg = config();
            cfg.commands[0].expires_at.as_mut().unwrap().endian = endian;
            let compiled = DeclarativeCodec::compile(&cfg, CodecLimits::default()).unwrap();
            for expiry in [None, Some(0), Some(1_800_000_000_000), Some(i64::MAX)] {
                let mut cmd = command();
                cmd.expires_at = expiry;
                let ctx = EncodeContext {
                    device: &cmd.device,
                };
                assert_eq!(
                    &codec.encode(&ctx, &cmd).unwrap()[23..31],
                    &expiry.unwrap_or(0).to_be_bytes()
                );
                let expected = match endian {
                    Endian::Big => expiry.unwrap_or(0).to_be_bytes(),
                    Endian::Little => expiry.unwrap_or(0).to_le_bytes(),
                };
                assert_eq!(&compiled.encode(&ctx, &cmd).unwrap()[23..31], &expected);
            }
        }
    }
}

#[test]
fn compiler_reports_original_field_indices_and_schema_paths() {
    let original: Value = serde_json::from_slice(CONFIG).unwrap();
    for (pointer, value, path) in [
        (
            "/messages/0/payload/fields/1/field/scale",
            json!(0),
            "messages[0].payload.fields[1].field.scale",
        ),
        (
            "/messages/0/payload/fields/1/field/offset",
            json!(9),
            "messages[0].payload.fields[1].field.offset",
        ),
        (
            "/messages/0/payload/fields/1/name",
            json!("temperature"),
            "messages[0].payload.fields[1].name",
        ),
        (
            "/commands/0/expires_at/type",
            json!("u32"),
            "commands[0].expires_at.type",
        ),
        (
            "/commands/0/expires_at/offset",
            json!(usize::MAX),
            "commands[0].expires_at.offset",
        ),
        (
            "/commands/0/command_id_offset",
            json!(0),
            "commands[0].command_id_offset",
        ),
        (
            "/messages/0/checksum/end",
            json!(999),
            "messages[0].checksum",
        ),
        (
            "/message_type/offset",
            json!(usize::MAX),
            "message_type.offset",
        ),
        ("/version", json!(0), "version"),
    ] {
        let mut value_config = original.clone();
        *value_config.pointer_mut(pointer).unwrap() = value;
        let c = serde_json::from_value(value_config).unwrap();
        let error = DeclarativeCodec::compile_diagnostic(&c, CodecLimits::default())
            .err()
            .unwrap();
        assert_eq!(error.path, path);
        assert!(DeclarativeCodec::compile(&c, CodecLimits::default()).is_err());
    }
}
