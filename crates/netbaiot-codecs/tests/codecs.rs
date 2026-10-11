use netbaiot_codecs::{CborV1, JsonV1, MsgpackV1, ProtobufV1, protobuf::v1::wire};
use netbaiot_core::*;
use prost::Message;
use std::collections::BTreeMap;
fn key() -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    }
}
fn fixtures(name: &str) -> Vec<(Box<dyn DeviceCodec>, Vec<u8>)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    [
        ("json", Box::new(JsonV1::default()) as Box<dyn DeviceCodec>),
        ("cbor", Box::new(CborV1::default())),
        ("msgpack", Box::new(MsgpackV1::default())),
        ("protobuf", Box::new(ProtobufV1::default())),
    ]
    .into_iter()
    .map(|(ext, c)| {
        (
            c,
            std::fs::read(root.join(format!("{name}.{ext}"))).unwrap(),
        )
    })
    .collect()
}
#[test]
fn independent_vectors_normalize_all_kinds_identically() {
    let key = key();
    let ctx = DecodeContext {
        device: &key,
        received_at: 1234,
    };
    for name in ["telemetry", "types", "event", "heartbeat", "command_ack"] {
        let mut expected = None;
        for (codec, p) in fixtures(name) {
            let events = codec.decode(&ctx, &p).unwrap();
            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.device, key);
            assert_eq!(e.received_at, 1234);
            assert_eq!(e.occurred_at, Some(1000));
            assert_eq!(e.source_message_id.as_str(), "sample:1");
            assert_eq!(
                codec.validate_payload(&ctx, &p).unwrap(),
                vec![e.kind.clone()]
            );
            if let Some(ref k) = expected {
                assert_eq!(&e.kind, k);
            } else {
                expected = Some(e.kind.clone());
            }
        }
    }
}
#[test]
fn binary_numeric_policy_is_strict_and_does_not_change_json() {
    let key = key();
    let ctx = DecodeContext {
        device: &key,
        received_at: 0,
    };
    for name in ["integer_overflow", "nonfinite", "unknown_execution"] {
        for (i, (codec, p)) in fixtures(name).into_iter().enumerate() {
            if name == "integer_overflow" && i == 0 {
                assert!(codec.decode(&ctx, &p).is_ok());
            } else {
                assert!(codec.decode(&ctx, &p).is_err(), "{name},{i}");
            }
        }
    }
}
#[test]
fn truncation_trailing_data_and_input_bounds() {
    let k = key();
    let ctx = DecodeContext {
        device: &k,
        received_at: 0,
    };
    for (codec, p) in fixtures("telemetry") {
        for length in 0..p.len() {
            assert!(codec.decode(&ctx, &p[..length]).is_err());
        }
        let mut trailing = p.clone();
        trailing.push(0xff);
        assert!(codec.decode(&ctx, &trailing).is_err());
    }
    let p = fixtures("telemetry");
    for (index, codec) in [
        Box::new(CborV1::new(CodecLimits {
            input_bytes: 1,
            ..CodecLimits::default()
        })) as Box<dyn DeviceCodec>,
        Box::new(MsgpackV1::new(CodecLimits {
            decoded_bytes: 1,
            ..CodecLimits::default()
        })),
        Box::new(ProtobufV1::new(CodecLimits {
            output_messages: 0,
            ..CodecLimits::default()
        })),
    ]
    .iter()
    .enumerate()
    {
        assert!(codec.decode(&ctx, &p[index + 1].1).is_err());
    }
}
fn binary_map_cases(v: serde_json::Value) -> Vec<Vec<u8>> {
    let mut cbor = Vec::new();
    ciborium::into_writer(&v, &mut cbor).unwrap();
    vec![cbor, rmp_serde::to_vec_named(&v).unwrap()]
}
#[test]
fn map_schema_errors_and_resource_boundaries() {
    use serde_json::json;
    let k = key();
    let ctx = DecodeContext {
        device: &k,
        received_at: 0,
    };
    let codecs: [Box<dyn DeviceCodec>; 2] =
        [Box::new(CborV1::default()), Box::new(MsgpackV1::default())];
    let base =
        json!({"schema_version":1,"source_message_id":"s","kind":"telemetry","data":{"n":1}});
    let mut bad = Vec::new();
    for version in [0, 2, 65536] {
        let mut v = base.clone();
        v["schema_version"] = json!(version);
        bad.push(v);
    }
    for name in [
        "identity",
        "kind",
        "empty",
        "long",
        "deep",
        "many",
        "timestamp",
        "source",
        "null",
        "array",
        "control",
    ] {
        let mut v = base.clone();
        match name {
            "identity" => v["device"] = json!("spoof"),
            "kind" => v["kind"] = json!("bogus"),
            "empty" => v["data"] = json!({}),
            "long" => v["data"] = json!({"n":"x".repeat(257)}),
            "deep" => v["data"] = json!({"n":{"x":{"x":{"x":{"x":{"x":{"x":{"x":1}}}}}}}}),
            "many" => {
                v["data"] = serde_json::Value::Object(
                    (0..65).map(|i| (format!("f{i}"), json!(1))).collect(),
                )
            }
            "timestamp" => v["occurred_at"] = json!(-1),
            "source" => v["source_message_id"] = json!("bad/id"),
            "null" => v["data"] = json!({"n":null}),
            "array" => v["data"] = json!({"n":[1]}),
            _ => v["data"] = json!({"n":"bad\ntext"}),
        }
        bad.push(v);
    }
    for v in bad {
        for (i, p) in binary_map_cases(v.clone()).iter().enumerate() {
            assert!(codecs[i].decode(&ctx, p).is_err(), "{v}");
        }
    }
    for p in [
        vec![0xbb, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0x7b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
        vec![0x81, 0x61, 0xff, 0],
        vec![0xc1, 0],
    ] {
        assert!(codecs[0].decode(&ctx, &p).is_err());
    }
    for p in [
        vec![0xdf, 0xff, 0xff, 0xff, 0xff],
        vec![0xdb, 0xff, 0xff, 0xff, 0xff],
        vec![0x81, 0xa1, 0xff, 0],
        vec![0xc7, 0xff, 0],
    ] {
        assert!(codecs[1].decode(&ctx, &p).is_err());
    }
}
#[test]
fn duplicate_map_keys_envelope_and_data_are_rejected() {
    let k = key();
    let ctx = DecodeContext {
        device: &k,
        received_at: 0,
    };
    for (mut codec, p) in fixtures("telemetry").into_iter().skip(1).take(2) {
        let is_cbor = p[0] == 0xa5;
        let key = if is_cbor {
            b"\x6eschema_version".as_slice()
        } else {
            b"\xaeschema_version".as_slice()
        };
        let mut duplicate = p.clone();
        duplicate[0] += 1;
        duplicate.extend_from_slice(key);
        duplicate.push(1);
        assert!(codec.decode(&ctx, &duplicate).is_err());
        // A full valid envelope containing two identical telemetry keys.
        let mut duplicate = p.clone();
        let data_key = if is_cbor {
            b"\x64data".as_slice()
        } else {
            b"\xa4data".as_slice()
        };
        let index = duplicate
            .windows(data_key.len())
            .position(|v| v == data_key)
            .unwrap()
            + data_key.len();
        duplicate.truncate(index);
        let mut map = if is_cbor {
            vec![0xa2, 0x61, b'n', 1, 0x61, b'n', 2]
        } else {
            vec![0x82, 0xa1, b'n', 1, 0xa1, b'n', 2]
        };
        duplicate.append(&mut map);
        assert!(codec.decode(&ctx, &duplicate).is_err());
        // Use a small decoded budget to reject even a compact legal wire map.
        codec = if is_cbor {
            Box::new(CborV1::new(CodecLimits {
                decoded_bytes: 512,
                ..CodecLimits::default()
            }))
        } else {
            Box::new(MsgpackV1::new(CodecLimits {
                decoded_bytes: 512,
                ..CodecLimits::default()
            }))
        };
        assert!(codec.decode(&ctx, &p).is_err());
    }
}
#[test]
fn protobuf_unknown_fields_are_skipped_but_known_duplicates_rejected() {
    let k = key();
    let ctx = DecodeContext {
        device: &k,
        received_at: 0,
    };
    let codec = ProtobufV1::default();
    let p = fixtures("telemetry").pop().unwrap().1;
    let mut unknown = p.clone();
    unknown.extend_from_slice(&[0xa0, 0x06, 0x01, 0xaa, 0x06, 3, 1, 2, 3]);
    assert_eq!(
        codec.decode(&ctx, &p).unwrap()[0].kind,
        codec.decode(&ctx, &unknown).unwrap()[0].kind
    );
    for suffix in [
        &[8, 1][..],
        &[0x62, 0][..],
        &[0xaa, 6, 0xff, 0xff, 0xff, 0xff, 0x7f][..],
        &[0xff; 11][..],
    ] {
        let mut bad = p.clone();
        bad.extend_from_slice(suffix);
        assert!(codec.decode(&ctx, &bad).is_err());
    }
    let mut wire = wire::Uplink::decode(p.as_slice()).unwrap();
    wire.schema_version = 2;
    assert!(codec.decode(&ctx, &wire.encode_to_vec()).is_err());
    wire.schema_version = 1;
    if let Some(wire::uplink::Kind::Telemetry(t)) = &mut wire.kind {
        t.fields.push(t.fields[0].clone());
    }
    assert!(codec.decode(&ctx, &wire.encode_to_vec()).is_err());
    wire.kind = Some(wire::uplink::Kind::Telemetry(wire::Telemetry {
        fields: vec![wire::Field {
            name: "x".into(),
            value: Some(wire::Scalar {
                value: Some(wire::scalar::Value::Number(f64::NAN)),
            }),
        }],
    }));
    assert!(codec.decode(&ctx, &wire.encode_to_vec()).is_err());
}
#[test]
fn all_command_encoders_preserve_identity_expiry_and_typed_arguments() {
    let k = key();
    let c = DeviceCommand {
        command_id: CommandId(uuid::Uuid::from_u128(1)),
        device: k.clone(),
        expires_at: Some(1000),
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: BTreeMap::from([
                ("number".into(), Scalar::Number(42.0)),
                ("boolean".into(), Scalar::Boolean(true)),
                ("text".into(), Scalar::Text("温度".into())),
            ]),
        },
    };
    for (i, (codec, _)) in fixtures("telemetry").iter().enumerate() {
        let bytes = codec.encode(&EncodeContext { device: &k }, &c).unwrap();
        if i == 3 {
            let w = wire::DeviceCommand::decode(bytes.as_slice()).unwrap();
            assert_eq!(w.schema_version, 1);
            assert_eq!(w.command_id, c.command_id.0.to_string());
            assert_eq!(w.expires_at, c.expires_at);
            assert_eq!(w.device.unwrap().device_id, "d");
            assert_eq!(w.arguments.len(), 3);
            assert_eq!(w.name, "set");
        } else {
            let value: serde_json::Value = match i {
                0 => serde_json::from_slice(&bytes).unwrap(),
                1 => ciborium::from_reader(bytes.as_slice()).unwrap(),
                _ => rmp_serde::from_slice(&bytes).unwrap(),
            };
            assert_eq!(value["command_id"], c.command_id.0.to_string());
            assert_eq!(value["device"]["device_id"], "d");
            assert_eq!(value["expires_at"], 1000);
            assert_eq!(value["payload"]["arguments"]["boolean"], true);
            assert_eq!(value["payload"]["arguments"]["text"], "温度");
        }
        let mut wrong = k.clone();
        wrong.device_id = DeviceId::new("other").unwrap();
        assert!(codec.encode(&EncodeContext { device: &wrong }, &c).is_err());
        let mut bad = c.clone();
        bad.payload
            .arguments
            .insert("nan".into(), Scalar::Number(f64::NAN));
        assert!(codec.encode(&EncodeContext { device: &k }, &bad).is_err());
    }
    for codec in [
        Box::new(CborV1::new(CodecLimits {
            decoded_bytes: 1,
            ..CodecLimits::default()
        })) as Box<dyn DeviceCodec>,
        Box::new(MsgpackV1::new(CodecLimits {
            decoded_bytes: 1,
            ..CodecLimits::default()
        })),
        Box::new(ProtobufV1::new(CodecLimits {
            decoded_bytes: 1,
            ..CodecLimits::default()
        })),
    ] {
        assert!(codec.encode(&EncodeContext { device: &k }, &c).is_err());
    }
}
#[test]
fn deterministic_arbitrary_binary_input_never_panics() {
    let k = key();
    let ctx = DecodeContext {
        device: &k,
        received_at: 0,
    };
    let codecs: [Box<dyn DeviceCodec>; 3] = [
        Box::new(CborV1::default()),
        Box::new(MsgpackV1::default()),
        Box::new(ProtobufV1::default()),
    ];
    let mut state = 42u64;
    for len in 0..512 {
        let mut p = Vec::new();
        for _ in 0..len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            p.push(state as u8);
        }
        for c in &codecs {
            assert_eq!(
                c.decode(&ctx, &p).is_ok(),
                c.validate_payload(&ctx, &p).is_ok()
            );
        }
    }
}

#[test]
fn exact_command_wire_matches_independent_fixed_vectors() {
    let key = key();
    let command = DeviceCommand {
        command_id: CommandId(uuid::Uuid::from_u128(1)),
        device: key.clone(),
        expires_at: Some(1000),
        payload: DeviceCommandPayload {
            name: "set".into(),
            arguments: BTreeMap::from([
                ("boolean".into(), Scalar::Boolean(true)),
                ("number".into(), Scalar::Number(42.0)),
                ("text".into(), Scalar::Text("温度".into())),
            ]),
        },
    };
    for (codec, expected) in fixtures("command") {
        assert_eq!(
            codec
                .encode(&EncodeContext { device: &key }, &command)
                .unwrap(),
            expected
        );
    }
}
