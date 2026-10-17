use netbaiot_codecs::vendor::declarative::*;
use netbaiot_core::*;

fn unsigned(bytes: &[u8], endian: Endian) -> u64 {
    match endian {
        Endian::Big => bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b)),
        Endian::Little => bytes.iter().rev().fold(0, |n, b| (n << 8) | u64::from(*b)),
    }
}

fn inverse_matches(calculated: f64, representable: f64) -> bool {
    if !calculated.is_finite() || !representable.is_finite() {
        return false;
    }
    if calculated == representable {
        return true;
    }
    (calculated - representable).abs()
        <= (4.0 * f64::EPSILON * calculated.abs().max(representable.abs())).min(1e-7)
}

// Independently inspect the actual command bytes. Encoding success must never
// silently quantize a number, lose timestamp bits, or exceed the codec bound.
pub fn commands(config: &DeclarativeConfig, codec: &dyn DeviceCodec, data: &[u8]) {
    let device = DeviceKey {
        tenant_id: TenantId::new("t").unwrap(),
        product_id: ProductId::new("p").unwrap(),
        device_id: DeviceId::new("d").unwrap(),
    };
    let mut seed = [0; 8];
    let len = data.len().min(8);
    seed[..len].copy_from_slice(&data[..len]);
    let arbitrary = f64::from_le_bytes(seed);
    for plan in &config.commands {
        for sample in [0.0, 1.25, arbitrary] {
            let mut command = DeviceCommand {
                command_id: serde_json::from_str("\"00000000-0000-0000-0000-000000000001\"")
                    .unwrap(),
                device: device.clone(),
                expires_at: Some(if sample == 0.0 {
                    i64::MAX
                } else {
                    i64::from_le_bytes(seed)
                }),
                payload: DeviceCommandPayload {
                    name: plan.name.clone(),
                    arguments: Default::default(),
                },
            };
            for named in &plan.fields {
                let field = &named.field;
                let value = match field.data_type {
                    DataType::Bool => Scalar::Boolean(seed[0] & 1 != 0),
                    DataType::Utf8 => Scalar::Text("x".repeat(field.length.unwrap())),
                    _ => Scalar::Number(sample),
                };
                command.payload.arguments.insert(named.name.clone(), value);
            }
            let Ok(wire) = codec.encode(&EncodeContext { device: &device }, &command) else {
                continue;
            };
            assert_eq!(wire.len(), plan.size);
            assert!(wire.len() <= CodecLimits::default().input_bytes);
            assert_eq!(
                &wire[plan.command_id_offset..plan.command_id_offset + 16],
                command.command_id.0.as_bytes()
            );
            if let Some(expiry) = &plan.expires_at {
                assert!(matches!(expiry.data_type, IntegerType::U64));
                assert_eq!(
                    unsigned(&wire[expiry.offset..expiry.offset + 8], expiry.endian),
                    command.expires_at.unwrap() as u64
                );
            }
            for named in &plan.fields {
                let field = &named.field;
                let width = match field.data_type {
                    DataType::U8 | DataType::I8 | DataType::Bool => 1,
                    DataType::U16 | DataType::I16 => 2,
                    DataType::U32 | DataType::I32 | DataType::F32 => 4,
                    DataType::U64 | DataType::I64 | DataType::F64 => 8,
                    DataType::Utf8 => field.length.unwrap(),
                };
                let bytes = &wire[field.offset..field.offset + width];
                if matches!(field.data_type, DataType::Utf8) {
                    assert_eq!(
                        Scalar::Text(std::str::from_utf8(bytes).unwrap().into()),
                        command.payload.arguments[&named.name]
                    );
                    continue;
                }
                let bits = unsigned(bytes, field.endian);
                let raw = match field.data_type {
                    DataType::Bool => {
                        assert_eq!(
                            Scalar::Boolean(bits == 1),
                            command.payload.arguments[&named.name]
                        );
                        continue;
                    }
                    DataType::F32 => f64::from(f32::from_bits(bits as u32)),
                    DataType::F64 => f64::from_bits(bits),
                    DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 => {
                        let shift = (8 - width) * 8;
                        (((bits << shift) as i64) >> shift) as f64
                    }
                    _ => bits as f64,
                };
                let actual = raw * field.scale + field.bias;
                let Scalar::Number(expected) = &command.payload.arguments[&named.name] else {
                    panic!("encoded numeric field has a nonnumeric argument");
                };
                assert!(inverse_matches(
                    (*expected - field.bias) / field.scale,
                    raw
                ));
                assert!(
                    actual == *expected
                        || ((field.scale != 1.0 || field.bias != 0.0)
                            && !(actual == field.bias && *expected != field.bias)
                            && (actual - *expected).abs()
                                <= 4.0 * f64::EPSILON * actual.abs().max(expected.abs()))
                );
            }
        }
    }
}
