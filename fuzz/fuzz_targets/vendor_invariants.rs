use netbaiot_codecs::vendor::declarative::*;
use netbaiot_core::*;
use num_bigint::BigInt;
use num_rational::BigRational;

fn unsigned(bytes: &[u8], endian: Endian) -> u64 {
    match endian {
        Endian::Big => bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b)),
        Endian::Little => bytes.iter().rev().fold(0, |n, b| (n << 8) | u64::from(*b)),
    }
}

fn decimal(text: &str) -> Option<BigRational> {
    let (mantissa, exponent) = if let Some(index) = text.find(['e', 'E']) {
        (&text[..index], text[index + 1..].parse::<i32>().ok()?)
    } else {
        (text, 0)
    };
    let (negative, mantissa) = mantissa
        .strip_prefix('-')
        .map_or((false, mantissa), |rest| (true, rest));
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let mut numerator = format!("{integer}{fraction}").parse::<BigInt>().ok()?;
    if negative {
        numerator = -numerator;
    }
    let decimal_places = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    if decimal_places >= 0 {
        Some(BigRational::new(
            numerator,
            BigInt::from(10u8).pow(decimal_places as u32),
        ))
    } else {
        Some(BigRational::from_integer(
            numerator * BigInt::from(10u8).pow(decimal_places.unsigned_abs()),
        ))
    }
}

fn scalar_decimal(scalar: &Scalar) -> Option<BigRational> {
    match scalar {
        Scalar::Number(number) if number.is_finite() => decimal(&number.to_string()),
        Scalar::Decimal(number) => decimal(number.as_str()),
        _ => None,
    }
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
        for (sample_index, sample) in [0.0, 1.25, arbitrary].into_iter().enumerate() {
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
                    _ if field.conversion.is_some() && sample_index == 1 => Scalar::Decimal(
                        DecimalScalar::new(["0", "0.1", "-5.1", "5.15"][usize::from(seed[0]) % 4])
                            .unwrap(),
                    ),
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
                match field.data_type {
                    DataType::Bool => {
                        assert_eq!(
                            Scalar::Boolean(bits == 1),
                            command.payload.arguments[&named.name]
                        );
                        continue;
                    }
                    DataType::F32 => {
                        assert!(f32::from_bits(bits as u32).is_finite());
                        continue;
                    }
                    DataType::F64 => {
                        assert!(f64::from_bits(bits).is_finite());
                        continue;
                    }
                    DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 => {
                        let shift = (8 - width) * 8;
                        let raw = ((bits << shift) as i64) >> shift;
                        let raw = BigRational::from_integer(BigInt::from(raw));
                        let (scale, bias) = if let Some(conversion) = &field.conversion {
                            (
                                decimal(&conversion.scale).unwrap(),
                                decimal(&conversion.bias).unwrap(),
                            )
                        } else {
                            (BigRational::from_integer(1.into()), BigRational::default())
                        };
                        assert_eq!(
                            scalar_decimal(&command.payload.arguments[&named.name]).unwrap(),
                            raw * scale + bias
                        );
                        continue;
                    }
                    _ => {
                        let raw = BigRational::from_integer(BigInt::from(bits));
                        let (scale, bias) = if let Some(conversion) = &field.conversion {
                            (
                                decimal(&conversion.scale).unwrap(),
                                decimal(&conversion.bias).unwrap(),
                            )
                        } else {
                            (BigRational::from_integer(1.into()), BigRational::default())
                        };
                        assert_eq!(
                            scalar_decimal(&command.payload.arguments[&named.name]).unwrap(),
                            raw * scale + bias
                        );
                    }
                }
            }
        }
    }
}
