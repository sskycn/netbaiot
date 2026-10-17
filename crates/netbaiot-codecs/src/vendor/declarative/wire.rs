use super::*;
use crate::common::valid_text;
use std::ops::Range;

pub(super) struct Integer {
    pub range: Range<usize>,
    pub endian: Endian,
}
fn unsigned(bytes: &[u8], endian: Endian) -> u64 {
    match endian {
        Endian::Big => bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b)),
        Endian::Little => bytes.iter().rev().fold(0, |n, b| (n << 8) | u64::from(*b)),
    }
}
fn put(bytes: &mut [u8], n: u64, endian: Endian) {
    let width = bytes.len();
    match endian {
        Endian::Big => bytes.copy_from_slice(&n.to_be_bytes()[8 - width..]),
        Endian::Little => bytes.copy_from_slice(&n.to_le_bytes()[..width]),
    }
}
impl Integer {
    pub fn read(&self, data: &[u8]) -> Result<u64, CodecError> {
        Ok(unsigned(
            data.get(self.range.clone()).ok_or(CodecError)?,
            self.endian,
        ))
    }
    pub fn write(&self, data: &mut [u8], n: u64) -> Result<(), CodecError> {
        let width = self.range.len();
        if width < 8 && n >= (1u64 << (width * 8)) {
            return Err(CodecError);
        }
        put(
            data.get_mut(self.range.clone()).ok_or(CodecError)?,
            n,
            self.endian,
        );
        Ok(())
    }
}

pub(super) struct Value {
    pub range: Range<usize>,
    pub data_type: DataType,
    pub endian: Endian,
    pub scale: f64,
    pub bias: f64,
}
impl Value {
    fn bytes<'a>(&self, data: &'a [u8]) -> Result<&'a [u8], CodecError> {
        data.get(self.range.clone()).ok_or(CodecError)
    }
    pub fn source(&self, data: &[u8]) -> Result<SourceMessageId, CodecError> {
        let bytes = self.bytes(data)?;
        match self.data_type {
            DataType::Utf8 => {
                SourceMessageId::new(std::str::from_utf8(bytes).map_err(|_| CodecError)?)
                    .map_err(|_| CodecError)
            }
            _ => crate::vendor::sequence_id(unsigned(bytes, self.endian)),
        }
    }
    pub fn read(&self, data: &[u8]) -> Result<Scalar, CodecError> {
        let bytes = self.bytes(data)?;
        if matches!(self.data_type, DataType::Utf8) {
            let text = std::str::from_utf8(bytes).map_err(|_| CodecError)?;
            if !valid_text(text, self.range.len()) {
                return Err(CodecError);
            }
            return Ok(Scalar::Text(text.to_owned()));
        }
        let bits = unsigned(bytes, self.endian);
        let raw = match self.data_type {
            DataType::Bool => {
                return match bits {
                    0 => Ok(Scalar::Boolean(false)),
                    1 => Ok(Scalar::Boolean(true)),
                    _ => Err(CodecError),
                };
            }
            DataType::F32 => f64::from(f32::from_bits(bits as u32)),
            DataType::F64 => f64::from_bits(bits),
            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 => {
                let shift = (8 - bytes.len()) * 8;
                let signed = ((bits << shift) as i64) >> shift;
                if signed.unsigned_abs() > (1 << 53) {
                    return Err(CodecError);
                }
                signed as f64
            }
            _ => {
                if bits > (1 << 53) {
                    return Err(CodecError);
                }
                bits as f64
            }
        };
        let number = raw * self.scale + self.bias;
        if !raw.is_finite() || !number.is_finite() {
            return Err(CodecError);
        }
        Ok(Scalar::Number(number))
    }
    pub fn write(&self, data: &mut [u8], scalar: &Scalar) -> Result<(), CodecError> {
        let bytes = data.get_mut(self.range.clone()).ok_or(CodecError)?;
        let bits = match (self.data_type, scalar) {
            (DataType::Utf8, Scalar::Text(text))
                if text.len() == bytes.len() && valid_text(text, bytes.len()) =>
            {
                bytes.copy_from_slice(text.as_bytes());
                return Ok(());
            }
            (DataType::Bool, Scalar::Boolean(b)) => u64::from(*b),
            (DataType::Utf8 | DataType::Bool, _) => return Err(CodecError),
            (kind, Scalar::Number(number)) if number.is_finite() => {
                let raw = (number - self.bias) / self.scale;
                if !raw.is_finite() || (raw == 0.0 && *number != self.bias) {
                    return Err(CodecError);
                }
                match kind {
                    DataType::F64 => raw.to_bits(),
                    DataType::F32 => {
                        let value = raw as f32;
                        if !value.is_finite()
                            || !crate::vendor::inverse_number_matches(raw, f64::from(value))
                        {
                            return Err(CodecError);
                        }
                        u64::from(value.to_bits())
                    }
                    _ => {
                        // Prove representability in the raw domain before checking the
                        // normalized readback. This prevents a large scale or bias from
                        // hiding integer quantization, especially a nonzero raw value
                        // rounded to zero.
                        let integer = raw.round();
                        if !crate::vendor::inverse_number_matches(raw, integer)
                            || integer.abs() > (1u64 << 53) as f64
                        {
                            return Err(CodecError);
                        }
                        let signed = matches!(
                            kind,
                            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64
                        );
                        let width = bytes.len() * 8;
                        let (min, max_exclusive) = if signed {
                            let magnitude = 2f64.powi((width - 1) as i32);
                            (-magnitude, magnitude)
                        } else {
                            (0.0, 2f64.powi(width as i32))
                        };
                        if integer < min || integer >= max_exclusive {
                            return Err(CodecError);
                        }
                        if signed {
                            integer as i64 as u64
                        } else {
                            integer as u64
                        }
                    }
                }
            }
            _ => return Err(CodecError),
        };
        put(bytes, bits, self.endian);
        // Quantization was checked in the raw domain above. The readback check now
        // has only one job: admit bounded multiply/add roundoff without allowing a
        // scale/bias combination to erase the entire nonzero signal.
        let actual = self.read(data)?;
        let preserves_value = match (&actual, scalar) {
            (Scalar::Number(actual), Scalar::Number(expected)) => {
                if self.scale == 1.0 && self.bias == 0.0 {
                    actual == expected
                } else {
                    !(*actual == self.bias && *expected != self.bias)
                        && crate::vendor::normalized_number_matches(*expected, *actual)
                }
            }
            _ => actual == *scalar,
        };
        if !preserves_value {
            return Err(CodecError);
        }
        Ok(())
    }
}

pub(super) struct Check {
    pub value: Integer,
    pub covered: Range<usize>,
    pub algorithm: ChecksumAlgorithm,
}
impl Check {
    pub fn calculate(&self, data: &[u8]) -> Result<u64, CodecError> {
        let bytes = data.get(self.covered.clone()).ok_or(CodecError)?;
        Ok(match self.algorithm {
            ChecksumAlgorithm::Sum8 => {
                u64::from(bytes.iter().fold(0u8, |sum, b| sum.wrapping_add(*b)))
            }
            ChecksumAlgorithm::Xor8 => u64::from(bytes.iter().fold(0u8, |sum, b| sum ^ b)),
            // CRC-16/MODBUS: init FFFF, reflected polynomial A001, xorout 0000.
            ChecksumAlgorithm::Crc16Modbus => {
                let mut crc = 0xffffu16;
                for byte in bytes {
                    crc ^= u16::from(*byte);
                    for _ in 0..8 {
                        crc = if crc & 1 == 1 {
                            (crc >> 1) ^ 0xa001
                        } else {
                            crc >> 1
                        };
                    }
                }
                u64::from(crc)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numeric_commands_reject_material_round_trip_loss() {
        for endian in [Endian::Big, Endian::Little] {
            for (kind, width, scale, bias, number) in [
                (DataType::F32, 4, 1.0, 0.0, 16_777_217.0),
                (DataType::F32, 4, 1.0, 0.0, 1e-50),
                (DataType::F32, 4, 1.0, 0.0, 1.0f64.next_up()),
                (DataType::I16, 2, 1.0, 0.0, 5.0f64.next_up()),
                (DataType::F64, 8, 1.0, 1e20, 1.0),
                (DataType::F64, 8, 1e300, 0.0, 1e-300),
                (DataType::I16, 2, 0.1, 0.0, 12.30000000001),
                (DataType::U64, 8, 1.0, 1e20, 1.0),
            ] {
                let field = Value {
                    range: 0..width,
                    data_type: kind,
                    endian,
                    scale,
                    bias,
                };
                assert!(
                    field.write(&mut [0; 8], &Scalar::Number(number)).is_err(),
                    "{kind:?} scale={scale} bias={bias} number={number}"
                );
            }
        }
    }

    #[test]
    fn decimal_scales_and_extremes_only_admit_bounded_roundoff() {
        for endian in [Endian::Big, Endian::Little] {
            for (kind, width, scale, bias, number) in [
                (DataType::I16, 2, 0.1, 0.0, 5.1),
                (DataType::I16, 2, -0.1, 0.0, -5.1),
                (DataType::U16, 2, 0.1, 0.0, 0.3),
                (DataType::F32, 4, 0.1, 0.0, 1.7),
                (DataType::F64, 8, 0.1, 2.0, 5.1),
                (DataType::F64, 8, 1e-300, 0.0, 1e-300),
                (DataType::F64, 8, 1e300, 0.0, 1e300),
            ] {
                let field = Value {
                    range: 0..width,
                    data_type: kind,
                    endian,
                    scale,
                    bias,
                };
                let mut bytes = [0; 8];
                field.write(&mut bytes, &Scalar::Number(number)).unwrap();
                let Scalar::Number(actual) = field.read(&bytes).unwrap() else {
                    panic!()
                };
                assert!(crate::vendor::normalized_number_matches(number, actual));
            }
            for (scale, number) in [(1e-300, 1e300), (1e300, 1e-300)] {
                let field = Value {
                    range: 0..8,
                    data_type: DataType::F64,
                    endian,
                    scale,
                    bias: 0.0,
                };
                assert!(field.write(&mut [0; 8], &Scalar::Number(number)).is_err());
            }
        }
        assert!(!crate::vendor::normalized_number_matches(
            0.0,
            f64::from_bits(1)
        ));
        assert!(!crate::vendor::normalized_number_matches(
            f64::from_bits(1),
            f64::from_bits(2)
        ));
    }

    #[test]
    fn biased_decimal_inverse_preserves_the_command_value() {
        for endian in [Endian::Big, Endian::Little] {
            let decimal = Value {
                range: 0..2,
                data_type: DataType::I16,
                endian,
                scale: 0.1,
                bias: 1.0,
            };
            for (number, raw) in [(-0.1, -11i16), (0.0, -10), (0.1, -9), (1.0, 0), (1.1, 1)] {
                let mut bytes = [0; 2];
                decimal.write(&mut bytes, &Scalar::Number(number)).unwrap();
                assert_eq!(unsigned(&bytes, endian) as u16, raw as u16, "{number}");
            }
        }
    }

    #[test]
    fn extreme_scale_does_not_hide_integer_quantization() {
        for endian in [Endian::Big, Endian::Little] {
            for (data_type, width) in [(DataType::I16, 2), (DataType::F32, 4)] {
                let extreme = Value {
                    range: 0..width,
                    data_type,
                    endian,
                    scale: 1e300,
                    bias: 1e20,
                };
                for number in [1e20f64.next_down(), 1e20f64.next_up()] {
                    assert!(
                        extreme.write(&mut [0; 8], &Scalar::Number(number)).is_err(),
                        "{data_type:?} {number}"
                    );
                }
            }
        }
    }

    #[test]
    fn representable_numeric_boundaries_survive_both_endiannesses() {
        for endian in [Endian::Big, Endian::Little] {
            for (kind, width, values) in [
                (DataType::U8, 1, [0.0, 1.0, 255.0]),
                (DataType::I8, 1, [-128.0, 0.0, 127.0]),
                (DataType::U16, 2, [0.0, 1.0, 65535.0]),
                (DataType::I16, 2, [-32768.0, 0.0, 32767.0]),
                (DataType::U32, 4, [0.0, 1.0, f64::from(u32::MAX)]),
                (
                    DataType::I32,
                    4,
                    [f64::from(i32::MIN), 0.0, f64::from(i32::MAX)],
                ),
                (DataType::U64, 8, [0.0, 1.0, 9007199254740992.0]),
                (
                    DataType::I64,
                    8,
                    [-9007199254740992.0, 0.0, 9007199254740992.0],
                ),
                (
                    DataType::F32,
                    4,
                    [f64::from(f32::from_bits(1)), 0.0, f64::from(f32::MAX)],
                ),
                (DataType::F64, 8, [f64::from_bits(1), 0.0, f64::MAX]),
            ] {
                for raw in values {
                    for (scale, bias) in [(1.0, 0.0), (-0.5, 0.0)] {
                        let field = Value {
                            range: 0..width,
                            data_type: kind,
                            endian,
                            scale,
                            bias,
                        };
                        let scalar = Scalar::Number(raw * scale + bias);
                        let mut bytes = [0; 8];
                        field.write(&mut bytes, &scalar).unwrap();
                        assert_eq!(field.read(&bytes).unwrap(), scalar);
                    }
                }
            }
        }
    }

    #[test]
    fn crc16_modbus_standard_check_vector() {
        let check = Check {
            value: Integer {
                range: 9..11,
                endian: Endian::Little,
            },
            covered: 0..9,
            algorithm: ChecksumAlgorithm::Crc16Modbus,
        };
        assert_eq!(check.calculate(b"123456789").unwrap(), 0x4b37);
    }
}
