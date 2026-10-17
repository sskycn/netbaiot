use netbaiot_core::{CodecError, DecimalScalar, Scalar};
use std::fmt::{self, Write};

pub(crate) const MAX_DECIMAL_PARAMETER_EXPONENT: i16 = 38;
pub(crate) const MAX_DECIMAL_SIGNIFICANT_DIGITS: usize = 38;
const MAX_DECIMAL_SCALAR_EXPONENT: i16 = 324;
const MAX_DECIMAL_SCALAR_SIGNIFICANT_DIGITS: usize = 39;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Decimal {
    coefficient: i128,
    exponent: i16,
}

impl Decimal {
    pub(crate) const ZERO: Self = Self {
        coefficient: 0,
        exponent: 0,
    };
    pub(crate) const ONE: Self = Self {
        coefficient: 1,
        exponent: 0,
    };

    pub(crate) fn parse(text: &str) -> Result<Self, &'static str> {
        Self::parse_bounded(
            text,
            MAX_DECIMAL_SCALAR_SIGNIFICANT_DIGITS,
            MAX_DECIMAL_SCALAR_EXPONENT,
        )
    }

    pub(crate) fn parse_parameter(text: &str) -> Result<Self, &'static str> {
        Self::parse_bounded(
            text,
            MAX_DECIMAL_SIGNIFICANT_DIGITS,
            MAX_DECIMAL_PARAMETER_EXPONENT,
        )
    }

    fn parse_bounded(
        text: &str,
        maximum_significant_digits: usize,
        maximum_exponent: i16,
    ) -> Result<Self, &'static str> {
        let bytes = text.as_bytes();
        if bytes.is_empty() || bytes.len() > netbaiot_core::MAX_DECIMAL_BYTES {
            return Err("Decimal text must contain between 1 and 64 bytes.");
        }
        let negative = bytes.first() == Some(&b'-');
        let mut at = usize::from(negative);
        let integer_start = at;
        while bytes.get(at).is_some_and(u8::is_ascii_digit) {
            at += 1;
        }
        if at == integer_start {
            return Err("Decimal requires digits before the decimal point.");
        }
        let mut fraction_digits = 0usize;
        if bytes.get(at) == Some(&b'.') {
            at += 1;
            let fraction_start = at;
            while bytes.get(at).is_some_and(u8::is_ascii_digit) {
                at += 1;
            }
            if at == fraction_start {
                return Err("Decimal point must be followed by digits.");
            }
            fraction_digits = at - fraction_start;
        }
        let mantissa_end = at;
        let explicit_exponent = if matches!(bytes.get(at), Some(b'e' | b'E')) {
            at += 1;
            let exponent_negative = bytes.get(at) == Some(&b'-');
            if matches!(bytes.get(at), Some(b'+' | b'-')) {
                at += 1;
            }
            let exponent_start = at;
            let mut exponent = 0i32;
            while let Some(digit) = bytes.get(at).filter(|digit| digit.is_ascii_digit()) {
                exponent = exponent
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(i32::from(*digit - b'0')))
                    .ok_or("Decimal exponent exceeds the supported range.")?;
                at += 1;
            }
            if at == exponent_start {
                return Err("Decimal exponent requires digits.");
            }
            if exponent_negative {
                exponent
                    .checked_neg()
                    .ok_or("Decimal exponent exceeds the supported range.")?
            } else {
                exponent
            }
        } else {
            0
        };
        if at != bytes.len() {
            return Err("Decimal contains an unsupported character.");
        }

        let mut first_nonzero = None;
        let mut last_nonzero = None;
        let mut digit_index = 0usize;
        for byte in &bytes[usize::from(negative)..mantissa_end] {
            if *byte == b'.' {
                continue;
            }
            if *byte != b'0' {
                first_nonzero.get_or_insert(digit_index);
                last_nonzero = Some(digit_index);
            }
            digit_index += 1;
        }
        let Some(first_nonzero) = first_nonzero else {
            if !(-i32::from(maximum_exponent)..=i32::from(maximum_exponent))
                .contains(&explicit_exponent)
            {
                return Err("Decimal exponent exceeds the supported bounded range.");
            }
            return Ok(Self::ZERO);
        };
        let last_nonzero = last_nonzero.ok_or("Decimal coefficient is invalid.")?;
        let significant_digits = last_nonzero - first_nonzero + 1;
        if significant_digits > maximum_significant_digits {
            return Err("Decimal has too many significant digits for the bounded decimal engine.");
        }

        let trailing_zeros = digit_index - last_nonzero - 1;
        let mut magnitude = 0u128;
        let mut current_digit = 0usize;
        for byte in &bytes[usize::from(negative)..mantissa_end] {
            if *byte == b'.' {
                continue;
            }
            if (first_nonzero..=last_nonzero).contains(&current_digit) {
                magnitude = magnitude
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(u128::from(*byte - b'0')))
                    .ok_or("Decimal coefficient exceeds i128.")?;
            }
            current_digit += 1;
        }
        let coefficient = if negative {
            if magnitude == (i128::MAX as u128) + 1 {
                i128::MIN
            } else {
                i128::try_from(magnitude)
                    .ok()
                    .and_then(i128::checked_neg)
                    .ok_or("Decimal coefficient exceeds i128.")?
            }
        } else {
            i128::try_from(magnitude).map_err(|_| "Decimal coefficient exceeds i128.")?
        };
        let exponent = explicit_exponent
            .checked_sub(i32::try_from(fraction_digits).map_err(|_| "Decimal is too long.")?)
            .and_then(|n| n.checked_add(i32::try_from(trailing_zeros).ok()?))
            .ok_or("Decimal exponent exceeds the supported range.")?;
        if !(-i32::from(maximum_exponent)..=i32::from(maximum_exponent)).contains(&exponent) {
            return Err("Decimal exponent exceeds the supported bounded range.");
        }
        Ok(Self {
            coefficient,
            exponent: i16::try_from(exponent)
                .map_err(|_| "Decimal exponent exceeds the supported range.")?,
        })
    }

    pub(crate) fn from_f64(value: f64) -> Result<Self, CodecError> {
        if !value.is_finite() {
            return Err(CodecError);
        }
        let mut buffer = ryu::Buffer::new();
        Self::parse(buffer.format_finite(value)).map_err(|_| CodecError)
    }

    fn into_scalar(self) -> Result<Scalar, CodecError> {
        let mut value = DecimalText::default();
        self.write_to(&mut value).map_err(|_| CodecError)?;
        Ok(Scalar::Decimal(
            DecimalScalar::new(value.as_str()?).map_err(|_| CodecError)?,
        ))
    }

    fn write_to(self, output: &mut impl Write) -> fmt::Result {
        if self.coefficient == 0 {
            return output.write_char('0');
        }
        if self.coefficient < 0 {
            output.write_char('-')?;
        }
        let mut digits = [0u8; 39];
        let mut start = digits.len();
        let mut magnitude = self.coefficient.unsigned_abs();
        while magnitude != 0 {
            start -= 1;
            digits[start] = b'0' + (magnitude % 10) as u8;
            magnitude /= 10;
        }
        let digits = &digits[start..];
        if self.exponent >= 0 {
            output.write_str(std::str::from_utf8(digits).map_err(|_| fmt::Error)?)?;
            for _ in 0..self.exponent {
                output.write_char('0')?;
            }
        } else {
            let fractional = usize::from(self.exponent.unsigned_abs());
            if fractional < digits.len() {
                let split = digits.len() - fractional;
                output.write_str(std::str::from_utf8(&digits[..split]).map_err(|_| fmt::Error)?)?;
                output.write_char('.')?;
                output.write_str(std::str::from_utf8(&digits[split..]).map_err(|_| fmt::Error)?)?;
            } else {
                output.write_str("0.")?;
                for _ in 0..fractional - digits.len() {
                    output.write_char('0')?;
                }
                output.write_str(std::str::from_utf8(digits).map_err(|_| fmt::Error)?)?;
            }
        }
        Ok(())
    }
}

fn normalize(mut coefficient: i128, mut exponent: i16) -> Option<Decimal> {
    if coefficient == 0 {
        return Some(Decimal::ZERO);
    }
    while coefficient % 10 == 0 {
        coefficient /= 10;
        exponent = exponent.checked_add(1)?;
    }
    Some(Decimal {
        coefficient,
        exponent,
    })
}

fn pow10(exponent: u32) -> Option<i128> {
    let mut value = 1i128;
    for _ in 0..exponent {
        value = value.checked_mul(10)?;
    }
    Some(value)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IntegerConversion {
    multiplier: i128,
    offset: i128,
    exponent: i16,
    minimum: i128,
    maximum: i128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntegerConversionError {
    Conversion(&'static str),
    FieldBytes,
}

impl IntegerConversionError {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::Conversion(message) => message,
            Self::FieldBytes => {
                "Exact decoded Decimal output exceeds CodecLimits.field_bytes for part of the integer range."
            }
        }
    }
}

impl IntegerConversion {
    pub(crate) fn compile(
        scale: Decimal,
        bias: Decimal,
        minimum: i128,
        maximum: i128,
        field_bytes: usize,
    ) -> Result<Self, IntegerConversionError> {
        if scale.coefficient == 0 {
            return Err(IntegerConversionError::Conversion("Scale must be nonzero."));
        }
        let exponent = if bias.coefficient == 0 {
            scale.exponent
        } else {
            scale.exponent.min(bias.exponent)
        };
        let align = |value: Decimal| {
            if value.coefficient == 0 {
                return Some(0);
            }
            value
                .coefficient
                .checked_mul(pow10(u32::from(value.exponent.abs_diff(exponent)))?)
        };
        let multiplier = align(scale).ok_or(IntegerConversionError::Conversion(
            "Integer conversion parameters exceed the bounded i128 decimal engine.",
        ))?;
        let offset = align(bias).ok_or(IntegerConversionError::Conversion(
            "Integer conversion parameters exceed the bounded i128 decimal engine.",
        ))?;
        let plan = Self {
            multiplier,
            offset,
            exponent,
            minimum,
            maximum,
        };

        // Every runtime coefficient is the affine integer `multiplier * raw + offset`
        // at one fixed decimal exponent. Multiplication magnitude and the affine result
        // reach their extrema at an endpoint, so checking both operations at both
        // endpoints proves that every raw value in the complete integer domain is
        // representable. This deliberately does not rely on endpoint normalization:
        // interior values need not contain the same helpful trailing zeroes.
        let mut endpoint_coefficients = [0i128; 2];
        for (index, raw) in [minimum, maximum].into_iter().enumerate() {
            multiplier
                .checked_mul(raw)
                .ok_or(IntegerConversionError::Conversion(
                    "Integer conversion exceeds the bounded i128 decimal engine.",
                ))?;
            endpoint_coefficients[index] = plan.coefficient(raw).ok_or(
                IntegerConversionError::Conversion(
                    "Integer conversion exceeds the bounded i128 decimal engine for part of the raw range.",
                ),
            )?;
        }
        let maximum_length = maximum_formatted_len(endpoint_coefficients, exponent).ok_or(
            IntegerConversionError::Conversion(
                "Exact decoded value length exceeds the bounded decimal engine.",
            ),
        )?;
        if maximum_length > netbaiot_core::MAX_DECIMAL_BYTES {
            return Err(IntegerConversionError::Conversion(
                "Exact decoded value exceeds the 64-byte decimal scalar limit.",
            ));
        }
        if maximum_length > field_bytes {
            return Err(IntegerConversionError::FieldBytes);
        }
        Ok(plan)
    }

    fn coefficient(self, raw: i128) -> Option<i128> {
        self.multiplier.checked_mul(raw)?.checked_add(self.offset)
    }

    fn decimal(self, raw: i128) -> Option<Decimal> {
        normalize(self.coefficient(raw)?, self.exponent)
    }

    pub(crate) fn decode(self, raw: i128) -> Result<Scalar, CodecError> {
        if !(self.minimum..=self.maximum).contains(&raw) {
            return Err(CodecError);
        }
        self.decimal(raw).ok_or(CodecError)?.into_scalar()
    }

    pub(crate) fn encode(self, scalar: &Scalar) -> Result<i128, CodecError> {
        let value = match scalar {
            Scalar::Number(number) => Decimal::from_f64(*number)?,
            Scalar::Decimal(decimal) => Decimal::parse(decimal.as_str()).map_err(|_| CodecError)?,
            _ => return Err(CodecError),
        };
        let exponent_difference = i32::from(value.exponent) - i32::from(self.exponent);
        let coefficient = if exponent_difference >= 0 {
            value
                .coefficient
                .checked_mul(pow10(exponent_difference as u32).ok_or(CodecError)?)
                .ok_or(CodecError)?
        } else {
            let mut coefficient = value.coefficient;
            for _ in 0..exponent_difference.unsigned_abs() {
                if coefficient % 10 != 0 {
                    return Err(CodecError);
                }
                coefficient /= 10;
            }
            coefficient
        };
        let numerator = coefficient.checked_sub(self.offset).ok_or(CodecError)?;
        if numerator.checked_rem(self.multiplier) != Some(0) {
            return Err(CodecError);
        }
        let raw = numerator.checked_div(self.multiplier).ok_or(CodecError)?;
        if !(self.minimum..=self.maximum).contains(&raw) || self.decimal(raw) != Some(value) {
            return Err(CodecError);
        }
        Ok(raw)
    }
}

fn formatted_len_upper_bound(coefficient: i128, exponent: i16) -> Option<usize> {
    if coefficient == 0 {
        return Some(1);
    }
    let maximum_magnitude = coefficient.unsigned_abs();
    let negative = coefficient < 0;
    let mut digits = 1usize;
    let mut remaining = maximum_magnitude;
    while remaining >= 10 {
        remaining /= 10;
        digits = digits.checked_add(1)?;
    }
    let sign = usize::from(negative);
    if exponent >= 0 {
        sign.checked_add(digits)?
            .checked_add(usize::from(exponent.unsigned_abs()))
    } else {
        let fractional = usize::from(exponent.unsigned_abs());
        if digits > fractional {
            sign.checked_add(digits)?.checked_add(1)
        } else {
            sign.checked_add(2)?.checked_add(fractional)
        }
    }
}

fn maximum_formatted_len(endpoint_coefficients: [i128; 2], exponent: i16) -> Option<usize> {
    endpoint_coefficients
        .into_iter()
        .map(|coefficient| formatted_len_upper_bound(coefficient, exponent))
        .try_fold(0usize, |maximum, length| Some(maximum.max(length?)))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FloatConversion {
    scale: f64,
    bias: f64,
}

impl FloatConversion {
    pub(crate) fn compile(scale: f64, bias: f64) -> Result<Self, &'static str> {
        if !scale.is_finite() || scale == 0.0 {
            return Err("Scale must be finite and nonzero.");
        }
        if !bias.is_finite() {
            return Err("Bias must be finite.");
        }
        Ok(Self { scale, bias })
    }

    pub(crate) fn decode(self, raw: f64) -> Result<Scalar, CodecError> {
        let number = raw * self.scale + self.bias;
        if !raw.is_finite() || !number.is_finite() {
            return Err(CodecError);
        }
        Ok(Scalar::Number(number))
    }

    pub(crate) fn encode_f64(self, scalar: &Scalar) -> Result<f64, CodecError> {
        let Scalar::Number(number) = scalar else {
            return Err(CodecError);
        };
        if !number.is_finite() {
            return Err(CodecError);
        }
        let raw = (*number - self.bias) / self.scale;
        if !raw.is_finite() || (raw == 0.0 && *number != self.bias) {
            return Err(CodecError);
        }
        Ok(raw)
    }

    pub(crate) fn preserves(self, raw: f64, expected: f64) -> bool {
        let actual = raw * self.scale + self.bias;
        if !actual.is_finite() {
            return false;
        }
        if actual == expected {
            return true;
        }
        !(actual == self.bias && expected != self.bias)
            && (actual - expected).abs() <= 4.0 * f64::EPSILON * actual.abs().max(expected.abs())
    }
}

struct DecimalText {
    bytes: [u8; netbaiot_core::MAX_DECIMAL_BYTES],
    length: usize,
}

impl Default for DecimalText {
    fn default() -> Self {
        Self {
            bytes: [0; netbaiot_core::MAX_DECIMAL_BYTES],
            length: 0,
        }
    }
}

impl DecimalText {
    fn as_str(&self) -> Result<&str, CodecError> {
        std::str::from_utf8(&self.bytes[..self.length]).map_err(|_| CodecError)
    }
}

impl Write for DecimalText {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.length.checked_add(text.len()).ok_or(fmt::Error)?;
        self.bytes
            .get_mut(self.length..end)
            .ok_or(fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.length = end;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_parser_and_formatter_are_canonical_and_bounded() {
        for (input, coefficient, exponent, output) in [
            ("0", 0, 0, "0"),
            ("-0.000", 0, 0, "0"),
            ("001.2300", 123, -2, "1.23"),
            ("1e-3", 1, -3, "0.001"),
            ("-1200e-2", -12, 0, "-12"),
            ("1e3", 1, 3, "1000"),
        ] {
            let value = Decimal::parse(input).unwrap();
            assert_eq!(value.coefficient, coefficient, "{input}");
            assert_eq!(value.exponent, exponent, "{input}");
            let Scalar::Decimal(output_value) = value.into_scalar().unwrap() else {
                panic!()
            };
            assert_eq!(output_value.as_str(), output, "{input}");
        }
        for invalid in ["", ".1", "1.", "+1", "1e", "NaN"] {
            assert!(Decimal::parse(invalid).is_err(), "{invalid}");
        }
        for invalid in ["1e39", "1e-39"] {
            assert!(Decimal::parse_parameter(invalid).is_err(), "{invalid}");
            assert!(Decimal::parse(invalid).is_ok(), "{invalid}");
        }
        assert!(Decimal::parse_parameter("0e39").is_err());
        assert!(Decimal::parse("0e325").is_err());
        assert_eq!(
            Decimal::parse("-170141183460469231731687303715884105728")
                .unwrap()
                .coefficient,
            i128::MIN
        );
    }

    #[test]
    fn exact_integer_inverse_never_rounds() {
        let plan = IntegerConversion::compile(
            Decimal::parse_parameter("0.1").unwrap(),
            Decimal::parse_parameter("0.3").unwrap(),
            i16::MIN.into(),
            i16::MAX.into(),
            netbaiot_core::MAX_DECIMAL_BYTES,
        )
        .unwrap();
        assert_eq!(plan.encode(&Scalar::Number(0.0)).unwrap(), -3);
        assert!(plan.encode(&Scalar::Number(5.15)).is_err());
        assert_eq!(
            plan.encode(&Scalar::Decimal(DecimalScalar::new("5.1").unwrap()))
                .unwrap(),
            48
        );
    }

    #[test]
    fn exact_mode_preserves_full_integer_type_boundaries() {
        for (minimum, maximum) in [
            (i128::from(i64::MIN), i128::from(i64::MAX)),
            (0, i128::from(u64::MAX)),
        ] {
            let plan = IntegerConversion::compile(
                Decimal::ONE,
                Decimal::ZERO,
                minimum,
                maximum,
                netbaiot_core::MAX_DECIMAL_BYTES,
            )
            .unwrap();
            for raw in [minimum, 0, maximum] {
                let scalar = plan.decode(raw).unwrap();
                assert_eq!(plan.encode(&scalar).unwrap(), raw);
            }
        }
        assert!(
            IntegerConversion::compile(
                Decimal::parse_parameter("99999999999999999999999999999999999999").unwrap(),
                Decimal::ZERO,
                0,
                i128::from(u64::MAX),
                netbaiot_core::MAX_DECIMAL_BYTES,
            )
            .is_err()
        );
    }

    #[test]
    fn compiled_integer_plans_round_trip_every_i16_and_extreme_exponents() {
        for (scale, bias) in [("0.1", "0.3"), ("-0.001", "1"), ("1e38", "0")] {
            let plan = IntegerConversion::compile(
                Decimal::parse_parameter(scale).unwrap(),
                Decimal::parse_parameter(bias).unwrap(),
                i16::MIN.into(),
                i16::MAX.into(),
                netbaiot_core::MAX_DECIMAL_BYTES,
            )
            .unwrap();
            for raw in i16::MIN..=i16::MAX {
                let decoded = plan.decode(i128::from(raw)).unwrap();
                assert_eq!(
                    plan.encode(&decoded).unwrap(),
                    i128::from(raw),
                    "{scale} {bias}"
                );
            }
        }
        let plan = IntegerConversion::compile(
            Decimal::parse_parameter("1e38").unwrap(),
            Decimal::ZERO,
            i16::MIN.into(),
            i16::MAX.into(),
            netbaiot_core::MAX_DECIMAL_BYTES,
        )
        .unwrap();
        let Scalar::Decimal(value) = plan.decode(10).unwrap() else {
            panic!()
        };
        assert_eq!(value.as_str(), "1000000000000000000000000000000000000000");
        assert_eq!(plan.encode(&Scalar::Decimal(value)).unwrap(), 10);
    }

    #[test]
    fn exact_identity_exhaustively_round_trips_all_small_integer_types() {
        for (minimum, maximum) in [
            (i128::from(i8::MIN), i128::from(i8::MAX)),
            (0, i128::from(u8::MAX)),
            (i128::from(i16::MIN), i128::from(i16::MAX)),
            (0, i128::from(u16::MAX)),
        ] {
            let plan = IntegerConversion::compile(
                Decimal::ONE,
                Decimal::ZERO,
                minimum,
                maximum,
                netbaiot_core::MAX_DECIMAL_BYTES,
            )
            .unwrap();
            for raw in minimum..=maximum {
                let decoded = plan.decode(raw).unwrap();
                assert!(matches!(decoded, Scalar::Decimal(_)));
                assert_eq!(plan.encode(&decoded).unwrap(), raw);
            }
        }
    }

    #[test]
    fn large_integer_domains_round_trip_boundaries_and_deterministic_samples() {
        let configurations = [("1", "0"), ("-0.001", "1.25"), ("0.0000001", "-12.3")];
        for (minimum, maximum) in [
            (i128::from(i32::MIN), i128::from(i32::MAX)),
            (0, i128::from(u32::MAX)),
            (i128::from(i64::MIN), i128::from(i64::MAX)),
            (0, i128::from(u64::MAX)),
        ] {
            for (scale, bias) in configurations {
                let plan = IntegerConversion::compile(
                    Decimal::parse_parameter(scale).unwrap(),
                    Decimal::parse_parameter(bias).unwrap(),
                    minimum,
                    maximum,
                    netbaiot_core::MAX_DECIMAL_BYTES,
                )
                .unwrap();
                for raw in [
                    minimum,
                    minimum + 1,
                    (-1i128).max(minimum),
                    0,
                    1,
                    maximum - 1,
                    maximum,
                ]
                .into_iter()
                .filter(|raw| (minimum..=maximum).contains(raw))
                {
                    let decoded = plan.decode(raw).unwrap();
                    assert_eq!(plan.encode(&decoded).unwrap(), raw, "{scale} {bias} {raw}");
                }
                let span = u128::try_from(maximum - minimum).unwrap() + 1;
                let mut state = 0x6a09_e667_f3bc_c909u64;
                for _ in 0..1_000 {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let raw = minimum + i128::try_from(u128::from(state) % span).unwrap();
                    let decoded = plan.decode(raw).unwrap();
                    assert_eq!(plan.encode(&decoded).unwrap(), raw, "{scale} {bias} {raw}");
                }
            }
        }
    }
}
