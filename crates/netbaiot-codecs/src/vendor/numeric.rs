use netbaiot_core::{CodecError, DecimalScalar, Scalar};
use std::fmt::{self, Write};

pub(crate) const MAX_DECIMAL_PARAMETER_EXPONENT: i16 = 38;
pub(crate) const MAX_DECIMAL_SIGNIFICANT_DIGITS: usize = 38;

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

    pub(crate) fn parse(text: &str) -> Result<Self, &'static str> {
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
            let mut exponent = 0i16;
            while let Some(digit) = bytes.get(at).filter(|digit| digit.is_ascii_digit()) {
                exponent = exponent
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(i16::from(*digit - b'0')))
                    .ok_or("Decimal exponent exceeds the supported range.")?;
                at += 1;
            }
            if at == exponent_start || exponent > MAX_DECIMAL_PARAMETER_EXPONENT {
                return Err("Decimal exponent exceeds the supported range of -38 through 38.");
            }
            if exponent_negative {
                -exponent
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
            return Ok(Self::ZERO);
        };
        let last_nonzero = last_nonzero.ok_or("Decimal coefficient is invalid.")?;
        let significant_digits = last_nonzero - first_nonzero + 1;
        if significant_digits > MAX_DECIMAL_SIGNIFICANT_DIGITS {
            return Err("Decimal has more than 38 significant digits.");
        }

        let trailing_zeros = digit_index - last_nonzero - 1;
        let mut coefficient = 0i128;
        let mut current_digit = 0usize;
        for byte in &bytes[usize::from(negative)..mantissa_end] {
            if *byte == b'.' {
                continue;
            }
            if (first_nonzero..=last_nonzero).contains(&current_digit) {
                coefficient = coefficient
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(i128::from(*byte - b'0')))
                    .ok_or("Decimal coefficient exceeds i128.")?;
            }
            current_digit += 1;
        }
        if negative {
            coefficient = -coefficient;
        }
        let exponent = i32::from(explicit_exponent)
            .checked_sub(i32::try_from(fraction_digits).map_err(|_| "Decimal is too long.")?)
            .and_then(|n| n.checked_add(i32::try_from(trailing_zeros).ok()?))
            .and_then(|n| i16::try_from(n).ok())
            .ok_or("Decimal exponent exceeds the supported range.")?;
        if !(-MAX_DECIMAL_PARAMETER_EXPONENT..=MAX_DECIMAL_PARAMETER_EXPONENT).contains(&exponent) {
            return Err("Decimal exponent exceeds the supported range of -38 through 38.");
        }
        Ok(Self {
            coefficient,
            exponent,
        })
    }

    pub(crate) fn from_f64(value: f64) -> Result<Self, CodecError> {
        if !value.is_finite() {
            return Err(CodecError);
        }
        let mut buffer = ryu::Buffer::new();
        Self::parse(buffer.format_finite(value)).map_err(|_| CodecError)
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        if self.coefficient == 0 {
            return Some(other);
        }
        if other.coefficient == 0 {
            return Some(self);
        }
        let exponent = self.exponent.min(other.exponent);
        let left = self
            .coefficient
            .checked_mul(pow10(u32::from(self.exponent.abs_diff(exponent)))?)?;
        let right = other
            .coefficient
            .checked_mul(pow10(u32::from(other.exponent.abs_diff(exponent)))?)?;
        normalize(left.checked_add(right)?, exponent)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_add(Self {
            coefficient: other.coefficient.checked_neg()?,
            exponent: other.exponent,
        })
    }

    fn checked_mul_integer(self, integer: i128) -> Option<Self> {
        normalize(self.coefficient.checked_mul(integer)?, self.exponent)
    }

    fn formatted_len(self) -> Option<usize> {
        let mut counter = Length(0);
        self.write_to(&mut counter).ok()?;
        Some(counter.0)
    }

    fn into_scalar(self) -> Result<Scalar, CodecError> {
        let mut value = String::with_capacity(self.formatted_len().ok_or(CodecError)?);
        self.write_to(&mut value).map_err(|_| CodecError)?;
        Ok(Scalar::Decimal(
            DecimalScalar::new(value).map_err(|_| CodecError)?,
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

fn gcd(mut left: u128, mut right: u128) -> u128 {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

fn exact_integer_ratio(numerator: Decimal, denominator: Decimal) -> Option<i128> {
    if denominator.coefficient == 0 {
        return None;
    }
    if numerator.coefficient == 0 {
        return Some(0);
    }
    let negative = (numerator.coefficient < 0) != (denominator.coefficient < 0);
    let mut top = numerator.coefficient.unsigned_abs();
    let mut bottom = denominator.coefficient.unsigned_abs();
    let common = gcd(top, bottom);
    top /= common;
    bottom /= common;
    let exponent = i32::from(numerator.exponent) - i32::from(denominator.exponent);
    if exponent >= 0 {
        let mut twos = exponent as u32;
        let mut fives = exponent as u32;
        while twos != 0 && bottom.is_multiple_of(2) {
            bottom /= 2;
            twos -= 1;
        }
        while fives != 0 && bottom.is_multiple_of(5) {
            bottom /= 5;
            fives -= 1;
        }
        if bottom != 1 {
            return None;
        }
        for _ in 0..twos {
            top = top.checked_mul(2)?;
        }
        for _ in 0..fives {
            top = top.checked_mul(5)?;
        }
    } else {
        if bottom != 1 {
            return None;
        }
        for _ in 0..exponent.unsigned_abs() {
            if !top.is_multiple_of(10) {
                return None;
            }
            top /= 10;
        }
    }
    if negative {
        if top == (i128::MAX as u128) + 1 {
            Some(i128::MIN)
        } else {
            i128::try_from(top).ok()?.checked_neg()
        }
    } else {
        i128::try_from(top).ok()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntegerOutput {
    Number,
    Decimal,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct IntegerConversion {
    scale: Decimal,
    bias: Decimal,
    output: IntegerOutput,
    minimum: i128,
    maximum: i128,
    legacy: Option<Legacy>,
}

#[derive(Clone, Copy, Debug)]
struct Legacy {
    scale: f64,
    bias: f64,
}

impl IntegerConversion {
    pub(crate) fn legacy(
        scale: f64,
        bias: f64,
        minimum: i128,
        maximum: i128,
    ) -> Result<Self, &'static str> {
        let legacy = (scale, bias);
        let scale = Decimal::from_f64(scale).map_err(
            |_| "Legacy scale cannot be represented by the bounded decimal conversion engine.",
        )?;
        let bias = Decimal::from_f64(bias).map_err(
            |_| "Legacy bias cannot be represented by the bounded decimal conversion engine.",
        )?;
        Self::legacy_compiled(scale, bias, legacy, minimum, maximum)
    }

    pub(crate) fn legacy_compiled(
        scale: Decimal,
        bias: Decimal,
        legacy: (f64, f64),
        minimum: i128,
        maximum: i128,
    ) -> Result<Self, &'static str> {
        let mut plan = Self::compile(scale, bias, IntegerOutput::Number, minimum, maximum)?;
        let (scale, bias) = legacy;
        let legacy = Legacy { scale, bias };
        plan.legacy = Some(legacy);
        Ok(plan)
    }

    pub(crate) fn compile(
        scale: Decimal,
        bias: Decimal,
        output: IntegerOutput,
        minimum: i128,
        maximum: i128,
    ) -> Result<Self, &'static str> {
        if scale.coefficient == 0 {
            return Err("Scale must be nonzero.");
        }
        let plan = Self {
            scale,
            bias,
            output,
            minimum,
            maximum,
            legacy: None,
        };
        for raw in [minimum, maximum] {
            let value = plan
                .decimal(raw)
                .ok_or("Integer conversion exceeds the bounded i128 decimal engine.")?;
            if output == IntegerOutput::Decimal
                && value
                    .formatted_len()
                    .is_none_or(|length| length > netbaiot_core::MAX_DECIMAL_BYTES)
            {
                return Err("Exact decoded value exceeds the 64-byte decimal scalar limit.");
            }
        }
        Ok(plan)
    }

    fn decimal(self, raw: i128) -> Option<Decimal> {
        self.scale.checked_mul_integer(raw)?.checked_add(self.bias)
    }

    pub(crate) fn decode(self, raw: i128) -> Result<Scalar, CodecError> {
        if !(self.minimum..=self.maximum).contains(&raw) {
            return Err(CodecError);
        }
        let decimal = self.decimal(raw).ok_or(CodecError)?;
        match self.output {
            IntegerOutput::Decimal => decimal.into_scalar(),
            IntegerOutput::Number => {
                let legacy = self.legacy.ok_or(CodecError)?;
                let number = raw as f64 * legacy.scale + legacy.bias;
                if !number.is_finite() || Decimal::from_f64(number)? != decimal {
                    return Err(CodecError);
                }
                for neighbor in [raw.checked_sub(1), raw.checked_add(1)]
                    .into_iter()
                    .flatten()
                    .filter(|neighbor| (self.minimum..=self.maximum).contains(neighbor))
                {
                    if neighbor as f64 * legacy.scale + legacy.bias == number {
                        return Err(CodecError);
                    }
                }
                Ok(Scalar::Number(number))
            }
        }
    }

    pub(crate) fn encode(self, scalar: &Scalar) -> Result<i128, CodecError> {
        let value = match scalar {
            Scalar::Number(number) => Decimal::from_f64(*number)?,
            Scalar::Decimal(decimal) if self.output == IntegerOutput::Decimal => {
                Decimal::parse(decimal.as_str()).map_err(|_| CodecError)?
            }
            _ => return Err(CodecError),
        };
        let difference = value.checked_sub(self.bias).ok_or(CodecError)?;
        let raw = exact_integer_ratio(difference, self.scale).ok_or(CodecError)?;
        if !(self.minimum..=self.maximum).contains(&raw)
            || self.decimal(raw).ok_or(CodecError)? != value
        {
            return Err(CodecError);
        }
        Ok(raw)
    }
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

struct Length(usize);

impl Write for Length {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
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
        for invalid in ["", ".1", "1.", "+1", "1e", "1e39", "1e-39", "NaN"] {
            assert!(Decimal::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn exact_integer_inverse_never_rounds() {
        let plan = IntegerConversion::compile(
            Decimal::parse("0.1").unwrap(),
            Decimal::parse("0.3").unwrap(),
            IntegerOutput::Decimal,
            i16::MIN.into(),
            i16::MAX.into(),
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
                Decimal::parse("1").unwrap(),
                Decimal::ZERO,
                IntegerOutput::Decimal,
                minimum,
                maximum,
            )
            .unwrap();
            for raw in [minimum, 0, maximum] {
                let scalar = plan.decode(raw).unwrap();
                assert_eq!(plan.encode(&scalar).unwrap(), raw);
            }
        }
        assert!(
            IntegerConversion::compile(
                Decimal::parse("99999999999999999999999999999999999999").unwrap(),
                Decimal::ZERO,
                IntegerOutput::Decimal,
                0,
                i128::from(u64::MAX),
            )
            .is_err()
        );
    }
}
