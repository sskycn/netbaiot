//! Extension point for synchronous, bounded, versioned vendor DeviceCodec implementations.
//! Register explicit ID/version pairs at composition; industrial framing belongs in adapters.
pub mod declarative;
pub mod demo;

use crate::CodecRegistration;
use netbaiot_core::{CodecError, CodecId, DeviceCodec};
use std::sync::Arc;

/// The inverse transform may accumulate representation, subtraction and division
/// roundoff. Bound that error relative to the raw signal and retain the absolute
/// 1e-7 ceiling. Zero is exact: treating a nonzero raw value as zero is quantization
/// or underflow, not roundoff.
pub(crate) fn inverse_number_matches(calculated: f64, representable: f64) -> bool {
    if !calculated.is_finite() || !representable.is_finite() {
        return false;
    }
    if calculated == representable {
        return true;
    }
    let difference = (calculated - representable).abs();
    difference <= (4.0 * f64::EPSILON * calculated.abs().max(representable.abs())).min(1e-7)
}

/// Once raw-value representability has been proved separately, allow only the
/// existing relative error budget for the multiply/add normalization. This is
/// deliberately not an absolute or ULP tolerance: cancellation can span more
/// than one output ULP while still being tiny relative to the command value.
pub(crate) fn normalized_number_matches(expected: f64, actual: f64) -> bool {
    if !expected.is_finite() || !actual.is_finite() {
        return false;
    }
    if expected == actual {
        return true;
    }
    let difference = (expected - actual).abs();
    difference <= 4.0 * f64::EPSILON * expected.abs().max(actual.abs())
}

/// A vendor ID occupies the existing codec namespace. Reserved built-ins cannot be replaced.
pub fn vendor_id(id: &str, version: u16) -> Result<CodecId, CodecError> {
    if version == 0 || !id.starts_with("vendor-") || id.len() <= "vendor-".len() {
        return Err(CodecError);
    }
    CodecId::new(id).map_err(|_| CodecError)
}

/// Register any independently implemented, synchronous native Rust DeviceCodec.
/// CodecRegistry rejects duplicate ID/version pairs and enforces its 64-entry bound.
pub fn registration(
    id: &str,
    version: u16,
    codec: Arc<dyn DeviceCodec>,
) -> Result<CodecRegistration, CodecError> {
    Ok((vendor_id(id, version)?, version, codec))
}

// Render an integer ID on the stack; only the final shared identifier allocates.
fn sequence_id(mut value: u64) -> Result<netbaiot_core::SourceMessageId, CodecError> {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let text = std::str::from_utf8(&digits[start..]).map_err(|_| CodecError)?;
    netbaiot_core::SourceMessageId::new(text).map_err(|_| CodecError)
}
