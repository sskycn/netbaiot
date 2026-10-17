//! Extension point for synchronous, bounded, versioned vendor DeviceCodec implementations.
//! Register explicit ID/version pairs at composition; industrial framing belongs in adapters.
pub mod declarative;
pub mod demo;

use crate::CodecRegistration;
use netbaiot_core::{CodecError, CodecId, DeviceCodec};
use std::sync::Arc;

/// The wire transform uses f64 arithmetic. Admit at most one adjacent f64 value
/// of roundoff, and never let an absolute ULP near zero hide relative loss.
pub(crate) fn normalized_number_matches(expected: f64, actual: f64) -> bool {
    if !expected.is_finite() || !actual.is_finite() {
        return false;
    }
    if expected == actual {
        return true;
    }
    let difference = (expected - actual).abs();
    (actual == expected.next_up() || actual == expected.next_down())
        && difference <= 4.0 * f64::EPSILON * expected.abs().max(actual.abs())
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
