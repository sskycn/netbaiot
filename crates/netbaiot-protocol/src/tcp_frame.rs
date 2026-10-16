//! Native TCP framing: bit 31 is the frame type; bits 0..30 are body length.
//! Payload (0) is opaque to framing. EventAccepted (1) is server-to-device only.
//! This is a breaking wire change, without negotiation or legacy ACK detection.
use crate::{ProtocolError, tcp_ack};

pub const ACK_FLAG: u32 = 1 << 31;
pub const LENGTH_MASK: u32 = ACK_FLAG - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    Payload = 0,
    EventAccepted = 1,
}

/// Validate type and length before allocating or waiting for the body.
/// Callers expecting authentication/uplink/command data must require Payload.
pub fn decode_header(header: [u8; 4], maximum: usize) -> Result<(FrameKind, usize), ProtocolError> {
    let raw = u32::from_be_bytes(header);
    let length = usize::try_from(raw & LENGTH_MASK).map_err(|_| ProtocolError)?;
    if length == 0 || length > maximum {
        return Err(ProtocolError);
    }
    let kind = if raw & ACK_FLAG == 0 {
        FrameKind::Payload
    } else {
        if length != tcp_ack::BODY_LEN {
            return Err(ProtocolError);
        }
        FrameKind::EventAccepted
    };
    Ok((kind, length))
}

pub fn payload_header(length: usize) -> Result<[u8; 4], ProtocolError> {
    let length = u32::try_from(length).map_err(|_| ProtocolError)?;
    if length == 0 || length > LENGTH_MASK {
        return Err(ProtocolError);
    }
    Ok(length.to_be_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_and_length_are_independent_of_body() {
        assert_eq!(
            decode_header([0, 0, 0, 42], 42),
            Ok((FrameKind::Payload, 42))
        );
        assert_eq!(
            decode_header([128, 0, 0, 42], 42),
            Ok((FrameKind::EventAccepted, 42))
        );
        for length in [1, 42, 65536, LENGTH_MASK as usize] {
            assert_eq!(
                decode_header(payload_header(length).unwrap(), length),
                Ok((FrameKind::Payload, length))
            );
            assert!(decode_header(payload_header(length).unwrap(), length - 1).is_err());
        }
        for length in [0, ACK_FLAG as usize, usize::MAX] {
            assert!(payload_header(length).is_err());
        }
    }

    #[test]
    fn malformed_headers_fail_before_body_read() {
        for raw in [0, ACK_FLAG, ACK_FLAG | 41, ACK_FLAG | 43, u32::MAX] {
            assert!(decode_header(raw.to_be_bytes(), usize::MAX).is_err());
        }
        assert!(decode_header([128, 0, 0, 42], 41).is_err());
    }
}
