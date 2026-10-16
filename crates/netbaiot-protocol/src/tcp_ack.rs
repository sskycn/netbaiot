//! Native TCP EventAccepted ACK v1. Independent of the authenticated device codec.
//! This replaces JSON TCP receipts without negotiation or a compatibility fallback.
use crate::{EventAccepted, EventId, ProtocolError};
use uuid::Uuid;

pub const VERSION: u8 = 1;
pub const EVENT_ACCEPTED: u8 = 1;
pub const BODY_LEN: usize = 42;
pub const FRAME_LEN: usize = 46;
pub const FRAME_HEADER: [u8; 4] = (crate::tcp_frame::ACK_FLAG | BODY_LEN as u32).to_be_bytes();

/// Encode the complete length-prefixed frame without heap allocation.
/// `accepted_at` is signed Unix time in milliseconds; integer fields are big-endian.
#[inline]
pub fn encode(receipt: &EventAccepted) -> Result<[u8; FRAME_LEN], ProtocolError> {
    let required = u64::try_from(receipt.required_deliveries).map_err(|_| ProtocolError)?;
    let best_effort = u64::try_from(receipt.best_effort_deliveries).map_err(|_| ProtocolError)?;
    let mut frame = [0; FRAME_LEN];
    frame[..4].copy_from_slice(&FRAME_HEADER);
    frame[4] = VERSION;
    frame[5] = EVENT_ACCEPTED;
    frame[6..22].copy_from_slice(receipt.event_id.0.as_bytes());
    frame[22..30].copy_from_slice(&receipt.accepted_at.to_be_bytes());
    frame[30..38].copy_from_slice(&required.to_be_bytes());
    frame[38..46].copy_from_slice(&best_effort.to_be_bytes());
    Ok(frame)
}

/// Decode exactly one complete 46-byte EventAccepted frame without allocation.
/// Validates the type/length header and body; rejects Payload frames, truncated
/// frames and trailing bytes. Buffer split reads and isolate one frame first.
pub fn decode_frame(frame: &[u8]) -> Result<EventAccepted, ProtocolError> {
    let frame: &[u8; FRAME_LEN] = frame.try_into().map_err(|_| ProtocolError)?;
    if frame[..4] != FRAME_HEADER {
        return Err(ProtocolError);
    }
    decode(&frame[4..])
}

/// Decode exactly one 42-byte body after TCP framing, with no allocation.
/// Prefer [`decode_frame`] when the complete frame is available.
/// The caller MUST first validate an EventAccepted header with `tcp_frame::decode_header`.
/// Body bytes alone cannot distinguish a receipt from an opaque command payload.
/// Unknown versions/types, trailing bytes and counts exceeding `usize` are errors.
pub fn decode(body: &[u8]) -> Result<EventAccepted, ProtocolError> {
    let body: &[u8; BODY_LEN] = body.try_into().map_err(|_| ProtocolError)?;
    if body[0] != VERSION || body[1] != EVENT_ACCEPTED {
        return Err(ProtocolError);
    }
    Ok(EventAccepted {
        event_id: EventId(Uuid::from_bytes(
            body[2..18].try_into().map_err(|_| ProtocolError)?,
        )),
        accepted_at: i64::from_be_bytes(body[18..26].try_into().map_err(|_| ProtocolError)?),
        required_deliveries: usize::try_from(u64::from_be_bytes(
            body[26..34].try_into().map_err(|_| ProtocolError)?,
        ))
        .map_err(|_| ProtocolError)?,
        best_effort_deliveries: usize::try_from(u64::from_be_bytes(
            body[34..42].try_into().map_err(|_| ProtocolError)?,
        ))
        .map_err(|_| ProtocolError)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt() -> EventAccepted {
        EventAccepted {
            event_id: EventId(Uuid::from_bytes([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            ])),
            accepted_at: -2,
            required_deliveries: 0x01020304,
            best_effort_deliveries: 0x05060708,
        }
    }

    #[test]
    fn golden_frame_and_round_trip() {
        let receipt = receipt();
        let frame = encode(&receipt).unwrap();
        assert_eq!(
            frame,
            [
                128, 0, 0, 42, 1, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 255,
                255, 255, 255, 255, 255, 255, 254, 0, 0, 0, 0, 1, 2, 3, 4, 0, 0, 0, 0, 5, 6, 7, 8,
            ]
        );
        for decoded in [decode(&frame[4..]).unwrap(), decode_frame(&frame).unwrap()] {
            assert_eq!(decoded.event_id, receipt.event_id);
            assert_eq!(decoded.accepted_at, -2);
            assert_eq!(decoded.required_deliveries, receipt.required_deliveries);
            assert_eq!(
                decoded.best_effort_deliveries,
                receipt.best_effort_deliveries
            );
        }
    }

    #[test]
    fn integer_boundaries() {
        for timestamp in [i64::MIN, 0, i64::MAX] {
            for count in [0, usize::MAX] {
                let mut receipt = receipt();
                receipt.accepted_at = timestamp;
                receipt.required_deliveries = count;
                receipt.best_effort_deliveries = count;
                let decoded = decode_frame(&encode(&receipt).unwrap()).unwrap();
                assert_eq!(decoded.accepted_at, timestamp);
                assert_eq!(decoded.required_deliveries, count);
                assert_eq!(decoded.best_effort_deliveries, count);
            }
        }
        let mut body = [255; BODY_LEN];
        body[..2].copy_from_slice(&[VERSION, EVENT_ACCEPTED]);
        assert_eq!(decode(&body).is_ok(), usize::BITS == 64);
    }

    #[test]
    fn complete_frame_requires_exact_header_length_and_body() {
        let frame = encode(&receipt()).unwrap();
        for length in 0..FRAME_LEN {
            assert!(decode_frame(&frame[..length]).is_err());
        }
        let mut extra = [0; FRAME_LEN + 1];
        extra[..FRAME_LEN].copy_from_slice(&frame);
        assert!(decode_frame(&extra).is_err());
        assert!(decode_frame(&[frame, frame].concat()).is_err());
        assert!(decode_frame(&frame[4..]).is_err());
        // Every bit of the fixed type/length header is significant, including
        // the type bit separating a byte-identical vendor Payload from an ACK.
        for bit in 0..32 {
            let mut bad = frame;
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(decode_frame(&bad).is_err());
        }
        for offset in [4, 5] {
            for value in [0, 2, 255] {
                let mut bad = frame;
                bad[offset] = value;
                assert!(decode_frame(&bad).is_err());
            }
        }
    }

    #[test]
    fn rejects_noncanonical_and_legacy_receipts() {
        let frame = encode(&receipt()).unwrap();
        for length in 0..BODY_LEN {
            assert!(decode(&frame[4..4 + length]).is_err());
        }
        assert!(decode(&frame).is_err());
        assert!(decode(&[0; BODY_LEN + 1]).is_err());
        assert!(decode(&serde_json::to_vec(&receipt()).unwrap()).is_err());
        for offset in [0, 1] {
            for value in [0, 2, 255] {
                let mut body = frame[4..].to_vec();
                body[offset] = value;
                assert!(decode(&body).is_err());
            }
        }
    }
}
