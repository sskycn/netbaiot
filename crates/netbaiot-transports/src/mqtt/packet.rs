//! Stable packet API for existing callers and fuzz targets.
//! Version-specific wire codecs live under `codec`.
pub use super::codec::common::{
    encode, fixed_header, remaining_length, valid_filter, valid_topic, valid_utf8,
};
pub use super::codec::v311::{Connect, Packet, Will, ack, connack, decode, publish};
