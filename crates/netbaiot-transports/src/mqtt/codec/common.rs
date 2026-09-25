use bytes::{Buf, Bytes};
use netbaiot_mqtt_wire as wire;
use netbaiot_runtime::{Error, Limits, Result};

fn adapt(error: wire::WireError) -> Error {
    match error {
        wire::WireError::Invalid => Error::Invalid,
        wire::WireError::TooLarge => Error::Overloaded,
    }
}

/// Decode Remaining Length without reserving or growing an input buffer.
pub fn remaining_length(input: &[u8], maximum: usize) -> Result<Option<(usize, usize)>> {
    wire::remaining_length(input, maximum).map_err(adapt)
}

pub fn fixed_header(input: &[u8], maximum: usize) -> Result<Option<(u8, usize, usize)>> {
    wire::fixed_header(input, maximum).map_err(adapt)
}

pub fn valid_utf8(bytes: &[u8]) -> Result<&str> {
    wire::valid_utf8(bytes).map_err(adapt)
}

#[inline]
pub fn valid_topic(topic: &str, limits: &Limits, filter: bool) -> bool {
    wire::valid_topic(
        topic,
        limits.max_topic_bytes,
        limits.max_topic_depth,
        filter,
    )
}

pub fn valid_filter(filter: &str) -> bool {
    wire::valid_filter(filter)
}

pub(super) struct Cursor {
    pub(super) bytes: Bytes,
}
impl Cursor {
    pub(super) fn byte(&mut self) -> Result<u8> {
        if !self.bytes.has_remaining() {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.get_u8())
    }
    pub(super) fn short(&mut self) -> Result<u16> {
        if self.bytes.len() < 2 {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.get_u16())
    }
    pub(super) fn binary(&mut self, maximum: usize) -> Result<Bytes> {
        let length = usize::from(self.short()?);
        if length > maximum || length > self.bytes.len() {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.split_to(length))
    }
    pub(super) fn string(&mut self, maximum: usize) -> Result<String> {
        let bytes = self.binary(maximum)?;
        Ok(valid_utf8(&bytes)?.to_owned())
    }
    pub(super) fn id(&mut self) -> Result<u16> {
        let id = self.short()?;
        if id == 0 {
            return Err(Error::Invalid);
        }
        Ok(id)
    }
    pub(super) fn done(&self) -> Result<()> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }
}

pub(super) fn encoded_size(remaining: usize) -> Result<usize> {
    wire::encoded_size(remaining).map_err(adapt)
}

pub fn encode(first: u8, body: &[u8], maximum: usize) -> Result<Vec<u8>> {
    wire::encode(first, body, maximum).map_err(adapt)
}
