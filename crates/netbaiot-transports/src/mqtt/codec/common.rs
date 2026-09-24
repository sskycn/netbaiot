use bytes::{Buf, Bytes};
use netbaiot_runtime::{Error, Limits, Result};

/// Decode Remaining Length without reserving or growing an input buffer.
pub fn remaining_length(input: &[u8], maximum: usize) -> Result<Option<(usize, usize)>> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (index, byte) in input.iter().take(4).enumerate() {
        value = value
            .checked_add(
                usize::from(byte & 0x7f)
                    .checked_mul(multiplier)
                    .ok_or(Error::Invalid)?,
            )
            .ok_or(Error::Invalid)?;
        if value > maximum {
            return Err(Error::Overloaded);
        }
        if byte & 0x80 == 0 {
            if index > 0 && *byte == 0 {
                return Err(Error::Invalid);
            }
            return Ok(Some((value, index + 1)));
        }
        if index == 3 {
            return Err(Error::Invalid);
        }
        multiplier = multiplier.checked_mul(128).ok_or(Error::Invalid)?;
    }
    Ok(None)
}

fn valid_flags(kind: u8, flags: u8) -> bool {
    match kind {
        1 | 2 | 4 | 5 | 7 | 9 | 11 | 12 | 13 | 14 | 15 => flags == 0,
        3 => {
            let qos = (flags >> 1) & 3;
            qos != 3 && !(qos == 0 && flags & 0x08 != 0)
        }
        6 | 8 | 10 => flags == 2,
        _ => false,
    }
}

pub fn fixed_header(input: &[u8], maximum: usize) -> Result<Option<(u8, usize, usize)>> {
    let Some(&first) = input.first() else {
        return Ok(None);
    };
    if !valid_flags(first >> 4, first & 0x0f) {
        return Err(Error::Invalid);
    }
    let Some((remaining, encoded)) = remaining_length(&input[1..], maximum)? else {
        return Ok(None);
    };
    let header = encoded.checked_add(1).ok_or(Error::Invalid)?;
    let total = remaining.checked_add(header).ok_or(Error::Invalid)?;
    if total > maximum {
        return Err(Error::Overloaded);
    }
    Ok(Some((first, header, total)))
}

pub fn valid_utf8(bytes: &[u8]) -> Result<&str> {
    let value = std::str::from_utf8(bytes).map_err(|_| Error::Invalid)?;
    if value.chars().any(|character| {
        let code = character as u32;
        code == 0
            || (0x0001..=0x001f).contains(&code)
            || (0x007f..=0x009f).contains(&code)
            || (0xfdd0..=0xfdef).contains(&code)
            || code & 0xffff >= 0xfffe
    }) {
        return Err(Error::Invalid);
    }
    Ok(value)
}

#[inline]
pub fn valid_topic(topic: &str, limits: &Limits, filter: bool) -> bool {
    !topic.is_empty()
        && topic.len() <= limits.max_topic_bytes
        && topic.split('/').count() <= limits.max_topic_depth
        && valid_utf8(topic.as_bytes()).is_ok()
        && if filter {
            valid_filter(topic)
        } else {
            !topic.contains(['+', '#'])
        }
}

pub fn valid_filter(filter: &str) -> bool {
    let levels = filter.split('/').collect::<Vec<_>>();
    !levels.is_empty()
        && levels.iter().enumerate().all(|(index, level)| {
            (!level.contains('+') || *level == "+")
                && (!level.contains('#') || (*level == "#" && index + 1 == levels.len()))
        })
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

fn variable(mut value: usize, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            byte |= 0x80
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

pub(super) fn encoded_size(remaining: usize) -> Result<usize> {
    let length_bytes = match remaining {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        2_097_152..=268_435_455 => 4,
        _ => return Err(Error::Invalid),
    };
    remaining
        .checked_add(1 + length_bytes)
        .ok_or(Error::Invalid)
}

pub fn encode(first: u8, body: &[u8], maximum: usize) -> Result<Vec<u8>> {
    let size = encoded_size(body.len())?;
    if size > maximum {
        return Err(Error::Overloaded);
    }
    let mut output = Vec::with_capacity(size);
    output.push(first);
    variable(body.len(), &mut output);
    output.extend_from_slice(body);
    Ok(output)
}
