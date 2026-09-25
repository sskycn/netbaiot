//! Bounded MQTT 3.1.1 and 5.0 wire primitives shared by the gateway and device profile.
//! This crate deliberately contains no broker, client transport or Tokio tasks.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Invalid,
    TooLarge,
}

pub type Result<T> = std::result::Result<T, WireError>;

/// The MQTT variable byte integer is at most four octets and 268_435_455.
pub fn remaining_length(input: &[u8], maximum: usize) -> Result<Option<(usize, usize)>> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (index, byte) in input.iter().take(4).enumerate() {
        value = value
            .checked_add(
                usize::from(byte & 0x7f)
                    .checked_mul(multiplier)
                    .ok_or(WireError::Invalid)?,
            )
            .ok_or(WireError::Invalid)?;
        if value > maximum {
            return Err(WireError::TooLarge);
        }
        if byte & 0x80 == 0 {
            if index > 0 && *byte == 0 {
                return Err(WireError::Invalid);
            }
            return Ok(Some((value, index + 1)));
        }
        if index == 3 {
            return Err(WireError::Invalid);
        }
        multiplier = multiplier.checked_mul(128).ok_or(WireError::Invalid)?;
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
        return Err(WireError::Invalid);
    }
    let Some((remaining, encoded)) = remaining_length(&input[1..], maximum)? else {
        return Ok(None);
    };
    let header = encoded.checked_add(1).ok_or(WireError::Invalid)?;
    let total = remaining.checked_add(header).ok_or(WireError::Invalid)?;
    if total > maximum {
        return Err(WireError::TooLarge);
    }
    Ok(Some((first, header, total)))
}

pub fn valid_utf8(bytes: &[u8]) -> Result<&str> {
    let value = std::str::from_utf8(bytes).map_err(|_| WireError::Invalid)?;
    if value.chars().any(|character| {
        let code = character as u32;
        code == 0
            || (0x0001..=0x001f).contains(&code)
            || (0x007f..=0x009f).contains(&code)
            || (0xfdd0..=0xfdef).contains(&code)
            || code & 0xffff >= 0xfffe
    }) {
        return Err(WireError::Invalid);
    }
    Ok(value)
}

pub fn valid_filter(filter: &str) -> bool {
    let mut levels = filter.split('/').peekable();
    while let Some(level) = levels.next() {
        if (level.contains('+') && level != "+")
            || (level.contains('#') && (level != "#" || levels.peek().is_some()))
        {
            return false;
        }
    }
    true
}

pub fn valid_topic(topic: &str, max_bytes: usize, max_depth: usize, filter: bool) -> bool {
    !topic.is_empty()
        && topic.len() <= max_bytes
        && topic.split('/').count() <= max_depth
        && valid_utf8(topic.as_bytes()).is_ok()
        && if filter {
            valid_filter(topic)
        } else {
            !topic.contains(['+', '#'])
        }
}

pub fn encoded_size(remaining: usize) -> Result<usize> {
    let length_bytes = match remaining {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        2_097_152..=268_435_455 => 4,
        _ => return Err(WireError::Invalid),
    };
    remaining
        .checked_add(1 + length_bytes)
        .ok_or(WireError::Invalid)
}

pub fn put_variable(mut value: usize, output: &mut Vec<u8>) -> Result<()> {
    if value > 268_435_455 {
        return Err(WireError::Invalid);
    }
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

pub fn encode(first: u8, body: &[u8], maximum: usize) -> Result<Vec<u8>> {
    let size = encoded_size(body.len())?;
    if size > maximum || !valid_flags(first >> 4, first & 0x0f) {
        return Err(WireError::TooLarge);
    }
    let mut output = Vec::with_capacity(size);
    output.push(first);
    put_variable(body.len(), &mut output)?;
    output.extend_from_slice(body);
    Ok(output)
}

pub mod client;
