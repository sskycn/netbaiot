use bytes::{Buf, Bytes, BytesMut};
use netbaiot_runtime::{Error, Limits, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Connect {
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub will: Option<Will>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    UnsupportedVersion(u8),
    Connack {
        session_present: bool,
        code: u8,
    },
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        packet_id: Option<u16>,
        retain: bool,
        dup: bool,
    },
    Puback(u16),
    Pubrec(u16),
    Pubrel(u16),
    Pubcomp(u16),
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    Suback {
        packet_id: u16,
        returns: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    Unsuback(u16),
    Pingreq,
    Pingresp,
    Disconnect,
}

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
        1 | 2 | 4 | 5 | 7 | 9 | 11 | 12 | 13 | 14 => flags == 0,
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

struct Cursor {
    bytes: Bytes,
}
impl Cursor {
    fn byte(&mut self) -> Result<u8> {
        if !self.bytes.has_remaining() {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.get_u8())
    }
    fn short(&mut self) -> Result<u16> {
        if self.bytes.len() < 2 {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.get_u16())
    }
    fn binary(&mut self, maximum: usize) -> Result<Bytes> {
        let length = usize::from(self.short()?);
        if length > maximum || length > self.bytes.len() {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.split_to(length))
    }
    fn string(&mut self, maximum: usize) -> Result<String> {
        let bytes = self.binary(maximum)?;
        Ok(valid_utf8(&bytes)?.to_owned())
    }
    fn id(&mut self) -> Result<u16> {
        let id = self.short()?;
        if id == 0 {
            return Err(Error::Invalid);
        }
        Ok(id)
    }
    fn done(&self) -> Result<()> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(Error::Invalid)
        }
    }
}

pub fn decode(input: &mut BytesMut, limits: &Limits) -> Result<Option<Packet>> {
    let Some((first, header, total)) = fixed_header(input, limits.max_mqtt_packet_size)? else {
        return Ok(None);
    };
    if input.len() < total {
        return Ok(None);
    }
    let mut frame = input.split_to(total).freeze();
    frame.advance(header);
    let mut cursor = Cursor { bytes: frame };
    let packet = match first >> 4 {
        1 => decode_connect(&mut cursor, limits)?,
        2 => {
            let flags = cursor.byte()?;
            let code = cursor.byte()?;
            cursor.done()?;
            if flags & 0xfe != 0 || code > 5 || (code != 0 && flags & 1 != 0) {
                return Err(Error::Invalid);
            }
            Packet::Connack {
                session_present: flags & 1 != 0,
                code,
            }
        }
        3 => {
            let qos = (first >> 1) & 3;
            let topic = cursor.string(limits.max_topic_bytes)?;
            if !valid_topic(&topic, limits, false) {
                return Err(Error::Invalid);
            }
            let packet_id = if qos > 0 { Some(cursor.id()?) } else { None };
            Packet::Publish {
                topic,
                payload: cursor.bytes,
                qos,
                packet_id,
                retain: first & 1 != 0,
                dup: first & 8 != 0,
            }
        }
        4 => Packet::Puback(decode_id(&mut cursor)?),
        5 => Packet::Pubrec(decode_id(&mut cursor)?),
        6 => Packet::Pubrel(decode_id(&mut cursor)?),
        7 => Packet::Pubcomp(decode_id(&mut cursor)?),
        8 => {
            let packet_id = cursor.id()?;
            let mut filters = Vec::new();
            while !cursor.bytes.is_empty() {
                if filters.len() >= limits.max_subscription_filters_per_packet {
                    return Err(Error::Overloaded);
                }
                let filter = cursor.string(limits.max_topic_bytes)?;
                let qos = cursor.byte()?;
                if qos > 2 || !valid_topic(&filter, limits, true) {
                    return Err(Error::Invalid);
                }
                filters.push((filter, qos));
            }
            if filters.is_empty() {
                return Err(Error::Invalid);
            }
            Packet::Subscribe { packet_id, filters }
        }
        9 => {
            let packet_id = cursor.id()?;
            if cursor.bytes.is_empty()
                || cursor
                    .bytes
                    .iter()
                    .any(|code| !matches!(code, 0 | 1 | 2 | 0x80))
            {
                return Err(Error::Invalid);
            }
            Packet::Suback {
                packet_id,
                returns: cursor.bytes.to_vec(),
            }
        }
        10 => {
            let packet_id = cursor.id()?;
            let mut filters = Vec::new();
            while !cursor.bytes.is_empty() {
                if filters.len() >= limits.max_subscription_filters_per_packet {
                    return Err(Error::Overloaded);
                }
                let filter = cursor.string(limits.max_topic_bytes)?;
                if !valid_topic(&filter, limits, true) {
                    return Err(Error::Invalid);
                }
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(Error::Invalid);
            }
            Packet::Unsubscribe { packet_id, filters }
        }
        11 => Packet::Unsuback(decode_id(&mut cursor)?),
        12 => {
            cursor.done()?;
            Packet::Pingreq
        }
        13 => {
            cursor.done()?;
            Packet::Pingresp
        }
        14 => {
            cursor.done()?;
            Packet::Disconnect
        }
        _ => return Err(Error::Invalid),
    };
    Ok(Some(packet))
}

fn decode_id(cursor: &mut Cursor) -> Result<u16> {
    let id = cursor.id()?;
    cursor.done()?;
    Ok(id)
}

fn decode_connect(cursor: &mut Cursor, limits: &Limits) -> Result<Packet> {
    if cursor.string(6)? != "MQTT" {
        return Err(Error::Invalid);
    }
    let version = cursor.byte()?;
    if version != 4 {
        return Ok(Packet::UnsupportedVersion(version));
    }
    let flags = cursor.byte()?;
    let keep_alive = cursor.short()?;
    let has_will = flags & 4 != 0;
    let will_qos = (flags >> 3) & 3;
    if flags & 1 != 0
        || will_qos == 3
        || (!has_will && flags & 0x38 != 0)
        || (flags & 0x40 != 0 && flags & 0x80 == 0)
    {
        return Err(Error::Invalid);
    }
    let client_id = cursor.string(limits.max_client_id_bytes)?;
    let will = if has_will {
        let topic = cursor.string(limits.max_topic_bytes)?;
        if !valid_topic(&topic, limits, false) {
            return Err(Error::Invalid);
        }
        Some(Will {
            topic,
            payload: cursor.binary(limits.max_will_payload_bytes)?,
            qos: will_qos,
            retain: flags & 0x20 != 0,
        })
    } else {
        None
    };
    let username = if flags & 0x80 != 0 {
        Some(cursor.string(limits.max_username_bytes)?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(cursor.binary(limits.max_password_bytes)?.to_vec())
    } else {
        None
    };
    cursor.done()?;
    Ok(Packet::Connect(Connect {
        client_id,
        username,
        password,
        keep_alive,
        clean_session: flags & 2 != 0,
        will,
    }))
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

fn encoded_size(remaining: usize) -> Result<usize> {
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

pub fn connack(session_present: bool, code: u8) -> Vec<u8> {
    vec![0x20, 2, u8::from(session_present), code]
}
pub fn ack(first: u8, id: u16) -> Vec<u8> {
    vec![first, 2, (id >> 8) as u8, id as u8]
}

#[allow(clippy::too_many_arguments)]
pub fn publish(
    topic: &str,
    payload: &[u8],
    qos: u8,
    packet_id: Option<u16>,
    retain: bool,
    dup: bool,
    limits: &Limits,
) -> Result<Vec<u8>> {
    if qos > 2
        || !valid_topic(topic, limits, false)
        || packet_id == Some(0)
        || (qos == 0) != packet_id.is_none()
        || (qos == 0 && dup)
    {
        return Err(Error::Invalid);
    }
    let topic_length = u16::try_from(topic.len()).map_err(|_| Error::Invalid)?;
    let size = topic
        .len()
        .checked_add(payload.len())
        .and_then(|value| value.checked_add(if packet_id.is_some() { 4 } else { 2 }))
        .ok_or(Error::Invalid)?;
    if encoded_size(size)? > limits.max_mqtt_packet_size {
        return Err(Error::Overloaded);
    }
    let mut body = Vec::with_capacity(size);
    body.extend_from_slice(&topic_length.to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if let Some(id) = packet_id {
        body.extend_from_slice(&id.to_be_bytes())
    }
    body.extend_from_slice(payload);
    let first = 0x30 | (u8::from(dup) << 3) | (qos << 1) | u8::from(retain);
    encode(first, &body, limits.max_mqtt_packet_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn text(value: &[u8], output: &mut Vec<u8>) {
        output.extend_from_slice(&(value.len() as u16).to_be_bytes());
        output.extend_from_slice(value);
    }
    #[test]
    fn all_control_packet_flags_and_identifiers_are_strict() {
        let limits = Limits::default();
        for first in [
            0x11, 0x21, 0x41, 0x51, 0x60, 0x71, 0x80, 0x91, 0xa0, 0xb1, 0xc1, 0xd1, 0xe1,
        ] {
            assert!(fixed_header(&[first, 0], 64).is_err(), "{first:02x}");
        }
        for first in [0x40, 0x50, 0x62, 0x70, 0xb0] {
            assert!(decode(&mut BytesMut::from(&[first, 2, 0, 0][..]), &limits).is_err());
        }
    }
    #[test]
    fn connect_will_and_utf8_validation() {
        let limits = Limits::default();
        let mut body = Vec::new();
        text(b"MQTT", &mut body);
        body.extend_from_slice(&[4, 0xee, 0, 30]);
        text(b"client", &mut body);
        text(b"v1/t/t/p/p/d/d/up", &mut body);
        text(b"will", &mut body);
        text(b"user", &mut body);
        text(b"secret", &mut body);
        let wire = encode(0x10, &body, 65_536).unwrap();
        let Some(Packet::Connect(connect)) =
            decode(&mut BytesMut::from(&wire[..]), &limits).unwrap()
        else {
            panic!()
        };
        assert_eq!(connect.will.unwrap().qos, 1);
        assert_eq!(connect.username.as_deref(), Some("user"));
        for invalid in [b"a\0b".as_slice(), &[0xc2, 0x80], &[0xef, 0xb7, 0x90]] {
            assert!(valid_utf8(invalid).is_err());
        }
    }
    #[test]
    fn wildcards_remaining_length_incremental_and_qos2() {
        let limits = Limits::default();
        for filter in ["+", "#", "sport/+/player1", "sport/#", "/+/"] {
            assert!(valid_topic(filter, &limits, true))
        }
        for filter in ["a+", "a/#/b", "##", "a/+b", "a/b#"] {
            assert!(!valid_topic(filter, &limits, true))
        }
        assert_eq!(
            remaining_length(&[0xff, 0xff, 0xff, 0x7f], usize::MAX).unwrap(),
            Some((268_435_455, 4))
        );
        assert!(remaining_length(&[0x80, 0x80, 0x80, 0x80], usize::MAX).is_err());
        let wire = publish("a/b", b"payload", 2, Some(7), true, false, &limits).unwrap();
        let mut partial = BytesMut::new();
        for (index, byte) in wire.iter().enumerate() {
            partial.extend_from_slice(&[*byte]);
            assert_eq!(
                decode(&mut partial, &limits).unwrap().is_some(),
                index + 1 == wire.len()
            );
        }
    }
    #[test]
    fn exact_packet_size_boundary_and_multiple_packets() {
        let limits = Limits {
            max_mqtt_packet_size: 256,
            ..Limits::default()
        };
        let bytes = publish("t", &[0; 248], 1, Some(1), false, false, &limits).unwrap();
        assert_eq!(bytes.len(), 256);
        assert!(publish("t", &[0; 249], 1, Some(1), false, false, &limits).is_err());
        let mut multiple = BytesMut::from(&[0xc0, 0, 0xe0, 0][..]);
        assert!(matches!(
            decode(&mut multiple, &limits).unwrap(),
            Some(Packet::Pingreq)
        ));
        assert!(matches!(
            decode(&mut multiple, &limits).unwrap(),
            Some(Packet::Disconnect)
        ));
    }
    #[test]
    fn arbitrary_bytes_smoke() {
        let limits = Limits::default();
        let mut seed = 19u64;
        for length in 0..512 {
            let mut bytes = Vec::new();
            for _ in 0..length {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                bytes.push((seed >> 32) as u8)
            }
            let _ = decode(&mut BytesMut::from(bytes.as_slice()), &limits);
        }
    }
}
