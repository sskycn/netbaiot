//! Device-profile client packet codec. Only packets legal from a server are decoded.
use crate::{
    Result, WireError, encode, fixed_header, put_variable, remaining_length, valid_topic,
    valid_utf8,
};
use bytes::{Buf, Bytes, BytesMut};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    V311,
    V5,
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub packet: usize,
    pub payload: usize,
    pub topic: usize,
    pub properties: usize,
    pub property_count: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            packet: 131_072,
            payload: 65_536,
            topic: 256,
            properties: 4096,
            property_count: 32,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Properties {
    pub session_expiry: Option<u32>,
    pub message_expiry: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub server_keep_alive: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub topic_alias: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connack {
        session_present: bool,
        reason: u8,
        properties: Properties,
    },
    Suback {
        packet_id: u16,
        reasons: Vec<u8>,
    },
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        packet_id: Option<u16>,
        dup: bool,
        properties: Properties,
    },
    Puback {
        packet_id: u16,
        reason: u8,
    },
    Pingresp,
    Disconnect {
        reason: u8,
    },
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or(WireError::Invalid)?;
        let value = self.bytes.get(self.at..end).ok_or(WireError::Invalid)?;
        self.at = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn short(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn integer(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn id(&mut self) -> Result<u16> {
        let id = self.short()?;
        if id == 0 {
            Err(WireError::Invalid)
        } else {
            Ok(id)
        }
    }
    fn binary(&mut self, limit: usize) -> Result<&'a [u8]> {
        let len = usize::from(self.short()?);
        if len > limit {
            return Err(WireError::TooLarge);
        }
        self.take(len)
    }
    fn string(&mut self, limit: usize) -> Result<String> {
        Ok(valid_utf8(self.binary(limit)?)?.to_owned())
    }
    fn variable(&mut self, limit: usize) -> Result<usize> {
        let Some((value, used)) = remaining_length(&self.bytes[self.at..], limit)? else {
            return Err(WireError::Invalid);
        };
        self.at += used;
        Ok(value)
    }
    fn done(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(WireError::Invalid)
        }
    }
}

#[derive(Clone, Copy)]
enum Context {
    Connack,
    Publish,
    Puback,
    Suback,
    Disconnect,
}
fn properties(reader: &mut Reader<'_>, context: Context, limits: Limits) -> Result<Properties> {
    let length = reader.variable(limits.properties)?;
    let bytes = reader.take(length)?;
    let mut r = Reader { bytes, at: 0 };
    let mut out = Properties::default();
    let mut seen = [false; 43];
    let mut count = 0usize;
    while r.remaining() > 0 {
        count += 1;
        if count > limits.property_count {
            return Err(WireError::TooLarge);
        }
        let id = r.byte()?;
        let slot = seen.get_mut(usize::from(id)).ok_or(WireError::Invalid)?;
        if *slot && id != 0x26 && id != 0x0b {
            return Err(WireError::Invalid);
        }
        *slot = true;
        match id {
            0x01 if matches!(context, Context::Publish) => {
                if r.byte()? > 1 {
                    return Err(WireError::Invalid);
                }
            }
            0x02 if matches!(context, Context::Publish) => out.message_expiry = Some(r.integer()?),
            0x03 if matches!(context, Context::Publish) => {
                r.string(limits.properties)?;
            }
            0x08 if matches!(context, Context::Publish) => {
                let value = r.string(limits.topic)?;
                if !valid_topic(&value, limits.topic, 8, false) {
                    return Err(WireError::Invalid);
                }
            }
            0x09 if matches!(context, Context::Publish) => {
                r.binary(limits.properties)?;
            }
            0x0b if matches!(context, Context::Publish) => {
                if r.variable(268_435_455)? == 0 {
                    return Err(WireError::Invalid);
                }
            }
            0x11 if matches!(context, Context::Connack | Context::Disconnect) => {
                out.session_expiry = Some(r.integer()?)
            }
            0x12 | 0x1a | 0x1c if matches!(context, Context::Connack) => {
                r.string(limits.properties)?;
            }
            0x13 if matches!(context, Context::Connack) => {
                out.server_keep_alive = Some(r.short()?);
            }
            0x15 if matches!(context, Context::Connack) => {
                r.string(limits.properties)?;
            }
            0x16 if matches!(context, Context::Connack) => {
                r.binary(limits.properties)?;
            }
            0x1f if !matches!(context, Context::Publish) => {
                r.string(limits.properties)?;
            }
            0x21 if matches!(context, Context::Connack) => {
                let v = r.short()?;
                if v == 0 {
                    return Err(WireError::Invalid);
                }
                out.receive_maximum = Some(v);
            }
            0x22 if matches!(context, Context::Connack) => {
                r.short()?;
            }
            0x23 if matches!(context, Context::Publish) => {
                let v = r.short()?;
                if v == 0 {
                    return Err(WireError::Invalid);
                }
                out.topic_alias = Some(v);
            }
            0x24 if matches!(context, Context::Connack) => {
                let v = r.byte()?;
                if v > 1 {
                    return Err(WireError::Invalid);
                }
                out.maximum_qos = Some(v);
            }
            0x25 | 0x28 | 0x29 | 0x2a if matches!(context, Context::Connack) => {
                if r.byte()? > 1 {
                    return Err(WireError::Invalid);
                }
            }
            0x26 => {
                r.string(limits.properties)?;
                r.string(limits.properties)?;
            }
            0x27 if matches!(context, Context::Connack) => {
                let v = r.integer()?;
                if v == 0 {
                    return Err(WireError::Invalid);
                }
                out.maximum_packet_size = Some(v);
            }
            _ => return Err(WireError::Invalid),
        }
    }
    Ok(out)
}

pub fn decode(input: &mut BytesMut, version: Version, limits: Limits) -> Result<Option<Packet>> {
    let Some((first, header, total)) = fixed_header(input, limits.packet)? else {
        return Ok(None);
    };
    if input.len() < total {
        return Ok(None);
    }
    let mut frame = input.split_to(total).freeze();
    frame.advance(header);
    let mut r = Reader {
        bytes: &frame,
        at: 0,
    };
    let packet = match first >> 4 {
        2 => {
            let flags = r.byte()?;
            let reason = r.byte()?;
            if flags & 0xfe != 0 || (reason != 0 && flags != 0) {
                return Err(WireError::Invalid);
            }
            let p = if version == Version::V5 {
                properties(&mut r, Context::Connack, limits)?
            } else {
                Properties::default()
            };
            if (version == Version::V311 && reason > 5)
                || (version == Version::V5
                    && !matches!(
                        reason,
                        0 | 0x80
                            | 0x81
                            | 0x82
                            | 0x83
                            | 0x84
                            | 0x85
                            | 0x86
                            | 0x87
                            | 0x88
                            | 0x89
                            | 0x8a
                            | 0x8c
                            | 0x90
                            | 0x95
                            | 0x97
                            | 0x99
                            | 0x9a
                            | 0x9b
                            | 0x9c
                            | 0x9d
                            | 0x9f
                    ))
            {
                return Err(WireError::Invalid);
            }
            r.done()?;
            Packet::Connack {
                session_present: flags == 1,
                reason,
                properties: p,
            }
        }
        3 => {
            let qos = (first >> 1) & 3;
            if qos > 1 {
                return Err(WireError::Invalid);
            }
            let topic = r.string(limits.topic)?;
            if !valid_topic(&topic, limits.topic, 8, false) {
                return Err(WireError::Invalid);
            }
            let packet_id = if qos == 1 { Some(r.id()?) } else { None };
            let p = if version == Version::V5 {
                properties(&mut r, Context::Publish, limits)?
            } else {
                Properties::default()
            };
            if p.topic_alias.is_some() {
                return Err(WireError::Invalid);
            }
            if r.remaining() > limits.payload {
                return Err(WireError::TooLarge);
            }
            let payload_start = r.at;
            r.take(r.remaining())?;
            let payload = frame.slice(payload_start..);
            Packet::Publish {
                topic,
                payload,
                qos,
                packet_id,
                dup: first & 8 != 0,
                properties: p,
            }
        }
        4 => {
            let packet_id = r.id()?;
            let reason = if version == Version::V5 && r.remaining() > 0 {
                r.byte()?
            } else {
                0
            };
            if version == Version::V5 {
                if !matches!(
                    reason,
                    0 | 0x10 | 0x80 | 0x83 | 0x87 | 0x90 | 0x91 | 0x97 | 0x99
                ) {
                    return Err(WireError::Invalid);
                }
                if r.remaining() > 0 {
                    properties(&mut r, Context::Puback, limits)?;
                }
            }
            r.done()?;
            Packet::Puback { packet_id, reason }
        }
        9 => {
            let packet_id = r.id()?;
            if version == Version::V5 {
                properties(&mut r, Context::Suback, limits)?;
            }
            if r.remaining() != 1 {
                return Err(WireError::Invalid);
            }
            let reason = r.byte()?;
            let valid = if version == Version::V311 {
                matches!(reason, 0 | 1 | 2 | 0x80)
            } else {
                matches!(
                    reason,
                    0 | 1 | 2 | 0x80 | 0x83 | 0x87 | 0x8f | 0x91 | 0x97 | 0x9e | 0xa1
                )
            };
            if !valid {
                return Err(WireError::Invalid);
            }
            Packet::Suback {
                packet_id,
                reasons: vec![reason],
            }
        }
        13 => {
            r.done()?;
            Packet::Pingresp
        }
        14 if version == Version::V5 => {
            let reason = if r.remaining() > 0 { r.byte()? } else { 0 };
            if !matches!(reason, 0 | 4 | 0x80..=0x83 | 0x87 | 0x89 | 0x8b | 0x8d..=0x90 | 0x93..=0xa2)
            {
                return Err(WireError::Invalid);
            }
            if r.remaining() > 0 {
                properties(&mut r, Context::Disconnect, limits)?;
            }
            r.done()?;
            Packet::Disconnect { reason }
        }
        _ => return Err(WireError::Invalid),
    };
    Ok(Some(packet))
}

pub struct Connect<'a> {
    pub version: Version,
    pub client_id: &'a str,
    pub username: &'a str,
    pub password: &'a [u8],
    pub keep_alive: u16,
    pub clean_start: bool,
    pub session_expiry: u32,
    pub receive_maximum: u16,
    pub maximum_packet_size: u32,
}
fn put_binary(body: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u16::try_from(value.len()).map_err(|_| WireError::TooLarge)?;
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(value);
    Ok(())
}
pub fn connect(value: Connect<'_>, maximum: usize) -> Result<Vec<u8>> {
    if value.client_id.is_empty()
        || value.client_id.len() > 64
        || value.username.is_empty()
        || value.username.len() > 64
        || value.password.is_empty()
        || value.password.len() > 256
        || value.receive_maximum == 0
    {
        return Err(WireError::Invalid);
    }
    let mut body = Vec::with_capacity(128 + value.password.len());
    put_binary(&mut body, b"MQTT")?;
    body.push(if value.version == Version::V5 { 5 } else { 4 });
    body.push(0xc0 | u8::from(value.clean_start) << 1);
    body.extend_from_slice(&value.keep_alive.to_be_bytes());
    if value.version == Version::V5 {
        let mut p = Vec::with_capacity(16);
        p.push(0x11);
        p.extend_from_slice(&value.session_expiry.to_be_bytes());
        p.push(0x21);
        p.extend_from_slice(&value.receive_maximum.to_be_bytes());
        p.push(0x27);
        p.extend_from_slice(&value.maximum_packet_size.to_be_bytes());
        p.push(0x22);
        p.extend_from_slice(&0u16.to_be_bytes());
        put_variable(p.len(), &mut body)?;
        body.extend_from_slice(&p);
    }
    put_binary(&mut body, value.client_id.as_bytes())?;
    put_binary(&mut body, value.username.as_bytes())?;
    put_binary(&mut body, value.password)?;
    encode(0x10, &body, maximum)
}
pub fn subscribe(version: Version, id: u16, topic: &str, maximum: usize) -> Result<Vec<u8>> {
    if id == 0 || !valid_topic(topic, 256, 8, true) {
        return Err(WireError::Invalid);
    }
    let mut body = Vec::with_capacity(topic.len() + 8);
    body.extend_from_slice(&id.to_be_bytes());
    if version == Version::V5 {
        body.push(0);
    }
    put_binary(&mut body, topic.as_bytes())?;
    body.push(1);
    encode(0x82, &body, maximum)
}
#[allow(clippy::too_many_arguments)]
pub fn publish(
    version: Version,
    topic: &str,
    payload: &[u8],
    qos: u8,
    id: Option<u16>,
    dup: bool,
    expiry: Option<u32>,
    maximum: usize,
) -> Result<Vec<u8>> {
    if qos > 1
        || !valid_topic(topic, 256, 8, false)
        || (qos == 0) != id.is_none()
        || id == Some(0)
        || (qos == 0 && dup)
    {
        return Err(WireError::Invalid);
    }
    let overhead = topic.len().checked_add(16).ok_or(WireError::TooLarge)?;
    if payload.len() > maximum.saturating_sub(overhead) {
        return Err(WireError::TooLarge);
    }
    let body_size = topic
        .len()
        .checked_add(payload.len())
        .and_then(|n| n.checked_add(16))
        .ok_or(WireError::TooLarge)?;
    let mut body = Vec::with_capacity(body_size);
    put_binary(&mut body, topic.as_bytes())?;
    if let Some(id) = id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    if version == Version::V5 {
        if let Some(seconds) = expiry {
            body.push(5);
            body.push(0x02);
            body.extend_from_slice(&seconds.to_be_bytes());
        } else {
            body.push(0);
        }
    }
    body.extend_from_slice(payload);
    encode(0x30 | (u8::from(dup) << 3) | (qos << 1), &body, maximum)
}
pub fn puback(version: Version, id: u16) -> Result<Vec<u8>> {
    if id == 0 {
        return Err(WireError::Invalid);
    }
    let _ = version;
    Ok(vec![0x40, 2, (id >> 8) as u8, id as u8])
}
pub fn pingreq() -> [u8; 2] {
    [0xc0, 0]
}
pub fn disconnect() -> [u8; 2] {
    [0xe0, 0]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v311_connect_and_subscribe_match_independent_vectors() {
        let packet = connect(
            Connect {
                version: Version::V311,
                client_id: "c",
                username: "u",
                password: b"p",
                keep_alive: 15,
                clean_start: false,
                session_expiry: 0,
                receive_maximum: 16,
                maximum_packet_size: 65_536,
            },
            128,
        )
        .unwrap();
        assert_eq!(
            packet,
            [
                0x10, 0x13, 0, 4, b'M', b'Q', b'T', b'T', 4, 0xc0, 0, 15, 0, 1, b'c', 0, 1, b'u',
                0, 1, b'p'
            ]
        );
        assert_eq!(
            subscribe(Version::V311, 1, "a/b", 128).unwrap(),
            [0x82, 8, 0, 1, 0, 3, b'a', b'/', b'b', 1]
        );
    }

    #[test]
    fn both_versions_fragment_and_reject_wrong_ack_or_header() {
        for (version, wire) in [
            (Version::V311, vec![0x20, 2, 0, 0]),
            (Version::V5, vec![0x20, 3, 0, 0, 0]),
        ] {
            for split in 0..=wire.len() {
                let mut input = BytesMut::from(&wire[..split]);
                if split < wire.len() {
                    assert_eq!(
                        decode(&mut input, version, Limits::default()).unwrap(),
                        None
                    );
                    input.extend_from_slice(&wire[split..]);
                }
                assert!(matches!(
                    decode(&mut input, version, Limits::default()).unwrap(),
                    Some(Packet::Connack { reason: 0, .. })
                ));
                assert!(input.is_empty());
            }
        }
        let mut input = BytesMut::from(&[0x40, 2, 0, 0][..]);
        assert_eq!(
            decode(&mut input, Version::V311, Limits::default()),
            Err(WireError::Invalid)
        );
        for invalid in [&[0x30, 0x80, 0][..], &[0x62, 0][..], &[0x40, 1, 0][..]] {
            let mut input = BytesMut::from(invalid);
            assert!(decode(&mut input, Version::V311, Limits::default()).is_err());
        }
    }

    #[test]
    fn v5_properties_and_negative_puback_are_validated() {
        // CONNACK: Receive Maximum=1, Server Keep Alive=2, Maximum QoS=1.
        let mut input = BytesMut::from(
            &[
                0x20, 0x0e, 0, 0, 11, 0x21, 0, 1, 0x13, 0, 2, 0x24, 1, 0x22, 0, 0,
            ][..],
        );
        let Some(Packet::Connack { properties, .. }) =
            decode(&mut input, Version::V5, Limits::default()).unwrap()
        else {
            panic!("CONNACK expected")
        };
        assert_eq!(properties.receive_maximum, Some(1));
        assert_eq!(properties.server_keep_alive, Some(2));
        let mut input = BytesMut::from(&[0x40, 4, 0, 7, 0x97, 0][..]);
        assert_eq!(
            decode(&mut input, Version::V5, Limits::default()).unwrap(),
            Some(Packet::Puback {
                packet_id: 7,
                reason: 0x97
            })
        );
        let mut duplicate = BytesMut::from(&[0x20, 9, 0, 0, 6, 0x21, 0, 1, 0x21, 0, 2][..]);
        assert_eq!(
            decode(&mut duplicate, Version::V5, Limits::default()),
            Err(WireError::Invalid)
        );
    }

    #[test]
    fn hostile_lengths_properties_and_packet_size_are_bounded() {
        for packet in [
            &[0x20, 6, 0, 0, 3, 0x21, 0, 0][..],       // Receive Maximum=0
            &[0x20, 8, 0, 0, 5, 0x27, 0, 0, 0, 0][..], // Maximum Packet Size=0
            &[0x20, 5, 0, 0, 2, 0x23, 1][..],          // Topic Alias in CONNACK
            &[0x20, 4, 0, 0, 1, 0xff][..],             // Unknown property
            &[0x20, 4, 0, 0, 0x80, 0][..],             // Noncanonical property length
            &[0x40, 2, 0, 0][..],                      // Zero Packet Identifier
        ] {
            let mut input = BytesMut::from(packet);
            assert!(decode(&mut input, Version::V5, Limits::default()).is_err());
        }
        let packet = publish(
            Version::V5,
            "a/b",
            &[1; 64],
            1,
            Some(7),
            false,
            Some(1),
            128,
        )
        .unwrap();
        assert_eq!(packet.len(), 79);
        assert_eq!(
            publish(Version::V5, "a/b", &[1; 64], 1, Some(7), false, Some(1), 78),
            Err(WireError::TooLarge)
        );
        let mut joined = BytesMut::from(&[0xd0, 0, 0x40, 2, 0, 7][..]);
        assert_eq!(
            decode(&mut joined, Version::V5, Limits::default()).unwrap(),
            Some(Packet::Pingresp)
        );
        assert_eq!(
            decode(&mut joined, Version::V5, Limits::default()).unwrap(),
            Some(Packet::Puback {
                packet_id: 7,
                reason: 0
            })
        );
        assert!(joined.is_empty());
    }
}
