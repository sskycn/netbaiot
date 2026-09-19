use bytes::{Buf, Bytes, BytesMut};
use netbaiot_runtime::{Error, Limits, Result};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MqttProtocolVersion {
    V3_1_1,
    V5,
}
pub struct Connect {
    pub client_id: String,
    pub username: String,
    pub password: Vec<u8>,
    pub keep_alive: u16,
    pub clean_session: bool,
    pub has_will: bool,
}
pub enum Packet {
    Connect(Connect),
    UnsupportedVersion(u8),
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        packet_id: Option<u16>,
        retain: bool,
    },
    Puback(u16),
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    Pingreq,
    Disconnect,
}
/// Returns Remaining Length and its encoded byte count without reserving memory.
pub fn remaining_length(input: &[u8], maximum: usize) -> Result<Option<(usize, usize)>> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (i, b) in input.iter().take(4).enumerate() {
        value = value
            .checked_add(
                usize::from(b & 127)
                    .checked_mul(multiplier)
                    .ok_or(Error::Invalid)?,
            )
            .ok_or(Error::Invalid)?;
        if value > maximum {
            return Err(Error::Invalid);
        }
        if b & 128 == 0 {
            if i > 0 && *b == 0 {
                return Err(Error::Invalid);
            }
            return Ok(Some((value, i + 1)));
        }
        if i == 3 {
            return Err(Error::Invalid);
        }
        multiplier = multiplier.checked_mul(128).ok_or(Error::Invalid)?;
    }
    Ok(None)
}
pub fn fixed_header(input: &[u8], maximum: usize) -> Result<Option<(u8, usize, usize)>> {
    let Some(&first) = input.first() else {
        return Ok(None);
    };
    let kind = first >> 4;
    let flags = first & 15;
    match kind {
        1 | 4 | 12 | 14 if flags == 0 => {}
        3 if flags & 6 != 6 && !(flags & 6 == 0 && flags & 8 != 0) => {}
        8 | 10 if flags == 2 => {}
        _ => return Err(Error::Invalid),
    }
    let Some((remaining, n)) = remaining_length(&input[1..], maximum)? else {
        return Ok(None);
    };
    let header = n.checked_add(1).ok_or(Error::Invalid)?;
    let total = remaining.checked_add(header).ok_or(Error::Invalid)?;
    if total > maximum {
        return Err(Error::Invalid);
    }
    Ok(Some((first, header, total)))
}
fn valid_utf8(bytes: &[u8]) -> Result<&str> {
    let s = std::str::from_utf8(bytes).map_err(|_| Error::Invalid)?;
    if s.chars().any(|c| {
        c == '\0'
            || c.is_control()
            || (0xfdd0..=0xfdef).contains(&(c as u32))
            || ((c as u32) & 0xffff) >= 0xfffe
    }) {
        return Err(Error::Invalid);
    }
    Ok(s)
}
#[inline]
pub fn valid_topic(topic: &str, l: &Limits, filter: bool) -> bool {
    !topic.is_empty()
        && topic.len() <= l.max_topic_bytes
        && topic.split('/').count() <= l.max_topic_depth
        && valid_utf8(topic.as_bytes()).is_ok()
        && if filter {
            valid_filter(topic)
        } else {
            !topic.contains(['+', '#'])
        }
}
// Keep subscription grammar separate from the PUBLISH Topic Name hot path.
fn valid_filter(topic: &str) -> bool {
    let count = topic.split('/').count();
    topic.split('/').enumerate().all(|(i, level)| {
        (!level.contains('+') || level == "+")
            && (!level.contains('#') || (level == "#" && i + 1 == count))
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
    fn binary(&mut self, max: usize) -> Result<Bytes> {
        let len = usize::from(self.short()?);
        if len > max || len > self.bytes.len() {
            return Err(Error::Invalid);
        }
        Ok(self.bytes.split_to(len))
    }
    fn string(&mut self, max: usize) -> Result<String> {
        let bytes = self.binary(max)?;
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
pub fn decode(input: &mut BytesMut, l: &Limits) -> Result<Option<Packet>> {
    let Some((first, header, total)) = fixed_header(input, l.max_mqtt_packet_size)? else {
        return Ok(None);
    };
    if input.len() < total {
        return Ok(None);
    }
    let mut frame = input.split_to(total).freeze();
    frame.advance(header);
    let mut c = Cursor { bytes: frame };
    let packet = match first >> 4 {
        1 => {
            if c.string(6)? != "MQTT" {
                return Err(Error::Invalid);
            }
            let version = c.byte()?;
            if version != 4 {
                return Ok(Some(Packet::UnsupportedVersion(version)));
            }
            let flags = c.byte()?;
            let keep_alive = c.short()?;
            let will = flags & 4 != 0;
            let will_qos = (flags >> 3) & 3;
            if flags & 1 != 0
                || will_qos == 3
                || (!will && (flags & 0x38 != 0))
                || (flags & 64 != 0 && flags & 128 == 0)
            {
                return Err(Error::Invalid);
            }
            let client_id = c.string(l.max_client_id_bytes)?;
            if will {
                let topic = c.string(l.max_topic_bytes)?;
                if !valid_topic(&topic, l, false) {
                    return Err(Error::Invalid);
                }
                c.binary(l.max_mqtt_packet_size)?;
            }
            let username = if flags & 128 != 0 {
                c.string(l.max_username_bytes)?
            } else {
                String::new()
            };
            let password = if flags & 64 != 0 {
                c.binary(l.max_password_bytes)?.to_vec()
            } else {
                Vec::new()
            };
            c.done()?;
            Packet::Connect(Connect {
                client_id,
                username,
                password,
                keep_alive,
                clean_session: flags & 2 != 0,
                has_will: will,
            })
        }
        3 => {
            let qos = (first >> 1) & 3;
            if qos > 1 {
                return Err(Error::Invalid);
            }
            let topic = c.string(l.max_topic_bytes)?;
            if !valid_topic(&topic, l, false) {
                return Err(Error::Invalid);
            }
            let packet_id = if qos == 1 { Some(c.id()?) } else { None };
            Packet::Publish {
                topic,
                payload: c.bytes,
                qos,
                packet_id,
                retain: first & 1 != 0,
            }
        }
        4 => {
            let id = c.id()?;
            c.done()?;
            Packet::Puback(id)
        }
        8 => {
            let packet_id = c.id()?;
            let mut filters = Vec::new();
            while !c.bytes.is_empty() {
                if filters.len() >= l.max_subscription_filters_per_packet {
                    return Err(Error::Invalid);
                }
                let topic = c.string(l.max_topic_bytes)?;
                let qos = c.byte()?;
                if qos > 2 || !valid_topic(&topic, l, true) {
                    return Err(Error::Invalid);
                }
                filters.push((topic, qos));
            }
            if filters.is_empty() {
                return Err(Error::Invalid);
            }
            Packet::Subscribe { packet_id, filters }
        }
        10 => {
            let packet_id = c.id()?;
            let mut filters = Vec::new();
            while !c.bytes.is_empty() {
                if filters.len() >= l.max_subscription_filters_per_packet {
                    return Err(Error::Invalid);
                }
                let topic = c.string(l.max_topic_bytes)?;
                if !valid_topic(&topic, l, true) {
                    return Err(Error::Invalid);
                }
                filters.push(topic);
            }
            if filters.is_empty() {
                return Err(Error::Invalid);
            }
            Packet::Unsubscribe { packet_id, filters }
        }
        12 => {
            c.done()?;
            Packet::Pingreq
        }
        14 => {
            c.done()?;
            Packet::Disconnect
        }
        _ => return Err(Error::Invalid),
    };
    Ok(Some(packet))
}
fn variable(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n != 0 {
            b |= 128;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
}
fn encoded_size(remaining: usize) -> Result<usize> {
    let length_bytes = match remaining {
        0..=127 => 1,
        128..=16383 => 2,
        16384..=2097151 => 3,
        2097152..=268435455 => 4,
        _ => return Err(Error::Invalid),
    };
    remaining
        .checked_add(1 + length_bytes)
        .ok_or(Error::Invalid)
}
pub fn encode(first: u8, body: &[u8], maximum: usize) -> Result<Vec<u8>> {
    let size = encoded_size(body.len())?;
    if size > maximum {
        return Err(Error::Invalid);
    }
    let mut out = Vec::with_capacity(size);
    out.push(first);
    variable(body.len(), &mut out);
    out.extend_from_slice(body);
    Ok(out)
}
pub fn connack(code: u8) -> Vec<u8> {
    vec![0x20, 2, 0, code]
}
pub fn ack(first: u8, id: u16) -> Vec<u8> {
    vec![first, 2, (id >> 8) as u8, id as u8]
}
pub fn publish(topic: &str, payload: &[u8], packet_id: Option<u16>, l: &Limits) -> Result<Vec<u8>> {
    if !valid_topic(topic, l, false) || packet_id == Some(0) {
        return Err(Error::Invalid);
    }
    let len = u16::try_from(topic.len()).map_err(|_| Error::Invalid)?;
    let size = topic
        .len()
        .checked_add(payload.len())
        .and_then(|s| s.checked_add(if packet_id.is_some() { 4 } else { 2 }))
        .ok_or(Error::Invalid)?;
    if encoded_size(size)? > l.max_mqtt_packet_size {
        return Err(Error::Invalid);
    }
    let mut body = Vec::with_capacity(size);
    body.extend_from_slice(&len.to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if let Some(id) = packet_id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    encode(
        if packet_id.is_some() { 0x32 } else { 0x30 },
        &body,
        l.max_mqtt_packet_size,
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audit_malformed_wildcards_are_protocol_errors() {
        let l = Limits::default();
        for filter in ["a+", "a/#/b", "##", "a/+b", "a/b#"] {
            for kind in [0x82, 0xa2] {
                let mut body = vec![0, 1];
                body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
                body.extend_from_slice(filter.as_bytes());
                if kind == 0x82 {
                    body.push(1);
                }
                let wire = encode(kind, &body, 65536).unwrap();
                assert!(
                    decode(&mut BytesMut::from(wire.as_slice()), &l).is_err(),
                    "{filter}"
                );
            }
        }
        for filter in ["+", "#", "a/+", "a/#", "/+/", "a//b"] {
            assert!(valid_topic(filter, &l, true), "legal syntax: {filter}");
        }
    }
    #[test]
    fn exact_packet_size_boundary() {
        let l = Limits {
            max_mqtt_packet_size: 256,
            ..Limits::default()
        };
        let bytes = publish("t", &[0; 248], Some(1), &l).unwrap();
        assert_eq!(bytes.len(), 256);
        assert!(
            decode(&mut BytesMut::from(bytes.as_slice()), &l)
                .unwrap()
                .is_some()
        );
        assert!(publish("t", &[0; 249], Some(1), &l).is_err());
        assert_eq!(encode(0xc0, &[], 2).unwrap(), vec![0xc0, 0]);
    }
    #[test]
    fn incremental_and_multiple() {
        let l = Limits::default();
        let frame = publish("v1/t/t/p/p/d/d/up", &[1; 200], Some(1), &l).unwrap();
        let mut b = BytesMut::new();
        for (i, byte) in frame.iter().enumerate() {
            b.extend_from_slice(&[*byte]);
            assert_eq!(decode(&mut b, &l).unwrap().is_some(), i == frame.len() - 1);
        }
        b.extend_from_slice(&[0xc0, 0, 0xe0, 0]);
        assert!(matches!(decode(&mut b, &l).unwrap(), Some(Packet::Pingreq)));
        assert!(matches!(
            decode(&mut b, &l).unwrap(),
            Some(Packet::Disconnect)
        ));
    }
    #[test]
    fn malformed_and_boundaries() {
        let l = Limits::default();
        for b in [
            &[0xc1, 0][..],
            &[0x30, 0xff, 0xff, 0xff, 0xff],
            &[0x30, 0x80, 0],
            &[0x40, 2, 0, 0],
            &[0x30, 3, 0, 1, 0xff],
            &[0x36, 0],
            &[0x30, 4, 0, 1, b'#', 0],
        ] {
            assert!(decode(&mut BytesMut::from(b), &l).is_err());
        }
        assert!(
            decode(&mut BytesMut::from(&[0x30, 127][..]), &l)
                .unwrap()
                .is_none()
        );
        let small = Limits {
            max_mqtt_packet_size: 4,
            ..l
        };
        assert!(fixed_header(&[0x30, 2], 4).is_ok());
        assert!(decode(&mut BytesMut::from(&[0x30, 3][..]), &small).is_err());
    }
    #[test]
    fn arbitrary_bytes_smoke() {
        let l = Limits::default();
        let mut seed = 19u64;
        for len in 0..512 {
            let mut bytes = Vec::new();
            for _ in 0..len {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                bytes.push((seed >> 32) as u8);
            }
            let _ = decode(&mut BytesMut::from(bytes.as_slice()), &l);
            let _ = remaining_length(&bytes, 65536);
        }
    }
}

#[cfg(test)]
mod audit {
    use super::*;
    fn string(out: &mut Vec<u8>, value: &[u8]) {
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        out.extend_from_slice(value);
    }
    fn connect(flags: u8) -> Vec<u8> {
        let mut b = Vec::new();
        string(&mut b, b"MQTT");
        b.extend_from_slice(&[4, flags, 0, 30]);
        string(&mut b, b"a");
        if flags & 4 != 0 {
            string(&mut b, b"will");
            string(&mut b, b"data");
        }
        if flags & 128 != 0 {
            string(&mut b, b"a");
        }
        if flags & 64 != 0 {
            string(&mut b, &[b'0'; 64]);
        }
        encode(0x10, &b, 65536).unwrap()
    }
    #[test]
    fn every_supported_packet_survives_every_split_and_coalescing() {
        let l = Limits::default();
        let mut sub = vec![0, 1];
        string(&mut sub, b"a");
        sub.push(1);
        let mut unsub = vec![0, 2];
        string(&mut unsub, b"a");
        let packets = vec![
            connect(0xc2),
            publish("a", &[0; 200], None, &l).unwrap(),
            publish("a", &[0; 16384], Some(65535), &l).unwrap(),
            ack(0x40, 1),
            encode(0x82, &sub, 65536).unwrap(),
            encode(0xa2, &unsub, 65536).unwrap(),
            vec![0xc0, 0],
            vec![0xe0, 0],
        ];
        for packet in &packets {
            for split in 0..packet.len() {
                let mut b = BytesMut::from(&packet[..split]);
                assert!(decode(&mut b, &l).unwrap().is_none());
                assert_eq!(b.len(), split);
                b.extend_from_slice(&packet[split..]);
                assert!(decode(&mut b, &l).unwrap().is_some());
                assert!(b.is_empty());
            }
            let mut b = BytesMut::new();
            for (i, byte) in packet.iter().enumerate() {
                b.extend_from_slice(&[*byte]);
                assert_eq!(decode(&mut b, &l).unwrap().is_some(), i + 1 == packet.len());
            }
        }
        let mut all = BytesMut::from(packets.concat().as_slice());
        all.extend_from_slice(&[0x30]);
        for _ in &packets {
            assert!(decode(&mut all, &l).unwrap().is_some());
        }
        assert!(decode(&mut all, &l).unwrap().is_none());
        assert_eq!(all.as_ref(), &[0x30]);
    }
    #[test]
    fn remaining_length_all_widths_and_invalid_continuations() {
        for value in [0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455] {
            let mut b = Vec::new();
            variable(value, &mut b);
            for i in 0..b.len() {
                assert_eq!(remaining_length(&b[..i], value).unwrap(), None);
            }
            assert_eq!(remaining_length(&b, value).unwrap(), Some((value, b.len())));
            if value > 0 {
                assert!(remaining_length(&b, value - 1).is_err());
            }
        }
        for bad in [
            &[128, 0][..],
            &[255, 255, 255, 128],
            &[128, 128, 128, 128, 0],
        ] {
            assert!(remaining_length(bad, usize::MAX).is_err());
        }
    }
    #[test]
    fn connect_flags_utf8_and_packet_identifiers_are_strict() {
        let l = Limits::default();
        for flags in [0xc3, 0x42, 0xca, 0xe2, 0xde] {
            assert!(
                decode(&mut BytesMut::from(connect(flags).as_slice()), &l).is_err(),
                "flags={flags:x}"
            );
        }
        for flags in [0xc0, 0xc6, 0xce, 0xd6] {
            let Packet::Connect(c) = decode(&mut BytesMut::from(connect(flags).as_slice()), &l)
                .unwrap()
                .unwrap()
            else {
                panic!()
            };
            assert!(!c.clean_session || c.has_will);
        }
        for invalid in [
            &[0][..],
            &[0xc0, 0x80],
            &[0xed, 0xa0, 0x80],
            &[0xef, 0xb7, 0x90],
            &[0xef, 0xbf, 0xbf],
            b"\n",
        ] {
            assert!(valid_utf8(invalid).is_err());
        }
        assert_eq!(valid_utf8(b"\xef\xbb\xbf").unwrap(), "\u{feff}");
        for first in [0x40, 0x82, 0xa2] {
            assert!(decode(&mut BytesMut::from(ack(first, 0).as_slice()), &l).is_err());
        }
        assert!(publish("a", b"b", Some(0), &l).is_err());
        for first in [0x00, 0xf0, 0xc1, 0xe1, 0x80, 0xa0, 0x38, 0x36] {
            assert!(fixed_header(&[first, 0], 65536).is_err());
        }
    }
}
