//! MQTT 5 wire codec. All client-controlled allocations are bounded by `Limits`.
use super::common::{encode, fixed_header, remaining_length, valid_topic, valid_utf8};
use bytes::{Buf, Bytes, BytesMut};
use netbaiot_runtime::{Error, Limits};

pub const MALFORMED_PACKET: u8 = 0x81;
pub const PROTOCOL_ERROR: u8 = 0x82;
pub const PACKET_TOO_LARGE: u8 = 0x95;
pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
pub const TOPIC_NAME_INVALID: u8 = 0x90;
pub const TOPIC_FILTER_INVALID: u8 = 0x8f;
pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
pub const PACKET_IDENTIFIER_IN_USE: u8 = 0x91;
pub const RECEIVE_MAXIMUM_EXCEEDED: u8 = 0x93;
pub const SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED: u8 = 0xa1;
pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0x9e;

/// Transport-only mapping; the public runtime `Error` keeps its existing meaning.
pub fn connect_reason(error: &Error) -> u8 {
    match error {
        Error::Authentication => 0x86,
        Error::Forbidden => 0x87,
        Error::Invalid => 0x85,
        Error::Overloaded | Error::Draining => 0x89,
        Error::Unavailable | Error::Timeout => 0x88,
        Error::Configuration
        | Error::Conflict
        | Error::Storage
        | Error::IncompatibleSpool
        | Error::Internal
        | Error::Codec => 0x83,
    }
}

pub fn disconnect_reason(error: &Error) -> u8 {
    match error {
        Error::Authentication | Error::Forbidden => 0x87,
        Error::Invalid => PROTOCOL_ERROR,
        Error::Overloaded => 0x97,
        Error::Conflict => 0x91,
        Error::Unavailable => 0x88,
        Error::Timeout => 0x8d,
        Error::Draining => 0x8b,
        Error::Codec => 0x99,
        Error::Configuration | Error::Storage | Error::IncompatibleSpool | Error::Internal => 0x83,
    }
}

pub fn subscription_reason(error: &Error) -> u8 {
    match error {
        Error::Forbidden | Error::Authentication => 0x87,
        Error::Invalid => 0x8f,
        Error::Overloaded => 0x97,
        Error::Draining | Error::Unavailable | Error::Timeout => 0x88,
        _ => 0x83,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError {
    pub reason: u8,
}

type Result<T> = std::result::Result<T, DecodeError>;
fn malformed() -> DecodeError {
    DecodeError {
        reason: MALFORMED_PACKET,
    }
}
fn protocol() -> DecodeError {
    DecodeError {
        reason: PROTOCOL_ERROR,
    }
}
fn too_large() -> DecodeError {
    DecodeError {
        reason: PACKET_TOO_LARGE,
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct Properties {
    pub payload_format: Option<u8>,
    pub message_expiry: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub session_expiry: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub request_response_information: Option<bool>,
    pub request_problem_information: Option<bool>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
    pub will_delay: Option<u32>,
    pub user_properties: Vec<(String, String)>,
}

impl Properties {
    pub fn retained_bytes(&self) -> usize {
        self.content_type.as_ref().map_or(0, String::len)
            + self.response_topic.as_ref().map_or(0, String::len)
            + self.correlation_data.as_ref().map_or(0, Bytes::len)
            + self
                .user_properties
                .iter()
                .map(|(key, value)| key.len() + value.len() + 4)
                .sum::<usize>()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PropertyContext {
    Connect,
    Will,
    Publish,
    Subscribe,
    Unsubscribe,
    Disconnect,
    Ack,
}

/// Decode a complete property field, including its Variable Byte Integer length.
pub fn decode_properties(
    input: &[u8],
    context: PropertyContext,
    limits: &Limits,
) -> Result<Properties> {
    if input.len() > limits.max_mqtt_property_bytes.saturating_add(4) {
        return Err(too_large());
    }
    let mut cursor = Cursor {
        bytes: Bytes::copy_from_slice(input),
    };
    let properties = cursor.properties(context, limits)?;
    if !cursor.bytes.is_empty() {
        return Err(malformed());
    }
    Ok(properties)
}

struct Cursor {
    bytes: Bytes,
}
impl Cursor {
    fn byte(&mut self) -> Result<u8> {
        if self.bytes.is_empty() {
            return Err(malformed());
        }
        Ok(self.bytes.get_u8())
    }
    fn short(&mut self) -> Result<u16> {
        if self.bytes.len() < 2 {
            return Err(malformed());
        }
        Ok(self.bytes.get_u16())
    }
    fn integer(&mut self) -> Result<u32> {
        if self.bytes.len() < 4 {
            return Err(malformed());
        }
        Ok(self.bytes.get_u32())
    }
    fn binary(&mut self, maximum: usize) -> Result<Bytes> {
        let length = usize::from(self.short()?);
        if length > maximum {
            return Err(too_large());
        }
        if length > self.bytes.len() {
            return Err(malformed());
        }
        Ok(self.bytes.split_to(length))
    }
    fn string(&mut self, maximum: usize) -> Result<String> {
        let bytes = self.binary(maximum)?;
        Ok(valid_utf8(&bytes).map_err(|_| malformed())?.to_owned())
    }
    fn id(&mut self) -> Result<u16> {
        let id = self.short()?;
        if id == 0 {
            return Err(protocol());
        }
        Ok(id)
    }
    fn variable(&mut self, maximum: usize) -> Result<usize> {
        let (value, used) = remaining_length(&self.bytes, maximum)
            .map_err(|error| {
                if matches!(error, Error::Overloaded) {
                    too_large()
                } else {
                    malformed()
                }
            })?
            .ok_or_else(malformed)?;
        self.bytes.advance(used);
        Ok(value)
    }
    fn properties(&mut self, context: PropertyContext, limits: &Limits) -> Result<Properties> {
        let length = self.variable(limits.max_mqtt_property_bytes)?;
        if length > self.bytes.len() {
            return Err(malformed());
        }
        let mut properties = Cursor {
            bytes: self.bytes.split_to(length),
        };
        let mut result = Properties::default();
        let mut seen = 0u64;
        let mut user_bytes = 0usize;
        while !properties.bytes.is_empty() {
            let id = properties.byte()?;
            if id >= 64 {
                return Err(malformed());
            }
            let repeatable = id == 0x26;
            if !repeatable && seen & (1u64 << id) != 0 {
                return Err(protocol());
            }
            seen |= 1u64 << id;
            let allowed = match id {
                0x01 | 0x02 | 0x03 | 0x08 | 0x09 => {
                    matches!(context, PropertyContext::Will | PropertyContext::Publish)
                }
                0x11 => matches!(
                    context,
                    PropertyContext::Connect | PropertyContext::Disconnect
                ),
                0x15 | 0x16 | 0x17 | 0x19 | 0x21 | 0x22 | 0x27 => {
                    context == PropertyContext::Connect
                }
                0x18 => context == PropertyContext::Will,
                0x1f => matches!(context, PropertyContext::Ack | PropertyContext::Disconnect),
                0x26 => true,
                0x23 => {
                    return Err(DecodeError {
                        reason: TOPIC_ALIAS_INVALID,
                    });
                }
                0x0b => {
                    return Err(DecodeError {
                        reason: SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED,
                    });
                }
                _ => return Err(malformed()),
            };
            if !allowed {
                return Err(malformed());
            }
            match id {
                0x01 => {
                    let value = properties.byte()?;
                    if value > 1 {
                        return Err(protocol());
                    }
                    result.payload_format = Some(value);
                }
                0x02 => result.message_expiry = Some(properties.integer()?),
                0x03 => {
                    result.content_type =
                        Some(properties.string(limits.max_mqtt_content_type_bytes)?)
                }
                0x08 => {
                    let value = properties.string(limits.max_mqtt_response_topic_bytes)?;
                    if !valid_topic(&value, limits, false) {
                        return Err(protocol());
                    }
                    result.response_topic = Some(value);
                }
                0x09 => {
                    result.correlation_data =
                        Some(properties.binary(limits.max_mqtt_correlation_data_bytes)?)
                }
                0x11 => result.session_expiry = Some(properties.integer()?),
                0x15 => {
                    result.authentication_method =
                        Some(properties.string(limits.max_mqtt_property_bytes)?)
                }
                0x16 => {
                    result.authentication_data =
                        Some(properties.binary(limits.max_mqtt_property_bytes)?)
                }
                0x17 | 0x19 => {
                    let value = properties.byte()?;
                    if value > 1 {
                        return Err(protocol());
                    }
                    if id == 0x17 {
                        result.request_problem_information = Some(value != 0);
                    } else {
                        result.request_response_information = Some(value != 0);
                    }
                }
                0x18 => result.will_delay = Some(properties.integer()?),
                0x1f => {
                    let _ = properties.string(limits.max_mqtt_property_bytes)?;
                }
                0x21 => {
                    let value = properties.short()?;
                    if value == 0 {
                        return Err(protocol());
                    }
                    result.receive_maximum = Some(value);
                }
                0x22 => result.topic_alias_maximum = Some(properties.short()?),
                0x26 => {
                    if result.user_properties.len() >= limits.max_mqtt_user_properties {
                        return Err(too_large());
                    }
                    let key = properties.string(limits.max_mqtt_user_property_bytes)?;
                    let value = properties.string(limits.max_mqtt_user_property_bytes)?;
                    user_bytes = user_bytes
                        .checked_add(key.len())
                        .and_then(|n| n.checked_add(value.len()))
                        .and_then(|n| n.checked_add(4))
                        .ok_or_else(too_large)?;
                    if user_bytes > limits.max_mqtt_user_property_bytes {
                        return Err(too_large());
                    }
                    result.user_properties.push((key, value));
                }
                0x27 => {
                    let value = properties.integer()?;
                    if value == 0 {
                        return Err(protocol());
                    }
                    result.maximum_packet_size = Some(value);
                }
                _ => return Err(malformed()),
            }
        }
        if result.authentication_data.is_some() && result.authentication_method.is_none() {
            return Err(protocol());
        }
        Ok(result)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub properties: Properties,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Connect {
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub properties: Properties,
    pub will: Option<Will>,
}

#[derive(Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Box<Connect>),
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        packet_id: Option<u16>,
        retain: bool,
        dup: bool,
        properties: Properties,
    },
    Puback {
        packet_id: u16,
        reason: u8,
    },
    Pubrec {
        packet_id: u16,
        reason: u8,
    },
    Pubrel {
        packet_id: u16,
        reason: u8,
    },
    Pubcomp {
        packet_id: u16,
        reason: u8,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, SubscriptionOptions)>,
        properties: Properties,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
        properties: Properties,
    },
    Pingreq,
    Disconnect {
        reason: u8,
        properties: Properties,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionOptions {
    pub qos: u8,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
}

fn decode_connect(cursor: &mut Cursor, limits: &Limits) -> Result<Packet> {
    if cursor.string(6)? != "MQTT" || cursor.byte()? != 5 {
        return Err(protocol());
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
        return Err(malformed());
    }
    let properties = cursor.properties(PropertyContext::Connect, limits)?;
    let client_id = cursor.string(limits.max_client_id_bytes)?;
    let will = if has_will {
        let properties = cursor.properties(PropertyContext::Will, limits)?;
        let topic = cursor.string(limits.max_topic_bytes)?;
        if !valid_topic(&topic, limits, false) {
            return Err(DecodeError {
                reason: TOPIC_NAME_INVALID,
            });
        }
        Some(Will {
            topic,
            payload: cursor.binary(limits.max_will_payload_bytes)?,
            qos: will_qos,
            retain: flags & 0x20 != 0,
            properties,
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
        Some(cursor.binary(limits.max_password_bytes)?)
    } else {
        None
    };
    if !cursor.bytes.is_empty() {
        return Err(malformed());
    }
    Ok(Packet::Connect(Box::new(Connect {
        client_id,
        username,
        password,
        keep_alive,
        clean_start: flags & 2 != 0,
        properties,
        will,
    })))
}

fn ack_reason(cursor: &mut Cursor, limits: &Limits, allowed: &[u8]) -> Result<(u16, u8)> {
    let id = cursor.id()?;
    let reason = if cursor.bytes.is_empty() {
        0
    } else {
        cursor.byte()?
    };
    if !allowed.contains(&reason) {
        return Err(protocol());
    }
    if !cursor.bytes.is_empty() {
        cursor.properties(PropertyContext::Ack, limits)?;
    }
    if !cursor.bytes.is_empty() {
        return Err(malformed());
    }
    Ok((id, reason))
}

pub fn decode(input: &mut BytesMut, limits: &Limits) -> Result<Option<Packet>> {
    let Some((first, header, total)) =
        fixed_header(input, limits.max_mqtt_packet_size).map_err(|error| {
            if matches!(error, Error::Overloaded) {
                too_large()
            } else {
                malformed()
            }
        })?
    else {
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
        3 => {
            let qos = (first >> 1) & 3;
            let topic = cursor.string(limits.max_topic_bytes)?;
            if !valid_topic(&topic, limits, false) {
                return Err(DecodeError {
                    reason: TOPIC_NAME_INVALID,
                });
            }
            let packet_id = if qos == 0 { None } else { Some(cursor.id()?) };
            let properties = cursor.properties(PropertyContext::Publish, limits)?;
            Packet::Publish {
                topic,
                payload: cursor.bytes,
                qos,
                packet_id,
                retain: first & 1 != 0,
                dup: first & 8 != 0,
                properties,
            }
        }
        4 => {
            let (packet_id, reason) = ack_reason(
                &mut cursor,
                limits,
                &[0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
            )?;
            Packet::Puback { packet_id, reason }
        }
        5 => {
            let (packet_id, reason) = ack_reason(
                &mut cursor,
                limits,
                &[0, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
            )?;
            Packet::Pubrec { packet_id, reason }
        }
        6 => {
            let (packet_id, reason) = ack_reason(&mut cursor, limits, &[0, 0x92])?;
            Packet::Pubrel { packet_id, reason }
        }
        7 => {
            let (packet_id, reason) = ack_reason(&mut cursor, limits, &[0, 0x92])?;
            Packet::Pubcomp { packet_id, reason }
        }
        8 => {
            let packet_id = cursor.id()?;
            let properties = cursor.properties(PropertyContext::Subscribe, limits)?;
            let mut filters = Vec::new();
            while !cursor.bytes.is_empty() {
                if filters.len() >= limits.max_subscription_filters_per_packet {
                    return Err(too_large());
                }
                let filter = cursor.string(limits.max_topic_bytes)?;
                if filter.starts_with("$share/") {
                    return Err(DecodeError {
                        reason: SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,
                    });
                }
                if !valid_topic(&filter, limits, true) {
                    return Err(DecodeError {
                        reason: TOPIC_FILTER_INVALID,
                    });
                }
                let options = cursor.byte()?;
                if options & 0xc0 != 0 || options & 3 > 2 || (options >> 4) & 3 > 2 {
                    return Err(protocol());
                }
                filters.push((
                    filter,
                    SubscriptionOptions {
                        qos: options & 3,
                        no_local: options & 4 != 0,
                        retain_as_published: options & 8 != 0,
                        retain_handling: (options >> 4) & 3,
                    },
                ));
            }
            if filters.is_empty() {
                return Err(protocol());
            }
            Packet::Subscribe {
                packet_id,
                filters,
                properties,
            }
        }
        10 => {
            let packet_id = cursor.id()?;
            let properties = cursor.properties(PropertyContext::Unsubscribe, limits)?;
            let mut filters = Vec::new();
            while !cursor.bytes.is_empty() {
                if filters.len() >= limits.max_subscription_filters_per_packet {
                    return Err(too_large());
                }
                let filter = cursor.string(limits.max_topic_bytes)?;
                if !valid_topic(&filter, limits, true) {
                    return Err(DecodeError {
                        reason: TOPIC_FILTER_INVALID,
                    });
                }
                filters.push(filter);
            }
            if filters.is_empty() {
                return Err(protocol());
            }
            Packet::Unsubscribe {
                packet_id,
                filters,
                properties,
            }
        }
        12 => {
            if !cursor.bytes.is_empty() {
                return Err(malformed());
            }
            Packet::Pingreq
        }
        14 => {
            let reason = if cursor.bytes.is_empty() {
                0
            } else {
                cursor.byte()?
            };
            if !matches!(
                reason,
                0 | 4
                    | 0x80
                    | 0x81
                    | 0x82
                    | 0x83
                    | 0x87
                    | 0x89
                    | 0x8b
                    | 0x8d
                    | 0x8e
                    | 0x8f
                    | 0x90
                    | 0x93
                    | 0x94
                    | 0x95
                    | 0x96
                    | 0x97
                    | 0x98
                    | 0x99
                    | 0x9a
                    | 0x9b
                    | 0x9c
                    | 0x9d
                    | 0x9e
                    | 0x9f
                    | 0xa0
                    | 0xa1
                    | 0xa2
            ) {
                return Err(protocol());
            }
            let properties = if cursor.bytes.is_empty() {
                Properties::default()
            } else {
                cursor.properties(PropertyContext::Disconnect, limits)?
            };
            if !cursor.bytes.is_empty() {
                return Err(malformed());
            }
            Packet::Disconnect { reason, properties }
        }
        _ => return Err(protocol()),
    };
    Ok(Some(packet))
}

pub fn connack(
    session_present: bool,
    reason: u8,
    limits: &Limits,
    assigned_client_id: Option<&str>,
) -> std::result::Result<Vec<u8>, Error> {
    let mut properties = Vec::with_capacity(24 + assigned_client_id.map_or(0, str::len));
    properties.extend_from_slice(&[0x21, 0, 0]);
    let receive_maximum = u16::try_from(
        limits
            .max_inflight_qos1_per_session
            .min(limits.max_inflight_qos2_per_session),
    )
    .map_err(|_| Error::Configuration)?
    .max(1);
    properties[1..3].copy_from_slice(&receive_maximum.to_be_bytes());
    properties.push(0x27);
    properties.extend_from_slice(
        &u32::try_from(limits.max_mqtt_packet_size)
            .map_err(|_| Error::Configuration)?
            .to_be_bytes(),
    );
    // Maximum QoS 2 is represented by omitting 0x24; MQTT 5 permits only 0 or 1 there.
    properties.extend_from_slice(&[0x22, 0, 0, 0x25, 1, 0x28, 1, 0x29, 0, 0x2a, 0]);
    if let Some(client_id) = assigned_client_id {
        properties.push(0x12);
        let length = u16::try_from(client_id.len()).map_err(|_| Error::Overloaded)?;
        properties.extend_from_slice(&length.to_be_bytes());
        properties.extend_from_slice(client_id.as_bytes());
    }
    let mut body = vec![u8::from(session_present), reason];
    variable(properties.len(), &mut body);
    body.extend_from_slice(&properties);
    encode(0x20, &body, limits.max_mqtt_packet_size)
}

fn variable(mut value: usize, output: &mut Vec<u8>) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn put_string(output: &mut Vec<u8>, value: &str) -> std::result::Result<(), Error> {
    let length = u16::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_binary(output: &mut Vec<u8>, value: &[u8]) -> std::result::Result<(), Error> {
    let length = u16::try_from(value.len()).map_err(|_| Error::Overloaded)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

pub struct OutboundPublish<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: u8,
    pub packet_id: Option<u16>,
    pub retain: bool,
    pub dup: bool,
    pub properties: &'a Properties,
}

pub fn publish(packet: OutboundPublish<'_>, maximum: usize) -> std::result::Result<Vec<u8>, Error> {
    let OutboundPublish {
        topic,
        payload,
        qos,
        packet_id,
        retain,
        dup,
        properties,
    } = packet;
    if qos > 2 || (qos > 0 && packet_id.is_none()) {
        return Err(Error::Invalid);
    }
    let mut body = Vec::with_capacity(
        topic
            .len()
            .saturating_add(payload.len())
            .saturating_add(128),
    );
    put_string(&mut body, topic)?;
    if let Some(id) = packet_id {
        body.extend_from_slice(&id.to_be_bytes());
    }
    let mut encoded = Vec::new();
    if let Some(value) = properties.payload_format {
        encoded.extend_from_slice(&[0x01, value]);
    }
    if let Some(value) = properties.message_expiry {
        encoded.push(0x02);
        encoded.extend_from_slice(&value.to_be_bytes());
    }
    if let Some(value) = &properties.content_type {
        encoded.push(0x03);
        put_string(&mut encoded, value)?;
    }
    if let Some(value) = &properties.response_topic {
        encoded.push(0x08);
        put_string(&mut encoded, value)?;
    }
    if let Some(value) = &properties.correlation_data {
        encoded.push(0x09);
        put_binary(&mut encoded, value)?;
    }
    for (key, value) in &properties.user_properties {
        encoded.push(0x26);
        put_string(&mut encoded, key)?;
        put_string(&mut encoded, value)?;
    }
    variable(encoded.len(), &mut body);
    body.extend_from_slice(&encoded);
    body.extend_from_slice(payload);
    encode(
        0x30 | (u8::from(dup) << 3) | (qos << 1) | u8::from(retain),
        &body,
        maximum,
    )
}

pub fn ack(
    first: u8,
    packet_id: u16,
    reason: u8,
    maximum: usize,
) -> std::result::Result<Vec<u8>, Error> {
    if reason == 0 {
        encode(first, &packet_id.to_be_bytes(), maximum)
    } else {
        encode(
            first,
            &[
                packet_id.to_be_bytes()[0],
                packet_id.to_be_bytes()[1],
                reason,
                0,
            ],
            maximum,
        )
    }
}

pub fn disconnect(reason: u8, maximum: usize) -> std::result::Result<Vec<u8>, Error> {
    encode(0xe0, &[reason, 0], maximum)
}

pub fn suback(
    packet_id: u16,
    reasons: &[u8],
    maximum: usize,
) -> std::result::Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(3 + reasons.len());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0); // Property Length
    body.extend_from_slice(reasons);
    encode(0x90, &body, maximum)
}

pub fn unsuback(
    packet_id: u16,
    reasons: &[u8],
    maximum: usize,
) -> std::result::Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(3 + reasons.len());
    body.extend_from_slice(&packet_id.to_be_bytes());
    body.push(0);
    body.extend_from_slice(reasons);
    encode(0xb0, &body, maximum)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(output: &mut Vec<u8>, value: &[u8]) {
        output.extend_from_slice(&(value.len() as u16).to_be_bytes());
        output.extend_from_slice(value);
    }

    fn properties(output: &mut Vec<u8>, values: &[u8]) {
        variable(values.len(), output);
        output.extend_from_slice(values);
    }

    fn connect(connect_properties: &[u8], will_properties: Option<&[u8]>) -> Vec<u8> {
        let mut body = Vec::new();
        string(&mut body, b"MQTT");
        body.extend_from_slice(&[
            5,
            if will_properties.is_some() {
                0x06
            } else {
                0x02
            },
            0,
            30,
        ]);
        properties(&mut body, connect_properties);
        string(&mut body, b"client");
        if let Some(will_properties) = will_properties {
            properties(&mut body, will_properties);
            string(&mut body, b"v1/t/t/p/p/d/d/up");
            string(&mut body, b"will");
        }
        encode(0x10, &body, 65_536).unwrap()
    }

    fn packet(first: u8, body: &[u8]) -> Result<Packet> {
        let wire = encode(first, body, 65_536).unwrap();
        decode(&mut BytesMut::from(wire.as_slice()), &Limits::default())?.ok_or_else(malformed)
    }

    #[test]
    fn connect_properties_and_will_are_bounded_and_parsed() {
        let mut props = vec![
            0x11, 0, 0, 0, 60, 0x21, 0, 7, 0x27, 0, 0, 4, 0, 0x22, 0, 0, 0x17, 0, 0x19, 1,
        ];
        props.push(0x26);
        string(&mut props, b"k");
        string(&mut props, b"v");
        let will = [0x18, 0, 0, 0, 2, 0x02, 0, 0, 0, 10, 0x01, 1];
        let wire = connect(&props, Some(&will));
        let Packet::Connect(value) =
            decode(&mut BytesMut::from(wire.as_slice()), &Limits::default())
                .unwrap()
                .unwrap()
        else {
            panic!("expected CONNECT")
        };
        assert_eq!(value.properties.session_expiry, Some(60));
        assert_eq!(value.properties.receive_maximum, Some(7));
        assert_eq!(value.properties.maximum_packet_size, Some(1024));
        assert_eq!(value.properties.user_properties.len(), 1);
        assert_eq!(value.will.as_ref().unwrap().properties.will_delay, Some(2));
        assert_eq!(
            value.will.as_ref().unwrap().properties.message_expiry,
            Some(10)
        );
    }

    #[test]
    fn duplicate_wrong_context_invalid_utf8_and_length_fail() {
        let cases = [
            connect(&[0x21, 0, 1, 0x21, 0, 2], None),
            connect(&[0x01, 1], None),
            connect(&[0x15, 0, 2, 0xc3, 0x28], None),
            connect(&[0x11, 0, 0], None),
            connect(&[0x16, 0, 1, 1], None),
            connect(&[0xff], None),
        ];
        for wire in cases {
            assert!(decode(&mut BytesMut::from(wire.as_slice()), &Limits::default()).is_err());
        }
        let limits = Limits {
            max_mqtt_property_bytes: 2,
            ..Limits::default()
        };
        assert_eq!(
            decode(
                &mut BytesMut::from(connect(&[0x21, 0, 1], None).as_slice()),
                &limits
            )
            .err()
            .unwrap()
            .reason,
            PACKET_TOO_LARGE
        );
        let mut user = vec![0x26];
        string(&mut user, b"key");
        string(&mut user, b"value");
        let limits = Limits {
            max_mqtt_user_property_bytes: 8,
            ..Limits::default()
        };
        assert_eq!(
            decode(
                &mut BytesMut::from(connect(&user, None).as_slice()),
                &limits
            )
            .err()
            .unwrap()
            .reason,
            PACKET_TOO_LARGE
        );
        assert_eq!(
            decode_properties(
                &[2, 0x0b, 1],
                PropertyContext::Subscribe,
                &Limits::default()
            )
            .err()
            .unwrap()
            .reason,
            SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
        );
    }

    #[test]
    fn publish_subscribe_disconnect_and_ack_reason_validation() {
        let mut body = Vec::new();
        string(&mut body, b"v1/t/t/p/p/d/d/up");
        body.extend_from_slice(&7u16.to_be_bytes());
        properties(
            &mut body,
            &[
                0x01, 1, 0x02, 0, 0, 0, 5, 0x03, 0, 4, b'j', b's', b'o', b'n',
            ],
        );
        body.extend_from_slice(b"data");
        let Packet::Publish {
            properties: publish_properties,
            payload,
            packet_id,
            ..
        } = packet(0x32, &body).unwrap()
        else {
            panic!("expected PUBLISH")
        };
        assert_eq!(publish_properties.message_expiry, Some(5));
        assert_eq!(publish_properties.content_type.as_deref(), Some("json"));
        assert_eq!(payload, Bytes::from_static(b"data"));
        assert_eq!(packet_id, Some(7));

        let mut subscribe = vec![0, 8];
        properties(&mut subscribe, &[]);
        string(&mut subscribe, b"v1/t/t/p/p/d/d/down");
        subscribe.push(0x1d);
        let Packet::Subscribe { filters, .. } = packet(0x82, &subscribe).unwrap() else {
            panic!("expected SUBSCRIBE")
        };
        assert_eq!(filters[0].1.retain_handling, 1);
        assert!(filters[0].1.no_local);
        assert!(filters[0].1.retain_as_published);
        assert_eq!(
            packet(0xe0, &[0, 2]).err().unwrap().reason,
            MALFORMED_PACKET
        );
        assert!(matches!(packet(0xe0, &[0]), Ok(Packet::Disconnect { .. })));
        assert_eq!(
            packet(0x40, &[0, 1, 0x7f]).err().unwrap().reason,
            PROTOCOL_ERROR
        );
    }

    #[test]
    fn arbitrary_bounded_input_never_panics() {
        let limits = Limits::default();
        let mut seed = 11u64;
        for len in 0..512 {
            let mut data = Vec::with_capacity(len);
            for _ in 0..len {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                data.push((seed >> 32) as u8);
            }
            let _ = decode(&mut BytesMut::from(data.as_slice()), &limits);
        }
    }

    #[test]
    fn runtime_errors_map_to_v5_reasons_without_changing_runtime_errors() {
        assert_eq!(connect_reason(&Error::Authentication), 0x86);
        assert_eq!(connect_reason(&Error::Overloaded), 0x89);
        assert_eq!(disconnect_reason(&Error::Overloaded), 0x97);
        assert_eq!(disconnect_reason(&Error::Conflict), 0x91);
        assert_eq!(disconnect_reason(&Error::Draining), 0x8b);
    }
}
