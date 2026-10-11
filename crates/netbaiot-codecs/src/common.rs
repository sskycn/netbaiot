use netbaiot_core::*;
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, Visitor},
};
use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Write},
};

pub(crate) fn valid_text(s: &str, limit: usize) -> bool {
    s.len() <= limit && !s.chars().any(char::is_control)
}
pub(crate) fn scalar(s: &Scalar, limit: usize) -> bool {
    match s {
        Scalar::Text(s) => valid_text(s, limit),
        Scalar::Number(n) => n.is_finite(),
        Scalar::Boolean(_) => true,
    }
}
pub(crate) fn validate_kind(payload: &DeviceEventKind, l: &CodecLimits) -> bool {
    match payload {
        DeviceEventKind::Telemetry(fields) => {
            !fields.is_empty()
                && fields.len() <= l.fields
                && fields.iter().all(|(k, v)| {
                    !k.is_empty() && valid_text(k, l.field_bytes) && scalar(v, l.field_bytes)
                })
        }
        DeviceEventKind::DeviceEvent(e) => {
            !e.name.is_empty()
                && valid_text(&e.name, l.field_bytes)
                && e.value.as_ref().is_none_or(|v| scalar(v, l.field_bytes))
        }
        DeviceEventKind::Heartbeat(_) => true,
        DeviceEventKind::CommandAck(a) => a.execution != ExecutionState::Unknown,
    }
}
pub(crate) fn validate_command(
    ctx: &EncodeContext<'_>,
    c: &DeviceCommand,
    l: &CodecLimits,
) -> bool {
    &c.device == ctx.device
        && !c.payload.name.is_empty()
        && valid_text(&c.payload.name, l.field_bytes)
        && c.payload.arguments.len() <= l.fields
        && c.payload
            .arguments
            .iter()
            .all(|(k, v)| valid_text(k, l.field_bytes) && scalar(v, l.field_bytes))
}

// Preserve integer type until schema conversion (heartbeat permits the full u64 range).
#[derive(Debug)]
pub(crate) enum Atom {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Null,
}
impl<'de> Deserialize<'de> for Atom {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = Atom;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a scalar")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Atom, E> {
                Ok(Atom::Unsigned(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Atom, E> {
                Ok(Atom::Signed(v))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Atom, E> {
                if v.is_finite() {
                    Ok(Atom::Float(v))
                } else {
                    Err(E::custom("nonfinite number"))
                }
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Atom, E> {
                Ok(Atom::Bool(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Atom, E> {
                Ok(Atom::Text(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Atom, E> {
                Ok(Atom::Text(v))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Atom, E> {
                Ok(Atom::Null)
            }
        }
        d.deserialize_any(V)
    }
}
pub(crate) fn unsigned_scalar(v: u64) -> Result<Scalar, CodecError> {
    if v > (1u64 << 53) {
        return Err(CodecError);
    }
    Ok(Scalar::Number(v as f64))
}
pub(crate) fn signed_scalar(v: i64) -> Result<Scalar, CodecError> {
    if v.unsigned_abs() > (1u64 << 53) {
        return Err(CodecError);
    }
    Ok(Scalar::Number(v as f64))
}
impl Atom {
    fn into_scalar(self) -> Result<Scalar, CodecError> {
        match self {
            Self::Unsigned(n) => unsigned_scalar(n),
            Self::Signed(n) => signed_scalar(n),
            Self::Float(n) => Ok(Scalar::Number(n)),
            Self::Bool(b) => Ok(Scalar::Boolean(b)),
            Self::Text(s) => Ok(Scalar::Text(s)),
            Self::Null => Err(CodecError),
        }
    }
}
#[derive(Debug)]
pub(crate) struct Fields(BTreeMap<String, Atom>);
impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Fields;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unique named scalar fields")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Fields, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((k, v)) = map.next_entry::<String, Atom>()? {
                    if fields.insert(k, v).is_some() {
                        return Err(de::Error::custom("duplicate field"));
                    }
                }
                Ok(Fields(fields))
            }
        }
        d.deserialize_map(V)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MapUplink {
    schema_version: u16,
    source_message_id: SourceMessageId,
    #[serde(default)]
    occurred_at: Option<Timestamp>,
    kind: String,
    data: Fields,
}
pub(crate) type Parsed = (SourceMessageId, Option<Timestamp>, DeviceEventKind);
impl MapUplink {
    pub(crate) fn into_event(self, l: &CodecLimits) -> Result<Parsed, CodecError> {
        if self.schema_version != 1 || self.occurred_at.is_some_and(|t| t < 0) {
            return Err(CodecError);
        }
        let mut data = self.data.0;
        let kind = match self.kind.as_str() {
            "telemetry" => DeviceEventKind::Telemetry(
                data.into_iter()
                    .map(|(k, v)| Ok((k, v.into_scalar()?)))
                    .collect::<Result<_, CodecError>>()?,
            ),
            "event" => {
                let Some(Atom::Text(name)) = data.remove("name") else {
                    return Err(CodecError);
                };
                let value = match data.remove("value") {
                    None | Some(Atom::Null) => None,
                    Some(v) => Some(v.into_scalar()?),
                };
                if !data.is_empty() {
                    return Err(CodecError);
                }
                DeviceEventKind::DeviceEvent(DeviceEventPayload { name, value })
            }
            "heartbeat" => {
                let sequence = match data.remove("sequence") {
                    Some(Atom::Unsigned(n)) => n,
                    Some(Atom::Signed(n)) if n >= 0 => n as u64,
                    _ => return Err(CodecError),
                };
                if !data.is_empty() {
                    return Err(CodecError);
                }
                DeviceEventKind::Heartbeat(Heartbeat { sequence })
            }
            "command_ack" => {
                let Some(Atom::Text(id)) = data.remove("command_id") else {
                    return Err(CodecError);
                };
                let Some(Atom::Text(execution)) = data.remove("execution") else {
                    return Err(CodecError);
                };
                let command_id = CommandId(uuid_parse(&id)?);
                let execution = match execution.as_str() {
                    "running" => ExecutionState::Running,
                    "succeeded" => ExecutionState::Succeeded,
                    "failed" => ExecutionState::Failed,
                    _ => return Err(CodecError),
                };
                if !data.is_empty() {
                    return Err(CodecError);
                }
                DeviceEventKind::CommandAck(CommandAck {
                    command_id,
                    execution,
                })
            }
            _ => return Err(CodecError),
        };
        if !validate_kind(&kind, l) {
            return Err(CodecError);
        }
        Ok((self.source_message_id, self.occurred_at, kind))
    }
}
// CommandId's public serde parser already validates UUID syntax.
pub(crate) fn uuid_parse(id: &str) -> Result<uuid::Uuid, CodecError> {
    uuid::Uuid::parse_str(id).map_err(|_| CodecError)
}
pub(crate) fn event(ctx: &DecodeContext<'_>, p: Parsed) -> Vec<DeviceEvent> {
    vec![DeviceEvent {
        event_id: EventId::generate(),
        source_message_id: p.0,
        device: ctx.device.clone(),
        received_at: ctx.received_at,
        occurred_at: p.1,
        kind: p.2,
    }]
}
#[derive(Serialize)]
pub(crate) struct MapCommand<'a> {
    pub schema_version: u16,
    pub command_id: String,
    pub device: &'a DeviceKey,
    pub expires_at: Option<Timestamp>,
    pub payload: &'a DeviceCommandPayload,
}
impl<'a> From<&'a DeviceCommand> for MapCommand<'a> {
    fn from(c: &'a DeviceCommand) -> Self {
        Self {
            schema_version: 1,
            command_id: c.command_id.0.to_string(),
            device: &c.device,
            expires_at: c.expires_at,
            payload: &c.payload,
        }
    }
}
pub(crate) struct BoundedWriter {
    pub bytes: Vec<u8>,
    maximum: usize,
}
impl BoundedWriter {
    pub fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
}
impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("codec byte limit"));
        }
        let required = self
            .bytes
            .len()
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::other("codec byte limit"))?;
        if required > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(required)
                .max(64)
                .min(self.maximum);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| io::Error::other("codec allocation limit"))?;
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
