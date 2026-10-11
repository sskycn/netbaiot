//! Prost V1 with schema-aware, allocation-free preflight before Message::decode.
use crate::{
    binary::{Budget, Reader},
    common::*,
};
use netbaiot_core::*;
use prost::Message;
use std::collections::BTreeMap;
/// Generated during a normal Cargo build with a pinned, bundled protoc.
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/netbaiot.device.v1.rs"));
}
#[derive(Clone, Copy)]
enum Schema {
    Uplink,
    Telemetry,
    Field,
    Scalar,
    Event,
    Heartbeat,
    Ack,
}
#[derive(Clone, Copy)]
enum Type {
    Varint,
    Boolean,
    Text(usize),
    Double,
    Message(Schema),
}
fn field(schema: Schema, n: u32, l: &CodecLimits) -> Option<Type> {
    match (schema, n) {
        (Schema::Uplink, 1 | 3)
        | (Schema::Heartbeat, 1)
        | (Schema::Ack, 2)
        | (Schema::Scalar, 4 | 5) => Some(Type::Varint),
        (Schema::Uplink, 2) => Some(Type::Text(64)),
        (Schema::Uplink, 10) => Some(Type::Message(Schema::Telemetry)),
        (Schema::Uplink, 11) => Some(Type::Message(Schema::Event)),
        (Schema::Uplink, 12) => Some(Type::Message(Schema::Heartbeat)),
        (Schema::Uplink, 13) => Some(Type::Message(Schema::Ack)),
        (Schema::Telemetry, 1) => Some(Type::Message(Schema::Field)),
        (Schema::Field, 1) | (Schema::Event, 1) | (Schema::Scalar, 3) => {
            Some(Type::Text(l.field_bytes))
        }
        (Schema::Ack, 1) => Some(Type::Text(64)),
        (Schema::Field, 2) | (Schema::Event, 2) => Some(Type::Message(Schema::Scalar)),
        (Schema::Scalar, 1) => Some(Type::Double),
        (Schema::Scalar, 2) => Some(Type::Boolean),
        _ => None,
    }
}
fn scan(
    input: &[u8],
    schema: Schema,
    depth: usize,
    budget: &mut Budget<'_>,
) -> Result<(), CodecError> {
    if depth > budget.limits.nesting_depth.min(64) {
        return Err(CodecError);
    }
    let mut reader = Reader {
        bytes: input,
        at: 0,
    };
    let (mut seen, mut repeated, mut oneof) = (0u64, 0usize, false);
    while reader.at < input.len() {
        budget.charge(128)?;
        let tag = reader.varint()?;
        let n = u32::try_from(tag >> 3).map_err(|_| CodecError)?;
        if n == 0 || n > 0x1fff_ffff {
            return Err(CodecError);
        }
        let wire = tag & 7;
        let ty = field(schema, n, budget.limits);
        if ty.is_some() {
            if matches!(schema, Schema::Telemetry) {
                repeated = repeated.checked_add(1).ok_or(CodecError)?;
                if repeated > budget.limits.fields {
                    return Err(CodecError);
                }
            } else {
                let bit = 1u64.checked_shl(n).ok_or(CodecError)?;
                if seen & bit != 0 {
                    return Err(CodecError);
                }
                seen |= bit;
                if matches!(schema, Schema::Scalar)
                    || (matches!(schema, Schema::Uplink) && (10..=13).contains(&n))
                {
                    if oneof {
                        return Err(CodecError);
                    }
                    oneof = true;
                }
            }
        }
        match wire {
            0 => {
                if ty.is_some_and(|t| !matches!(t, Type::Varint | Type::Boolean)) {
                    return Err(CodecError);
                }
                let value = reader.varint()?;
                if matches!(ty, Some(Type::Boolean)) && value > 1 {
                    return Err(CodecError);
                }
                if matches!(schema, Schema::Uplink) && n == 1 && value != 1 {
                    return Err(CodecError);
                }
            }
            1 => {
                if ty.is_some_and(|t| !matches!(t, Type::Double)) {
                    return Err(CodecError);
                }
                reader.take(8)?;
            }
            2 => {
                if ty.is_some_and(|t| !matches!(t, Type::Text(_) | Type::Message(_))) {
                    return Err(CodecError);
                }
                let n = Reader::len(reader.varint()?)?;
                let bytes = reader.take(n)?;
                match ty {
                    Some(Type::Text(max)) => {
                        if n > max {
                            return Err(CodecError);
                        }
                        std::str::from_utf8(bytes).map_err(|_| CodecError)?;
                        budget.charge(n.checked_mul(3).ok_or(CodecError)?)?;
                    }
                    Some(Type::Message(s)) => scan(bytes, s, depth + 1, budget)?,
                    // Unknown length-delimited fields are opaque, skipped without allocation.
                    None => budget.charge(n)?,
                    _ => return Err(CodecError),
                }
            }
            5 => {
                if ty.is_some() {
                    return Err(CodecError);
                }
                reader.take(4)?;
            }
            _ => return Err(CodecError), // Groups are outside this bounded proto3 profile.
        }
    }
    Ok(())
}
fn domain_scalar(value: wire::Scalar) -> Result<Scalar, CodecError> {
    use wire::scalar::Value;
    match value.value.ok_or(CodecError)? {
        Value::Number(n) if n.is_finite() => Ok(Scalar::Number(n)),
        Value::Boolean(b) => Ok(Scalar::Boolean(b)),
        Value::Text(t) => Ok(Scalar::Text(t)),
        Value::SignedInteger(n) => signed_scalar(n),
        Value::UnsignedInteger(n) => unsigned_scalar(n),
        _ => Err(CodecError),
    }
}
fn wire_scalar(v: &Scalar) -> wire::Scalar {
    use wire::scalar::Value;
    wire::Scalar {
        value: Some(match v {
            Scalar::Number(n) => Value::Number(*n),
            Scalar::Boolean(b) => Value::Boolean(*b),
            Scalar::Text(t) => Value::Text(t.clone()),
        }),
    }
}
pub struct ProtobufV1 {
    limits: CodecLimits,
}
impl ProtobufV1 {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }
    fn parse(&self, payload: &[u8]) -> Result<Parsed, CodecError> {
        let mut budget = Budget::new(&self.limits, payload)?;
        scan(payload, Schema::Uplink, 1, &mut budget)?;
        let uplink = wire::Uplink::decode(payload).map_err(|_| CodecError)?;
        if uplink.schema_version != 1 || uplink.occurred_at.is_some_and(|t| t < 0) {
            return Err(CodecError);
        }
        let source = SourceMessageId::new(uplink.source_message_id).map_err(|_| CodecError)?;
        use wire::uplink::Kind;
        let kind = match uplink.kind.ok_or(CodecError)? {
            Kind::Telemetry(t) => {
                let mut fields = BTreeMap::new();
                for f in t.fields {
                    let value = domain_scalar(f.value.ok_or(CodecError)?)?;
                    if fields.insert(f.name, value).is_some() {
                        return Err(CodecError);
                    }
                }
                DeviceEventKind::Telemetry(fields)
            }
            Kind::Event(e) => DeviceEventKind::DeviceEvent(DeviceEventPayload {
                name: e.name,
                value: e.value.map(domain_scalar).transpose()?,
            }),
            Kind::Heartbeat(h) => DeviceEventKind::Heartbeat(Heartbeat {
                sequence: h.sequence,
            }),
            Kind::CommandAck(a) => {
                let execution = match wire::Execution::try_from(a.execution) {
                    Ok(wire::Execution::Running) => ExecutionState::Running,
                    Ok(wire::Execution::Succeeded) => ExecutionState::Succeeded,
                    Ok(wire::Execution::Failed) => ExecutionState::Failed,
                    _ => return Err(CodecError),
                };
                DeviceEventKind::CommandAck(CommandAck {
                    command_id: CommandId(uuid_parse(&a.command_id)?),
                    execution,
                })
            }
        };
        if !validate_kind(&kind, &self.limits) {
            return Err(CodecError);
        }
        Ok((source, uplink.occurred_at, kind))
    }
}
impl Default for ProtobufV1 {
    fn default() -> Self {
        Self::new(CodecLimits::default())
    }
}
impl DeviceCodec for ProtobufV1 {
    fn validate_payload(
        &self,
        _: &DecodeContext<'_>,
        p: &[u8],
    ) -> Result<Vec<DeviceEventKind>, CodecError> {
        Ok(vec![self.parse(p)?.2])
    }
    fn decode(&self, ctx: &DecodeContext<'_>, p: &[u8]) -> Result<Vec<DeviceEvent>, CodecError> {
        Ok(event(ctx, self.parse(p)?))
    }
    fn encode(&self, ctx: &EncodeContext<'_>, c: &DeviceCommand) -> Result<Vec<u8>, CodecError> {
        if !validate_command(ctx, c, &self.limits) {
            return Err(CodecError);
        }
        // Compute a conservative owned-structure budget before cloning command fields.
        let names = c
            .payload
            .arguments
            .iter()
            .try_fold(c.payload.name.len(), |n, (k, v)| {
                n.checked_add(k.len())?.checked_add(match v {
                    Scalar::Text(s) => s.len(),
                    _ => 0,
                })
            })
            .ok_or(CodecError)?;
        let memory = c
            .payload
            .arguments
            .len()
            .checked_mul(256)
            .and_then(|n| n.checked_add(names))
            .and_then(|n| n.checked_add(512))
            .ok_or(CodecError)?;
        if memory > self.limits.decoded_bytes {
            return Err(CodecError);
        }
        let d = &c.device;
        let cmd = wire::DeviceCommand {
            schema_version: 1,
            command_id: c.command_id.0.to_string(),
            device: Some(wire::DeviceKey {
                tenant_id: d.tenant_id.as_str().into(),
                product_id: d.product_id.as_str().into(),
                device_id: d.device_id.as_str().into(),
            }),
            expires_at: c.expires_at,
            name: c.payload.name.clone(),
            arguments: c
                .payload
                .arguments
                .iter()
                .map(|(k, v)| wire::Field {
                    name: k.clone(),
                    value: Some(wire_scalar(v)),
                })
                .collect(),
        };
        if cmd.encoded_len() > self.limits.decoded_bytes {
            return Err(CodecError);
        }
        Ok(cmd.encode_to_vec())
    }
}
