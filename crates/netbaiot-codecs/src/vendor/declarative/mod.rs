//! Compiled fixed-layout binary payloads. No transport framing, I/O or runtime configuration parsing.
mod config;
mod plan;
mod wire;
use crate::common::{Parsed, event, validate_command};
pub use config::*;
use netbaiot_core::*;
use plan::{CommandPlan, MessagePlan};

pub const MAX_CONFIG_BYTES: usize = 256 * 1024;
pub const MAX_FRAME_BYTES: usize = 65_536;
pub const MAX_LAYOUTS: usize = 64;
pub const MAX_FIELDS: usize = 64;
pub const MAX_FIELD_BYTES: usize = 256;

/// Immutable startup plan, safely shared by all connections bound to this profile.
pub struct DeclarativeCodec {
    limits: CodecLimits,
    header: Box<[u8]>,
    message_type: wire::Integer,
    length: Option<(wire::Integer, i32)>,
    messages: Vec<MessagePlan>,
    commands: Vec<CommandPlan>,
}
impl DeclarativeCodec {
    /// JSON is parsed only at startup. Server configuration has its own outer byte limit.
    pub fn from_json(json: &[u8], limits: CodecLimits) -> Result<Self, CodecError> {
        if json.len() > MAX_CONFIG_BYTES {
            return Err(CodecError);
        }
        let config = serde_json::from_slice(json).map_err(|_| CodecError)?;
        Self::compile(&config, limits)
    }

    fn parse(&self, payload: &[u8]) -> Result<Parsed, CodecError> {
        if payload.len() > self.limits.input_bytes || !payload.starts_with(&self.header) {
            return Err(CodecError);
        }
        let tag = self.message_type.read(payload)?;
        let index = self
            .messages
            .binary_search_by_key(&tag, |p| p.tag)
            .map_err(|_| CodecError)?;
        let message = &self.messages[index];
        message.layout.validate(payload, self.length.as_ref())?;
        let source = message.source.source(payload)?;
        let occurred_at = message
            .occurred_at
            .as_ref()
            .map(|f| i64::try_from(f.read(payload)?).map_err(|_| CodecError))
            .transpose()?;
        let kind = message.kind.read(payload)?;
        Ok((source, occurred_at, kind))
    }
}
impl DeviceCodec for DeclarativeCodec {
    fn validate_payload(
        &self,
        _ctx: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceEventKind>, CodecError> {
        Ok(vec![self.parse(payload)?.2])
    }
    fn decode(
        &self,
        ctx: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceEvent>, CodecError> {
        Ok(event(ctx, self.parse(payload)?))
    }
    fn encode(
        &self,
        ctx: &EncodeContext<'_>,
        command: &DeviceCommand,
    ) -> Result<Vec<u8>, CodecError> {
        if !validate_command(ctx, command, &self.limits)
            || command.expires_at.is_some_and(|t| t < 0)
        {
            return Err(CodecError);
        }
        let index = self
            .commands
            .binary_search_by(|p| p.name.as_str().cmp(&command.payload.name))
            .map_err(|_| CodecError)?;
        let plan = &self.commands[index];
        if command.payload.arguments.len() != plan.fields.len() {
            return Err(CodecError);
        }
        // One bounded output allocation; no clone of command arguments or payload intermediates.
        let mut out = vec![0; plan.layout.size];
        out[..self.header.len()].copy_from_slice(&self.header);
        self.message_type.write(&mut out, plan.tag)?;
        if let Some((field, adjustment)) = &self.length {
            field.write(&mut out, plan.layout.wire_length(*adjustment)?)?;
        }
        out[plan.command_id.clone()].copy_from_slice(command.command_id.0.as_bytes());
        if let Some(field) = &plan.expires_at {
            field.write(&mut out, command.expires_at.unwrap_or(0) as u64)?;
        }
        for (name, field) in &plan.fields {
            field.write(
                &mut out,
                command.payload.arguments.get(name).ok_or(CodecError)?,
            )?;
        }
        plan.layout.write_checksum(&mut out)?;
        Ok(out)
    }
}
