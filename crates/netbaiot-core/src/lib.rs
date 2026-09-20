//! Internal domain services built on the public protocol model.

pub use netbaiot_protocol::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Compatibility name retained for callers of the original core crate.
pub type DomainError = ProtocolError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Presence {
    pub connected: bool,
    pub last_seen: Timestamp,
    pub transport: Transport,
    pub session_generation: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Permissions {
    pub publish: bool,
    pub commands: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthenticatedDevice {
    pub device_key: DeviceKey,
    pub credential_version: u32,
    #[serde(default = "default_auth_generation")]
    pub auth_generation: u64,
    pub codec_id: CodecId,
    pub codec_version: u16,
    pub permissions: Permissions,
}

fn default_auth_generation() -> u64 {
    1
}

#[derive(Clone, Debug)]
pub struct CodecLimits {
    pub input_bytes: usize,
    pub output_messages: usize,
    pub decoded_bytes: usize,
    pub fields: usize,
    pub field_bytes: usize,
    pub nesting_depth: usize,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            input_bytes: 65_536,
            output_messages: 1,
            decoded_bytes: 65_536,
            fields: 64,
            field_bytes: 256,
            nesting_depth: 8,
        }
    }
}

pub struct DecodeContext<'a> {
    pub device: &'a DeviceKey,
    pub received_at: Timestamp,
}

pub struct EncodeContext<'a> {
    pub device: &'a DeviceKey,
}

#[derive(Debug, Error)]
#[error("invalid or oversized device payload")]
pub struct CodecError;

pub trait DeviceCodec: Send + Sync {
    fn decode(
        &self,
        ctx: &DecodeContext<'_>,
        payload: &[u8],
    ) -> Result<Vec<DeviceEvent>, CodecError>;

    fn encode(
        &self,
        ctx: &EncodeContext<'_>,
        command: &DeviceCommand,
    ) -> Result<Vec<u8>, CodecError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_are_namespace_safe() {
        for invalid in ["", "a/b", "+", "#", "a\0b", "a\nb", "设备"] {
            assert!(DeviceId::new(invalid).is_err());
        }
        assert!(DeviceId::new("a".repeat(64)).is_ok());
        assert!(DeviceId::new("a".repeat(65)).is_err());
    }
}
