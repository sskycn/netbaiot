//! Transport-independent domain and synchronous protocol boundary.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_IDENTIFIER_BYTES: usize = 64;
#[derive(Debug, Error)]
#[error("invalid domain value")]
pub struct DomainError;

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Arc<str>);
        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, DomainError> {
                let s = value.as_ref();
                if s.is_empty()
                    || s.len() > MAX_IDENTIFIER_BYTES
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
                {
                    return Err(DomainError);
                }
                Ok(Self(Arc::from(s)))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = DomainError;
            fn try_from(s: String) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }
        impl From<$name> for String {
            fn from(s: $name) -> String {
                s.0.to_string()
            }
        }
    };
}
identifier!(TenantId);
identifier!(ProductId);
identifier!(DeviceId);
identifier!(SourceMessageId);
identifier!(CodecId);
identifier!(SinkId);
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub Uuid);
/// Compatibility name for integrations compiled against the original model.
pub type MessageId = EventId;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandId(pub Uuid);
impl EventId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}
impl CommandId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceKey {
    pub tenant_id: TenantId,
    pub product_id: ProductId,
    pub device_id: DeviceId,
}
/// Unix milliseconds, checked at protocol boundaries.
pub type Timestamp = i64;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    Number(f64),
    Boolean(bool),
    Text(String),
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum DeviceEventKind {
    Telemetry(BTreeMap<String, Scalar>),
    DeviceEvent(DeviceEventPayload),
    Heartbeat(Heartbeat),
    Connected(DeviceConnected),
    Disconnected(DeviceDisconnected),
    ConfigAck(ConfigAck),
    CommandAck(CommandAck),
}
impl DeviceEventKind {
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::Telemetry(_) => "telemetry",
            Self::DeviceEvent(_) => "device_event",
            Self::Heartbeat(_) => "heartbeat",
            Self::Connected(_) => "connected",
            Self::Disconnected(_) => "disconnected",
            Self::ConfigAck(_) => "config_ack",
            Self::CommandAck(_) => "command_ack",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceEventPayload {
    pub name: String,
    pub value: Option<Scalar>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Heartbeat {
    pub sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandAck {
    pub command_id: CommandId,
    pub execution: ExecutionState,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConnected {
    pub session_generation: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceDisconnected {
    pub session_generation: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigAck {
    pub revision: u64,
    pub status: ConfigApplyStatus,
    pub error: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigApplyStatus {
    Applied,
    Failed,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeviceEvent {
    pub event_id: EventId,
    pub source_message_id: SourceMessageId,
    pub device: DeviceKey,
    pub received_at: Timestamp,
    pub occurred_at: Option<Timestamp>,
    pub kind: DeviceEventKind,
}
pub type DeviceMessage = DeviceEvent;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Queued,
    Dispatching,
    Sent,
    Received,
    Failed,
    Expired,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Unknown,
    Running,
    Succeeded,
    Failed,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCommandPayload {
    pub name: String,
    pub arguments: BTreeMap<String, Scalar>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCommand {
    pub command_id: CommandId,
    pub device: DeviceKey,
    pub expires_at: Timestamp,
    pub payload: DeviceCommandPayload,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Http,
    Mqtt,
    Tcp,
    Udp,
}
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
            input_bytes: 65536,
            output_messages: 1,
            decoded_bytes: 65536,
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
