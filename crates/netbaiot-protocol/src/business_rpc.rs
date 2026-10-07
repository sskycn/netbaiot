//! Shared method DTOs and authorization/error vocabulary for current Business RPC.
use crate::{AuthInvalidation, CodecId, CommandDispatch, DeviceCommand, DeviceKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BUSINESS_RPC_HELLO_MAX_BYTES: usize = 4 * 1024;
pub const BUSINESS_RPC_AUTH_MAX_BYTES: usize = 16 * 1024;
pub const BUSINESS_RPC_MAX_TOKEN_BYTES: usize = 256;
pub const BUSINESS_RPC_MAX_ERROR_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub enum BusinessRole {
    Events,
    AuthControl,
    Multiplexed,
    Commands,
    Application,
}

impl BusinessRole {
    pub fn events(self) -> bool {
        matches!(self, Self::Events | Self::Multiplexed | Self::Application)
    }
    pub fn auth_control(self) -> bool {
        matches!(self, Self::AuthControl | Self::Multiplexed)
    }
    pub fn commands(self) -> bool {
        matches!(self, Self::Commands | Self::Application)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCommandSendRequest {
    pub command: DeviceCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCommandSendResponse {
    pub dispatch: CommandDispatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcErrorCode {
    InvalidRequest,
    UnknownMethod,
    Unauthenticated,
    Forbidden,
    DeviceRejected,
    Unavailable,
    Overloaded,
    Timeout,
    Conflict,
    StaleRevision,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    pub code: RpcErrorCode,
    pub message: String,
}

impl RpcError {
    pub fn new(code: RpcErrorCode, message: &str) -> Self {
        let mut bounded = String::new();
        for character in message.chars() {
            if bounded.len() + character.len_utf8() > BUSINESS_RPC_MAX_ERROR_BYTES {
                break;
            }
            bounded.push(character);
        }
        Self {
            code,
            message: bounded,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceAuthenticateRequest {
    pub credential_id: String,
    pub secret_hex: String,
    pub min_auth_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedDeviceWire {
    pub device_key: DeviceKey,
    pub credential_version: u32,
    pub auth_generation: u64,
    pub codec_id: CodecId,
    pub codec_version: u16,
    pub publish: bool,
    pub commands: bool,
    pub auth_revision: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveVerifierRequest {
    pub credential_id: String,
    pub min_auth_revision: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveVerifierResponse {
    pub identity: AuthenticatedDeviceWire,
    pub verifier_key_hex: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSyncRequest {
    pub reset: bool,
    pub authority_incarnation: Uuid,
    pub auth_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSyncResponse {
    pub applied_revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthInvalidateRequest {
    pub authority_incarnation: Uuid,
    pub auth_revision: u64,
    pub invalidation: AuthInvalidation,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthInvalidateResponse {
    pub applied_revision: u64,
    pub invalidated_cache_entries: usize,
    pub disconnected_connections: usize,
    pub invalidated_mqtt_sessions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandId, DeviceCommandPayload, DeviceId, ProductId, TenantId};
    #[test]
    fn current_principal_roles_preserve_authorization_capabilities() {
        for (role, wire, events, auth, commands) in [
            (BusinessRole::Events, "events", true, false, false),
            (
                BusinessRole::AuthControl,
                "auth_control",
                false,
                true,
                false,
            ),
            (BusinessRole::Multiplexed, "multiplexed", true, true, false),
            (BusinessRole::Commands, "commands", false, false, true),
            (BusinessRole::Application, "application", true, false, true),
        ] {
            assert_eq!(serde_json::to_value(role).unwrap(), wire);
            assert!(
                matches!(serde_json::from_str::<BusinessRole>(&format!("\"{wire}\"")), Ok(value) if value == role)
            );
            assert_eq!(
                (role.events(), role.auth_control(), role.commands()),
                (events, auth, commands)
            );
        }
    }

    #[test]
    fn command_dto_rejects_unknown_and_malformed_fields() {
        let request = DeviceCommandSendRequest {
            command: DeviceCommand {
                command_id: CommandId::generate(),
                device: DeviceKey {
                    tenant_id: TenantId::new("demo").unwrap(),
                    product_id: ProductId::new("sensor").unwrap(),
                    device_id: DeviceId::new("one").unwrap(),
                },
                expires_at: None,
                payload: DeviceCommandPayload {
                    name: "reboot".into(),
                    arguments: Default::default(),
                },
            },
        };
        let wire = serde_json::to_value(&request).unwrap();
        assert!(serde_json::from_value::<DeviceCommandSendRequest>(wire.clone()).is_ok());
        let mut unknown = wire.clone();
        unknown["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<DeviceCommandSendRequest>(unknown).is_err());
        let mut malformed = wire;
        malformed["command"]["payload"]["arguments"] = serde_json::json!({"bad": [1, 2]});
        assert!(serde_json::from_value::<DeviceCommandSendRequest>(malformed).is_err());
        let oversized = crate::business_rpc_v3::V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "device.command.send".into(),
            deadline_ms: 1_000,
            content_length: (crate::business_rpc_v3::V3_MAX_MESSAGE_BYTES + 1) as u32,
        };
        assert!(oversized.validate().is_err());
    }
}
