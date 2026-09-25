//! Business RPC Stream V2 wire contract. V1 stream frames remain in `lib.rs`.
use crate::{
    AuthInvalidation, CodecId, CommandDispatch, DeviceCommand, DeviceKey, EventAck, EventDelivery,
    EventFilter, ProtocolError, SubscriptionId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BUSINESS_RPC_VERSION: u16 = 2;
pub const BUSINESS_RPC_HELLO_MAX_BYTES: usize = 4 * 1024;
pub const BUSINESS_RPC_AUTH_MAX_BYTES: usize = 16 * 1024;
pub const BUSINESS_RPC_MAX_TOKEN_BYTES: usize = 256;
pub const BUSINESS_RPC_MAX_ERROR_BYTES: usize = 256;
pub const BUSINESS_RPC_EVENT_WINDOW: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessLimits {
    pub max_frame_bytes: u32,
    pub auth_max_inflight: u16,
    pub event_max_inflight: u16,
    pub heartbeat_ms: u32,
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

/// The JSON body is decoded into the method DTO only after method authorization.
/// This allows unknown methods to receive a structured error using their request ID.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BusinessRpcFrame {
    Hello {
        version: u16,
        role: BusinessRole,
        token: Option<String>,
        limits: BusinessLimits,
    },
    Ready {
        version: u16,
        role: BusinessRole,
        connection_epoch: u64,
        limits: BusinessLimits,
    },
    Request {
        request_id: Uuid,
        method: String,
        deadline_ms: u32,
        body: serde_json::Value,
    },
    Response {
        request_id: Uuid,
        method: String,
        body: Option<serde_json::Value>,
        error: Option<RpcError>,
    },
    Subscribe {
        subscription_id: SubscriptionId,
        filter: EventFilter,
    },
    Subscribed {
        subscription_id: SubscriptionId,
    },
    Event {
        delivery: EventDelivery,
    },
    EventAck {
        ack: EventAck,
    },
    EventNack {
        ack: EventAck,
        error: RpcError,
    },
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
    Cancel {
        request_id: Uuid,
    },
    GoAway {
        error: RpcError,
    },
}

impl BusinessRpcFrame {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        fn method(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'.' || byte == b'_')
        }
        fn error(value: &RpcError) -> bool {
            value.message.len() <= BUSINESS_RPC_MAX_ERROR_BYTES
        }
        match self {
            Self::Hello {
                version,
                token,
                limits,
                ..
            } => {
                if *version != BUSINESS_RPC_VERSION
                    || token
                        .as_ref()
                        .is_some_and(|value| value.len() > BUSINESS_RPC_MAX_TOKEN_BYTES)
                    || limits.max_frame_bytes == 0
                    || limits.auth_max_inflight == 0
                    || limits.event_max_inflight != BUSINESS_RPC_EVENT_WINDOW
                    || limits.heartbeat_ms == 0
                {
                    return Err(ProtocolError);
                }
            }
            Self::Ready {
                version,
                connection_epoch,
                limits,
                ..
            } => {
                if *version != BUSINESS_RPC_VERSION
                    || *connection_epoch == 0
                    || limits.max_frame_bytes == 0
                    || limits.auth_max_inflight == 0
                    || limits.event_max_inflight != BUSINESS_RPC_EVENT_WINDOW
                    || limits.heartbeat_ms == 0
                {
                    return Err(ProtocolError);
                }
            }
            Self::Request {
                request_id,
                method: name,
                deadline_ms,
                body,
            } => {
                if request_id.is_nil()
                    || !method(name)
                    || *deadline_ms == 0
                    || serde_json::to_vec(body)
                        .map_or(true, |bytes| bytes.len() > BUSINESS_RPC_AUTH_MAX_BYTES)
                {
                    return Err(ProtocolError);
                }
            }
            Self::Response {
                request_id,
                method: name,
                body,
                error: failure,
            } => {
                if request_id.is_nil()
                    || !method(name)
                    || (body.is_some() == failure.is_some())
                    || failure.as_ref().is_some_and(|value| !error(value))
                    || body.as_ref().is_some_and(|value| {
                        serde_json::to_vec(value)
                            .map_or(true, |bytes| bytes.len() > BUSINESS_RPC_AUTH_MAX_BYTES)
                    })
                {
                    return Err(ProtocolError);
                }
            }
            Self::Subscribe {
                subscription_id,
                filter,
            } => {
                if subscription_id.0.is_nil() || filter.validate().is_err() {
                    return Err(ProtocolError);
                }
            }
            Self::Subscribed { subscription_id } => {
                if subscription_id.0.is_nil() {
                    return Err(ProtocolError);
                }
            }
            Self::Event { delivery } => {
                if delivery.subscription_id.0.is_nil()
                    || delivery.delivery_id.0.is_nil()
                    || delivery.event.event_id.0.is_nil()
                {
                    return Err(ProtocolError);
                }
            }
            Self::EventAck { ack } => {
                if ack.subscription_id.0.is_nil()
                    || ack.delivery_id.0.is_nil()
                    || ack.event_id.0.is_nil()
                {
                    return Err(ProtocolError);
                }
            }
            Self::EventNack {
                ack,
                error: failure,
            } => {
                if ack.subscription_id.0.is_nil()
                    || ack.delivery_id.0.is_nil()
                    || ack.event_id.0.is_nil()
                    || !error(failure)
                {
                    return Err(ProtocolError);
                }
            }
            Self::GoAway { error: failure } => {
                if !error(failure) {
                    return Err(ProtocolError);
                }
            }
            Self::Cancel { request_id } => {
                if request_id.is_nil() {
                    return Err(ProtocolError);
                }
            }
            Self::Ping { .. } | Self::Pong { .. } => {}
        }
        Ok(())
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
    fn v2_contract_and_v1_version_are_independent() {
        assert_eq!(crate::PROTOCOL_VERSION, 1);
        let frame = BusinessRpcFrame::Hello {
            version: BUSINESS_RPC_VERSION,
            role: BusinessRole::Multiplexed,
            token: None,
            limits: BusinessLimits {
                max_frame_bytes: 65536,
                auth_max_inflight: 8,
                event_max_inflight: 1,
                heartbeat_ms: 5000,
            },
        };
        let wire = serde_json::to_value(&frame).unwrap();
        assert_eq!(wire["type"], "hello");
        assert_eq!(wire["role"], "multiplexed");
        assert_eq!(wire["version"], 2);
        assert!(serde_json::from_value::<BusinessRpcFrame>(wire).is_ok());
    }

    #[test]
    fn roles_preserve_v2_names_and_capabilities() {
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
        let oversized = BusinessRpcFrame::Request {
            request_id: Uuid::new_v4(),
            method: "device.command.send".into(),
            deadline_ms: 1_000,
            body: serde_json::json!({ "command": { "payload": "x".repeat(BUSINESS_RPC_AUTH_MAX_BYTES) } }),
        };
        assert!(oversized.validate().is_err());
    }
}
