//! Stable, transport-neutral NetbaIoT public protocol types.
//!
//! This crate intentionally has no async runtime, HTTP, MQTT, server, or storage
//! dependency. `PROTOCOL_VERSION` versions wire semantics independently from the
//! crate's SemVer version.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, sync::Arc};
use thiserror::Error;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_IDENTIFIER_BYTES: usize = 64;
pub const MAX_FILTER_EVENT_TYPES: usize = 16;

pub mod paths {
    pub const HEALTH: &str = "/api/v1/health";
    pub const READY: &str = "/api/v1/ready";
    pub const STATUS: &str = "/api/v1/status";
    pub const CONNECTIONS: &str = "/api/v1/connections";
    pub const DEVICE_CONNECTION: &str = "/api/v1/devices/connection";
    pub const DEVICE_CONFIG_MANAGEMENT: &str = "/api/v1/devices/config";
    pub const COMMANDS: &str = "/api/v1/devices/commands";
    pub const AUTH_INVALIDATE: &str = "/api/v1/auth/invalidate";
    pub const CONFIG_INVALIDATE: &str = "/api/v1/config/invalidate";
    pub const CONTROL_SNAPSHOT: &str = "/api/v1/control/snapshot";
    pub const ROUTES: &str = "/api/v1/routes";
    pub const DRAIN: &str = "/api/v1/drain";
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("invalid public protocol value")]
pub struct ProtocolError;

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, ProtocolError> {
                let value = value.as_ref();
                if value.is_empty()
                    || value.len() > MAX_IDENTIFIER_BYTES
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
                {
                    return Err(ProtocolError);
                }
                Ok(Self(Arc::from(value)))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = ProtocolError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.to_string()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
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

macro_rules! uuid_identifier {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_identifier!(EventId);
uuid_identifier!(CommandId);
uuid_identifier!(DeliveryId);
uuid_identifier!(SubscriptionId);
pub type MessageId = EventId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ConfigRevision(u64);

impl ConfigRevision {
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for ConfigRevision {
    type Error = ProtocolError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(ProtocolError)
    }
}

impl<'de> Deserialize<'de> for ConfigRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| serde::de::Error::custom("revision must be non-zero"))
    }
}

impl fmt::Display for ConfigRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceKey {
    pub tenant_id: TenantId,
    pub product_id: ProductId,
    pub device_id: DeviceId,
}

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
    pub const fn event_type(&self) -> EventType {
        match self {
            Self::Telemetry(_) => EventType::Telemetry,
            Self::DeviceEvent(_) => EventType::DeviceEvent,
            Self::Heartbeat(_) => EventType::Heartbeat,
            Self::Connected(_) => EventType::Connected,
            Self::Disconnected(_) => EventType::Disconnected,
            Self::ConfigAck(_) => EventType::ConfigAck,
            Self::CommandAck(_) => EventType::CommandAck,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Telemetry,
    DeviceEvent,
    Heartbeat,
    Connected,
    Disconnected,
    ConfigAck,
    CommandAck,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceEventPayload {
    pub name: String,
    #[serde(default)]
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
    pub revision: ConfigRevision,
    pub status: ConfigApplyStatus,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigApplyStatus {
    Applied,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceEvent {
    pub event_id: EventId,
    pub source_message_id: SourceMessageId,
    pub device: DeviceKey,
    pub received_at: Timestamp,
    #[serde(default)]
    pub occurred_at: Option<Timestamp>,
    pub kind: DeviceEventKind,
}

/// Versioned device-to-gateway JSON v1 envelope used by MQTT, TCP and UDP uplinks.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceUplink {
    pub schema_version: u16,
    pub source_message_id: SourceMessageId,
    #[serde(default)]
    pub occurred_at: Option<Timestamp>,
    #[serde(flatten)]
    pub kind: DeviceUplinkKind,
}

impl DeviceUplink {
    pub fn new(source_message_id: SourceMessageId, kind: DeviceUplinkKind) -> Self {
        Self {
            schema_version: PROTOCOL_VERSION,
            source_message_id,
            occurred_at: None,
            kind,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum DeviceUplinkKind {
    Telemetry(BTreeMap<String, Scalar>),
    Event(DeviceEventPayload),
    Heartbeat(Heartbeat),
    ConfigAck(ConfigAck),
    CommandAck(CommandAck),
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
    #[serde(default)]
    pub arguments: BTreeMap<String, Scalar>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceCommand {
    pub command_id: CommandId,
    pub device: DeviceKey,
    #[serde(default)]
    pub expires_at: Option<Timestamp>,
    pub payload: DeviceCommandPayload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandDeliveryStatus {
    Accepted,
    Sent,
    DeviceReceived,
    DeviceExecuted,
    DeviceOffline,
    Failed,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandDispatch {
    pub command_id: CommandId,
    pub state: DeliveryState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Mqtt,
    Tcp,
    Udp,
}

pub type Transport = TransportKind;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeviceConnectionInfo {
    pub device: DeviceKey,
    pub connected: bool,
    #[serde(default)]
    pub transport: Option<TransportKind>,
    #[serde(default)]
    pub connected_at: Option<Timestamp>,
    #[serde(default)]
    pub last_seen: Option<Timestamp>,
    #[serde(default)]
    pub session_generation: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    pub device: DeviceKey,
    pub revision: ConfigRevision,
    pub payload: Arc<serde_json::Value>,
}

pub type DeviceConfigSnapshot = DeviceConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigStatus {
    Current,
    Updated,
    Applied,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventFilter {
    #[serde(default)]
    pub tenant: Option<TenantId>,
    #[serde(default)]
    pub product: Option<ProductId>,
    #[serde(default)]
    pub device: Option<DeviceId>,
    #[serde(default)]
    pub event_types: Vec<EventType>,
}

impl EventFilter {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.event_types.len() > MAX_FILTER_EVENT_TYPES {
            return Err(ProtocolError);
        }
        Ok(())
    }

    pub fn matches(&self, event: &DeviceEvent) -> bool {
        self.tenant
            .as_ref()
            .is_none_or(|id| id == &event.device.tenant_id)
            && self
                .product
                .as_ref()
                .is_none_or(|id| id == &event.device.product_id)
            && self
                .device
                .as_ref()
                .is_none_or(|id| id == &event.device.device_id)
            && (self.event_types.is_empty() || self.event_types.contains(&event.kind.event_type()))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventDelivery {
    pub delivery_id: DeliveryId,
    pub subscription_id: SubscriptionId,
    pub event: DeviceEvent,
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventAck {
    pub delivery_id: DeliveryId,
    pub subscription_id: SubscriptionId,
    pub event_id: EventId,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamClientFrame {
    Hello {
        version: u16,
        token: String,
    },
    Subscribe {
        version: u16,
        subscription_id: SubscriptionId,
        filter: EventFilter,
    },
    Ack {
        version: u16,
        ack: EventAck,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamServerFrame {
    Ready {
        version: u16,
        subscription_id: SubscriptionId,
    },
    Event {
        version: u16,
        delivery: EventDelivery,
    },
    Error {
        version: u16,
        error: ApiError,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthenticated,
    Forbidden,
    InvalidRequest,
    InvalidProtocolVersion,
    DeviceOffline,
    Overloaded,
    ServiceDraining,
    Timeout,
    ConnectionLost,
    NotFound,
    Conflict,
    ServerUnavailable,
    Internal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub required_scope: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
pub enum AuthInvalidation {
    Device {
        device: DeviceKey,
    },
    Product {
        tenant_id: TenantId,
        product_id: ProductId,
    },
    Tenant {
        tenant_id: TenantId,
    },
    CredentialVersion {
        version: u32,
    },
    AuthGeneration {
        generation: u64,
    },
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvalidationResult {
    /// Legacy cache-entry count retained for wire compatibility.
    pub invalidated: usize,
    /// Legacy live-connection count; offline MQTT state is not included.
    pub disconnected: usize,
    #[serde(default)]
    pub invalidated_cache_entries: usize,
    #[serde(default)]
    pub disconnected_connections: usize,
    #[serde(default)]
    pub invalidated_mqtt_sessions: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Starting,
    Running,
    Quiescing,
    Draining,
    Spooling,
    Drained,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeStatus {
    pub lifecycle: LifecycleState,
    pub event_count: usize,
    pub event_bytes: usize,
    pub pending_required: usize,
    pub auth_cache_entries: usize,
    pub auth_cache_bytes: usize,
    pub config_cache_entries: usize,
    pub config_cache_bytes: usize,
    pub runtime_tasks: usize,
    pub active_connections: ConnectionCounts,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionCounts {
    pub mqtt: usize,
    pub tcp: usize,
    pub udp: usize,
}

impl ConnectionCounts {
    pub const fn total(self) -> usize {
        self.mqtt + self.tcp + self.udp
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventAccepted {
    pub event_id: EventId,
    pub accepted_at: Timestamp,
    pub required_deliveries: usize,
    pub best_effort_deliveries: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductRuntimeConfig {
    pub tenant_id: TenantId,
    pub product_id: ProductId,
    pub codec_id: CodecId,
    pub codec_version: u16,
    pub revision: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteDefinition {
    #[serde(default)]
    pub tenant: Option<TenantId>,
    pub sinks: Vec<SinkId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlSnapshot {
    pub revision: u64,
    pub products: Vec<ProductRuntimeConfig>,
    pub devices: Vec<DeviceConfig>,
    pub routes: Vec<RouteDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutesUpdate {
    pub revision: u64,
    pub routes: Vec<RouteDefinition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_event_ids_keep_uuid_v4_and_canonical_serde_contract() {
        let first = EventId::generate();
        let second = EventId::generate();
        assert_ne!(first, second);
        for id in [first, second] {
            assert_eq!(id.0.get_variant(), uuid::Variant::RFC4122);
            assert_eq!(id.0.get_version(), Some(uuid::Version::Random));
            let text = id.to_string();
            assert_eq!(text.len(), 36);
            assert_eq!(text, id.0.hyphenated().to_string());
            assert_eq!(text, text.to_lowercase());
            assert_eq!(Uuid::parse_str(&text).unwrap(), id.0);
            let wire = serde_json::to_string(&id).unwrap();
            assert_eq!(wire, format!("\"{text}\""));
            assert_eq!(serde_json::from_str::<EventId>(&wire).unwrap(), id);
        }
        // Existing public parsing also supports UUIDs other than generated v4.
        let nil = EventId(Uuid::nil());
        assert_eq!(
            serde_json::from_str::<EventId>(&serde_json::to_string(&nil).unwrap()).unwrap(),
            nil
        );
    }

    #[test]
    fn device_transports_and_connection_counts_have_only_three_wire_fields() {
        for (transport, wire) in [
            (TransportKind::Mqtt, "mqtt"),
            (TransportKind::Tcp, "tcp"),
            (TransportKind::Udp, "udp"),
        ] {
            let value = serde_json::json!(wire);
            assert_eq!(serde_json::to_value(transport).unwrap(), value);
            assert_eq!(
                serde_json::from_value::<TransportKind>(value).unwrap(),
                transport
            );
        }
        assert!(serde_json::from_str::<TransportKind>(r#""http""#).is_err());
        let counts = ConnectionCounts {
            mqtt: 1,
            tcp: 2,
            udp: 0,
        };
        assert_eq!(counts.total(), 3);
        let value = serde_json::json!({"mqtt":1,"tcp":2,"udp":0});
        assert_eq!(serde_json::to_value(counts).unwrap(), value);
        assert_eq!(
            serde_json::from_value::<ConnectionCounts>(value).unwrap(),
            counts
        );
    }

    #[test]
    fn stable_error_and_ack_json() {
        let error = ApiError {
            code: ErrorCode::DeviceOffline,
            message: "device is not currently connected".into(),
            request_id: Some("r-1".into()),
            required_scope: None,
        };
        assert_eq!(
            serde_json::to_string(&error).unwrap(),
            r#"{"code":"device_offline","message":"device is not currently connected","request_id":"r-1","required_scope":null}"#
        );

        let event_id = EventId(Uuid::nil());
        let ack = EventAck {
            delivery_id: DeliveryId(Uuid::nil()),
            subscription_id: SubscriptionId(Uuid::nil()),
            event_id,
        };
        let value = serde_json::to_value(ack).unwrap();
        assert_eq!(value["event_id"], event_id.0.to_string());
    }

    #[test]
    fn filter_is_bounded_and_matches_identity() {
        let filter = EventFilter {
            event_types: vec![EventType::Heartbeat],
            ..EventFilter::default()
        };
        assert!(filter.validate().is_ok());
        let event = DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("1").unwrap(),
            device: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new("d").unwrap(),
            },
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
        };
        assert!(filter.matches(&event));
    }

    #[test]
    fn revision_rejects_zero_and_response_types_accept_additive_fields() {
        assert!(serde_json::from_str::<ConfigRevision>("0").is_err());
        assert_eq!(
            serde_json::from_str::<ConfigRevision>("7").unwrap().get(),
            7
        );

        let accepted = serde_json::json!({
            "event_id": Uuid::nil(),
            "accepted_at": 1,
            "required_deliveries": 1,
            "best_effort_deliveries": 0,
            "future_server_field": {"enabled": true}
        });
        assert!(serde_json::from_value::<EventAccepted>(accepted).is_ok());

        let strict_client = serde_json::json!({
            "type": "hello",
            "version": PROTOCOL_VERSION,
            "token": "x",
            "future_client_field": true
        });
        assert!(serde_json::from_value::<StreamClientFrame>(strict_client).is_err());
    }
}
