//! Business RPC V3 bootstrap and binary framing contract.
//! The sole supported Business RPC wire format.
use crate::{EventFilter, SubscriptionId, business_rpc::RpcError};
// The V3 application method uses the established command JSON DTO unchanged.
pub use crate::business_rpc::{DeviceCommandSendRequest, DeviceCommandSendResponse};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BUSINESS_RPC_PROTOCOL_VERSION: u16 = 3;
pub const RPC_HEADER_BYTES: usize = 12;
pub const RPC_MAX_METADATA_BYTES: usize = 4 * 1024;
pub const RPC_HARD_MAX_FRAME_PAYLOAD_BYTES: usize = 16 * 1024;
pub const RPC_MAX_STREAM_ID: u32 = 0x7fff_ffff;
pub const RPC_END_STREAM: u8 = 0x01;
pub const RPC_MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RpcFrameType {
    Open = 0x01,
    Accept = 0x02,
    Response = 0x03,
    Data = 0x04,
    WindowUpdate = 0x05,
    ResetStream = 0x06,
    CloseStream = 0x07,
    Ping = 0x08,
    Pong = 0x09,
    GoAway = 0x0a,
}
impl TryFrom<u8> for RpcFrameType {
    type Error = RpcWireError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Open),
            2 => Ok(Self::Accept),
            3 => Ok(Self::Response),
            4 => Ok(Self::Data),
            5 => Ok(Self::WindowUpdate),
            6 => Ok(Self::ResetStream),
            7 => Ok(Self::CloseStream),
            8 => Ok(Self::Ping),
            9 => Ok(Self::Pong),
            10 => Ok(Self::GoAway),
            _ => Err(RpcWireError::UnknownFrameType),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcWireError {
    Truncated,
    UnknownFrameType,
    Reserved,
    Flags,
    StreamId,
    Length,
    Metadata,
    Limits,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcFrameHeader {
    pub payload_len: u32,
    pub stream_id: u32,
    pub frame_type: RpcFrameType,
    pub flags: u8,
}
impl RpcFrameHeader {
    pub fn parse(bytes: &[u8], max_frame_payload: usize) -> Result<Self, RpcWireError> {
        let raw: [u8; RPC_HEADER_BYTES] = bytes.try_into().map_err(|_| RpcWireError::Truncated)?;
        let payload_len =
            u32::from_be_bytes(raw[0..4].try_into().map_err(|_| RpcWireError::Truncated)?);
        let stream_id =
            u32::from_be_bytes(raw[4..8].try_into().map_err(|_| RpcWireError::Truncated)?);
        let frame_type = RpcFrameType::try_from(raw[8])?;
        let flags = raw[9];
        if raw[10] != 0 || raw[11] != 0 {
            return Err(RpcWireError::Reserved);
        }
        if flags & !RPC_END_STREAM != 0
            || (flags != 0 && !matches!(frame_type, RpcFrameType::Data | RpcFrameType::Response))
        {
            return Err(RpcWireError::Flags);
        }
        if stream_id > RPC_MAX_STREAM_ID
            || (stream_id == 0
                && !matches!(
                    frame_type,
                    RpcFrameType::Ping
                        | RpcFrameType::Pong
                        | RpcFrameType::WindowUpdate
                        | RpcFrameType::GoAway
                ))
            || (stream_id != 0
                && matches!(
                    frame_type,
                    RpcFrameType::Ping | RpcFrameType::Pong | RpcFrameType::GoAway
                ))
        {
            return Err(RpcWireError::StreamId);
        }
        let len = usize::try_from(payload_len).map_err(|_| RpcWireError::Length)?;
        if len > max_frame_payload.min(RPC_HARD_MAX_FRAME_PAYLOAD_BYTES)
            || (!matches!(frame_type, RpcFrameType::Data) && len > RPC_MAX_METADATA_BYTES)
            || (matches!(frame_type, RpcFrameType::WindowUpdate) && len != 4)
            || (matches!(frame_type, RpcFrameType::Ping | RpcFrameType::Pong) && len != 8)
            || (matches!(frame_type, RpcFrameType::CloseStream) && len != 0)
            || (matches!(frame_type, RpcFrameType::Data) && len == 0 && flags != RPC_END_STREAM)
            || (matches!(
                frame_type,
                RpcFrameType::Open
                    | RpcFrameType::Accept
                    | RpcFrameType::Response
                    | RpcFrameType::ResetStream
                    | RpcFrameType::GoAway
            ) && len == 0)
        {
            return Err(RpcWireError::Length);
        }
        Ok(Self {
            payload_len,
            stream_id,
            frame_type,
            flags,
        })
    }
    pub fn encode(self) -> [u8; RPC_HEADER_BYTES] {
        let mut raw = [0; RPC_HEADER_BYTES];
        raw[0..4].copy_from_slice(&self.payload_len.to_be_bytes());
        raw[4..8].copy_from_slice(&self.stream_id.to_be_bytes());
        raw[8] = self.frame_type as u8;
        raw[9] = self.flags;
        raw
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct RpcLimits {
    pub max_frame_payload_bytes: u32,
    pub max_concurrent_streams: u32,
    pub initial_stream_window_bytes: u32,
    pub initial_connection_window_bytes: u32,
    pub heartbeat_ms: u32,
}
impl Default for RpcLimits {
    fn default() -> Self {
        Self {
            max_frame_payload_bytes: 8192,
            max_concurrent_streams: 256,
            initial_stream_window_bytes: 256 * 1024,
            initial_connection_window_bytes: 4 * 1024 * 1024,
            heartbeat_ms: 5000,
        }
    }
}
impl RpcLimits {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        if !(4096..=RPC_HARD_MAX_FRAME_PAYLOAD_BYTES as u32).contains(&self.max_frame_payload_bytes)
            || self.max_concurrent_streams == 0
            || self.max_concurrent_streams > 4096
            || self.initial_stream_window_bytes < self.max_frame_payload_bytes
            || self.initial_connection_window_bytes < self.initial_stream_window_bytes
            || self.initial_connection_window_bytes > i32::MAX as u32
            || self.heartbeat_ms == 0
            || self.heartbeat_ms > 60_000
        {
            return Err(RpcWireError::Limits);
        }
        Ok(())
    }
    pub fn negotiate(&self, peer: &Self) -> Result<Self, RpcWireError> {
        self.validate()?;
        peer.validate()?;
        let limits = Self {
            max_frame_payload_bytes: self
                .max_frame_payload_bytes
                .min(peer.max_frame_payload_bytes),
            max_concurrent_streams: self.max_concurrent_streams.min(peer.max_concurrent_streams),
            initial_stream_window_bytes: self
                .initial_stream_window_bytes
                .min(peer.initial_stream_window_bytes),
            initial_connection_window_bytes: self
                .initial_connection_window_bytes
                .min(peer.initial_connection_window_bytes),
            heartbeat_ms: self.heartbeat_ms.max(peer.heartbeat_ms),
        };
        limits.validate()?;
        Ok(limits)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RpcBootstrap {
    Hello {
        version: u16,
        token: Option<String>,
        limits: RpcLimits,
    },
    Ready {
        version: u16,
        connection_epoch: u64,
        limits: RpcLimits,
    },
}
impl std::fmt::Debug for RpcBootstrap {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hello {
                version,
                token,
                limits,
            } => formatter
                .debug_struct("Hello")
                .field("version", version)
                .field("token", &token.as_ref().map(|_| "[REDACTED]"))
                .field("limits", limits)
                .finish(),
            Self::Ready {
                version,
                connection_epoch,
                limits,
            } => formatter
                .debug_struct("Ready")
                .field("version", version)
                .field("connection_epoch", connection_epoch)
                .field("limits", limits)
                .finish(),
        }
    }
}
impl RpcBootstrap {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        match self {
            Self::Hello {
                version,
                token,
                limits,
            } => {
                if *version != BUSINESS_RPC_PROTOCOL_VERSION
                    || token.as_ref().is_some_and(|s| s.len() > 256)
                {
                    return Err(RpcWireError::Limits);
                }
                limits.validate()
            }
            Self::Ready {
                version,
                connection_epoch,
                limits,
            } => {
                if *version != BUSINESS_RPC_PROTOCOL_VERSION || *connection_epoch == 0 {
                    return Err(RpcWireError::Limits);
                }
                limits.validate()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RpcOpen {
    Provider {
        provider_id: String,
    },
    EventSubscription {
        subscription_id: SubscriptionId,
        filter: EventFilter,
    },
    Rpc {
        parent_stream_id: Option<u32>,
        request_id: Uuid,
        method: String,
        deadline_ms: u32,
        content_length: u32,
    },
    EventDelivery {
        parent_stream_id: u32,
        delivery_id: Uuid,
        event_id: Uuid,
        attempt: u32,
        content_length: u32,
    },
}
impl RpcOpen {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        match self {
            Self::Provider { provider_id } => {
                if provider_id.is_empty() || provider_id.len() > 64 {
                    return Err(RpcWireError::Metadata);
                }
            }
            Self::EventSubscription {
                subscription_id,
                filter,
            } => {
                if subscription_id.0.is_nil() || filter.validate().is_err() {
                    return Err(RpcWireError::Metadata);
                }
            }
            Self::Rpc {
                parent_stream_id,
                request_id,
                method,
                deadline_ms,
                content_length,
            } => {
                if request_id.is_nil()
                    || *deadline_ms == 0
                    || *deadline_ms > 60_000
                    || method.is_empty()
                    || method.len() > 64
                    || !method
                        .bytes()
                        .all(|byte| byte.is_ascii_lowercase() || byte == b'.' || byte == b'_')
                    || parent_stream_id.is_some_and(|id| id == 0 || id > RPC_MAX_STREAM_ID)
                    || *content_length as usize > RPC_MAX_MESSAGE_BYTES
                {
                    return Err(RpcWireError::Metadata);
                }
            }
            Self::EventDelivery {
                parent_stream_id,
                delivery_id,
                event_id,
                attempt,
                content_length,
            } => {
                if *parent_stream_id == 0
                    || *parent_stream_id > RPC_MAX_STREAM_ID
                    || delivery_id.is_nil()
                    || event_id.is_nil()
                    || *attempt == 0
                    || *content_length as usize > RPC_MAX_MESSAGE_BYTES
                {
                    return Err(RpcWireError::Metadata);
                }
            }
        }
        Ok(())
    }
    pub fn content_length(&self) -> usize {
        match self {
            Self::Rpc { content_length, .. } | Self::EventDelivery { content_length, .. } => {
                *content_length as usize
            }
            Self::Provider { .. } | Self::EventSubscription { .. } => 0,
        }
    }
    pub fn parent(&self) -> Option<u32> {
        match self {
            Self::Rpc {
                parent_stream_id, ..
            } => *parent_stream_id,
            Self::EventDelivery {
                parent_stream_id, ..
            } => Some(*parent_stream_id),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcAccept {
    pub provider_epoch: Option<u64>,
    pub sync_required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcResponse {
    pub content_length: u32,
    pub error: Option<RpcError>,
}
impl RpcResponse {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        if self.content_length as usize > RPC_MAX_MESSAGE_BYTES
            || self
                .error
                .as_ref()
                .is_some_and(|error| error.message.len() > 256)
        {
            Err(RpcWireError::Metadata)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcEventStatus {
    Ok,
    Error,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcEventAck {
    pub status: RpcEventStatus,
    pub delivery_id: Uuid,
    pub event_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcResetCode {
    Cancel,
    RefusedStream,
    ProtocolError,
    FlowControlError,
    MessageTooLarge,
    StreamClosed,
    Overloaded,
    Internal,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcReset {
    pub code: RpcResetCode,
    pub message: String,
}
impl RpcReset {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        if self.message.len() > 256 {
            Err(RpcWireError::Metadata)
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcGoAwayCode {
    NoError,
    ProtocolError,
    FlowControlError,
    Unauthorized,
    Shutdown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcGoAway {
    pub last_stream_id: u32,
    pub code: RpcGoAwayCode,
    pub message: String,
}
impl RpcGoAway {
    pub fn validate(&self) -> Result<(), RpcWireError> {
        if self.last_stream_id > RPC_MAX_STREAM_ID || self.message.len() > 256 {
            Err(RpcWireError::Metadata)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frozen_wire_numbers_and_versions() {
        assert_eq!(BUSINESS_RPC_PROTOCOL_VERSION, 3);
        assert_eq!(
            [
                RpcFrameType::Open as u8,
                RpcFrameType::Accept as u8,
                RpcFrameType::Response as u8,
                RpcFrameType::Data as u8,
                RpcFrameType::WindowUpdate as u8,
                RpcFrameType::ResetStream as u8,
                RpcFrameType::CloseStream as u8,
                RpcFrameType::Ping as u8,
                RpcFrameType::Pong as u8,
                RpcFrameType::GoAway as u8
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }
    #[test]
    fn header_rejects_before_payload_allocation() {
        let header = RpcFrameHeader {
            payload_len: 8192,
            stream_id: 2,
            frame_type: RpcFrameType::Data,
            flags: RPC_END_STREAM,
        };
        assert_eq!(RpcFrameHeader::parse(&header.encode(), 8192), Ok(header));
        let mut bad = header.encode();
        bad[10] = 1;
        assert_eq!(
            RpcFrameHeader::parse(&bad, 8192),
            Err(RpcWireError::Reserved)
        );
        bad = header.encode();
        bad[9] = 2;
        assert_eq!(RpcFrameHeader::parse(&bad, 8192), Err(RpcWireError::Flags));
        bad = header.encode();
        bad[8] = 99;
        assert_eq!(
            RpcFrameHeader::parse(&bad, 8192),
            Err(RpcWireError::UnknownFrameType)
        );
        bad = header.encode();
        bad[0..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(RpcFrameHeader::parse(&bad, 8192), Err(RpcWireError::Length));
        bad = header.encode();
        bad[4..8].copy_from_slice(&0u32.to_be_bytes());
        assert_eq!(
            RpcFrameHeader::parse(&bad, 8192),
            Err(RpcWireError::StreamId)
        );
        assert_eq!(
            RpcFrameHeader::parse(&bad[..11], 8192),
            Err(RpcWireError::Truncated)
        );
    }
    #[test]
    fn bootstrap_is_versioned_and_role_free() {
        let hello = RpcBootstrap::Hello {
            version: 3,
            token: None,
            limits: RpcLimits::default(),
        };
        let wire = serde_json::to_value(&hello).unwrap();
        assert_eq!(wire["version"], 3);
        assert!(wire.get("role").is_none());
        assert!(
            serde_json::from_value::<RpcBootstrap>(wire)
                .unwrap()
                .validate()
                .is_ok()
        );
    }
    #[test]
    fn hello_debug_redacts_token_without_changing_wire() {
        let hello = RpcBootstrap::Hello {
            version: BUSINESS_RPC_PROTOCOL_VERSION,
            token: Some("private-test-token".into()),
            limits: RpcLimits::default(),
        };
        assert!(!format!("{hello:?}").contains("private-test-token"));
        let wire = serde_json::to_value(&hello).unwrap();
        assert_eq!(wire["token"], "private-test-token");
    }
    #[test]
    fn current_bootstrap_rejects_old_unknown_and_corrupt_versions() {
        for version in [0, 1, 2, 4, u16::MAX] {
            let hello = RpcBootstrap::Hello {
                version,
                token: None,
                limits: RpcLimits::default(),
            };
            assert!(hello.validate().is_err());
        }
        assert!(
            RpcBootstrap::Hello {
                version: BUSINESS_RPC_PROTOCOL_VERSION,
                token: None,
                limits: RpcLimits::default()
            }
            .validate()
            .is_ok()
        );
    }
}
