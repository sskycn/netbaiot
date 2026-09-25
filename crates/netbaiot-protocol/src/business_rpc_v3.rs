//! Business RPC V3 bootstrap and binary framing contract.
//! V2's length-prefixed JSON frame contract remains in `business_rpc`.
use crate::{EventFilter, SubscriptionId, business_rpc::RpcError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const BUSINESS_RPC_V3_VERSION: u16 = 3;
pub const V3_HEADER_BYTES: usize = 12;
pub const V3_MAX_METADATA_BYTES: usize = 4 * 1024;
pub const V3_HARD_MAX_FRAME_PAYLOAD_BYTES: usize = 16 * 1024;
pub const V3_MAX_STREAM_ID: u32 = 0x7fff_ffff;
pub const V3_END_STREAM: u8 = 0x01;
pub const V3_MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum V3FrameType {
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
impl TryFrom<u8> for V3FrameType {
    type Error = V3WireError;
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
            _ => Err(V3WireError::UnknownFrameType),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V3WireError {
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
pub struct V3FrameHeader {
    pub payload_len: u32,
    pub stream_id: u32,
    pub frame_type: V3FrameType,
    pub flags: u8,
}
impl V3FrameHeader {
    pub fn parse(bytes: &[u8], max_frame_payload: usize) -> Result<Self, V3WireError> {
        let raw: [u8; V3_HEADER_BYTES] = bytes.try_into().map_err(|_| V3WireError::Truncated)?;
        let payload_len =
            u32::from_be_bytes(raw[0..4].try_into().map_err(|_| V3WireError::Truncated)?);
        let stream_id =
            u32::from_be_bytes(raw[4..8].try_into().map_err(|_| V3WireError::Truncated)?);
        let frame_type = V3FrameType::try_from(raw[8])?;
        let flags = raw[9];
        if raw[10] != 0 || raw[11] != 0 {
            return Err(V3WireError::Reserved);
        }
        if flags & !V3_END_STREAM != 0
            || (flags != 0 && !matches!(frame_type, V3FrameType::Data | V3FrameType::Response))
        {
            return Err(V3WireError::Flags);
        }
        if stream_id > V3_MAX_STREAM_ID
            || (stream_id == 0
                && !matches!(
                    frame_type,
                    V3FrameType::Ping
                        | V3FrameType::Pong
                        | V3FrameType::WindowUpdate
                        | V3FrameType::GoAway
                ))
            || (stream_id != 0
                && matches!(
                    frame_type,
                    V3FrameType::Ping | V3FrameType::Pong | V3FrameType::GoAway
                ))
        {
            return Err(V3WireError::StreamId);
        }
        let len = usize::try_from(payload_len).map_err(|_| V3WireError::Length)?;
        if len > max_frame_payload.min(V3_HARD_MAX_FRAME_PAYLOAD_BYTES)
            || (!matches!(frame_type, V3FrameType::Data) && len > V3_MAX_METADATA_BYTES)
            || (matches!(frame_type, V3FrameType::WindowUpdate) && len != 4)
            || (matches!(frame_type, V3FrameType::Ping | V3FrameType::Pong) && len != 8)
            || (matches!(frame_type, V3FrameType::CloseStream) && len != 0)
            || (matches!(frame_type, V3FrameType::Data) && len == 0 && flags != V3_END_STREAM)
            || (matches!(
                frame_type,
                V3FrameType::Open
                    | V3FrameType::Accept
                    | V3FrameType::Response
                    | V3FrameType::ResetStream
                    | V3FrameType::GoAway
            ) && len == 0)
        {
            return Err(V3WireError::Length);
        }
        Ok(Self {
            payload_len,
            stream_id,
            frame_type,
            flags,
        })
    }
    pub fn encode(self) -> [u8; V3_HEADER_BYTES] {
        let mut raw = [0; V3_HEADER_BYTES];
        raw[0..4].copy_from_slice(&self.payload_len.to_be_bytes());
        raw[4..8].copy_from_slice(&self.stream_id.to_be_bytes());
        raw[8] = self.frame_type as u8;
        raw[9] = self.flags;
        raw
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V3Limits {
    pub max_frame_payload_bytes: u32,
    pub max_concurrent_streams: u32,
    pub initial_stream_window_bytes: u32,
    pub initial_connection_window_bytes: u32,
    pub heartbeat_ms: u32,
}
impl Default for V3Limits {
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
impl V3Limits {
    pub fn validate(&self) -> Result<(), V3WireError> {
        if !(4096..=V3_HARD_MAX_FRAME_PAYLOAD_BYTES as u32).contains(&self.max_frame_payload_bytes)
            || self.max_concurrent_streams == 0
            || self.max_concurrent_streams > 4096
            || self.initial_stream_window_bytes < self.max_frame_payload_bytes
            || self.initial_connection_window_bytes < self.initial_stream_window_bytes
            || self.initial_connection_window_bytes > i32::MAX as u32
            || self.heartbeat_ms == 0
            || self.heartbeat_ms > 60_000
        {
            return Err(V3WireError::Limits);
        }
        Ok(())
    }
    pub fn negotiate(&self, peer: &Self) -> Result<Self, V3WireError> {
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
pub enum V3Bootstrap {
    Hello {
        version: u16,
        token: Option<String>,
        limits: V3Limits,
    },
    Ready {
        version: u16,
        connection_epoch: u64,
        limits: V3Limits,
    },
}
impl std::fmt::Debug for V3Bootstrap {
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
impl V3Bootstrap {
    pub fn validate(&self) -> Result<(), V3WireError> {
        match self {
            Self::Hello {
                version,
                token,
                limits,
            } => {
                if *version != BUSINESS_RPC_V3_VERSION
                    || token.as_ref().is_some_and(|s| s.len() > 256)
                {
                    return Err(V3WireError::Limits);
                }
                limits.validate()
            }
            Self::Ready {
                version,
                connection_epoch,
                limits,
            } => {
                if *version != BUSINESS_RPC_V3_VERSION || *connection_epoch == 0 {
                    return Err(V3WireError::Limits);
                }
                limits.validate()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum V3Open {
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
impl V3Open {
    pub fn validate(&self) -> Result<(), V3WireError> {
        match self {
            Self::Provider { provider_id } => {
                if provider_id.is_empty() || provider_id.len() > 64 {
                    return Err(V3WireError::Metadata);
                }
            }
            Self::EventSubscription {
                subscription_id,
                filter,
            } => {
                if subscription_id.0.is_nil() || filter.validate().is_err() {
                    return Err(V3WireError::Metadata);
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
                    || parent_stream_id.is_some_and(|id| id == 0 || id > V3_MAX_STREAM_ID)
                    || *content_length as usize > V3_MAX_MESSAGE_BYTES
                {
                    return Err(V3WireError::Metadata);
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
                    || *parent_stream_id > V3_MAX_STREAM_ID
                    || delivery_id.is_nil()
                    || event_id.is_nil()
                    || *attempt == 0
                    || *content_length as usize > V3_MAX_MESSAGE_BYTES
                {
                    return Err(V3WireError::Metadata);
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
pub struct V3Accept {
    pub provider_epoch: Option<u64>,
    pub sync_required: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V3Response {
    pub content_length: u32,
    pub error: Option<RpcError>,
}
impl V3Response {
    pub fn validate(&self) -> Result<(), V3WireError> {
        if self.content_length as usize > V3_MAX_MESSAGE_BYTES
            || self
                .error
                .as_ref()
                .is_some_and(|error| error.message.len() > 256)
        {
            Err(V3WireError::Metadata)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum V3EventStatus {
    Ok,
    Error,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V3EventAck {
    pub status: V3EventStatus,
    pub delivery_id: Uuid,
    pub event_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum V3ResetCode {
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
pub struct V3Reset {
    pub code: V3ResetCode,
    pub message: String,
}
impl V3Reset {
    pub fn validate(&self) -> Result<(), V3WireError> {
        if self.message.len() > 256 {
            Err(V3WireError::Metadata)
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum V3GoAwayCode {
    NoError,
    ProtocolError,
    FlowControlError,
    Unauthorized,
    Shutdown,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V3GoAway {
    pub last_stream_id: u32,
    pub code: V3GoAwayCode,
    pub message: String,
}
impl V3GoAway {
    pub fn validate(&self) -> Result<(), V3WireError> {
        if self.last_stream_id > V3_MAX_STREAM_ID || self.message.len() > 256 {
            Err(V3WireError::Metadata)
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
        assert_eq!(crate::business_rpc::BUSINESS_RPC_VERSION, 2);
        assert_eq!(BUSINESS_RPC_V3_VERSION, 3);
        assert_eq!(
            [
                V3FrameType::Open as u8,
                V3FrameType::Accept as u8,
                V3FrameType::Response as u8,
                V3FrameType::Data as u8,
                V3FrameType::WindowUpdate as u8,
                V3FrameType::ResetStream as u8,
                V3FrameType::CloseStream as u8,
                V3FrameType::Ping as u8,
                V3FrameType::Pong as u8,
                V3FrameType::GoAway as u8
            ],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }
    #[test]
    fn header_rejects_before_payload_allocation() {
        let header = V3FrameHeader {
            payload_len: 8192,
            stream_id: 2,
            frame_type: V3FrameType::Data,
            flags: V3_END_STREAM,
        };
        assert_eq!(V3FrameHeader::parse(&header.encode(), 8192), Ok(header));
        let mut bad = header.encode();
        bad[10] = 1;
        assert_eq!(V3FrameHeader::parse(&bad, 8192), Err(V3WireError::Reserved));
        bad = header.encode();
        bad[9] = 2;
        assert_eq!(V3FrameHeader::parse(&bad, 8192), Err(V3WireError::Flags));
        bad = header.encode();
        bad[8] = 99;
        assert_eq!(
            V3FrameHeader::parse(&bad, 8192),
            Err(V3WireError::UnknownFrameType)
        );
        bad = header.encode();
        bad[0..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(V3FrameHeader::parse(&bad, 8192), Err(V3WireError::Length));
        bad = header.encode();
        bad[4..8].copy_from_slice(&0u32.to_be_bytes());
        assert_eq!(V3FrameHeader::parse(&bad, 8192), Err(V3WireError::StreamId));
        assert_eq!(
            V3FrameHeader::parse(&bad[..11], 8192),
            Err(V3WireError::Truncated)
        );
    }
    #[test]
    fn bootstrap_is_versioned_and_role_free() {
        let hello = V3Bootstrap::Hello {
            version: 3,
            token: None,
            limits: V3Limits::default(),
        };
        let wire = serde_json::to_value(&hello).unwrap();
        assert_eq!(wire["version"], 3);
        assert!(wire.get("role").is_none());
        assert!(
            serde_json::from_value::<V3Bootstrap>(wire)
                .unwrap()
                .validate()
                .is_ok()
        );
    }
    #[test]
    fn hello_debug_redacts_token_without_changing_wire() {
        let hello = V3Bootstrap::Hello {
            version: BUSINESS_RPC_V3_VERSION,
            token: Some("private-test-token".into()),
            limits: V3Limits::default(),
        };
        assert!(!format!("{hello:?}").contains("private-test-token"));
        let wire = serde_json::to_value(&hello).unwrap();
        assert_eq!(wire["token"], "private-test-token");
    }
}
