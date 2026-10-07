//! Official Rust client for NetbaIoT business and management APIs.
//!
//! Event delivery is manual-ACK by default. A delivery is never acknowledged
//! merely because it was decoded or placed in the application's bounded stream.

pub mod business_rpc;

use futures_core::Stream;
use netbaiot_protocol::*;
use reqwest::{Method, StatusCode, Url};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

const DEFAULT_EVENT_ITEMS: usize = 32;
const DEFAULT_EVENT_BYTES: usize = 1_048_576;
const DEFAULT_MAX_FRAME_BYTES: usize = 1_048_576;
const DEFAULT_RESPONSE_BYTES: usize = 1_048_576;

#[derive(Clone)]
struct Secret(Arc<str>);

impl Secret {
    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Error, Clone)]
pub enum ClientError {
    #[error("authentication failed")]
    Unauthenticated { request_id: Option<String> },
    #[error("operation is forbidden")]
    Forbidden {
        required_scope: Option<String>,
        request_id: Option<String>,
    },
    #[error("device is offline")]
    DeviceOffline { request_id: Option<String> },
    #[error("client or server capacity is exhausted")]
    Overloaded { request_id: Option<String> },
    #[error("service is draining")]
    ServiceDraining { request_id: Option<String> },
    #[error("operation timed out")]
    Timeout,
    #[error("connection was lost")]
    ConnectionLost,
    #[error("resource was not found")]
    NotFound { request_id: Option<String> },
    #[error("request conflicts with current state")]
    Conflict { request_id: Option<String> },
    #[error("server is unavailable")]
    ServerUnavailable { request_id: Option<String> },
    #[error("request is invalid: {message}")]
    InvalidRequest {
        message: String,
        request_id: Option<String>,
    },
    #[error("protocol version mismatch: server supports {server_version}")]
    VersionMismatch { server_version: u16 },
    #[error("invalid protocol data: {0}")]
    Protocol(String),
    #[error("transport failure: {0}")]
    Transport(String),
    #[error("internal server error")]
    Internal { request_id: Option<String> },
}

impl ClientError {
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Unauthenticated { request_id }
            | Self::DeviceOffline { request_id }
            | Self::Overloaded { request_id }
            | Self::ServiceDraining { request_id }
            | Self::NotFound { request_id }
            | Self::Conflict { request_id }
            | Self::ServerUnavailable { request_id }
            | Self::InvalidRequest { request_id, .. }
            | Self::Internal { request_id }
            | Self::Forbidden { request_id, .. } => request_id.as_deref(),
            _ => None,
        }
    }

    fn from_api(error: ApiError) -> Self {
        match error.code {
            ErrorCode::Unauthenticated => Self::Unauthenticated {
                request_id: error.request_id,
            },
            ErrorCode::Forbidden => Self::Forbidden {
                required_scope: error.required_scope,
                request_id: error.request_id,
            },
            ErrorCode::DeviceOffline => Self::DeviceOffline {
                request_id: error.request_id,
            },
            ErrorCode::Overloaded => Self::Overloaded {
                request_id: error.request_id,
            },
            ErrorCode::ServiceDraining => Self::ServiceDraining {
                request_id: error.request_id,
            },
            ErrorCode::Timeout => Self::Timeout,
            ErrorCode::ConnectionLost => Self::ConnectionLost,
            ErrorCode::NotFound => Self::NotFound {
                request_id: error.request_id,
            },
            ErrorCode::Conflict => Self::Conflict {
                request_id: error.request_id,
            },
            ErrorCode::ServerUnavailable => Self::ServerUnavailable {
                request_id: error.request_id,
            },
            ErrorCode::Internal => Self::Internal {
                request_id: error.request_id,
            },
            ErrorCode::InvalidProtocolVersion => Self::VersionMismatch {
                server_version: PROTOCOL_VERSION,
            },
            ErrorCode::InvalidRequest => Self::InvalidRequest {
                message: error.message,
                request_id: error.request_id,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckMode {
    Manual,
    Immediate,
}

#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub maximum_backoff: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            maximum_backoff: Duration::from_secs(5),
        }
    }
}

#[derive(Default)]
struct Metrics {
    reconnects: AtomicU64,
    events_received: AtomicU64,
    events_acked: AtomicU64,
    command_calls: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientMetrics {
    pub reconnects: u64,
    pub events_received: u64,
    pub events_acked: u64,
    pub command_calls: u64,
}

struct ClientInner {
    endpoint: Url,
    token: Secret,
    api_key: bool,
    event_token: Secret,
    http: reqwest::Client,
    event_address: Option<SocketAddr>,
    event_tls: Option<business_rpc::BusinessRpcTls>,
    stream_handshake_timeout: Duration,
    ack_timeout: Duration,
    event_buffer_items: usize,
    event_buffer_bytes: usize,
    max_frame_bytes: usize,
    max_response_bytes: usize,
    reconnect: ReconnectPolicy,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone)]
pub struct NetbaIoTClient {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for NetbaIoTClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetbaIoTClient")
            .field("endpoint", &self.inner.endpoint)
            .field("token", &self.inner.token)
            .field("event_address", &self.inner.event_address)
            .finish_non_exhaustive()
    }
}

pub struct ClientBuilder {
    endpoint: Option<String>,
    token: Option<String>,
    api_key: bool,
    event_token: Option<String>,
    event_address: Option<SocketAddr>,
    event_tls: Option<business_rpc::BusinessRpcTls>,
    connect_timeout: Duration,
    request_timeout: Duration,
    stream_handshake_timeout: Duration,
    ack_timeout: Duration,
    event_buffer_items: usize,
    event_buffer_bytes: usize,
    max_frame_bytes: usize,
    max_response_bytes: usize,
    reconnect: ReconnectPolicy,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientBuilder")
            .field("endpoint", &self.endpoint)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("event_address", &self.event_address)
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("event_buffer_items", &self.event_buffer_items)
            .field("event_buffer_bytes", &self.event_buffer_bytes)
            .finish_non_exhaustive()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            token: None,
            api_key: false,
            event_token: None,
            event_address: None,
            event_tls: None,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            stream_handshake_timeout: Duration::from_secs(5),
            ack_timeout: Duration::from_secs(5),
            event_buffer_items: DEFAULT_EVENT_ITEMS,
            event_buffer_bytes: DEFAULT_EVENT_BYTES,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_response_bytes: DEFAULT_RESPONSE_BYTES,
            reconnect: ReconnectPolicy::default(),
        }
    }
}

impl ClientBuilder {
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self.api_key = false;
        self
    }

    /// Uses `Authorization: ApiKey <key_id>.<secret>` for management HTTP.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.token = Some(key.into());
        self.api_key = true;
        self
    }

    /// Sets the separately scoped current Business RPC development token.
    /// Management credentials are never used implicitly for event connections.
    pub fn event_token(mut self, token: impl Into<String>) -> Self {
        self.event_token = Some(token.into());
        self
    }

    pub fn event_address(mut self, address: SocketAddr) -> Self {
        self.event_address = Some(address);
        self
    }

    /// Configures mutual TLS for the current Business RPC event connection.
    pub fn event_tls(mut self, tls: business_rpc::BusinessRpcTls) -> Self {
        self.event_tls = Some(tls);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn stream_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.stream_handshake_timeout = timeout;
        self
    }

    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    pub fn event_buffer_limits(mut self, items: usize, bytes: usize) -> Self {
        self.event_buffer_items = items;
        self.event_buffer_bytes = bytes;
        self
    }

    pub fn reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    pub async fn connect(self) -> Result<NetbaIoTClient, ClientError> {
        let endpoint = self.endpoint.ok_or_else(|| ClientError::InvalidRequest {
            message: "endpoint is required".into(),
            request_id: None,
        })?;
        let mut endpoint = Url::parse(&endpoint).map_err(|error| ClientError::InvalidRequest {
            message: format!("invalid endpoint: {error}"),
            request_id: None,
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.cannot_be_a_base() {
            return Err(ClientError::InvalidRequest {
                message: "endpoint must be an HTTP or HTTPS base URL".into(),
                request_id: None,
            });
        }
        let loopback = endpoint.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if endpoint.scheme() == "http" && !loopback {
            return Err(ClientError::InvalidRequest {
                message: "non-loopback management endpoints require HTTPS".into(),
                request_id: None,
            });
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let token = self.token.ok_or_else(|| ClientError::InvalidRequest {
            message: "management token is required".into(),
            request_id: None,
        })?;
        if self.api_key {
            let valid = token.split_once('.').is_some_and(|(id, secret)| {
                !id.is_empty()
                    && id.len() <= 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-.:/@".contains(&b))
                    && secret.len() == 64
                    && secret.bytes().all(|b| b.is_ascii_hexdigit())
            });
            if !valid {
                return Err(ClientError::InvalidRequest {
                    message: "invalid API key".into(),
                    request_id: None,
                });
            }
        }
        if self.event_address.is_some()
            && self.event_tls.is_none()
            && self.event_token.as_ref().is_none_or(String::is_empty)
        {
            return Err(ClientError::InvalidRequest {
                message: "a separate current RPC event token is required".into(),
                request_id: None,
            });
        }
        let event_token = self.event_token.unwrap_or_default();
        if token.is_empty()
            || self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.stream_handshake_timeout.is_zero()
            || self.ack_timeout.is_zero()
            || self.event_buffer_items == 0
            || self.event_buffer_bytes == 0
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > u32::MAX as usize
            || self.reconnect.initial_backoff.is_zero()
            || self.reconnect.initial_backoff > self.reconnect.maximum_backoff
        {
            return Err(ClientError::InvalidRequest {
                message: "timeouts, buffer limits, and reconnect bounds must be valid and nonzero"
                    .into(),
                request_id: None,
            });
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .user_agent(concat!("netbaiot-client/", env!("CARGO_PKG_VERSION")))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(NetbaIoTClient {
            inner: Arc::new(ClientInner {
                endpoint,
                token: Secret(Arc::from(token)),
                api_key: self.api_key,
                event_token: Secret(Arc::from(event_token)),
                http,
                event_address: self.event_address,
                event_tls: self.event_tls,
                stream_handshake_timeout: self.stream_handshake_timeout,
                ack_timeout: self.ack_timeout,
                event_buffer_items: self.event_buffer_items,
                event_buffer_bytes: self.event_buffer_bytes,
                max_frame_bytes: self.max_frame_bytes,
                max_response_bytes: self.max_response_bytes,
                reconnect: self.reconnect,
                shutdown: CancellationToken::new(),
                metrics: Arc::new(Metrics::default()),
            }),
        })
    }
}

impl NetbaIoTClient {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn events(&self) -> Events {
        Events(self.clone())
    }

    pub fn commands(&self) -> Commands {
        Commands(self.clone())
    }

    pub fn devices(&self) -> Devices {
        Devices(self.clone())
    }

    pub fn runtime(&self) -> Runtime {
        Runtime(self.clone())
    }

    pub fn auth_cache(&self) -> AuthCache {
        AuthCache(self.clone())
    }

    pub fn routes(&self) -> Routes {
        Routes(self.clone())
    }

    pub fn metrics(&self) -> ClientMetrics {
        ClientMetrics {
            reconnects: self.inner.metrics.reconnects.load(Ordering::Relaxed),
            events_received: self.inner.metrics.events_received.load(Ordering::Relaxed),
            events_acked: self.inner.metrics.events_acked.load(Ordering::Relaxed),
            command_calls: self.inner.metrics.command_calls.load(Ordering::Relaxed),
        }
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
    }

    async fn request<B: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R, ClientError> {
        let url = self
            .inner
            .endpoint
            .join(path.trim_start_matches('/'))
            .map_err(|error| ClientError::Protocol(error.to_string()))?;
        let mut request = self.inner.http.request(method, url);
        request = if self.inner.api_key {
            request.header(
                reqwest::header::AUTHORIZATION,
                format!("ApiKey {}", self.inner.token.expose()),
            )
        } else {
            request.bearer_auth(self.inner.token.expose())
        };
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(map_reqwest)?;
        let status = response.status();
        let bytes = read_response(response, self.inner.max_response_bytes).await?;
        if !status.is_success() {
            return Err(parse_error(status, &bytes));
        }
        serde_json::from_slice(&bytes).map_err(|error| ClientError::Protocol(error.to_string()))
    }

    async fn request_empty<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<(), ClientError> {
        let url = self
            .inner
            .endpoint
            .join(path.trim_start_matches('/'))
            .map_err(|error| ClientError::Protocol(error.to_string()))?;
        let mut request = self.inner.http.request(method, url);
        request = if self.inner.api_key {
            request.header(
                reqwest::header::AUTHORIZATION,
                format!("ApiKey {}", self.inner.token.expose()),
            )
        } else {
            request.bearer_auth(self.inner.token.expose())
        };
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(map_reqwest)?;
        let status = response.status();
        let bytes = read_response(response, self.inner.max_response_bytes).await?;
        if status.is_success() {
            Ok(())
        } else {
            Err(parse_error(status, &bytes))
        }
    }
}

async fn read_response(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, ClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(ClientError::Protocol(
            "response exceeds configured limit".into(),
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
        let length = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| ClientError::Protocol("response length overflow".into()))?;
        if length > maximum {
            return Err(ClientError::Protocol(
                "response exceeds configured limit".into(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn map_reqwest(error: reqwest::Error) -> ClientError {
    if error.is_timeout() {
        ClientError::Timeout
    } else if error.is_connect() {
        ClientError::ServerUnavailable { request_id: None }
    } else {
        ClientError::Transport(error.to_string())
    }
}

fn parse_error(status: StatusCode, bytes: &[u8]) -> ClientError {
    if let Ok(error) = serde_json::from_slice::<ApiError>(bytes) {
        return ClientError::from_api(error);
    }
    match status {
        StatusCode::UNAUTHORIZED => ClientError::Unauthenticated { request_id: None },
        StatusCode::FORBIDDEN => ClientError::Forbidden {
            required_scope: None,
            request_id: None,
        },
        StatusCode::NOT_FOUND => ClientError::NotFound { request_id: None },
        StatusCode::CONFLICT => ClientError::Conflict { request_id: None },
        StatusCode::TOO_MANY_REQUESTS => ClientError::Overloaded { request_id: None },
        StatusCode::SERVICE_UNAVAILABLE => ClientError::ServerUnavailable { request_id: None },
        _ if status.is_server_error() => ClientError::ServerUnavailable { request_id: None },
        _ => ClientError::InvalidRequest {
            message: format!("server returned HTTP {status}"),
            request_id: None,
        },
    }
}

#[derive(Clone)]
pub struct Commands(NetbaIoTClient);

impl Commands {
    /// Sends one live-session command. The SDK never retries this operation
    /// automatically; callers may retry with the same `command_id` only when
    /// their application semantics permit duplicate device execution.
    pub async fn send(&self, command: &DeviceCommand) -> Result<CommandDispatch, ClientError> {
        self.0
            .inner
            .metrics
            .command_calls
            .fetch_add(1, Ordering::Relaxed);
        self.0
            .request(Method::POST, paths::COMMANDS, Some(command))
            .await
    }
}

#[derive(Clone)]
pub struct Devices(NetbaIoTClient);

impl Devices {
    pub async fn connection(
        &self,
        device: &DeviceKey,
    ) -> Result<DeviceConnectionInfo, ClientError> {
        self.0
            .request(Method::POST, paths::DEVICE_CONNECTION, Some(device))
            .await
    }
}

#[derive(Clone)]
pub struct Runtime(NetbaIoTClient);

impl Runtime {
    pub async fn status(&self) -> Result<RuntimeStatus, ClientError> {
        self.0
            .request::<(), RuntimeStatus>(Method::GET, paths::STATUS, None)
            .await
    }

    /// Requests administrative graceful drain. This changes server lifecycle.
    pub async fn drain(&self) -> Result<(), ClientError> {
        self.0
            .request_empty::<()>(Method::POST, paths::DRAIN, None)
            .await
    }
}

#[derive(Clone)]
pub struct AuthCache(NetbaIoTClient);

impl AuthCache {
    pub async fn invalidate(
        &self,
        invalidation: &AuthInvalidation,
    ) -> Result<InvalidationResult, ClientError> {
        self.0
            .request(Method::POST, paths::AUTH_INVALIDATE, Some(invalidation))
            .await
    }

    pub async fn invalidate_device(
        &self,
        device: DeviceKey,
    ) -> Result<InvalidationResult, ClientError> {
        self.invalidate(&AuthInvalidation::Device { device }).await
    }
}

#[derive(Clone)]
pub struct Routes(NetbaIoTClient);

impl Routes {
    pub async fn apply(&self, update: &RoutesUpdate) -> Result<(), ClientError> {
        self.0
            .request_empty(Method::PUT, paths::ROUTES, Some(update))
            .await
    }
}

#[derive(Clone)]
pub struct Events(NetbaIoTClient);

impl Events {
    /// Opens a confirmed event stream. Manual ACK is the correctness-first default.
    pub async fn subscribe(&self, filter: EventFilter) -> Result<EventStream, ClientError> {
        self.subscribe_with_mode(filter, AckMode::Manual).await
    }

    pub async fn subscribe_with_mode(
        &self,
        filter: EventFilter,
        ack_mode: AckMode,
    ) -> Result<EventStream, ClientError> {
        filter.validate().map_err(|_| ClientError::InvalidRequest {
            message: "event filter exceeds protocol bounds".into(),
            request_id: None,
        })?;
        let address = self
            .0
            .inner
            .event_address
            .ok_or_else(|| ClientError::InvalidRequest {
                message: "event stream address is not configured".into(),
                request_id: None,
            })?;
        let mut settings = business_rpc::BusinessRpcV3ClientConfig::development(
            address,
            self.0.inner.event_token.expose().to_owned(),
        );
        settings.tls = self.0.inner.event_tls.clone();
        if settings.tls.is_some() {
            settings.token = None;
        }
        settings.provider = false;
        settings.events = true;
        settings.filter = filter;
        settings.connect_timeout = self.0.inner.stream_handshake_timeout;
        settings.request_timeout = self.0.inner.ack_timeout;
        settings.reconnect_initial = self.0.inner.reconnect.initial_backoff;
        settings.reconnect_max = self.0.inner.reconnect.maximum_backoff;
        let (rpc, deliveries) = business_rpc::BusinessRpcV3Client::connect(settings, None)
            .map_err(current_rpc_error)?;
        tokio::time::timeout(self.0.inner.stream_handshake_timeout, rpc.wait_ready())
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(current_rpc_error)?;
        let (sender, receiver) = mpsc::channel(self.0.inner.event_buffer_items);
        let cancel = self.0.inner.shutdown.child_token();
        let task_cancel = cancel.clone();
        let inner = self.0.inner.clone();
        let task = tokio::spawn(async move {
            run_event_subscription(inner, ack_mode, rpc, deliveries, sender, task_cancel).await;
        });
        Ok(EventStream {
            receiver,
            cancel,
            task: Some(task),
        })
    }
}

enum DeliveryPayload {
    Manual(business_rpc::BusinessRpcV3Delivery),
    Immediate(EventDelivery),
}
pub struct Delivery {
    delivery: DeliveryPayload,
    metrics: Arc<Metrics>,
    _byte_permit: OwnedSemaphorePermit,
}
impl Delivery {
    fn wire(&self) -> &EventDelivery {
        match &self.delivery {
            DeliveryPayload::Manual(delivery) => &delivery.delivery,
            DeliveryPayload::Immediate(delivery) => delivery,
        }
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("delivery_id", &self.wire().delivery_id)
            .field("event_id", &self.wire().event.event_id)
            .field("attempt", &self.wire().attempt)
            .finish_non_exhaustive()
    }
}

impl Delivery {
    pub fn event(&self) -> &DeviceEvent {
        &self.wire().event
    }

    pub fn event_id(&self) -> EventId {
        self.wire().event.event_id
    }

    pub fn delivery_id(&self) -> DeliveryId {
        self.wire().delivery_id
    }

    pub fn attempt(&self) -> u32 {
        self.wire().attempt
    }

    /// Confirms application processing, then waits for the ACK frame write.
    pub async fn ack(self) -> Result<(), ClientError> {
        if let DeliveryPayload::Manual(delivery) = self.delivery {
            delivery.ack().await.map_err(|error| match error {
                business_rpc::BusinessRpcClientError::Unavailable
                | business_rpc::BusinessRpcClientError::OutcomeUnknown => {
                    ClientError::ConnectionLost
                }
                other => current_rpc_error(other),
            })?;
            self.metrics.events_acked.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

pub struct EventStream {
    receiver: mpsc::Receiver<Result<Delivery, ClientError>>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl EventStream {
    pub async fn recv(&mut self) -> Option<Result<Delivery, ClientError>> {
        self.receiver.recv().await
    }

    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl Stream for EventStream {
    type Item = Result<Delivery, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn current_rpc_error(error: business_rpc::BusinessRpcClientError) -> ClientError {
    use business_rpc::BusinessRpcClientError as Error;
    match error {
        Error::Unauthorized | Error::Remote(RpcCode::Unauthenticated) => {
            ClientError::Unauthenticated { request_id: None }
        }
        Error::Remote(RpcCode::Forbidden) => ClientError::Forbidden {
            required_scope: None,
            request_id: None,
        },
        Error::Overloaded | Error::Remote(RpcCode::Overloaded) => {
            ClientError::Overloaded { request_id: None }
        }
        Error::Timeout => ClientError::Timeout,
        Error::Unavailable | Error::Remote(RpcCode::Unavailable) => {
            ClientError::ServerUnavailable { request_id: None }
        }
        Error::InvalidConfig => ClientError::InvalidRequest {
            message: "invalid current RPC stream configuration".into(),
            request_id: None,
        },
        Error::Protocol | Error::Remote(_) | Error::OutcomeUnknown => {
            ClientError::Protocol("current business RPC failure".into())
        }
    }
}
use netbaiot_protocol::business_rpc::RpcErrorCode as RpcCode;

async fn run_event_subscription(
    inner: Arc<ClientInner>,
    ack_mode: AckMode,
    rpc: business_rpc::BusinessRpcV3Client,
    mut deliveries: mpsc::Receiver<business_rpc::BusinessRpcV3Delivery>,
    sender: mpsc::Sender<Result<Delivery, ClientError>>,
    cancel: CancellationToken,
) {
    let byte_budget = Arc::new(Semaphore::new(inner.event_buffer_bytes));
    let mut last_epoch = 0;
    loop {
        let delivery = tokio::select! {
            _ = cancel.cancelled() => break,
            value = deliveries.recv() => match value { Some(value) => value, None => break },
        };
        if last_epoch != 0 && last_epoch != delivery.connection_epoch {
            inner.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        last_epoch = delivery.connection_epoch;
        let encoded = match serde_json::to_vec(&delivery.delivery) {
            Ok(value)
                if value.len() <= inner.max_frame_bytes
                    && value.len() <= inner.event_buffer_bytes =>
            {
                value.len()
            }
            _ => {
                let _ = sender.try_send(Err(ClientError::Protocol(
                    "event exceeds configured byte bounds".into(),
                )));
                break;
            }
        };
        let Ok(permits) = u32::try_from(encoded) else {
            break;
        };
        let permit = tokio::select! {
            _ = cancel.cancelled() => break,
            value = byte_budget.clone().acquire_many_owned(permits) => match value { Ok(value) => value, Err(_) => break },
        };
        inner
            .metrics
            .events_received
            .fetch_add(1, Ordering::Relaxed);
        let payload = if ack_mode == AckMode::Immediate {
            let wire = delivery.delivery.clone();
            if let Err(error) = delivery.ack().await {
                let _ = sender.try_send(Err(current_rpc_error(error)));
                break;
            }
            inner.metrics.events_acked.fetch_add(1, Ordering::Relaxed);
            DeliveryPayload::Immediate(wire)
        } else {
            DeliveryPayload::Manual(delivery)
        };
        let item = Delivery {
            delivery: payload,
            metrics: inner.metrics.clone(),
            _byte_permit: permit,
        };
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = sender.send(Ok(item)) => { if result.is_err() { break; } }
        }
    }
    rpc.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builder_rejects_invalid_limits_and_redacts_token() {
        let builder = NetbaIoTClient::builder()
            .endpoint("http://127.0.0.1:1")
            .token("top-secret")
            .event_buffer_limits(0, 1);
        assert!(!format!("{builder:?}").contains("top-secret"));
        assert!(builder.connect().await.is_err());

        let client = NetbaIoTClient::builder()
            .endpoint("http://127.0.0.1:1")
            .token("top-secret")
            .connect()
            .await
            .unwrap();
        assert!(!format!("{client:?}").contains("top-secret"));
    }

    #[tokio::test]
    async fn api_key_builder_validates_credential_and_keeps_it_redacted() {
        let key = format!("backend.{}", "a".repeat(64));
        let builder = NetbaIoTClient::builder()
            .endpoint("http://127.0.0.1:1")
            .api_key(&key);
        assert!(!format!("{builder:?}").contains(&key));
        let client = builder.connect().await.unwrap();
        assert!(!format!("{client:?}").contains(&key));
        assert!(
            NetbaIoTClient::builder()
                .endpoint("http://127.0.0.1:1")
                .api_key("bad")
                .connect()
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn management_bearer_requires_https_off_loopback() {
        let error = NetbaIoTClient::builder()
            .endpoint("http://192.0.2.1:8081")
            .token("top-secret")
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::InvalidRequest { .. }));
        NetbaIoTClient::builder()
            .endpoint("http://localhost:8081")
            .token("top-secret")
            .connect()
            .await
            .unwrap();
    }
}
