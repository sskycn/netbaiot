//! Official Rust client for NetbaIoT business and management APIs.
//!
//! Event delivery is manual-ACK by default. A delivery is never acknowledged
//! merely because it was decoded or placed in the application's bounded stream.

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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};
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
    stream_handshake_timeout: Duration,
    ack_timeout: Duration,
    event_buffer_items: usize,
    event_buffer_bytes: usize,
    max_frame_bytes: usize,
    max_response_bytes: usize,
    reconnect: ReconnectPolicy,
    shutdown: CancellationToken,
    metrics: Metrics,
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

    /// Sets the separately scoped confirmed-stream bearer token. When omitted,
    /// the management token is used for deployments with one shared credential.
    pub fn event_token(mut self, token: impl Into<String>) -> Self {
        self.event_token = Some(token.into());
        self
    }

    pub fn event_address(mut self, address: SocketAddr) -> Self {
        self.event_address = Some(address);
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
            if !valid || (self.event_address.is_some() && self.event_token.is_none()) {
                return Err(ClientError::InvalidRequest {
                    message: "invalid API key or missing separate event token".into(),
                    request_id: None,
                });
            }
        }
        let event_token = self.event_token.unwrap_or_else(|| token.clone());
        if token.is_empty()
            || event_token.is_empty()
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
                stream_handshake_timeout: self.stream_handshake_timeout,
                ack_timeout: self.ack_timeout,
                event_buffer_items: self.event_buffer_items,
                event_buffer_bytes: self.event_buffer_bytes,
                max_frame_bytes: self.max_frame_bytes,
                max_response_bytes: self.max_response_bytes,
                reconnect: self.reconnect,
                shutdown: CancellationToken::new(),
                metrics: Metrics::default(),
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
        let subscription_id = SubscriptionId::generate();
        let stream = connect_event_stream(
            address,
            self.0.inner.event_token.clone(),
            subscription_id,
            &filter,
            self.0.inner.max_frame_bytes,
            self.0.inner.stream_handshake_timeout,
        )
        .await?;
        let (sender, receiver) = mpsc::channel(self.0.inner.event_buffer_items);
        let cancel = self.0.inner.shutdown.child_token();
        let task_cancel = cancel.clone();
        let inner = self.0.inner.clone();
        let task = tokio::spawn(async move {
            run_event_subscription(
                inner,
                address,
                subscription_id,
                filter,
                ack_mode,
                stream,
                sender,
                task_cancel,
            )
            .await;
        });
        Ok(EventStream {
            receiver,
            cancel,
            task: Some(task),
        })
    }
}

type AckCompletion = oneshot::Sender<Result<(), ClientError>>;

pub struct Delivery {
    delivery: EventDelivery,
    ack: Option<oneshot::Sender<AckCompletion>>,
    _byte_permit: OwnedSemaphorePermit,
}

impl fmt::Debug for Delivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Delivery")
            .field("delivery_id", &self.delivery.delivery_id)
            .field("event_id", &self.delivery.event.event_id)
            .field("attempt", &self.delivery.attempt)
            .finish_non_exhaustive()
    }
}

impl Delivery {
    pub fn event(&self) -> &DeviceEvent {
        &self.delivery.event
    }

    pub fn event_id(&self) -> EventId {
        self.delivery.event.event_id
    }

    pub fn delivery_id(&self) -> DeliveryId {
        self.delivery.delivery_id
    }

    pub fn attempt(&self) -> u32 {
        self.delivery.attempt
    }

    /// Confirms application processing, then waits for the ACK frame write.
    pub async fn ack(mut self) -> Result<(), ClientError> {
        let Some(sender) = self.ack.take() else {
            return Ok(());
        };
        let (done, completed) = oneshot::channel();
        sender.send(done).map_err(|_| ClientError::ConnectionLost)?;
        completed.await.map_err(|_| ClientError::ConnectionLost)?
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

#[allow(clippy::too_many_arguments)]
async fn run_event_subscription(
    inner: Arc<ClientInner>,
    address: SocketAddr,
    subscription_id: SubscriptionId,
    filter: EventFilter,
    ack_mode: AckMode,
    mut stream: TcpStream,
    sender: mpsc::Sender<Result<Delivery, ClientError>>,
    cancel: CancellationToken,
) {
    let byte_budget = Arc::new(Semaphore::new(inner.event_buffer_bytes));
    let mut reconnect_attempt = 0u32;
    loop {
        let result = run_connected_stream(
            &inner,
            subscription_id,
            ack_mode,
            &mut stream,
            &sender,
            &byte_budget,
            &cancel,
        )
        .await;
        if cancel.is_cancelled() || sender.is_closed() {
            break;
        }
        if let Err(
            error @ (ClientError::Unauthenticated { .. }
            | ClientError::Forbidden { .. }
            | ClientError::VersionMismatch { .. }
            | ClientError::Protocol(_)),
        ) = result
        {
            let _ = sender.send(Err(error)).await;
            break;
        }
        reconnect_attempt = reconnect_attempt.saturating_add(1);
        inner.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
        let delay = reconnect_delay(&inner.reconnect, reconnect_attempt, subscription_id);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
        loop {
            match connect_event_stream(
                address,
                inner.event_token.clone(),
                subscription_id,
                &filter,
                inner.max_frame_bytes,
                inner.stream_handshake_timeout,
            )
            .await
            {
                Ok(next) => {
                    stream = next;
                    reconnect_attempt = 0;
                    break;
                }
                Err(
                    error @ (ClientError::Unauthenticated { .. }
                    | ClientError::Forbidden { .. }
                    | ClientError::VersionMismatch { .. }),
                ) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
                Err(_) => {
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    inner.metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                    let delay =
                        reconnect_delay(&inner.reconnect, reconnect_attempt, subscription_id);
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }
}

async fn run_connected_stream(
    inner: &ClientInner,
    subscription_id: SubscriptionId,
    ack_mode: AckMode,
    stream: &mut TcpStream,
    sender: &mpsc::Sender<Result<Delivery, ClientError>>,
    byte_budget: &Arc<Semaphore>,
    cancel: &CancellationToken,
) -> Result<(), ClientError> {
    loop {
        let frame = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            frame = read_frame(stream, inner.max_frame_bytes) => frame?,
        };
        let frame: StreamServerFrame = serde_json::from_slice(&frame)
            .map_err(|error| ClientError::Protocol(error.to_string()))?;
        let delivery = match frame {
            StreamServerFrame::Event { version, delivery }
                if version == PROTOCOL_VERSION && delivery.subscription_id == subscription_id =>
            {
                delivery
            }
            StreamServerFrame::Error { error, .. } => return Err(ClientError::from_api(error)),
            StreamServerFrame::Event { version, .. } | StreamServerFrame::Ready { version, .. } => {
                if version != PROTOCOL_VERSION {
                    return Err(ClientError::VersionMismatch {
                        server_version: version,
                    });
                }
                return Err(ClientError::Protocol("unexpected stream frame".into()));
            }
        };
        let encoded_bytes = serde_json::to_vec(&delivery)
            .map_err(|error| ClientError::Protocol(error.to_string()))?
            .len();
        let permits = u32::try_from(encoded_bytes)
            .map_err(|_| ClientError::Protocol("event exceeds byte budget".into()))?;
        if encoded_bytes > inner.event_buffer_bytes {
            return Err(ClientError::Protocol(
                "event exceeds configured event buffer bytes".into(),
            ));
        }
        let permit = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            permit = byte_budget.clone().acquire_many_owned(permits) => {
                permit.map_err(|_| ClientError::ConnectionLost)?
            }
        };
        inner
            .metrics
            .events_received
            .fetch_add(1, Ordering::Relaxed);
        if ack_mode == AckMode::Immediate {
            send_ack(stream, &delivery, inner.ack_timeout, inner.max_frame_bytes).await?;
            inner.metrics.events_acked.fetch_add(1, Ordering::Relaxed);
            sender
                .send(Ok(Delivery {
                    delivery,
                    ack: None,
                    _byte_permit: permit,
                }))
                .await
                .map_err(|_| ClientError::ConnectionLost)?;
            continue;
        }
        let ack = EventAck {
            delivery_id: delivery.delivery_id,
            subscription_id,
            event_id: delivery.event.event_id,
        };
        let (ack_sender, ack_receiver) = oneshot::channel();
        sender
            .send(Ok(Delivery {
                delivery,
                ack: Some(ack_sender),
                _byte_permit: permit,
            }))
            .await
            .map_err(|_| ClientError::ConnectionLost)?;
        tokio::pin!(ack_receiver);
        let completed = loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                request = &mut ack_receiver => {
                    break request.map_err(|_| ClientError::ConnectionLost)?;
                }
                ready = stream.readable() => {
                    ready.map_err(|_| ClientError::ConnectionLost)?;
                    let mut byte = [0u8; 1];
                    match stream.try_read(&mut byte) {
                        Ok(0) => return Err(ClientError::ConnectionLost),
                        Ok(_) => return Err(ClientError::Protocol(
                            "server sent a frame before the outstanding delivery was ACKed".into(),
                        )),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                        Err(_) => return Err(ClientError::ConnectionLost),
                    }
                }
            }
        };
        let result = send_ack_value(stream, ack, inner.ack_timeout, inner.max_frame_bytes).await;
        if result.is_ok() {
            inner.metrics.events_acked.fetch_add(1, Ordering::Relaxed);
        }
        let _ = completed.send(result.clone());
        result?;
    }
}

fn reconnect_delay(
    policy: &ReconnectPolicy,
    attempt: u32,
    subscription_id: SubscriptionId,
) -> Duration {
    let exponent = attempt.min(20);
    let base = policy
        .initial_backoff
        .saturating_mul(1u32.checked_shl(exponent).unwrap_or(u32::MAX))
        .min(policy.maximum_backoff);
    let max_ms = u64::try_from(base.as_millis()).unwrap_or(u64::MAX).max(1);
    let seed = subscription_id.0.as_u128() as u64 ^ u64::from(attempt);
    Duration::from_millis(1 + seed % max_ms)
}

async fn connect_event_stream(
    address: SocketAddr,
    token: Secret,
    subscription_id: SubscriptionId,
    filter: &EventFilter,
    maximum: usize,
    timeout: Duration,
) -> Result<TcpStream, ClientError> {
    tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(|_| ClientError::ServerUnavailable { request_id: None })?;
        write_client_frame(
            &mut stream,
            &StreamClientFrame::Hello {
                version: PROTOCOL_VERSION,
                token: token.expose().to_owned(),
            },
            maximum,
        )
        .await?;
        write_client_frame(
            &mut stream,
            &StreamClientFrame::Subscribe {
                version: PROTOCOL_VERSION,
                subscription_id,
                filter: filter.clone(),
            },
            maximum,
        )
        .await?;
        let frame = read_frame(&mut stream, maximum).await?;
        match serde_json::from_slice::<StreamServerFrame>(&frame)
            .map_err(|error| ClientError::Protocol(error.to_string()))?
        {
            StreamServerFrame::Ready {
                version,
                subscription_id: ready,
            } if version == PROTOCOL_VERSION && ready == subscription_id => Ok(stream),
            StreamServerFrame::Error { error, .. } => Err(ClientError::from_api(error)),
            StreamServerFrame::Ready { version, .. } | StreamServerFrame::Event { version, .. }
                if version != PROTOCOL_VERSION =>
            {
                Err(ClientError::VersionMismatch {
                    server_version: version,
                })
            }
            _ => Err(ClientError::Protocol("invalid stream handshake".into())),
        }
    })
    .await
    .map_err(|_| ClientError::Timeout)?
}

async fn send_ack(
    stream: &mut TcpStream,
    delivery: &EventDelivery,
    timeout: Duration,
    maximum: usize,
) -> Result<(), ClientError> {
    send_ack_value(
        stream,
        EventAck {
            delivery_id: delivery.delivery_id,
            subscription_id: delivery.subscription_id,
            event_id: delivery.event.event_id,
        },
        timeout,
        maximum,
    )
    .await
}

async fn send_ack_value(
    stream: &mut TcpStream,
    ack: EventAck,
    timeout: Duration,
    maximum: usize,
) -> Result<(), ClientError> {
    tokio::time::timeout(
        timeout,
        write_client_frame(
            stream,
            &StreamClientFrame::Ack {
                version: PROTOCOL_VERSION,
                ack,
            },
            maximum,
        ),
    )
    .await
    .map_err(|_| ClientError::Timeout)?
}

async fn write_client_frame(
    stream: &mut TcpStream,
    frame: &StreamClientFrame,
    maximum: usize,
) -> Result<(), ClientError> {
    let payload =
        serde_json::to_vec(frame).map_err(|error| ClientError::Protocol(error.to_string()))?;
    if payload.is_empty() || payload.len() > maximum {
        return Err(ClientError::Protocol(
            "outbound frame exceeds configured limit".into(),
        ));
    }
    let length = u32::try_from(payload.len())
        .map_err(|_| ClientError::Protocol("outbound frame length overflow".into()))?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|_| ClientError::ConnectionLost)?;
    stream
        .write_all(&payload)
        .await
        .map_err(|_| ClientError::ConnectionLost)
}

async fn read_frame(stream: &mut TcpStream, maximum: usize) -> Result<Vec<u8>, ClientError> {
    let mut length = [0u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .map_err(|_| ClientError::ConnectionLost)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| ClientError::Protocol("frame length overflow".into()))?;
    if length == 0 || length > maximum {
        return Err(ClientError::Protocol("invalid or oversized frame".into()));
    }
    let mut payload = vec![0; length];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|_| ClientError::ConnectionLost)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn test_stream_server(
        listener: TcpListener,
        event_sent: oneshot::Sender<()>,
        no_ack_observed: oneshot::Sender<()>,
    ) {
        let (mut socket, _) = listener.accept().await.unwrap();
        let hello: StreamClientFrame = serde_json::from_slice(
            &read_frame(&mut socket, DEFAULT_MAX_FRAME_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            hello,
            StreamClientFrame::Hello {
                version: PROTOCOL_VERSION,
                ..
            }
        ));
        let subscribe: StreamClientFrame = serde_json::from_slice(
            &read_frame(&mut socket, DEFAULT_MAX_FRAME_BYTES)
                .await
                .unwrap(),
        )
        .unwrap();
        let subscription_id = match subscribe {
            StreamClientFrame::Subscribe {
                version: PROTOCOL_VERSION,
                subscription_id,
                ..
            } => subscription_id,
            _ => panic!("expected v1 subscribe"),
        };
        write_test_server_frame(
            &mut socket,
            &StreamServerFrame::Ready {
                version: PROTOCOL_VERSION,
                subscription_id,
            },
        )
        .await;
        write_test_server_frame(
            &mut socket,
            &StreamServerFrame::Event {
                version: PROTOCOL_VERSION,
                delivery: EventDelivery {
                    delivery_id: DeliveryId::generate(),
                    subscription_id,
                    event: DeviceEvent {
                        event_id: EventId::generate(),
                        source_message_id: SourceMessageId::new("slow-consumer").unwrap(),
                        device: DeviceKey {
                            tenant_id: TenantId::new("tenant").unwrap(),
                            product_id: ProductId::new("product").unwrap(),
                            device_id: DeviceId::new("device").unwrap(),
                        },
                        received_at: 1,
                        occurred_at: None,
                        kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
                    },
                    attempt: 1,
                },
            },
        )
        .await;
        event_sent.send(()).unwrap();

        assert!(
            tokio::time::timeout(
                Duration::from_millis(100),
                read_frame(&mut socket, DEFAULT_MAX_FRAME_BYTES),
            )
            .await
            .is_err(),
            "an unread manual delivery must not be acknowledged"
        );
        no_ack_observed.send(()).unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.read_u8()).await;
        assert!(matches!(closed, Ok(Err(_))), "stream task did not close");
    }

    async fn write_test_server_frame(stream: &mut TcpStream, frame: &StreamServerFrame) {
        let payload = serde_json::to_vec(frame).unwrap();
        let length = u32::try_from(payload.len()).unwrap();
        stream.write_all(&length.to_be_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
    }

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

    #[test]
    fn reconnect_backoff_is_bounded() {
        let policy = ReconnectPolicy::default();
        let id = SubscriptionId::generate();
        for attempt in 0..100 {
            assert!(reconnect_delay(&policy, attempt, id) <= policy.maximum_backoff);
        }
    }

    #[tokio::test]
    async fn slow_consumer_is_not_acked_and_stream_drop_cancels_socket_task() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (event_sent, event_received) = oneshot::channel();
        let (no_ack_observed, no_ack_confirmed) = oneshot::channel();
        let server = tokio::spawn(test_stream_server(listener, event_sent, no_ack_observed));
        let client = NetbaIoTClient::builder()
            .endpoint("http://127.0.0.1:1")
            .token("management")
            .event_token("events")
            .event_address(address)
            .event_buffer_limits(1, 1_024)
            .connect()
            .await
            .unwrap();
        let stream = client
            .events()
            .subscribe(EventFilter::default())
            .await
            .unwrap();
        event_received.await.unwrap();
        no_ack_confirmed.await.unwrap();
        assert_eq!(client.metrics().events_acked, 0);
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server did not observe subscription cancellation")
            .unwrap();
    }
}
