//! Business RPC V2 client. The connection driver runs independently of event consumption.
use async_trait::async_trait;
use netbaiot_protocol::{
    AuthInvalidation, EventAck, EventDelivery, EventFilter, SubscriptionId,
    business_rpc::{
        AuthInvalidateRequest, AuthInvalidateResponse, AuthSyncRequest, AuthenticatedDeviceWire,
        BUSINESS_RPC_EVENT_WINDOW, BUSINESS_RPC_HELLO_MAX_BYTES, BUSINESS_RPC_VERSION,
        BusinessLimits, BusinessRole, BusinessRpcFrame, DeviceAuthenticateRequest,
        ResolveVerifierRequest, ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::{TlsConnector, rustls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

#[derive(Clone)]
pub struct BusinessRpcTls {
    pub server_name: String,
    pub ca_pem: PathBuf,
    pub certificate_pem: PathBuf,
    pub private_key_pem: PathBuf,
}
#[derive(Clone)]
pub struct BusinessRpcClientConfig {
    pub address: SocketAddr,
    pub role: BusinessRole,
    pub token: Option<String>,
    pub tls: Option<BusinessRpcTls>,
    pub authority_incarnation: Uuid,
    pub auth_revision: u64,
    pub filter: EventFilter,
    pub max_frame_bytes: usize,
    pub auth_max_inflight: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub heartbeat: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
}
impl BusinessRpcClientConfig {
    pub fn development(address: SocketAddr, token: String, role: BusinessRole) -> Self {
        Self {
            address,
            role,
            token: Some(token),
            tls: None,
            authority_incarnation: Uuid::new_v4(),
            auth_revision: 1,
            filter: EventFilter::default(),
            max_frame_bytes: 8 * 1024 * 1024,
            auth_max_inflight: 128,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            heartbeat: Duration::from_secs(5),
            reconnect_initial: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
        }
    }
    fn validate(&self) -> Result<(), BusinessRpcClientError> {
        if self.max_frame_bytes < 16 * 1024
            || self.max_frame_bytes > 8 * 1024 * 1024
            || self.auth_max_inflight == 0
            || self.auth_max_inflight > u16::MAX as usize
            || self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.heartbeat.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_initial > self.reconnect_max
            || self.auth_revision == 0
            || self.filter.validate().is_err()
        {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        if self.tls.is_none()
            && (!self.address.ip().is_loopback()
                || self
                    .token
                    .as_ref()
                    .is_none_or(|token| token.is_empty() || token.len() > 256))
        {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        if self.tls.is_some() && self.token.is_some() {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Error)]
pub enum BusinessRpcClientError {
    #[error("invalid business RPC client configuration")]
    InvalidConfig,
    #[error("business RPC connection unavailable")]
    Unavailable,
    #[error("business RPC operation timed out")]
    Timeout,
    #[error("business RPC protocol error")]
    Protocol,
    #[error("business RPC authorization failed")]
    Unauthorized,
    #[error("business RPC overloaded")]
    Overloaded,
    #[error("business RPC request rejected: {0:?}")]
    Remote(RpcErrorCode),
}

#[async_trait]
pub trait BusinessAuthHandler: Send + Sync + 'static {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> std::result::Result<AuthenticatedDeviceWire, RpcError>;
    async fn resolve_verifier(
        &self,
        request: ResolveVerifierRequest,
    ) -> std::result::Result<ResolveVerifierResponse, RpcError>;
}

pub struct BusinessDelivery {
    pub delivery: EventDelivery,
    epoch: u64,
    writer: mpsc::Sender<Outbound>,
    budget: Arc<Semaphore>,
}
impl BusinessDelivery {
    pub fn connection_epoch(&self) -> u64 {
        self.epoch
    }
    /// ACK only after application processing. Dropping an unacknowledged delivery never confirms it.
    pub async fn ack(self) -> Result<(), BusinessRpcClientError> {
        self.respond(true).await
    }
    pub async fn nack(self) -> Result<(), BusinessRpcClientError> {
        self.respond(false).await
    }
    async fn respond(self, success: bool) -> Result<(), BusinessRpcClientError> {
        let ack = EventAck {
            delivery_id: self.delivery.delivery_id,
            subscription_id: self.delivery.subscription_id,
            event_id: self.delivery.event.event_id,
        };
        let frame = if success {
            BusinessRpcFrame::EventAck { ack }
        } else {
            BusinessRpcFrame::EventNack {
                ack,
                error: RpcError::new(RpcErrorCode::Unavailable, "application did not commit"),
            }
        };
        let (done, receive) = oneshot::channel();
        enqueue(&self.writer, &self.budget, frame, Some(done))?;
        receive
            .await
            .map_err(|_| BusinessRpcClientError::Unavailable)?
    }
}
struct Outbound {
    frame: BusinessRpcFrame,
    written: Option<oneshot::Sender<Result<(), BusinessRpcClientError>>>,
    _bytes: OwnedSemaphorePermit,
}
fn enqueue(
    tx: &mpsc::Sender<Outbound>,
    budget: &Arc<Semaphore>,
    frame: BusinessRpcFrame,
    written: Option<oneshot::Sender<Result<(), BusinessRpcClientError>>>,
) -> Result<(), BusinessRpcClientError> {
    frame
        .validate()
        .map_err(|_| BusinessRpcClientError::Protocol)?;
    let size = serde_json::to_vec(&frame)
        .map_err(|_| BusinessRpcClientError::Protocol)?
        .len();
    let permits = u32::try_from(size).map_err(|_| BusinessRpcClientError::Overloaded)?;
    let bytes = budget
        .clone()
        .try_acquire_many_owned(permits)
        .map_err(|_| BusinessRpcClientError::Overloaded)?;
    tx.try_send(Outbound {
        frame,
        written,
        _bytes: bytes,
    })
    .map_err(|_| BusinessRpcClientError::Overloaded)
}
enum Command {
    Invalidate(
        AuthInvalidateRequest,
        tokio::time::Instant,
        oneshot::Sender<Result<AuthInvalidateResponse, BusinessRpcClientError>>,
    ),
}
struct ClientInner {
    commands: mpsc::Sender<Command>,
    revision: Arc<AtomicU64>,
    incarnation: Uuid,
    request_timeout: Duration,
    shutdown: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
    ready: watch::Receiver<bool>,
    connection_timing: Arc<Mutex<Option<BusinessRpcConnectionTiming>>>,
    last_connection_error: Arc<Mutex<Option<BusinessRpcClientError>>>,
    last_protocol_context: Arc<Mutex<Option<&'static str>>>,
}
/// Client-observed timing for the most recent successful connection. TLS is
/// measured inside the existing verified rustls path, before the V2 handshake.
#[derive(Clone, Copy, Debug)]
pub struct BusinessRpcConnectionTiming {
    pub tcp_connect: Duration,
    pub tls_handshake: Option<Duration>,
    pub sync_to_ready: Duration,
    pub full_ready: Duration,
}
struct DriverSignals {
    ready: watch::Sender<bool>,
    timing: Arc<Mutex<Option<BusinessRpcConnectionTiming>>>,
    last_connection_error: Arc<Mutex<Option<BusinessRpcClientError>>>,
    last_protocol_context: Arc<Mutex<Option<&'static str>>>,
}
impl Drop for ClientInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Ok(mut task) = self.task.lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
    }
}
#[derive(Clone)]
pub struct BusinessRpcClient {
    inner: Arc<ClientInner>,
}
impl BusinessRpcClient {
    /// Returns the most recent connection failure observed by the reconnect driver.
    pub fn last_connection_error(&self) -> Option<BusinessRpcClientError> {
        self.inner
            .last_connection_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
    }
    /// Returns the handshake or frame-processing stage for the latest protocol error.
    pub fn last_protocol_context(&self) -> Option<&'static str> {
        self.inner
            .last_protocol_context
            .lock()
            .ok()
            .and_then(|context| *context)
    }
    pub fn connection_timing(&self) -> Option<BusinessRpcConnectionTiming> {
        self.inner
            .connection_timing
            .lock()
            .ok()
            .and_then(|timing| *timing)
    }
    pub fn ready(&self) -> bool {
        *self.inner.ready.borrow()
    }
    pub async fn wait_ready(&self) -> Result<(), BusinessRpcClientError> {
        let mut ready = self.inner.ready.clone();
        loop {
            if *ready.borrow() {
                return Ok(());
            }
            ready
                .changed()
                .await
                .map_err(|_| BusinessRpcClientError::Unavailable)?;
        }
    }
    pub async fn invalidate(
        &self,
        auth_revision: u64,
        invalidation: AuthInvalidation,
    ) -> Result<AuthInvalidateResponse, BusinessRpcClientError> {
        if auth_revision == 0 {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        self.inner
            .revision
            .fetch_max(auth_revision, Ordering::SeqCst);
        let request = AuthInvalidateRequest {
            authority_incarnation: self.inner.incarnation,
            auth_revision,
            invalidation,
        };
        let (send, receive) = oneshot::channel();
        self.inner
            .commands
            .try_send(Command::Invalidate(
                request,
                tokio::time::Instant::now() + self.inner.request_timeout,
                send,
            ))
            .map_err(|_| BusinessRpcClientError::Overloaded)?;
        tokio::time::timeout(self.inner.request_timeout, receive)
            .await
            .map_err(|_| BusinessRpcClientError::Timeout)?
            .map_err(|_| BusinessRpcClientError::Unavailable)?
    }
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let task = self.inner.task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
            let _ = task.await;
        }
    }
    pub fn connect(
        config: BusinessRpcClientConfig,
        handler: Option<Arc<dyn BusinessAuthHandler>>,
    ) -> Result<(Self, mpsc::Receiver<BusinessDelivery>), BusinessRpcClientError> {
        config.validate()?;
        if config.role.auth_control() && handler.is_none() {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        let (commands, command_rx) = mpsc::channel(32);
        let (deliveries, delivery_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = watch::channel(false);
        let shutdown = CancellationToken::new();
        let revision = Arc::new(AtomicU64::new(config.auth_revision));
        let connection_timing = Arc::new(Mutex::new(None));
        let last_connection_error = Arc::new(Mutex::new(None));
        let last_protocol_context = Arc::new(Mutex::new(None));
        let task = tokio::spawn(driver(
            config.clone(),
            handler,
            command_rx,
            deliveries,
            revision.clone(),
            DriverSignals {
                ready: ready_tx,
                timing: connection_timing.clone(),
                last_connection_error: last_connection_error.clone(),
                last_protocol_context: last_protocol_context.clone(),
            },
            shutdown.clone(),
        ));
        let client = Self {
            inner: Arc::new(ClientInner {
                commands,
                revision,
                incarnation: config.authority_incarnation,
                request_timeout: config.request_timeout,
                shutdown,
                task: Mutex::new(Some(task)),
                ready: ready_rx,
                connection_timing,
                last_connection_error,
                last_protocol_context,
            }),
        };
        Ok((client, delivery_rx))
    }
}

async fn connect_io(
    config: &BusinessRpcClientConfig,
) -> Result<(Box<dyn Io>, Duration, Option<Duration>), BusinessRpcClientError> {
    let tcp_started = Instant::now();
    let socket = tokio::time::timeout(config.connect_timeout, TcpStream::connect(config.address))
        .await
        .map_err(|_| BusinessRpcClientError::Timeout)?
        .map_err(|_| BusinessRpcClientError::Unavailable)?;
    let tcp_connect = tcp_started.elapsed();
    if let Some(tls) = &config.tls {
        let ca = std::fs::read(&tls.ca_pem).map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let cert = std::fs::read(&tls.certificate_pem)
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let key = std::fs::read(&tls.private_key_pem)
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        if ca.len() > 1_048_576 || cert.len() > 1_048_576 || key.len() > 65_536 {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        let mut roots = rustls::RootCertStore::empty();
        for item in rustls_pemfile::certs(&mut ca.as_slice()) {
            roots
                .add(item.map_err(|_| BusinessRpcClientError::InvalidConfig)?)
                .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        }
        if roots.is_empty() {
            return Err(BusinessRpcClientError::InvalidConfig);
        }
        let certs = rustls_pemfile::certs(&mut cert.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let key = rustls_pemfile::private_key(&mut key.as_slice())
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?
            .ok_or(BusinessRpcClientError::InvalidConfig)?;
        let rustls_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| BusinessRpcClientError::InvalidConfig)?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let name = rustls::pki_types::ServerName::try_from(tls.server_name.clone())
            .map_err(|_| BusinessRpcClientError::InvalidConfig)?;
        let tls_started = Instant::now();
        let stream = tokio::time::timeout(
            config.connect_timeout,
            TlsConnector::from(Arc::new(rustls_config)).connect(name, socket),
        )
        .await
        .map_err(|_| BusinessRpcClientError::Timeout)?
        .map_err(|_| BusinessRpcClientError::Unauthorized)?;
        Ok((Box::new(stream), tcp_connect, Some(tls_started.elapsed())))
    } else {
        Ok((Box::new(socket), tcp_connect, None))
    }
}
async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &BusinessRpcFrame,
    max: usize,
    timeout: Duration,
) -> Result<(), BusinessRpcClientError> {
    let bytes = serde_json::to_vec(frame).map_err(|_| BusinessRpcClientError::Protocol)?;
    let length = u32::try_from(bytes.len()).map_err(|_| BusinessRpcClientError::Overloaded)?;
    if bytes.is_empty() || bytes.len() > max {
        return Err(BusinessRpcClientError::Overloaded);
    }
    tokio::time::timeout(timeout, async {
        writer
            .write_all(&length.to_be_bytes())
            .await
            .map_err(|_| BusinessRpcClientError::Unavailable)?;
        writer
            .write_all(&bytes)
            .await
            .map_err(|_| BusinessRpcClientError::Unavailable)
    })
    .await
    .map_err(|_| BusinessRpcClientError::Timeout)?
}
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
    timeout: Duration,
) -> Result<BusinessRpcFrame, BusinessRpcClientError> {
    let bytes = tokio::time::timeout(timeout, async {
        let mut header = [0u8; 4];
        reader
            .read_exact(&mut header)
            .await
            .map_err(|_| BusinessRpcClientError::Unavailable)?;
        let length = usize::try_from(u32::from_be_bytes(header))
            .map_err(|_| BusinessRpcClientError::Protocol)?;
        if length == 0 || length > max {
            return Err(BusinessRpcClientError::Protocol);
        }
        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| BusinessRpcClientError::Unavailable)?;
        Ok(payload)
    })
    .await
    .map_err(|_| BusinessRpcClientError::Timeout)??;
    let frame: BusinessRpcFrame =
        serde_json::from_slice(&bytes).map_err(|_| BusinessRpcClientError::Protocol)?;
    frame
        .validate()
        .map_err(|_| BusinessRpcClientError::Protocol)?;
    Ok(frame)
}
async fn handshake(
    io: &mut Box<dyn Io>,
    config: &BusinessRpcClientConfig,
    revision: u64,
    signals: &DriverSignals,
) -> Result<(u64, SubscriptionId), BusinessRpcClientError> {
    let limits = BusinessLimits {
        max_frame_bytes: config.max_frame_bytes as u32,
        auth_max_inflight: config.auth_max_inflight as u16,
        event_max_inflight: BUSINESS_RPC_EVENT_WINDOW,
        heartbeat_ms: config.heartbeat.as_millis().min(u32::MAX as u128) as u32,
    };
    let hello = BusinessRpcFrame::Hello {
        version: BUSINESS_RPC_VERSION,
        role: config.role,
        token: config.token.clone(),
        limits,
    };
    write_frame(
        io,
        &hello,
        BUSINESS_RPC_HELLO_MAX_BYTES,
        config.connect_timeout,
    )
    .await?;
    let ready = read_frame(io, BUSINESS_RPC_HELLO_MAX_BYTES, config.connect_timeout)
        .await
        .map_err(|error| protocol_context(error, signals, "ready_frame"))?;
    let BusinessRpcFrame::Ready {
        version,
        role,
        connection_epoch,
        limits,
    } = ready
    else {
        return Err(BusinessRpcClientError::Unauthorized);
    };
    if version != BUSINESS_RPC_VERSION
        || role != config.role
        || limits.event_max_inflight != 1
        || limits.max_frame_bytes == 0
    {
        return Err(protocol_at(signals, "ready_contract"));
    }
    if config.role.auth_control() {
        let id = Uuid::new_v4();
        let body = serde_json::to_value(AuthSyncRequest {
            reset: true,
            authority_incarnation: config.authority_incarnation,
            auth_revision: revision,
        })
        .map_err(|_| BusinessRpcClientError::Protocol)?;
        write_frame(
            io,
            &BusinessRpcFrame::Request {
                request_id: id,
                method: "auth.sync".into(),
                deadline_ms: config.request_timeout.as_millis().min(u32::MAX as u128) as u32,
                body,
            },
            config.max_frame_bytes,
            config.request_timeout,
        )
        .await?;
        match read_frame(io, config.max_frame_bytes, config.request_timeout)
            .await
            .map_err(|error| protocol_context(error, signals, "auth_sync_response"))?
        {
            BusinessRpcFrame::Response {
                request_id,
                method,
                error: None,
                ..
            } if request_id == id && method == "auth.sync" => {}
            BusinessRpcFrame::Response {
                error: Some(error), ..
            } => return Err(BusinessRpcClientError::Remote(error.code)),
            _ => return Err(protocol_at(signals, "auth_sync_response")),
        }
        let nonce = Uuid::new_v4().as_u128() as u64;
        write_frame(
            io,
            &BusinessRpcFrame::Ping { nonce },
            config.max_frame_bytes,
            config.request_timeout,
        )
        .await?;
        match read_frame(io, config.max_frame_bytes, config.request_timeout)
            .await
            .map_err(|error| protocol_context(error, signals, "auth_sync_pong"))?
        {
            BusinessRpcFrame::Pong { nonce: received } if received == nonce => {}
            _ => return Err(protocol_at(signals, "auth_sync_pong")),
        }
    }
    let subscription = SubscriptionId::generate();
    if config.role.events() {
        write_frame(
            io,
            &BusinessRpcFrame::Subscribe {
                subscription_id: subscription,
                filter: config.filter.clone(),
            },
            config.max_frame_bytes,
            config.request_timeout,
        )
        .await?;
        let acknowledgement = read_frame(io, config.max_frame_bytes, config.request_timeout)
            .await
            .map_err(|error| protocol_context(error, signals, "event_subscription_ack"))?;
        match acknowledgement {
            BusinessRpcFrame::Subscribed {
                subscription_id: received,
            } if received == subscription => {}
            BusinessRpcFrame::Subscribed { .. } => {
                return Err(protocol_at(signals, "event_subscription_wrong_id"));
            }
            BusinessRpcFrame::Event { .. } => {
                return Err(protocol_at(signals, "event_before_subscription_ack"));
            }
            BusinessRpcFrame::Response { .. } => {
                return Err(protocol_at(signals, "response_before_subscription_ack"));
            }
            BusinessRpcFrame::Pong { .. } => {
                return Err(protocol_at(signals, "pong_before_subscription_ack"));
            }
            _ => return Err(protocol_at(signals, "unexpected_subscription_frame")),
        }
    }
    Ok((connection_epoch, subscription))
}
fn protocol_at(signals: &DriverSignals, context: &'static str) -> BusinessRpcClientError {
    if let Ok(mut last_context) = signals.last_protocol_context.lock() {
        *last_context = Some(context);
    }
    BusinessRpcClientError::Protocol
}
fn protocol_context(
    error: BusinessRpcClientError,
    signals: &DriverSignals,
    context: &'static str,
) -> BusinessRpcClientError {
    if matches!(error, BusinessRpcClientError::Protocol) {
        protocol_at(signals, context)
    } else {
        error
    }
}
async fn driver(
    config: BusinessRpcClientConfig,
    handler: Option<Arc<dyn BusinessAuthHandler>>,
    mut commands: mpsc::Receiver<Command>,
    deliveries: mpsc::Sender<BusinessDelivery>,
    revision: Arc<AtomicU64>,
    signals: DriverSignals,
    stop: CancellationToken,
) {
    let mut attempt = 0u32;
    loop {
        if stop.is_cancelled() {
            break;
        }
        if let Ok(mut context) = signals.last_protocol_context.lock() {
            *context = None;
        }
        let result = connected(
            &config,
            handler.clone(),
            &mut commands,
            &deliveries,
            &revision,
            &signals,
            &stop,
        )
        .await;
        if let Ok(mut last_error) = signals.last_connection_error.lock() {
            *last_error = result.as_ref().err().cloned();
        }
        let _ = signals.ready.send(false);
        if stop.is_cancelled() {
            break;
        }
        if matches!(
            result,
            Err(BusinessRpcClientError::InvalidConfig
                | BusinessRpcClientError::Unauthorized
                | BusinessRpcClientError::Protocol
                | BusinessRpcClientError::Remote(
                    RpcErrorCode::Forbidden
                        | RpcErrorCode::Unauthenticated
                        | RpcErrorCode::InvalidRequest
                ))
        ) {
            break;
        }
        attempt = attempt.saturating_add(1);
        let cap = config
            .reconnect_initial
            .saturating_mul(1u32.checked_shl(attempt.min(20)).unwrap_or(u32::MAX))
            .min(config.reconnect_max);
        let millis = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX).max(1);
        let jitter = 1 + (Uuid::new_v4().as_u128() as u64 % millis);
        tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(jitter)) => {} }
    }
}
struct Pending {
    method: &'static str,
    deadline: tokio::time::Instant,
    result: oneshot::Sender<Result<AuthInvalidateResponse, BusinessRpcClientError>>,
}
async fn connected(
    config: &BusinessRpcClientConfig,
    handler: Option<Arc<dyn BusinessAuthHandler>>,
    commands: &mut mpsc::Receiver<Command>,
    deliveries: &mpsc::Sender<BusinessDelivery>,
    revision: &AtomicU64,
    signals: &DriverSignals,
    stop: &CancellationToken,
) -> Result<(), BusinessRpcClientError> {
    let started = Instant::now();
    let (mut io, tcp_connect, tls_handshake) = connect_io(config).await?;
    let sync_started = Instant::now();
    let (epoch, subscription) =
        handshake(&mut io, config, revision.load(Ordering::SeqCst), signals).await?;
    let (mut reader, mut writer) = tokio::io::split(io);
    let (read_tx, mut read_rx) = mpsc::channel::<(BusinessRpcFrame, OwnedSemaphorePermit)>(64);
    let read_budget = Arc::new(Semaphore::new(config.max_frame_bytes + 64 * 16 * 1024));
    let (out_tx, mut out_rx) = mpsc::channel::<Outbound>(64);
    let out_budget = Arc::new(Semaphore::new(64 * 16 * 1024));
    let read_max = config.max_frame_bytes;
    let read_timeout = config.heartbeat.saturating_mul(3);
    let reader_task = tokio::spawn(async move {
        while let Ok(frame) = read_frame(&mut reader, read_max, read_timeout).await {
            let size = match serde_json::to_vec(&frame)
                .ok()
                .and_then(|bytes| u32::try_from(bytes.len()).ok())
            {
                Some(size) => size,
                None => break,
            };
            let Ok(bytes) = read_budget.clone().try_acquire_many_owned(size) else {
                break;
            };
            if read_tx.try_send((frame, bytes)).is_err() {
                break;
            }
        }
    });
    let max = config.max_frame_bytes;
    let timeout = config.request_timeout;
    let writer_task = tokio::spawn(async move {
        while let Some(outbound) = out_rx.recv().await {
            let result = write_frame(&mut writer, &outbound.frame, max, timeout).await;
            if let Some(written) = outbound.written {
                let _ = written.send(result.clone());
            }
            if result.is_err() {
                break;
            }
        }
    });
    if let Ok(mut timing) = signals.timing.lock() {
        *timing = Some(BusinessRpcConnectionTiming {
            tcp_connect,
            tls_handshake,
            sync_to_ready: sync_started.elapsed(),
            full_ready: started.elapsed(),
        });
    }
    let _ = signals.ready.send(true);
    let mut pending = HashMap::<Uuid, Pending>::new();
    let slots = Arc::new(Semaphore::new(config.auth_max_inflight));
    let mut handlers = JoinSet::new();
    let mut handler_cancellations = HashMap::<Uuid, CancellationToken>::new();
    let mut heartbeat = tokio::time::interval(config.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut expiry = tokio::time::interval(Duration::from_millis(100));
    expiry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            _ = stop.cancelled() => break Ok(()),
            frame = read_rx.recv() => {
                let Some((frame, _bytes)) = frame else { break Err(BusinessRpcClientError::Unavailable) };
                match frame {
                    BusinessRpcFrame::Request { request_id, method, body, deadline_ms } if config.role.auth_control() => {
                        let Some(handler) = handler.clone() else { break Err(BusinessRpcClientError::InvalidConfig) };
                        let permit = match slots.clone().try_acquire_owned() {
                            Ok(permit) => permit,
                            Err(_) => { if enqueue(&out_tx, &out_budget, rpc_error(request_id, &method, RpcErrorCode::Overloaded), None).is_err() { break Err(BusinessRpcClientError::Overloaded); } continue; }
                        };
                        let outbound = out_tx.clone();
                        let budget = out_budget.clone();
                        let handler_timeout = Duration::from_millis(u64::from(deadline_ms)).min(config.request_timeout);
                        let cancel = CancellationToken::new();
                        handler_cancellations.insert(request_id, cancel.clone());
                        handlers.spawn(async move {
                            let _permit = permit;
                            let answer = tokio::select! {
                                _ = cancel.cancelled() => Err(RpcError::new(RpcErrorCode::Unavailable, "cancelled")),
                                result = tokio::time::timeout(handler_timeout, async { match method.as_str() {
                                "device.authenticate" => match serde_json::from_value::<DeviceAuthenticateRequest>(body) {
                                    Ok(request) if request.credential_id.len() <= 64 && request.secret_hex.len() <= 512 => handler.authenticate(request).await.and_then(|reply| serde_json::to_value(reply).map_err(|_| RpcError::new(RpcErrorCode::Internal, "encode failed"))),
                                    _ => Err(RpcError::new(RpcErrorCode::InvalidRequest, "invalid authentication request")),
                                },
                                "device.resolve_verifier" => match serde_json::from_value::<ResolveVerifierRequest>(body) {
                                    Ok(request) if request.credential_id.len() <= 64 => handler.resolve_verifier(request).await.and_then(|reply| serde_json::to_value(reply).map_err(|_| RpcError::new(RpcErrorCode::Internal, "encode failed"))),
                                    _ => Err(RpcError::new(RpcErrorCode::InvalidRequest, "invalid verifier request")),
                                },
                                _ => Err(RpcError::new(RpcErrorCode::UnknownMethod, "unknown method")),
                            }}) => result.unwrap_or_else(|_| Err(RpcError::new(RpcErrorCode::Timeout, "handler deadline exceeded"))),
                            };
                            let frame = match answer { Ok(body) => BusinessRpcFrame::Response { request_id, method, body: Some(body), error: None }, Err(error) => BusinessRpcFrame::Response { request_id, method, body: None, error: Some(error) } };
                            enqueue(&outbound, &budget, frame, None).map(|_| request_id)
                        });
                    }
                    BusinessRpcFrame::Response { request_id, method, body, error } => {
                        let stale = error.as_ref().is_some_and(|failure| failure.code == RpcErrorCode::StaleRevision);
                        if let Some(pending) = pending.remove(&request_id) {
                            let result = if pending.method != method { Err(protocol_at(signals, "rpc_response_method")) }
                                else if let Some(error) = error { Err(BusinessRpcClientError::Remote(error.code)) }
                                else { body.and_then(|body| serde_json::from_value(body).ok()).ok_or_else(|| protocol_at(signals, "rpc_response_body")) };
                            let _ = pending.result.send(result);
                        }
                        if stale { break Err(BusinessRpcClientError::Remote(RpcErrorCode::StaleRevision)); }
                    }
                    BusinessRpcFrame::Event { delivery } if config.role.events() && delivery.subscription_id == subscription => {
                        if deliveries.try_send(BusinessDelivery { delivery, epoch, writer: out_tx.clone(), budget: out_budget.clone() }).is_err() { break Err(BusinessRpcClientError::Overloaded); }
                    }
                    BusinessRpcFrame::Ping { nonce } => { if enqueue(&out_tx, &out_budget, BusinessRpcFrame::Pong { nonce }, None).is_err() { break Err(BusinessRpcClientError::Overloaded); } }
                    BusinessRpcFrame::Pong { .. } => {},
                    BusinessRpcFrame::Cancel { request_id } => { if let Some(cancel) = handler_cancellations.get(&request_id) { cancel.cancel(); } },
                    BusinessRpcFrame::GoAway { error } => break Err(BusinessRpcClientError::Remote(error.code)),
                    _ => break Err(protocol_at(signals, "connected_frame")),
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break Ok(()) };
                let Command::Invalidate(request, deadline, result) = command;
                if result.is_closed() { continue; }
                if deadline <= tokio::time::Instant::now() { let _ = result.send(Err(BusinessRpcClientError::Timeout)); continue; }
                if !config.role.auth_control() { let _ = result.send(Err(BusinessRpcClientError::Unauthorized)); continue; }
                if pending.len() >= config.auth_max_inflight { let _ = result.send(Err(BusinessRpcClientError::Overloaded)); continue; }
                let id = Uuid::new_v4();
                let body = match serde_json::to_value(request) { Ok(value) => value, Err(_) => { let _ = result.send(Err(BusinessRpcClientError::Protocol)); continue; } };
                let frame = BusinessRpcFrame::Request { request_id: id, method: "auth.invalidate".into(), deadline_ms: deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis().min(u32::MAX as u128).max(1) as u32, body };
                if enqueue(&out_tx, &out_budget, frame, None).is_err() { let _ = result.send(Err(BusinessRpcClientError::Overloaded)); continue; }
                pending.insert(id, Pending { method: "auth.invalidate", deadline, result });
            }
            _ = heartbeat.tick() => {
                if enqueue(&out_tx, &out_budget, BusinessRpcFrame::Ping { nonce: Uuid::new_v4().as_u128() as u64 }, None).is_err() { break Err(BusinessRpcClientError::Overloaded); }

            }
            _ = expiry.tick() => {
                let now = tokio::time::Instant::now();
                let expired = pending.iter().filter_map(|(id, entry)| (entry.deadline <= now || entry.result.is_closed()).then_some(*id)).collect::<Vec<_>>();
                for id in expired { if let Some(entry) = pending.remove(&id) { let _ = entry.result.send(Err(BusinessRpcClientError::Timeout)); } }
            }
            result = handlers.join_next(), if !handlers.is_empty() => {
                match result {
                    Some(Ok(Ok(id))) => { handler_cancellations.remove(&id); },
                    _ => break Err(BusinessRpcClientError::Unavailable),
                }
            }
        }
    };
    for (_, pending) in pending {
        let _ = pending
            .result
            .send(Err(BusinessRpcClientError::Unavailable));
    }
    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
    drop(out_tx);
    reader_task.abort();
    writer_task.abort();
    let _ = reader_task.await;
    let _ = writer_task.await;
    result
}
fn rpc_error(id: Uuid, method: &str, code: RpcErrorCode) -> BusinessRpcFrame {
    BusinessRpcFrame::Response {
        request_id: id,
        method: method.into(),
        body: None,
        error: Some(RpcError::new(code, "request rejected")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_protocol::{
        DeliveryId, DeviceEvent, DeviceEventKind, DeviceId, DeviceKey, EventId, Heartbeat,
        ProductId, SourceMessageId, TenantId,
    };
    use tokio::{net::TcpListener, sync::Notify};

    struct HeldHandler {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        dropped: Mutex<Option<oneshot::Sender<()>>>,
        release: Notify,
    }
    struct HandlerDrop(Option<oneshot::Sender<()>>);
    impl Drop for HandlerDrop {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }
    #[async_trait]
    impl BusinessAuthHandler for HeldHandler {
        async fn authenticate(
            &self,
            _request: DeviceAuthenticateRequest,
        ) -> std::result::Result<AuthenticatedDeviceWire, RpcError> {
            let _drop = HandlerDrop(self.dropped.lock().unwrap().take());
            if let Some(signal) = self.entered.lock().unwrap().take() {
                let _ = signal.send(());
            }
            self.release.notified().await;
            Err(RpcError::new(RpcErrorCode::Unavailable, "released"))
        }
        async fn resolve_verifier(
            &self,
            _request: ResolveVerifierRequest,
        ) -> std::result::Result<ResolveVerifierResponse, RpcError> {
            Err(RpcError::new(RpcErrorCode::Unavailable, "unused"))
        }
    }

    async fn auth_server_handshake(socket: &mut TcpStream, generation: u64) {
        let BusinessRpcFrame::Hello { role, limits, .. } =
            read_frame(socket, BUSINESS_RPC_HELLO_MAX_BYTES, Duration::from_secs(2))
                .await
                .unwrap()
        else {
            panic!("hello expected")
        };
        write_frame(
            socket,
            &BusinessRpcFrame::Ready {
                version: BUSINESS_RPC_VERSION,
                role,
                connection_epoch: generation,
                limits,
            },
            BUSINESS_RPC_HELLO_MAX_BYTES,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let BusinessRpcFrame::Request {
            request_id, method, ..
        } = read_frame(socket, 16 * 1024, Duration::from_secs(2))
            .await
            .unwrap()
        else {
            panic!("sync request expected")
        };
        assert_eq!(method, "auth.sync");
        write_frame(
            socket,
            &BusinessRpcFrame::Response {
                request_id,
                method,
                body: Some(serde_json::json!({})),
                error: None,
            },
            16 * 1024,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let BusinessRpcFrame::Ping { nonce } =
            read_frame(socket, 16 * 1024, Duration::from_secs(2))
                .await
                .unwrap()
        else {
            panic!("sync confirmation expected")
        };
        write_frame(
            socket,
            &BusinessRpcFrame::Pong { nonce },
            16 * 1024,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn old_auth_handler_is_cancelled_before_replacement_writer_is_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let handler = Arc::new(HeldHandler {
            entered: Mutex::new(Some(entered_tx)),
            dropped: Mutex::new(Some(dropped_tx)),
            release: Notify::new(),
        });
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            auth_server_handshake(&mut first, 1).await;
            write_frame(
                &mut first,
                &BusinessRpcFrame::Request {
                    request_id: Uuid::new_v4(),
                    method: "device.authenticate".into(),
                    deadline_ms: 5_000,
                    body: serde_json::json!({
                        "credential_id": "cred-device",
                        "secret_hex": "00",
                        "min_auth_revision": 1
                    }),
                },
                16 * 1024,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            entered_rx.await.unwrap();
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            auth_server_handshake(&mut second, 2).await;
            let _ = second_tx.send(());
            let until = tokio::time::Instant::now() + Duration::from_millis(300);
            while let Ok(Ok(frame)) = tokio::time::timeout_at(
                until,
                read_frame(&mut second, 16 * 1024, Duration::from_secs(2)),
            )
            .await
            {
                match frame {
                    BusinessRpcFrame::Ping { nonce } => {
                        write_frame(
                            &mut second,
                            &BusinessRpcFrame::Pong { nonce },
                            16 * 1024,
                            Duration::from_secs(2),
                        )
                        .await
                        .unwrap();
                    }
                    BusinessRpcFrame::Response { .. } => panic!("old response crossed generation"),
                    _ => panic!("unexpected replacement frame"),
                }
            }
        });
        let mut config = BusinessRpcClientConfig::development(
            address,
            "test-token".into(),
            BusinessRole::AuthControl,
        );
        config.reconnect_initial = Duration::from_millis(1);
        config.reconnect_max = Duration::from_millis(1);
        let (client, _events) = BusinessRpcClient::connect(config, Some(handler.clone())).unwrap();
        tokio::time::timeout(Duration::from_secs(3), dropped_rx)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), second_rx)
            .await
            .unwrap()
            .unwrap();
        handler.release.notify_waiters();
        server.await.unwrap();
        client.shutdown().await;
    }

    #[tokio::test]
    async fn one_hundred_reconnect_generations_and_shutdown_release_driver() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for generation in 1..=100u64 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let BusinessRpcFrame::Hello { role, limits, .. } = read_frame(
                    &mut socket,
                    BUSINESS_RPC_HELLO_MAX_BYTES,
                    Duration::from_secs(2),
                )
                .await
                .unwrap() else {
                    panic!("hello expected")
                };
                write_frame(
                    &mut socket,
                    &BusinessRpcFrame::Ready {
                        version: BUSINESS_RPC_VERSION,
                        role,
                        connection_epoch: generation,
                        limits,
                    },
                    BUSINESS_RPC_HELLO_MAX_BYTES,
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
                let BusinessRpcFrame::Subscribe {
                    subscription_id, ..
                } = read_frame(&mut socket, 16 * 1024, Duration::from_secs(2))
                    .await
                    .unwrap()
                else {
                    panic!("subscribe expected")
                };
                write_frame(
                    &mut socket,
                    &BusinessRpcFrame::Subscribed { subscription_id },
                    16 * 1024,
                    Duration::from_secs(2),
                )
                .await
                .unwrap();
                // Closing the socket forces the SDK to discard its reader, writer,
                // pending requests and delivery generation before retrying.
            }
        });
        let mut config = BusinessRpcClientConfig::development(
            address,
            "test-token".into(),
            BusinessRole::Events,
        );
        config.reconnect_initial = Duration::from_millis(1);
        config.reconnect_max = Duration::from_millis(1);
        let (client, _events) = BusinessRpcClient::connect(config, None).unwrap();
        tokio::time::timeout(Duration::from_secs(20), server)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
        assert!(!client.ready());
        let weak = Arc::downgrade(&client.inner);
        drop(client);
        assert!(
            weak.upgrade().is_none(),
            "last handle must release ownership"
        );
    }

    #[tokio::test]
    async fn old_delivery_ack_cannot_use_replacement_writer() {
        let (old_writer, old_receiver) = mpsc::channel(1);
        drop(old_receiver);
        let (new_writer, mut new_receiver) = mpsc::channel::<Outbound>(1);
        let delivery = BusinessDelivery {
            delivery: EventDelivery {
                delivery_id: DeliveryId::generate(),
                subscription_id: SubscriptionId::generate(),
                event: DeviceEvent {
                    event_id: EventId::generate(),
                    source_message_id: SourceMessageId::new("old-delivery").unwrap(),
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
            epoch: 1,
            writer: old_writer,
            budget: Arc::new(Semaphore::new(1024)),
        };
        assert!(delivery.ack().await.is_err());
        assert!(new_receiver.try_recv().is_err());
        drop(new_writer);
    }
}
