//! Business RPC V2 transport: one reader, one writer, one event window per connection.
use crate::mqtt::broker::MqttBroker;
use netbaiot_core::{
    AuthInvalidation, EventAck, EventDelivery, SubscriptionId, TenantId,
    business_rpc::{
        AuthInvalidateRequest, AuthInvalidateResponse, AuthSyncRequest, AuthSyncResponse,
        BUSINESS_RPC_AUTH_MAX_BYTES, BUSINESS_RPC_EVENT_WINDOW, BUSINESS_RPC_HELLO_MAX_BYTES,
        BUSINESS_RPC_MAX_TOKEN_BYTES, BUSINESS_RPC_VERSION, BusinessLimits, BusinessRole,
        BusinessRpcFrame, RpcError, RpcErrorCode,
    },
};
use netbaiot_runtime::{
    BusinessEventRequest, BusinessProviderScope, BusinessRpcEventSink, BusinessRpcOutbound,
    BusinessRpcRegistry, Error, Ingress, ProviderLease, Result, SinkAck, SinkError,
    metrics::{BusinessRpcQueueClass, Histogram, Metric, Metrics},
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

struct ActiveBusinessConnection(Arc<Metrics>);
impl Drop for ActiveBusinessConnection {
    fn drop(&mut self) {
        self.0.business_rpc_connection_finished();
    }
}

#[derive(Clone)]
pub struct BusinessPrincipal {
    pub id: String,
    pub role: BusinessRole,
    pub provider_id: Option<String>,
    pub sink_id: Option<String>,
    pub provide_methods: Vec<String>,
    pub call_methods: Vec<String>,
    pub global: bool,
    pub tenants: Vec<TenantId>,
    pub expires_at_ms: Option<i64>,
}
impl BusinessPrincipal {
    fn allows_tenant(&self, tenant: &TenantId) -> bool {
        self.global || self.tenants.contains(tenant)
    }
    fn permits_invalidation(&self, invalidation: &AuthInvalidation) -> bool {
        match invalidation {
            AuthInvalidation::Device { device } => self.allows_tenant(&device.tenant_id),
            AuthInvalidation::Product { tenant_id, .. }
            | AuthInvalidation::Tenant { tenant_id } => self.allows_tenant(tenant_id),
            AuthInvalidation::CredentialVersion { .. }
            | AuthInvalidation::AuthGeneration { .. }
            | AuthInvalidation::All => self.global,
        }
    }
    fn permits_role(&self, role: BusinessRole) -> bool {
        (!role.auth_control()
            || (self.role.auth_control()
                && self.provider_id.as_deref() == Some("primary")
                && self
                    .provide_methods
                    .iter()
                    .any(|m| m == "device.authenticate")
                && self
                    .provide_methods
                    .iter()
                    .any(|m| m == "device.resolve_verifier")
                && self.call_methods.iter().any(|m| m == "auth.sync")
                && self.call_methods.iter().any(|m| m == "auth.invalidate")))
            && (!role.events()
                || (self.role.events() && self.sink_id.as_deref() == Some("tcp-rpc")))
    }
}

/// The caller must supply either a loopback development token or a verified mTLS identity map.
#[derive(Clone)]
pub enum BusinessIdentity {
    Development {
        token_hash: [u8; 32],
        principal: BusinessPrincipal,
    },
    Mtls {
        identities: Vec<([u8; 32], BusinessPrincipal)>,
    },
}

#[derive(Clone)]
pub struct BusinessRpcTransportConfig {
    pub identity: BusinessIdentity,
    pub tls: Option<TlsAcceptor>,
    pub max_connections: usize,
    pub max_frame_bytes: usize,
    pub auth_max_inflight: usize,
    pub heartbeat_ms: u32,
    pub handshake_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub event_ack_timeout: Duration,
}
impl BusinessRpcTransportConfig {
    pub fn validate(&self, address: SocketAddr) -> Result<()> {
        if self.max_connections == 0
            || self.max_connections > 1024
            || self.max_frame_bytes < BUSINESS_RPC_AUTH_MAX_BYTES
            || self.max_frame_bytes > 8 * 1024 * 1024
            || self.auth_max_inflight == 0
            || self.auth_max_inflight > u16::MAX as usize
            || self.heartbeat_ms == 0
            || self.handshake_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.event_ack_timeout.is_zero()
        {
            return Err(Error::Configuration);
        }
        match &self.identity {
            BusinessIdentity::Development { principal, .. }
                if address.ip().is_loopback() && self.tls.is_none() && principal.id.len() <= 64 => {
            }
            BusinessIdentity::Mtls { identities }
                if self.tls.is_some() && !identities.is_empty() && identities.len() <= 64 => {}
            _ => return Err(Error::Configuration),
        }
        Ok(())
    }
    fn negotiated(&self, requested: &BusinessLimits) -> BusinessLimits {
        BusinessLimits {
            max_frame_bytes: requested.max_frame_bytes.min(self.max_frame_bytes as u32),
            auth_max_inflight: requested
                .auth_max_inflight
                .min(self.auth_max_inflight as u16),
            event_max_inflight: BUSINESS_RPC_EVENT_WINDOW,
            heartbeat_ms: requested.heartbeat_ms.max(self.heartbeat_ms),
        }
    }
}

pub struct BusinessRpcServices {
    pub registry: Arc<BusinessRpcRegistry>,
    pub sink: Arc<BusinessRpcEventSink>,
    pub ingress: Arc<Ingress>,
    pub mqtt: Arc<MqttBroker>,
}

pub async fn serve(
    listener: TcpListener,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop_accepting: CancellationToken,
    stop_connections: CancellationToken,
) -> Result<()> {
    config.validate(listener.local_addr().map_err(|_| Error::Unavailable)?)?;
    let mut tasks = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            _ = stop_accepting.cancelled() => break,
            finished = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = finished { tracing::warn!(%error, "business rpc connection task failed"); }
                continue;
            }
            accepted = listener.accept() => accepted.map_err(|_| Error::Unavailable)?,
        };
        if tasks.len() >= config.max_connections {
            drop(accepted.0);
            continue;
        }
        let (stream, _) = accepted;
        let config = config.clone();
        let services = services.clone();
        let stop = stop_connections.child_token();
        tasks.spawn(async move {
            let _ = connection(stream, config, services, stop, None).await;
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    maximum: usize,
    timeout: Duration,
) -> Result<Vec<u8>> {
    tokio::time::timeout(timeout, async {
        let mut header = [0u8; 4];
        reader
            .read_exact(&mut header)
            .await
            .map_err(|_| Error::Unavailable)?;
        let length = usize::try_from(u32::from_be_bytes(header)).map_err(|_| Error::Invalid)?;
        if length == 0 || length > maximum {
            return Err(Error::Invalid);
        }
        let mut payload = vec![0u8; length];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(payload)
    })
    .await
    .map_err(|_| Error::Timeout)?
}
async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &BusinessRpcFrame,
    maximum: usize,
    timeout: Duration,
) -> Result<()> {
    let payload = serde_json::to_vec(frame).map_err(|_| Error::Internal)?;
    let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
    if length == 0 || payload.len() > maximum {
        return Err(Error::Overloaded);
    }
    // A partial write failure closes the connection; the writer never attempts another frame.
    tokio::time::timeout(timeout, async {
        writer
            .write_all(&length.to_be_bytes())
            .await
            .map_err(|_| Error::Unavailable)?;
        writer
            .write_all(&payload)
            .await
            .map_err(|_| Error::Unavailable)?;
        Ok(())
    })
    .await
    .map_err(|_| Error::Timeout)?
}

struct Queued {
    frame: BusinessRpcFrame,
    written: Option<oneshot::Sender<Result<()>>>,
    _bytes: OwnedSemaphorePermit,
    _tracking: Option<QueueTrack>,
}
struct QueueTrack {
    metrics: Arc<Metrics>,
    class: BusinessRpcQueueClass,
    bytes: u64,
}
impl Drop for QueueTrack {
    fn drop(&mut self) {
        self.metrics.business_rpc_queue_sub(self.class, self.bytes);
    }
}
fn response<T: Serialize>(
    id: Uuid,
    method: &str,
    result: std::result::Result<T, RpcError>,
) -> BusinessRpcFrame {
    match result {
        Ok(value) => BusinessRpcFrame::Response {
            request_id: id,
            method: method.into(),
            body: serde_json::to_value(value).ok(),
            error: None,
        },
        Err(error) => BusinessRpcFrame::Response {
            request_id: id,
            method: method.into(),
            body: None,
            error: Some(error),
        },
    }
}
fn error(id: Uuid, method: &str, code: RpcErrorCode, message: &str) -> BusinessRpcFrame {
    response::<()>(id, method, Err(RpcError::new(code, message)))
}
fn queue(
    tx: &mpsc::Sender<Queued>,
    budget: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
    class: BusinessRpcQueueClass,
    frame: BusinessRpcFrame,
    written: Option<oneshot::Sender<Result<()>>>,
) -> Result<()> {
    let size = serde_json::to_vec(&frame)
        .map_err(|_| Error::Internal)?
        .len();
    let permits = u32::try_from(size).map_err(|_| Error::Overloaded)?;
    let bytes = budget
        .clone()
        .try_acquire_many_owned(permits)
        .map_err(|_| {
            metrics.inc(Metric::BusinessRpcOverloads);
            Error::Overloaded
        })?;
    metrics.business_rpc_queue_add(class, size as u64);
    tx.try_send(Queued {
        frame,
        written,
        _bytes: bytes,
        _tracking: Some(QueueTrack {
            metrics: metrics.clone(),
            class,
            bytes: size as u64,
        }),
    })
    .map_err(|_| {
        metrics.inc(Metric::BusinessRpcOverloads);
        Error::Overloaded
    })
}

/// Serve a plaintext V2 connection whose Hello frame was consumed by an explicit
/// loopback V1/V2 dispatcher. TLS listeners use `serve` and never sniff protocols.
pub async fn serve_accepted(
    stream: TcpStream,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop: CancellationToken,
    first_frame: Vec<u8>,
) -> Result<()> {
    if config.tls.is_some() {
        return Err(Error::Configuration);
    }
    connection(stream, config, services, stop, Some(first_frame)).await
}

async fn connection(
    stream: TcpStream,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop: CancellationToken,
    first_frame: Option<Vec<u8>>,
) -> Result<()> {
    let metrics = services.ingress.metrics.clone();
    metrics.inc(Metric::BusinessRpcConnections);
    metrics.business_rpc_connection_started();
    let _active = ActiveBusinessConnection(metrics.clone());
    let result = connection_inner(stream, config, services, stop, first_frame).await;
    if result.is_err() {
        metrics.inc(Metric::BusinessRpcAbnormalClosures);
    }
    result
}

async fn connection_inner(
    stream: TcpStream,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop: CancellationToken,
    first_frame: Option<Vec<u8>>,
) -> Result<()> {
    let metrics = services.ingress.metrics.clone();
    let (mut io, certificate): (Box<dyn Io>, Option<Vec<u8>>) = if let Some(acceptor) = &config.tls
    {
        let tls = tokio::time::timeout(config.handshake_timeout, acceptor.accept(stream))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Authentication)?;
        let cert = tls
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certs| certs.first())
            .map(|cert| cert.as_ref().to_vec());
        (Box::new(tls), cert)
    } else {
        (Box::new(stream), None)
    };
    let hello = match first_frame {
        Some(frame) => frame,
        None => {
            read_frame(
                &mut io,
                BUSINESS_RPC_HELLO_MAX_BYTES,
                config.handshake_timeout,
            )
            .await?
        }
    };
    let hello_frame: BusinessRpcFrame =
        serde_json::from_slice(&hello).map_err(|_| Error::Invalid)?;
    hello_frame.validate().map_err(|_| Error::Invalid)?;
    let BusinessRpcFrame::Hello {
        version,
        role,
        token,
        limits,
    } = hello_frame
    else {
        return Err(Error::Invalid);
    };
    if version != BUSINESS_RPC_VERSION
        || limits.max_frame_bytes < BUSINESS_RPC_AUTH_MAX_BYTES as u32
        || limits.auth_max_inflight == 0
        || limits.event_max_inflight != BUSINESS_RPC_EVENT_WINDOW
        || limits.heartbeat_ms == 0
    {
        return Err(Error::Invalid);
    }
    let principal = match (&config.identity, certificate.as_deref()) {
        (
            BusinessIdentity::Development {
                token_hash,
                principal,
            },
            None,
        ) => token
            .as_deref()
            .filter(|token| {
                token.len() <= BUSINESS_RPC_MAX_TOKEN_BYTES
                    && bool::from(
                        Sha256::digest(token.as_bytes())
                            .as_slice()
                            .ct_eq(token_hash),
                    )
            })
            .map(|_| principal.clone()),
        (BusinessIdentity::Mtls { identities }, Some(cert)) if token.is_none() => {
            let hash: [u8; 32] = Sha256::digest(cert).into();
            identities
                .iter()
                .find(|(fingerprint, _)| bool::from(hash.ct_eq(fingerprint)))
                .map(|(_, principal)| principal.clone())
        }
        _ => None,
    };
    let Some(principal) = principal else {
        let _ = write_frame(
            &mut io,
            &BusinessRpcFrame::GoAway {
                error: RpcError::new(
                    RpcErrorCode::Unauthenticated,
                    "business identity not authorized",
                ),
            },
            BUSINESS_RPC_HELLO_MAX_BYTES,
            config.write_timeout,
        )
        .await;
        return Err(Error::Authentication);
    };
    if principal
        .expires_at_ms
        .is_some_and(|expiry| expiry <= netbaiot_runtime::now_ms())
        || !principal.permits_role(role)
    {
        let _ = write_frame(
            &mut io,
            &BusinessRpcFrame::GoAway {
                error: RpcError::new(RpcErrorCode::Forbidden, "business role not permitted"),
            },
            BUSINESS_RPC_HELLO_MAX_BYTES,
            config.write_timeout,
        )
        .await;
        return Err(Error::Forbidden);
    }
    let negotiated = config.negotiated(&limits);
    let effective_max = negotiated.max_frame_bytes as usize;
    let (mut reader, mut writer) = tokio::io::split(io);
    let (control_tx, control_rx) = mpsc::channel::<Queued>(16);
    let control_budget = Arc::new(Semaphore::new(16 * BUSINESS_RPC_AUTH_MAX_BYTES));
    let event_budget = Arc::new(Semaphore::new(effective_max));
    let (auth_tx, auth_rx) =
        mpsc::channel::<BusinessRpcOutbound>(negotiated.auth_max_inflight as usize);
    let (event_tx, event_rx) = mpsc::channel::<Queued>(1);
    let (ack_tx, ack_rx) = mpsc::channel::<EventSignal>(2);
    let mut provider = if role.auth_control() {
        Some(Arc::new(services.registry.register_with_expiry(
            auth_tx,
            BusinessProviderScope {
                global: principal.global,
                tenants: principal.tenants.clone(),
            },
            principal.expires_at_ms,
        )?))
    } else {
        None
    };
    let epoch = provider.as_ref().map_or(0, |lease| lease.epoch());
    let lease_epoch = if epoch == 0 {
        (Uuid::new_v4().as_u128() as u64).max(1)
    } else {
        epoch
    };
    write_frame(
        &mut writer,
        &BusinessRpcFrame::Ready {
            version: BUSINESS_RPC_VERSION,
            role,
            connection_epoch: lease_epoch,
            limits: negotiated.clone(),
        },
        effective_max,
        config.write_timeout,
    )
    .await?;
    let connection_stop = stop.child_token();
    let writer_stop = connection_stop.child_token();
    let writer_metrics = metrics.clone();
    let writer_handle = tokio::spawn(async move {
        let result = writer_loop(
            writer,
            control_rx,
            auth_rx,
            event_rx,
            effective_max,
            config.write_timeout,
            WriterSignals {
                metrics: writer_metrics,
                stop: writer_stop.clone(),
            },
        )
        .await;
        writer_stop.cancel();
        result
    });
    let mut subscription: Option<(SubscriptionId, u64, tokio::task::JoinHandle<()>)> = None;
    let mut ack_rx = Some(ack_rx);
    let sync_confirmation = Arc::new(AtomicU64::new(0));
    let (control_work_tx, control_work_rx) = mpsc::channel::<ControlRequest>(16);
    let control_worker = provider.as_ref().map(|lease| {
        tokio::spawn(control_loop(
            control_work_rx,
            control_tx.clone(),
            control_budget.clone(),
            services.clone(),
            (lease.clone(), principal.clone()),
            sync_confirmation.clone(),
            connection_stop.clone(),
        ))
    });
    let expiry_deadline = principal.expires_at_ms.and_then(|expires_at| {
        let remaining_ms = expires_at.saturating_sub(netbaiot_runtime::now_ms()).max(0) as u64;
        tokio::time::Instant::now().checked_add(Duration::from_millis(remaining_ms))
    });
    let result = loop {
        if principal
            .expires_at_ms
            .is_some_and(|expiry| expiry <= netbaiot_runtime::now_ms())
        {
            break Err(Error::Forbidden);
        }
        let bytes = tokio::select! {
            _ = connection_stop.cancelled() => break Ok(()),
            _ = tokio::time::sleep_until(expiry_deadline.unwrap_or_else(tokio::time::Instant::now)), if expiry_deadline.is_some() => break Err(Error::Forbidden),
            result = read_frame(&mut reader, effective_max, config.read_timeout) => match result { Ok(value) => value, Err(error) => break Err(error) },
        };
        let frame: BusinessRpcFrame = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => break Err(Error::Invalid),
        };
        if frame.validate().is_err() {
            break Err(Error::Invalid);
        }
        match frame {
            BusinessRpcFrame::Response {
                request_id,
                method,
                body,
                error,
            } if role.auth_control() => {
                if bytes.len() > BUSINESS_RPC_AUTH_MAX_BYTES
                    || !principal
                        .provide_methods
                        .iter()
                        .any(|allowed| allowed == &method)
                {
                    break Err(Error::Forbidden);
                }
                let remote_code = error.as_ref().map(|error| error.code);
                let result = match (body, error) {
                    (Some(body), None) => Ok(body),
                    (None, Some(error)) => Err(netbaiot_runtime::rpc_error_to_runtime(error.code)),
                    _ => Err(Error::Invalid),
                };
                if services
                    .registry
                    .complete(epoch, request_id, &method, result)
                    && let Some(code) = remote_code
                {
                    metrics.business_rpc_remote_error(&method, code);
                }
            }
            BusinessRpcFrame::Subscribe {
                subscription_id,
                filter,
            } if role.events() => {
                if subscription.is_some()
                    || filter.validate().is_err()
                    || (!principal.global
                        && filter
                            .tenant
                            .as_ref()
                            .is_none_or(|tenant| !principal.allows_tenant(tenant)))
                {
                    break Err(Error::Invalid);
                }
                let ack_receiver = ack_rx.take().ok_or(Error::Internal)?;
                let (send, recv) = mpsc::channel(1);
                let generation = services.sink.claim(send, filter)?;
                let (written, subscribed_written) = oneshot::channel();
                if let Err(error) = queue(
                    &control_tx,
                    &control_budget,
                    &metrics,
                    BusinessRpcQueueClass::Control,
                    BusinessRpcFrame::Subscribed { subscription_id },
                    Some(written),
                ) {
                    let _ = services.sink.release(generation);
                    break Err(error);
                }
                match subscribed_written.await {
                    Ok(Ok(())) => {}
                    _ => {
                        let _ = services.sink.release(generation);
                        break Err(Error::Unavailable);
                    }
                }
                let worker_stop = connection_stop.clone();
                let handle = tokio::spawn(event_loop(
                    recv,
                    ack_receiver,
                    event_tx.clone(),
                    (event_budget.clone(), metrics.clone()),
                    subscription_id,
                    config.event_ack_timeout,
                    worker_stop,
                ));
                subscription = Some((subscription_id, generation, handle));
            }
            BusinessRpcFrame::EventAck { ack } if role.events() => {
                if subscription
                    .as_ref()
                    .is_none_or(|(id, _, _)| *id != ack.subscription_id)
                {
                    break Err(Error::Invalid);
                }
                let _ = ack_tx.try_send(EventSignal { ack, success: true });
            }
            BusinessRpcFrame::EventNack { ack, .. } if role.events() => {
                if subscription
                    .as_ref()
                    .is_none_or(|(id, _, _)| *id != ack.subscription_id)
                {
                    break Err(Error::Invalid);
                }
                let _ = ack_tx.try_send(EventSignal {
                    ack,
                    success: false,
                });
            }
            BusinessRpcFrame::Ping { nonce } => {
                let revision = sync_confirmation.load(Ordering::Acquire);
                if revision != 0
                    && revision != u64::MAX
                    && sync_confirmation
                        .compare_exchange(revision, 0, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    && let Some(lease) = provider.as_ref()
                {
                    lease.mark_serving(revision)?;
                }
                queue(
                    &control_tx,
                    &control_budget,
                    &metrics,
                    BusinessRpcQueueClass::Control,
                    BusinessRpcFrame::Pong { nonce },
                    None,
                )?;
            }
            BusinessRpcFrame::Pong { .. } => {}
            BusinessRpcFrame::Cancel { .. } => {}
            BusinessRpcFrame::GoAway { .. } => break Ok(()),
            BusinessRpcFrame::Request {
                request_id,
                method,
                deadline_ms,
                body,
            } if role.auth_control() => {
                if bytes.len() > BUSINESS_RPC_AUTH_MAX_BYTES || deadline_ms == 0 {
                    break Err(Error::Invalid);
                }
                if matches!(method.as_str(), "auth.sync" | "auth.invalidate")
                    && !principal
                        .call_methods
                        .iter()
                        .any(|allowed| allowed == &method)
                {
                    queue(
                        &control_tx,
                        &control_budget,
                        &metrics,
                        BusinessRpcQueueClass::Control,
                        error(
                            request_id,
                            &method,
                            RpcErrorCode::Forbidden,
                            "method not permitted",
                        ),
                        None,
                    )?;
                    continue;
                }
                if method == "auth.sync" {
                    if sync_confirmation
                        .compare_exchange(0, u64::MAX, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        queue(
                            &control_tx,
                            &control_budget,
                            &metrics,
                            BusinessRpcQueueClass::Control,
                            error(
                                request_id,
                                &method,
                                RpcErrorCode::Conflict,
                                "sync already pending",
                            ),
                            None,
                        )?;
                        continue;
                    }
                    if let Some(lease) = provider.as_ref() {
                        lease.mark_syncing()?;
                    }
                }
                let method_for_error = method.clone();
                if control_work_tx
                    .try_send(ControlRequest {
                        request_id,
                        method,
                        body,
                    })
                    .is_err()
                {
                    if method_for_error == "auth.sync" {
                        sync_confirmation.store(0, Ordering::Release);
                    }
                    queue(
                        &control_tx,
                        &control_budget,
                        &metrics,
                        BusinessRpcQueueClass::Control,
                        error(
                            request_id,
                            &method_for_error,
                            RpcErrorCode::Overloaded,
                            "control queue full",
                        ),
                        None,
                    )?;
                }
            }
            _ => break Err(Error::Invalid),
        }
    };
    connection_stop.cancel();
    if let Some((_, generation, handle)) = subscription {
        let _ = services.sink.release(generation);
        let _ = handle.await;
    }
    if let Some(worker) = control_worker {
        worker.abort();
        let _ = worker.await;
    }
    provider.take();
    let _ = writer_handle.await;
    result
}

struct ControlRequest {
    request_id: Uuid,
    method: String,
    body: serde_json::Value,
}
async fn control_loop(
    mut requests: mpsc::Receiver<ControlRequest>,
    writer: mpsc::Sender<Queued>,
    budget: Arc<Semaphore>,
    services: Arc<BusinessRpcServices>,
    owner: (Arc<ProviderLease>, BusinessPrincipal),
    confirmation: Arc<AtomicU64>,
    stop: CancellationToken,
) {
    let (lease, principal) = owner;
    let mut revision: Option<(Uuid, u64)> = None;
    while let Some(ControlRequest {
        request_id,
        method,
        body,
    }) = tokio::select! { _ = stop.cancelled() => None, request = requests.recv() => request }
    {
        let reply = match method.as_str() {
            "auth.sync" => match serde_json::from_value::<AuthSyncRequest>(body) {
                Ok(request)
                    if request.reset
                        && request.auth_revision > 0
                        && request.auth_revision < u64::MAX =>
                {
                    let invalidate = AuthInvalidation::All;
                    let mqtt = services.mqtt.clone();
                    match services
                        .ingress
                        .invalidate_auth_with(&invalidate, || mqtt.invalidate_sessions(&invalidate))
                    {
                        Ok(_) => {
                            revision = Some((request.authority_incarnation, request.auth_revision));
                            confirmation.store(request.auth_revision, Ordering::Release);
                            response(
                                request_id,
                                &method,
                                Ok(AuthSyncResponse {
                                    applied_revision: request.auth_revision,
                                }),
                            )
                        }
                        Err(_) => error(request_id, &method, RpcErrorCode::Internal, "sync failed"),
                    }
                }
                _ => error(
                    request_id,
                    &method,
                    RpcErrorCode::InvalidRequest,
                    "invalid sync request",
                ),
            },
            "auth.invalidate" => match serde_json::from_value::<AuthInvalidateRequest>(body) {
                Ok(request) if request.auth_revision == 0 || request.auth_revision == u64::MAX => {
                    error(
                        request_id,
                        &method,
                        RpcErrorCode::InvalidRequest,
                        "invalid revision",
                    )
                }
                Ok(request) if !principal.permits_invalidation(&request.invalidation) => error(
                    request_id,
                    &method,
                    RpcErrorCode::Forbidden,
                    "invalidation scope not permitted",
                ),
                Ok(request)
                    if revision == Some((request.authority_incarnation, request.auth_revision)) =>
                {
                    response(
                        request_id,
                        &method,
                        Ok(AuthInvalidateResponse {
                            applied_revision: request.auth_revision,
                            invalidated_cache_entries: 0,
                            disconnected_connections: 0,
                            invalidated_mqtt_sessions: 0,
                        }),
                    )
                }
                Ok(request)
                    if revision.is_some_and(|(inc, rev)| {
                        inc == request.authority_incarnation && request.auth_revision < rev
                    }) =>
                {
                    error(
                        request_id,
                        &method,
                        RpcErrorCode::StaleRevision,
                        "stale invalidation revision",
                    )
                }
                Ok(request)
                    if revision.is_some_and(|(inc, rev)| {
                        inc == request.authority_incarnation
                            && rev.checked_add(1) == Some(request.auth_revision)
                    }) && services.registry.is_serving() =>
                {
                    let mqtt = services.mqtt.clone();
                    match services
                        .ingress
                        .invalidate_auth_with(&request.invalidation, || {
                            mqtt.invalidate_sessions(&request.invalidation)
                        }) {
                        Ok((devices, disconnected, mqtt_sessions)) => {
                            revision = Some((request.authority_incarnation, request.auth_revision));
                            if lease.advance_revision(request.auth_revision).is_err() {
                                stop.cancel();
                                break;
                            }
                            response(
                                request_id,
                                &method,
                                Ok(AuthInvalidateResponse {
                                    applied_revision: request.auth_revision,
                                    invalidated_cache_entries: devices.len(),
                                    disconnected_connections: disconnected,
                                    invalidated_mqtt_sessions: mqtt_sessions,
                                }),
                            )
                        }
                        Err(_) => error(
                            request_id,
                            &method,
                            RpcErrorCode::Internal,
                            "invalidation failed",
                        ),
                    }
                }
                Ok(_) => {
                    services
                        .ingress
                        .metrics
                        .inc(Metric::BusinessRpcRevisionGaps);
                    confirmation.store(0, Ordering::Release);
                    if lease.mark_syncing().is_err() {
                        stop.cancel();
                        break;
                    }
                    revision = None;
                    // A missing revision can hide a device revocation. Revoke all
                    // local authorization before accepting the reset handshake.
                    let invalidate = AuthInvalidation::All;
                    let mqtt = services.mqtt.clone();
                    if services
                        .ingress
                        .invalidate_auth_with(&invalidate, || mqtt.invalidate_sessions(&invalidate))
                        .is_err()
                    {
                        stop.cancel();
                        break;
                    }
                    error(
                        request_id,
                        &method,
                        RpcErrorCode::StaleRevision,
                        "reset sync required",
                    )
                }
                Err(_) => error(
                    request_id,
                    &method,
                    RpcErrorCode::InvalidRequest,
                    "invalid invalidation request",
                ),
            },
            _ => error(
                request_id,
                &method,
                RpcErrorCode::UnknownMethod,
                "unknown method",
            ),
        };
        if method == "auth.sync"
            && !matches!(&reply, BusinessRpcFrame::Response { error: None, .. })
        {
            confirmation.store(0, Ordering::Release);
        }
        let success = matches!(&reply, BusinessRpcFrame::Response { error: None, .. });
        let metric = match (method.as_str(), success) {
            ("auth.sync", true) => Some(Metric::BusinessRpcProviderSyncSuccess),
            ("auth.sync", false) => Some(Metric::BusinessRpcProviderSyncFailure),
            ("auth.invalidate", true) => Some(Metric::BusinessRpcInvalidationSuccess),
            ("auth.invalidate", false) => Some(Metric::BusinessRpcInvalidationFailure),
            _ => None,
        };
        if let Some(metric) = metric {
            services.ingress.metrics.inc(metric);
        }
        if queue(
            &writer,
            &budget,
            &services.ingress.metrics,
            BusinessRpcQueueClass::Control,
            reply,
            None,
        )
        .is_err()
        {
            stop.cancel();
            break;
        }
    }
}

struct EventSignal {
    ack: EventAck,
    success: bool,
}
async fn event_loop(
    mut requests: mpsc::Receiver<BusinessEventRequest>,
    mut acks: mpsc::Receiver<EventSignal>,
    writer: mpsc::Sender<Queued>,
    resources: (Arc<Semaphore>, Arc<Metrics>),
    subscription_id: SubscriptionId,
    timeout: Duration,
    stop: CancellationToken,
) {
    let (event_budget, metrics) = resources;
    while let Some(request) =
        tokio::select! { _ = stop.cancelled() => None, request = requests.recv() => request }
    {
        let delivery_id = netbaiot_core::DeliveryId::generate();
        let event_id = request.delivery.event.event_id;
        let frame = BusinessRpcFrame::Event {
            delivery: EventDelivery {
                delivery_id,
                subscription_id,
                event: (*request.delivery.event).clone(),
                attempt: request.delivery.attempt,
            },
        };
        if queue(
            &writer,
            &event_budget,
            &metrics,
            BusinessRpcQueueClass::Event,
            frame,
            None,
        )
        .is_err()
        {
            let _ = request.result.send(Err(SinkError::Retryable));
            break;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        let started = std::time::Instant::now();
        let result = loop {
            let signal = tokio::select! {
                _ = stop.cancelled() => break Err(SinkError::Retryable),
                signal = tokio::time::timeout_at(deadline, acks.recv()) => match signal { Ok(Some(value)) => value, _ => break Err(SinkError::Retryable) },
            };
            if signal.ack.delivery_id == delivery_id
                && signal.ack.subscription_id == subscription_id
                && signal.ack.event_id == event_id
            {
                break if signal.success {
                    Ok(SinkAck)
                } else {
                    Err(SinkError::Retryable)
                };
            }
        };
        let failed = result.is_err();
        if result.is_ok() {
            metrics.inc(Metric::BusinessRpcEventAcks);
            metrics.observe(
                Histogram::BusinessRpcEventAckLatency,
                started.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );
        } else if tokio::time::Instant::now() >= deadline {
            metrics.inc(Metric::BusinessRpcTimeouts);
        }
        let _ = request.result.send(result);
        if failed {
            stop.cancel();
            break;
        }
    }
}
struct WriterSignals {
    metrics: Arc<Metrics>,
    stop: CancellationToken,
}
async fn writer_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut control: mpsc::Receiver<Queued>,
    mut auth: mpsc::Receiver<BusinessRpcOutbound>,
    mut events: mpsc::Receiver<Queued>,
    maximum: usize,
    timeout: Duration,
    signals: WriterSignals,
) -> Result<()> {
    loop {
        let item = tokio::select! {
            _ = signals.stop.cancelled() => return Ok(()),
            item = control.recv(), if !control.is_closed() => item,
            item = auth.recv(), if !auth.is_closed() => item.map(|out| {
                if matches!(out.frame, BusinessRpcFrame::Request { .. }) {
                    signals.metrics.observe(Histogram::BusinessRpcQueueWait, out.queued_at.elapsed().as_micros().min(u64::MAX as u128) as u64);
                }
                Queued { frame: out.frame, written: None, _bytes: out._bytes, _tracking: None }
            }),
            item = events.recv(), if !events.is_closed() => item,
        };
        let Some(item) = item else { return Ok(()) };
        let result = write_frame(&mut writer, &item.frame, maximum, timeout).await;
        if let Some(written) = item.written {
            let _ = written.send(result.as_ref().map(|_| ()).map_err(|_| Error::Unavailable));
        }
        result?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::{
        DeviceEvent, DeviceEventKind, DeviceId, DeviceKey, EventId, Heartbeat, ProductId, SinkId,
        SourceMessageId,
    };
    use netbaiot_runtime::DeliveryEnvelope;

    #[tokio::test]
    async fn control_queue_pressure_rejects_without_leaking_byte_permits() {
        let (send, mut receive) = mpsc::channel(1);
        let budget = Arc::new(Semaphore::new(256));
        let metrics = Arc::new(Metrics::default());
        queue(
            &send,
            &budget,
            &metrics,
            BusinessRpcQueueClass::Control,
            BusinessRpcFrame::Ping { nonce: 1 },
            None,
        )
        .unwrap();
        assert!(matches!(
            queue(
                &send,
                &budget,
                &metrics,
                BusinessRpcQueueClass::Control,
                BusinessRpcFrame::Pong { nonce: 2 },
                None
            ),
            Err(Error::Overloaded)
        ));
        assert!(budget.available_permits() < 256);
        drop(receive.recv().await.unwrap());
        assert_eq!(budget.available_permits(), 256);
        assert_eq!(metrics.get(Metric::BusinessRpcOverloads), 1);
    }

    #[test]
    fn scoped_business_principal_cannot_invalidate_other_tenants_or_all() {
        let allowed = TenantId::new("allowed").unwrap();
        let principal = BusinessPrincipal {
            id: "scoped".into(),
            role: BusinessRole::AuthControl,
            provider_id: Some("primary".into()),
            sink_id: None,
            provide_methods: vec![
                "device.authenticate".into(),
                "device.resolve_verifier".into(),
            ],
            call_methods: vec!["auth.sync".into(), "auth.invalidate".into()],
            global: false,
            tenants: vec![allowed.clone()],
            expires_at_ms: None,
        };
        assert!(principal.permits_invalidation(&AuthInvalidation::Tenant { tenant_id: allowed }));
        assert!(!principal.permits_invalidation(&AuthInvalidation::Tenant {
            tenant_id: TenantId::new("other").unwrap()
        }));
        assert!(!principal.permits_invalidation(&AuthInvalidation::All));
        assert!(
            !principal.permits_invalidation(&AuthInvalidation::CredentialVersion { version: 1 })
        );
    }

    #[tokio::test]
    async fn event_ack_requires_exact_delivery_subscription_and_event() {
        let (send, receive) = mpsc::channel(1);
        let (ack_send, ack_receive) = mpsc::channel(2);
        let (write_send, mut write_receive) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let metrics = Arc::new(Metrics::default());
        let subscription_id = SubscriptionId::generate();
        let worker = tokio::spawn(event_loop(
            receive,
            ack_receive,
            write_send,
            (Arc::new(Semaphore::new(65_536)), metrics.clone()),
            subscription_id,
            Duration::from_secs(1),
            stop.clone(),
        ));
        let event = Arc::new(DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("ack-check").unwrap(),
            device: DeviceKey {
                tenant_id: TenantId::new("tenant").unwrap(),
                product_id: ProductId::new("product").unwrap(),
                device_id: DeviceId::new("device").unwrap(),
            },
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
        });
        let (result, mut done) = oneshot::channel();
        send.send(BusinessEventRequest {
            delivery: DeliveryEnvelope {
                event: event.clone(),
                sink_id: SinkId::new("tcp-rpc").unwrap(),
                attempt: 1,
                accepted_at: 1,
            },
            result,
        })
        .await
        .unwrap();
        let outbound = write_receive.recv().await.unwrap();
        let BusinessRpcFrame::Event { delivery } = outbound.frame else {
            panic!("event expected")
        };
        let mut ack = EventAck {
            delivery_id: delivery.delivery_id,
            subscription_id,
            event_id: event.event_id,
        };
        ack.event_id = EventId::generate();
        ack_send
            .send(EventSignal {
                ack: ack.clone(),
                success: true,
            })
            .await
            .unwrap();
        assert!(matches!(
            done.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        ack.event_id = event.event_id;
        ack_send
            .send(EventSignal { ack, success: true })
            .await
            .unwrap();
        assert!(done.await.unwrap().is_ok());
        assert_eq!(metrics.get(Metric::BusinessRpcEventAcks), 1);
        stop.cancel();
        worker.await.unwrap();
    }

    #[tokio::test]
    async fn framing_handles_split_and_coalesced_frames() {
        let (mut sender, mut receiver) = tokio::io::duplex(256);
        let first = b"{\"type\":\"ping\",\"nonce\":1}";
        let second = b"{\"type\":\"pong\",\"nonce\":2}";
        let first_len = u32::try_from(first.len()).unwrap().to_be_bytes();
        let second_len = u32::try_from(second.len()).unwrap().to_be_bytes();
        sender.write_all(&first_len[..2]).await.unwrap();
        let reader = tokio::spawn(async move {
            let a = read_frame(&mut receiver, 256, Duration::from_secs(1))
                .await
                .unwrap();
            let b = read_frame(&mut receiver, 256, Duration::from_secs(1))
                .await
                .unwrap();
            (a, b)
        });
        let mut remainder = Vec::new();
        remainder.extend_from_slice(&first_len[2..]);
        remainder.extend_from_slice(first);
        remainder.extend_from_slice(&second_len);
        remainder.extend_from_slice(second);
        sender.write_all(&remainder).await.unwrap();
        let (a, b) = reader.await.unwrap();
        assert_eq!(a, first);
        assert_eq!(b, second);
    }

    #[tokio::test]
    async fn framing_rejects_zero_oversize_truncated_and_malformed_json() {
        for (header, payload) in [
            ([0, 0, 0, 0], vec![]),
            ([0, 0, 1, 1], vec![]),
            ([0, 0, 0, 5], vec![1, 2]),
        ] {
            let (mut sender, mut receiver) = tokio::io::duplex(32);
            sender.write_all(&header).await.unwrap();
            sender.write_all(&payload).await.unwrap();
            drop(sender);
            assert!(
                read_frame(&mut receiver, 256, Duration::from_secs(1))
                    .await
                    .is_err()
            );
        }
        assert!(serde_json::from_slice::<BusinessRpcFrame>(b"{not json}").is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn slow_partial_header_times_out_without_allocation() {
        let (mut sender, mut receiver) = tokio::io::duplex(8);
        sender.write_all(&[0, 0]).await.unwrap();
        assert!(matches!(
            read_frame(&mut receiver, 256, Duration::from_millis(5)).await,
            Err(Error::Timeout)
        ));
    }
}
