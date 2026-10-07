//! Current Business RPC transport and shared authorization/control state.
mod v3;
use crate::mqtt::broker::MqttBroker;
use netbaiot_core::{
    AuthInvalidation, EventDelivery, SubscriptionId, TenantId,
    business_rpc::{
        AuthInvalidateRequest, AuthInvalidateResponse, AuthSyncRequest, AuthSyncResponse,
        BUSINESS_RPC_AUTH_MAX_BYTES, BUSINESS_RPC_HELLO_MAX_BYTES, BUSINESS_RPC_MAX_TOKEN_BYTES,
        BusinessRole, DeviceCommandSendRequest, DeviceCommandSendResponse, RpcError, RpcErrorCode,
    },
};
use netbaiot_runtime::{
    BusinessEventRequest, BusinessProviderScope, BusinessRpcCall, BusinessRpcEventSink,
    BusinessRpcOutbound, BusinessRpcRegistry, CommandService, Error, Ingress, ProviderLease,
    Result, SinkAck, SinkError,
    metrics::{BusinessRpcQueueClass, Histogram, Metric, Metrics},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Experiment-only observation at the Tokio socket boundary. `Ready(Ok(n))` means
/// the socket accepted `n` bytes, not that the peer received or ACKed them.
struct ObservedSocket {
    inner: TcpStream,
    trace: bool,
}
impl AsyncRead for ObservedSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if self.trace
            && let Poll::Ready(Ok(bytes)) = &result
        {
            tracing::debug!(bytes, "business RPC socket accepted write bytes");
        }
        result
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        if self.trace
            && let Poll::Ready(Ok(bytes)) = &result
        {
            tracing::debug!(bytes, "business RPC socket accepted vectored write bytes");
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

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
}

/// Explicit current-protocol token or verified certificate identity, never device credentials.
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
    pub limits: netbaiot_core::business_rpc_v3::V3Limits,
    pub send_ahead: Option<V3SendAhead>,
    /// Experiment only: OS socket send buffer request, outside the V3 protocol.
    pub experiment_socket_send_buffer_bytes: Option<usize>,
    pub max_connections: usize,
    pub auth_max_inflight: usize,
    pub handshake_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub event_ack_timeout: Duration,
}
/// Sender-local V3 scheduling limits; these are never sent in the V3 Hello.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct V3SendAhead {
    pub stream_bytes: u32,
    pub connection_bytes: u32,
}
impl V3SendAhead {
    pub fn as_mux(self) -> netbaiot_v3_mux::SendAheadLimits {
        netbaiot_v3_mux::SendAheadLimits {
            stream_bytes: self.stream_bytes,
            connection_bytes: self.connection_bytes,
        }
    }
}
impl BusinessRpcTransportConfig {
    pub fn validate(&self, address: SocketAddr) -> Result<()> {
        if self.max_connections == 0
            || self.limits.validate().is_err()
            || self
                .send_ahead
                .is_some_and(|policy| policy.as_mux().validate(&self.limits).is_err())
            || self
                .experiment_socket_send_buffer_bytes
                .is_some_and(|size| !(4096..=4 * 1024 * 1024).contains(&size))
            || self.max_connections > 1024
            || self.auth_max_inflight == 0
            || self.auth_max_inflight > u16::MAX as usize
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
}

pub struct BusinessRpcServices {
    pub registry: Arc<BusinessRpcRegistry>,
    pub sink: Arc<BusinessRpcEventSink>,
    pub ingress: Arc<Ingress>,
    pub mqtt: Arc<MqttBroker>,
    pub commands: Arc<CommandService>,
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
            let _ = connection(stream, config, services, stop).await;
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
#[derive(Serialize)]
struct RpcReply {
    request_id: Uuid,
    method: String,
    body: Option<serde_json::Value>,
    error: Option<RpcError>,
}

struct Queued {
    frame: RpcReply,
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
) -> RpcReply {
    match result {
        Ok(value) => RpcReply {
            request_id: id,
            method: method.into(),
            body: serde_json::to_value(value).ok(),
            error: None,
        },
        Err(error) => RpcReply {
            request_id: id,
            method: method.into(),
            body: None,
            error: Some(error),
        },
    }
}
fn error(id: Uuid, method: &str, code: RpcErrorCode, message: &str) -> RpcReply {
    response::<()>(id, method, Err(RpcError::new(code, message)))
}
fn queue(
    tx: &mpsc::Sender<Queued>,
    budget: &Arc<Semaphore>,
    metrics: &Arc<Metrics>,
    class: BusinessRpcQueueClass,
    frame: RpcReply,
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

async fn connection(
    stream: TcpStream,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop: CancellationToken,
) -> Result<()> {
    let metrics = services.ingress.metrics.clone();
    metrics.inc(Metric::BusinessRpcConnections);
    metrics.business_rpc_connection_started();
    let _active = ActiveBusinessConnection(metrics.clone());
    let result = connection_inner(stream, config, services, stop).await;
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
) -> Result<()> {
    {
        let socket = socket2::SockRef::from(&stream);
        if let Some(bytes) = config.experiment_socket_send_buffer_bytes {
            socket
                .set_send_buffer_size(bytes)
                .map_err(|_| Error::Configuration)?;
        }
        let actual = socket.send_buffer_size().map_err(|_| Error::Unavailable)?;
        tracing::info!(
            send_buffer_bytes = actual,
            "business RPC socket send buffer"
        );
    }
    let stream = ObservedSocket {
        inner: stream,
        trace: std::env::var_os("NETBAIOT_V3_SOCKET_TRACE").is_some(),
    };
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
    let hello = read_frame(
        &mut io,
        BUSINESS_RPC_HELLO_MAX_BYTES,
        config.handshake_timeout,
    )
    .await?;
    v3::connection(io, certificate, hello, config, services, stop).await
}

struct ControlRequest {
    request_id: Uuid,
    method: String,
    body: serde_json::Value,
}

fn command_error(error: Error) -> RpcErrorCode {
    match error {
        Error::Invalid | Error::Codec | Error::Configuration => RpcErrorCode::InvalidRequest,
        Error::Forbidden => RpcErrorCode::Forbidden,
        Error::Conflict => RpcErrorCode::Conflict,
        Error::Overloaded => RpcErrorCode::Overloaded,
        Error::Timeout => RpcErrorCode::Timeout,
        Error::Unavailable
        | Error::Draining
        | Error::Storage
        | Error::IncompatibleSpool
        | Error::Authentication => RpcErrorCode::Unavailable,
        Error::Internal => RpcErrorCode::Internal,
    }
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
        // Current protocol control work uses this worker. Keep the guard until all auth and
        // revision side effects finish; queued requests acquire no earlier rights.
        let reply = match services.ingress.lifecycle.begin_admission() {
            Err(_) => error(
                request_id,
                &method,
                RpcErrorCode::Unavailable,
                "service is draining",
            ),
            Ok(admission) => {
                let reply = match method.as_str() {
                    "auth.sync" => match serde_json::from_value::<AuthSyncRequest>(body) {
                        Ok(request)
                            if request.reset
                                && request.auth_revision > 0
                                && request.auth_revision < u64::MAX =>
                        {
                            if lease.mark_syncing().is_err() {
                                stop.cancel();
                                break;
                            }
                            let invalidate = AuthInvalidation::All;
                            let mqtt = services.mqtt.clone();
                            match services.ingress.invalidate_auth_admitted_with(
                                &admission,
                                &invalidate,
                                || mqtt.invalidate_sessions(&invalidate),
                            ) {
                                Ok(_) => {
                                    revision = Some((
                                        request.authority_incarnation,
                                        request.auth_revision,
                                    ));
                                    confirmation.store(request.auth_revision, Ordering::Release);
                                    response(
                                        request_id,
                                        &method,
                                        Ok(AuthSyncResponse {
                                            applied_revision: request.auth_revision,
                                        }),
                                    )
                                }
                                Err(_) => error(
                                    request_id,
                                    &method,
                                    RpcErrorCode::Internal,
                                    "sync failed",
                                ),
                            }
                        }
                        _ => error(
                            request_id,
                            &method,
                            RpcErrorCode::InvalidRequest,
                            "invalid sync request",
                        ),
                    },
                    "auth.invalidate" => {
                        match serde_json::from_value::<AuthInvalidateRequest>(body) {
                            Ok(request)
                                if request.auth_revision == 0
                                    || request.auth_revision == u64::MAX =>
                            {
                                error(
                                    request_id,
                                    &method,
                                    RpcErrorCode::InvalidRequest,
                                    "invalid revision",
                                )
                            }
                            Ok(request)
                                if !principal.permits_invalidation(&request.invalidation) =>
                            {
                                error(
                                    request_id,
                                    &method,
                                    RpcErrorCode::Forbidden,
                                    "invalidation scope not permitted",
                                )
                            }
                            Ok(request)
                                if revision
                                    == Some((
                                        request.authority_incarnation,
                                        request.auth_revision,
                                    )) =>
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
                                    inc == request.authority_incarnation
                                        && request.auth_revision < rev
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
                                match services.ingress.invalidate_auth_admitted_with(
                                    &admission,
                                    &request.invalidation,
                                    || mqtt.invalidate_sessions(&request.invalidation),
                                ) {
                                    Ok((devices, disconnected, mqtt_sessions)) => {
                                        revision = Some((
                                            request.authority_incarnation,
                                            request.auth_revision,
                                        ));
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
                                    .invalidate_auth_admitted_with(&admission, &invalidate, || {
                                        mqtt.invalidate_sessions(&invalidate)
                                    })
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
                        }
                    }
                    _ => error(
                        request_id,
                        &method,
                        RpcErrorCode::UnknownMethod,
                        "unknown method",
                    ),
                };
                if method == "auth.sync" && reply.error.is_some() {
                    confirmation.store(0, Ordering::Release);
                }
                drop(admission);
                reply
            }
        };
        let success = reply.error.is_none();
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
        )
        .is_err()
        {
            stop.cancel();
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::SinkId;

    #[test]
    fn command_runtime_capacity_and_offline_errors_keep_rpc_semantics() {
        assert_eq!(command_error(Error::Overloaded), RpcErrorCode::Overloaded);
        assert_eq!(command_error(Error::Unavailable), RpcErrorCode::Unavailable);
        assert_eq!(command_error(Error::Draining), RpcErrorCode::Unavailable);
        assert_eq!(command_error(Error::Conflict), RpcErrorCode::Conflict);
        assert_eq!(command_error(Error::Codec), RpcErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn command_response_pressure_preserves_control_queue_capacity() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let command_bytes = Arc::new(Semaphore::new(256));
        let control_bytes = Arc::new(Semaphore::new(256));
        let metrics = Arc::new(Metrics::default());
        queue(
            &command_tx,
            &command_bytes,
            &metrics,
            BusinessRpcQueueClass::Command,
            response(Uuid::new_v4(), "probe", Ok(true)),
        )
        .unwrap();
        assert!(matches!(
            queue(
                &command_tx,
                &command_bytes,
                &metrics,
                BusinessRpcQueueClass::Command,
                response(Uuid::new_v4(), "probe", Ok(true))
            ),
            Err(Error::Overloaded)
        ));
        let id = Uuid::new_v4();
        queue(
            &control_tx,
            &control_bytes,
            &metrics,
            BusinessRpcQueueClass::Control,
            error(
                id,
                "auth.invalidate",
                RpcErrorCode::Forbidden,
                "revision denied",
            ),
        )
        .unwrap();
        assert!(matches!(control_rx.recv().await.map(|item| item.frame),
            Some(RpcReply { request_id, .. }) if request_id == id));
    }

    #[tokio::test]
    async fn command_response_byte_limit_rejects_and_releases_permits() {
        let (tx, mut rx) = mpsc::channel(2);
        let budget = Arc::new(Semaphore::new(128));
        let metrics = Arc::new(Metrics::default());
        queue(
            &tx,
            &budget,
            &metrics,
            BusinessRpcQueueClass::Command,
            response(Uuid::new_v4(), "probe", Ok(true)),
        )
        .unwrap();
        assert!(matches!(
            queue(
                &tx,
                &budget,
                &metrics,
                BusinessRpcQueueClass::Command,
                response(Uuid::new_v4(), "probe", Ok(true))
            ),
            Err(Error::Overloaded)
        ));
        drop(rx.recv().await);
        assert!(
            queue(
                &tx,
                &budget,
                &metrics,
                BusinessRpcQueueClass::Command,
                response(Uuid::new_v4(), "probe", Ok(true))
            )
            .is_ok()
        );
    }

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
            response(Uuid::new_v4(), "probe", Ok(true)),
        )
        .unwrap();
        assert!(matches!(
            queue(
                &send,
                &budget,
                &metrics,
                BusinessRpcQueueClass::Control,
                response(Uuid::new_v4(), "probe", Ok(true))
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
    async fn framing_handles_split_and_coalesced_frames() {
        let (mut sender, mut receiver) = tokio::io::duplex(256);
        let first = br#"{"type":"hello","version":3,"token":"one"}"#;
        let second = br#"{"type":"hello","version":3,"token":"two"}"#;
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
        assert!(
            serde_json::from_slice::<netbaiot_core::business_rpc_v3::V3Bootstrap>(b"{not json}")
                .is_err()
        );
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
    #[tokio::test]
    async fn current_control_worker_rejects_sync_and_invalidation_after_quiesce() {
        use netbaiot_runtime::*;
        let limits = Arc::new(Limits::default());
        let metrics = Arc::new(Metrics::default());
        let registry = BusinessRpcRegistry::new(4, 65536, Duration::from_secs(1)).unwrap();
        let sink = BusinessRpcEventSink::new();
        let sink_id = SinkId::new("rpc").unwrap();
        let events = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![SinkDefinition::bounded(
                sink_id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![netbaiot_core::RouteDefinition {
                tenant: None,
                sinks: vec![sink_id],
            }],
            1,
        )
        .unwrap();
        let lifecycle = Arc::new(Lifecycle::starting());
        lifecycle.mark_running().unwrap();
        let ingress = Arc::new(Ingress::new(
            limits.clone(),
            AuthCache::new(
                BusinessRpcAuthProvider::new(registry.clone()),
                limits.clone(),
                metrics.clone(),
            ),
            CodecRegistry::new(vec![(
                netbaiot_core::CodecId::new("netbaiot-json").unwrap(),
                1,
                Arc::new(netbaiot_codecs::JsonV1::default()),
            )])
            .unwrap(),
            events.clone(),
            GatewayControl::empty(limits.clone()),
            metrics,
            Sessions::new(limits.clone()),
            lifecycle.clone(),
        ));
        let mqtt = MqttBroker::new(limits);
        let services = Arc::new(BusinessRpcServices {
            registry: registry.clone(),
            sink,
            mqtt,
            commands: Arc::new(CommandService::new(Arc::new(CommandRouter::new(
                ingress.clone(),
            )))),
            ingress,
        });
        let (provider_tx, _provider_rx) = mpsc::channel(4);
        let lease = Arc::new(
            registry
                .register(
                    provider_tx,
                    BusinessProviderScope {
                        global: true,
                        tenants: vec![],
                    },
                )
                .unwrap(),
        );
        let principal = BusinessPrincipal {
            id: "test".into(),
            role: BusinessRole::AuthControl,
            provider_id: Some("primary".into()),
            sink_id: None,
            provide_methods: vec![
                "device.authenticate".into(),
                "device.resolve_verifier".into(),
            ],
            call_methods: vec!["auth.sync".into(), "auth.invalidate".into()],
            global: true,
            tenants: vec![],
            expires_at_ms: None,
        };
        let (work_tx, work_rx) = mpsc::channel(4);
        let (reply_tx, mut reply_rx) = mpsc::channel(4);
        let confirmation = Arc::new(AtomicU64::new(0));
        let stop = CancellationToken::new();
        let worker = tokio::spawn(control_loop(
            work_rx,
            reply_tx,
            Arc::new(Semaphore::new(65536)),
            services,
            (lease.clone(), principal),
            confirmation.clone(),
            stop.clone(),
        ));
        let incarnation = Uuid::new_v4();
        let sync = serde_json::to_value(AuthSyncRequest {
            authority_incarnation: incarnation,
            auth_revision: 1,
            reset: true,
        })
        .unwrap();
        work_tx
            .send(ControlRequest {
                request_id: Uuid::new_v4(),
                method: "auth.sync".into(),
                body: sync.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(
            reply_rx.recv().await.unwrap().frame,
            RpcReply { error: None, .. }
        ));
        lease.mark_serving(1).unwrap();
        lifecycle.begin_quiesce().await.unwrap();
        for (method, body) in [
            ("auth.sync", sync),
            (
                "auth.invalidate",
                serde_json::to_value(AuthInvalidateRequest {
                    authority_incarnation: incarnation,
                    auth_revision: 2,
                    invalidation: AuthInvalidation::All,
                })
                .unwrap(),
            ),
        ] {
            work_tx
                .send(ControlRequest {
                    request_id: Uuid::new_v4(),
                    method: method.into(),
                    body,
                })
                .await
                .unwrap();
            assert!(matches!(
                reply_rx.recv().await.unwrap().frame,
                RpcReply {
                    error: Some(RpcError {
                        code: RpcErrorCode::Unavailable,
                        ..
                    }),
                    ..
                }
            ));
        }
        assert!(registry.is_serving()); // Rejected sync did not demote the authority.
        assert_eq!(confirmation.load(Ordering::Acquire), 1);
        stop.cancel();
        worker.await.unwrap();
        events.stop_workers().await.unwrap();
    }
}
