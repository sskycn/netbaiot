use crate::mqtt::broker::MqttBroker;
use bytes::BytesMut;
use netbaiot_core::Transport;
use netbaiot_runtime::*;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::Instant,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub type BoxStream = Box<dyn Stream>;
pub struct Services {
    pub admin: Option<Arc<AdminAccess>>,
    pub shutdown: CancellationToken,
    pub control_lock: Arc<tokio::sync::Mutex<()>>,
    pub http_slots: Arc<tokio::sync::Semaphore>,
    pub ingress: Arc<Ingress>,
    pub router: Arc<CommandRouter>,
    pub connections: Arc<Connections>,
    pub rates: Arc<RateLimiter>,
    pub protocol_admission: Arc<Admission>,
    pub mqtt: Arc<MqttBroker>,
}
impl Services {
    pub fn new(ingress: Arc<Ingress>, shutdown: CancellationToken) -> Arc<Self> {
        let limits = ingress.limits.clone();
        let mqtt = MqttBroker::new_with_metrics(limits.clone(), ingress.metrics.clone());
        Self::new_with_mqtt(ingress, shutdown, mqtt)
    }

    pub fn new_with_mqtt(
        ingress: Arc<Ingress>,
        shutdown: CancellationToken,
        mqtt: Arc<MqttBroker>,
    ) -> Arc<Self> {
        let limits = ingress.limits.clone();
        Arc::new(Self {
            admin: None,
            shutdown,
            control_lock: Arc::new(tokio::sync::Mutex::new(())),
            http_slots: Arc::new(tokio::sync::Semaphore::new(limits.max_ingress)),
            router: Arc::new(CommandRouter {
                ingress: ingress.clone(),
            }),
            connections: Connections::new(limits.clone(), ingress.metrics.clone()),
            rates: Arc::new(RateLimiter::new(limits.clone())),
            protocol_admission: Admission::new(limits.clone()),
            mqtt,
            ingress,
        })
    }
}
/// Incremental buffer survives cancellation of `read_more` in a select loop.
pub struct Reader {
    pub buffer: BytesMut,
    started: Option<Instant>,
    maximum: usize,
    read_timeout: Duration,
}
impl Reader {
    pub fn new(maximum: usize, timeout_ms: u64) -> Self {
        Self {
            buffer: BytesMut::with_capacity(maximum.min(4096)),
            started: None,
            maximum,
            read_timeout: Duration::from_millis(timeout_ms),
        }
    }
    pub fn consumed(&mut self) {
        // Buffered tail bytes have already arrived. Processing the preceding
        // packet must not grant them a fresh read budget.
        if self.buffer.is_empty() {
            self.started = None;
            // A rare large frame must not pin its allocation for the lifetime of
            // an otherwise idle connection. The threshold avoids churn for normal traffic.
            if self.buffer.capacity() > 16 * 1024 {
                self.buffer = BytesMut::with_capacity(self.maximum.min(4096));
            }
        }
    }
    pub async fn read_more(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + ?Sized),
        idle_deadline: Instant,
    ) -> Result<()> {
        let deadline = self
            .started
            .map(|t| t + self.read_timeout)
            .unwrap_or(idle_deadline)
            .min(idle_deadline);
        // Tokio timeout polls the I/O future first: ready bytes alone must not
        // let an already expired incomplete packet escape the whole deadline.
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        let room = self
            .maximum
            .checked_sub(self.buffer.len())
            .ok_or(Error::Invalid)?;
        if room == 0 {
            return Err(Error::Invalid);
        }
        let mut chunk = [0u8; 4096];
        let n = room.min(chunk.len());
        let read = tokio::time::timeout_at(deadline, stream.read(&mut chunk[..n]))
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Unavailable)?;
        if read == 0 {
            return Err(Error::Unavailable);
        }
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
        self.buffer.extend_from_slice(&chunk[..read]);
        Ok(())
    }
}
pub async fn write(
    stream: &mut (impl AsyncWrite + Unpin + ?Sized),
    bytes: &[u8],
    timeout_ms: u64,
) -> Result<()> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), stream.write_all(bytes))
        .await
        .map_err(|_| Error::Timeout)?
        .map_err(|_| Error::Unavailable)
}
/// Authentication does not own a session yet. Keep observing EOF and shutdown;
/// any pipelined bytes stay in the same bounded connection reader.
pub async fn authenticate_stream(
    s: &Services,
    request: AuthenticationRequest<'_>,
    reader: &mut Reader,
    stream: &mut BoxStream,
    stop: &CancellationToken,
) -> Result<AuthenticatedSessionCandidate> {
    let auth = s.ingress.authenticate_session(request);
    tokio::pin!(auth);
    let until = Instant::now() + Duration::from_millis(s.ingress.limits.authentication_timeout_ms);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(Error::Draining),
            read = reader.read_more(stream, until) => read?,
            result = &mut auth => {
                if s.ingress.is_draining() { return Err(Error::Draining); }
                return result;
            }
        }
    }
}
pub async fn serve_stream(
    listener: TcpListener,
    transport: Transport,
    services: Arc<Services>,
    tls: Option<TlsAcceptor>,
    stop: CancellationToken,
) -> Result<()> {
    serve_listener(
        listener,
        ListenerKind::Device(transport),
        services,
        tls,
        stop,
    )
    .await
}

/// One device listener for MQTT and framed TCP.
pub async fn serve_device_ingress(
    listener: TcpListener,
    services: Arc<Services>,
    tls: Option<TlsAcceptor>,
    stop: CancellationToken,
) -> Result<()> {
    serve_listener(listener, ListenerKind::DeviceIngress, services, tls, stop).await
}

/// Independent control-plane listener, never dispatched by device classification.
pub async fn serve_management_http(
    listener: TcpListener,
    services: Arc<Services>,
    tls: Option<TlsAcceptor>,
    stop: CancellationToken,
) -> Result<()> {
    serve_listener(listener, ListenerKind::Management, services, tls, stop).await
}

#[derive(Clone, Copy, Debug)]
enum ListenerKind {
    DeviceIngress,
    Device(Transport),
    Management,
}

async fn serve_listener(
    listener: TcpListener,
    kind: ListenerKind,
    services: Arc<Services>,
    tls: Option<TlsAcceptor>,
    stop: CancellationToken,
) -> Result<()> {
    let mut tasks = JoinSet::new();
    let l = &services.ingress.limits;
    loop {
        tokio::select! {biased;
            _=stop.cancelled()=>break,
            completed=tasks.join_next(),if !tasks.is_empty()=>{if let Some(Err(e))=completed{tracing::warn!(error=%e,"connection task failed");}},
            accepted=listener.accept()=>{
                let (socket,peer)=accepted.map_err(|_|Error::Unavailable)?;
                if tasks.len()>=l.max_connections||services.rates.take(peer.ip()).is_err(){services.ingress.metrics.inc(Metric::ConnectionsRejected);continue;}
                let reservation = if matches!(kind, ListenerKind::DeviceIngress) {
                    services.connections.acquire_device_pending(peer.ip())
                } else {
                    services.connections.acquire_pending(peer.ip())
                };
                let lease=match reservation{Ok(l)=>l,Err(_)=>{services.ingress.metrics.inc(Metric::ConnectionsRejected);continue;}};
                tasks.spawn(serve_accepted(socket, peer, kind, services.clone(), lease, tls.clone(), stop.child_token()));
            }
        }
    }
    drop(listener);
    if tokio::time::timeout(Duration::from_millis(l.shutdown_timeout_ms), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}
async fn serve_accepted(
    socket: TcpStream,
    peer: SocketAddr,
    kind: ListenerKind,
    services: Arc<Services>,
    lease: PendingConnectionLease,
    tls: Option<TlsAcceptor>,
    stop: CancellationToken,
) {
    let _ = socket.set_nodelay(true);
    let result = async {
        let deadline = lease.connect_deadline();
        let stream: BoxStream = if let Some(tls) = tls {
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(Error::Draining),
                result = tokio::time::timeout_at(deadline, tls.accept(socket)) => {
                    Box::new(result.map_err(|_| Error::Timeout)?.map_err(|error| {
                        tracing::debug!(%error, "TLS handshake rejected");
                        Error::Invalid
                    })?)
                }
            }
        } else {
            Box::new(socket)
        };
        if matches!(kind, ListenerKind::Management) {
            return crate::management_http::connection(stream, peer, services.clone(), lease.into_management()?, stop).await;
        }
        let (classified, stream) = if let ListenerKind::Device(transport) = kind {
            (transport, stream)
        } else {
            let limits = &services.ingress.limits;
            let result = tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(Error::Draining),
                result = crate::classifier::classify_device_stream(stream, limits.max_tcp_frame_size, limits.max_mqtt_packet_size, deadline) => result,
            };
            result.map_err(|reason| {
                services.ingress.metrics.inc(if matches!(reason, crate::classifier::DetectionError::Timeout) {
                    Metric::ProtocolDetectionTimeouts
                } else {
                    Metric::ProtocolDetectionFailures
                });
                tracing::debug!(?reason, "device protocol detection rejected");
                reason.error()
            })?
        };
        let lease = lease.classify(classified).inspect_err(|_| {
            services.ingress.metrics.inc(Metric::ConnectionsRejected);
        })?;
        match classified {
            Transport::Mqtt => crate::mqtt::connection(stream, services.clone(), lease, stop).await,
            Transport::Tcp => crate::tcp::connection(stream, services.clone(), lease, stop).await,
            Transport::Udp => Err(Error::Invalid),
        }
    }.await;
    if let Err(error) = result {
        if matches!(error, Error::Timeout) {
            services.ingress.metrics.inc(Metric::Timeouts);
        }
        tracing::debug!(?kind, %error, "connection closed");
    }
}

pub fn local_addr(listener: &TcpListener) -> Result<SocketAddr> {
    listener.local_addr().map_err(|_| Error::Unavailable)
}

#[cfg(test)]
mod audit_deadlines {
    use super::*;
    #[test]
    fn large_empty_read_buffer_is_replaced_with_small_initial_capacity() {
        let mut reader = Reader::new(65_536, 30);
        assert!(reader.buffer.capacity() <= 4_096);
        reader.buffer.reserve(32_768);
        reader.buffer.extend_from_slice(&[0; 32_768]);
        reader.buffer.clear();
        reader.consumed();
        assert!(reader.buffer.capacity() <= 4_096);
    }
    #[tokio::test(start_paused = true)]
    async fn audit_expired_packet_rejects_even_ready_socket_bytes() {
        let (mut peer, mut stream) = tokio::io::duplex(64);
        let mut reader = Reader::new(64, 30);
        let idle = Instant::now() + Duration::from_secs(10);
        peer.write_all(b"a").await.unwrap();
        reader.read_more(&mut stream, idle).await.unwrap();
        tokio::time::advance(Duration::from_millis(31)).await;
        peer.write_all(b"b").await.unwrap();
        assert!(matches!(
            reader.read_more(&mut stream, idle).await,
            Err(Error::Timeout)
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn audit_partial_next_packet_keeps_original_deadline() {
        let (mut peer, mut stream) = tokio::io::duplex(64);
        let mut reader = Reader::new(64, 30);
        let idle = Instant::now() + Duration::from_secs(10);
        peer.write_all(&[0xc0, 0, 0x30]).await.unwrap();
        reader.read_more(&mut stream, idle).await.unwrap();
        let _ping = reader.buffer.split_to(2);
        tokio::time::advance(Duration::from_millis(20)).await;
        reader.consumed();
        tokio::time::advance(Duration::from_millis(11)).await;
        peer.write_all(&[0x03]).await.unwrap();
        assert!(matches!(
            reader.read_more(&mut stream, idle).await,
            Err(Error::Timeout)
        ));
    }
}
