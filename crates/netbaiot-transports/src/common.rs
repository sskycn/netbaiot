use crate::mqtt::broker::MqttBroker;
use bytes::{BufMut, Bytes, BytesMut};
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
    pub management_auth: Option<Arc<ManagementAuthService>>,
    pub business_auth: Option<Arc<BusinessRpcRegistry>>,
    pub shutdown: CancellationToken,
    pub control_lock: Arc<tokio::sync::Mutex<()>>,
    pub http_slots: Arc<tokio::sync::Semaphore>,
    pub tls_handshakes: Arc<tokio::sync::Semaphore>,
    pub ingress: Arc<Ingress>,
    pub router: Arc<CommandRouter>,
    pub commands: Arc<CommandService>,
    pub connections: Arc<Connections>,
    pub rates: Arc<RateLimiter>,
    pub management_request_rates: Arc<RateLimiter>,
    pub protocol_admission: Arc<Admission>,
    pub protocol_control_admission: Arc<Admission>,
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
        let router = Arc::new(CommandRouter::new(ingress.clone()));
        Arc::new(Self {
            admin: None,
            management_auth: None,
            business_auth: None,
            shutdown,
            control_lock: Arc::new(tokio::sync::Mutex::new(())),
            http_slots: Arc::new(tokio::sync::Semaphore::new(limits.max_ingress)),
            tls_handshakes: Arc::new(tokio::sync::Semaphore::new(limits.max_tls_handshakes)),
            commands: Arc::new(CommandService::new(router.clone())),
            router,
            connections: Connections::new(limits.clone(), ingress.metrics.clone()),
            rates: Arc::new(RateLimiter::new(limits.clone())),
            management_request_rates: Arc::new(RateLimiter::new(limits.clone())),
            protocol_admission: Admission::new(limits.clone()),
            protocol_control_admission: Admission::new(limits.clone()),
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
    budget: Option<ConnectionReadBudget>,
    allocation: Option<Arc<ReadAllocationPermit>>,
}
struct ReadFrame {
    bytes: Bytes,
    _allocation: Arc<ReadAllocationPermit>,
}
impl AsRef<[u8]> for ReadFrame {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Reader {
    pub fn new(maximum: usize, timeout_ms: u64) -> Self {
        Self {
            buffer: BytesMut::new(),
            started: None,
            maximum,
            read_timeout: Duration::from_millis(timeout_ms),
            budget: None,
            allocation: None,
        }
    }
    pub fn with_budget(maximum: usize, timeout_ms: u64, budget: ConnectionReadBudget) -> Self {
        Self {
            budget: Some(budget),
            ..Self::new(maximum, timeout_ms)
        }
    }
    pub fn authenticate(&mut self, device: &netbaiot_core::DeviceKey) -> Result<()> {
        if let Some(budget) = &mut self.budget {
            budget.authenticate(device)?;
        }
        Ok(())
    }
    /// Decoded zero-copy fields retain the allocation's byte charge until their
    /// last alias is dropped, even if the reader or connection has gone away.
    pub fn take_frame(&mut self, length: usize) -> Result<Bytes> {
        if length > self.buffer.len() {
            return Err(Error::Invalid);
        }
        let frame = self.buffer.split_to(length).freeze();
        Ok(self.own_frame(frame))
    }
    pub fn own_frame(&self, frame: Bytes) -> Bytes {
        if let Some(allocation) = &self.allocation {
            Bytes::from_owner(ReadFrame {
                bytes: frame,
                _allocation: allocation.clone(),
            })
        } else {
            frame
        }
    }
    fn allocate(
        &mut self,
        capacity: usize,
    ) -> Result<(BytesMut, Option<Arc<ReadAllocationPermit>>)> {
        if capacity > self.maximum {
            return Err(Error::Invalid);
        }
        let allocation = self
            .budget
            .as_mut()
            .map(|budget| budget.reserve_allocation(capacity).map(Arc::new))
            .transpose()?;
        let buffer = Self::checked_buffer(capacity)?;
        Ok((buffer, allocation))
    }
    fn checked_buffer(capacity: usize) -> Result<BytesMut> {
        let buffer = BytesMut::with_capacity(capacity);
        // Do not silently undercharge a dependency/allocator capacity change.
        // The current BytesMut uses Vec::with_capacity with the exact request.
        if buffer.capacity() != capacity {
            return Err(Error::Internal);
        }
        Ok(buffer)
    }
    fn grow(&mut self, target: usize) -> Result<()> {
        let room = target
            .checked_sub(self.buffer.len())
            .ok_or(Error::Invalid)?;
        // A split buffer can have a large charged allocation behind a small
        // visible tail. Reclaim its prefix without allocating, when unshared.
        if self.buffer.try_reclaim(room) {
            return Ok(());
        }
        if self.buffer.len() <= 8 {
            let exclusive = self.allocation.as_mut().and_then(Arc::get_mut);
            if let (Some(budget), Some(previous)) = (&mut self.budget, exclusive) {
                // Reserve the larger allocation before touching the prefix.
                // The eight-byte stack staging fits the fixed allowance and
                // lets an aggregate budget fit exactly one maximum frame.
                let capacity = target.max(previous.capacity());
                budget.expand_allocation(previous, capacity)?;
                let mut prefix = [0; 8];
                let length = self.buffer.len();
                prefix[..length].copy_from_slice(&self.buffer);
                self.buffer = BytesMut::new();
                let mut next = match Self::checked_buffer(capacity) {
                    Ok(next) => next,
                    Err(error) => {
                        self.allocation = None;
                        return Err(error);
                    }
                };
                next.extend_from_slice(&prefix[..length]);
                self.buffer = next;
                return Ok(());
            }
        }
        let (allocation, replacement) = if let Some(budget) = &mut self.budget {
            if let Some(previous) = self.allocation.as_mut().and_then(Arc::get_mut) {
                (None, Some(budget.reserve_replacement(target, previous)?))
            } else {
                (Some(Arc::new(budget.reserve_allocation(target)?)), None)
            }
        } else {
            (None, None)
        };
        let mut next = Self::checked_buffer(target)?;
        next.extend_from_slice(&self.buffer);
        // No mutation before every resource check succeeds. Release the old
        // physical allocation before its permit, then end the exclusive copy.
        self.buffer = next;
        self.allocation = allocation;
        if let Some(replacement) = replacement {
            match replacement.commit() {
                Ok(allocation) => self.allocation = Some(Arc::new(allocation)),
                Err((error, replacement)) => {
                    self.buffer = BytesMut::new();
                    drop(replacement);
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub fn consumed(&mut self) {
        // Buffered tail bytes have already arrived. Processing the preceding
        // packet must not grant them a fresh read budget.
        if self.buffer.is_empty() {
            self.started = None;
            // A completed packet must not pin its read allocation for the
            // lifetime of an otherwise idle connection, including small packets.
            self.buffer = BytesMut::new();
            self.allocation = None;
        }
    }
    pub async fn read_more(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + ?Sized),
        idle_deadline: Instant,
    ) -> Result<()> {
        let target = self.buffer.len().saturating_add(4096).min(self.maximum);
        self.read_more_to(stream, idle_deadline, target).await
    }
    /// The protocol validates its header before asking for a complete frame.
    /// No socket read is allowed to grow beyond this checked target.
    pub async fn read_more_to(
        &mut self,
        stream: &mut (impl AsyncRead + Unpin + ?Sized),
        idle_deadline: Instant,
        target: usize,
    ) -> Result<()> {
        let deadline = self.deadline(idle_deadline);
        // Tokio timeout polls the I/O future first: ready bytes alone must not
        // let an already expired incomplete packet escape the whole deadline.
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        if target > self.maximum {
            return Err(Error::Invalid);
        }
        let room = target
            .checked_sub(self.buffer.len())
            .ok_or(Error::Invalid)?;
        if room == 0 {
            return Err(Error::Invalid);
        }
        let read = if self.buffer.is_empty() {
            // The idle future retains eight bytes, and no heap allocation. Most
            // MQTT control packets fit here. Allocate only after bytes arrive.
            let mut first = [0u8; 8];
            let n = room.min(first.len());
            let read = tokio::time::timeout_at(deadline, stream.read(&mut first[..n]))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Unavailable)?;
            if read != 0 {
                let (buffer, allocation) = self.allocate(read)?;
                self.buffer = buffer;
                self.allocation = allocation;
                self.buffer.extend_from_slice(&first[..read]);
            }
            read
        } else {
            if self.buffer.capacity() < target {
                self.grow(target)?;
            }
            let mut destination = (&mut self.buffer).limit(room);
            tokio::time::timeout_at(deadline, stream.read_buf(&mut destination))
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(|_| Error::Unavailable)?
        };
        // timeout_at polls ready I/O before its timer, including after a
        // suspended read is rescheduled. Never accept bytes past this deadline.
        if Instant::now() >= deadline {
            return Err(Error::Timeout);
        }
        if read == 0 {
            return Err(Error::Unavailable);
        }
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
        Ok(())
    }
    fn deadline(&self, idle_deadline: Instant) -> Instant {
        self.started
            .map(|t| t + self.read_timeout)
            .unwrap_or(idle_deadline)
            .min(idle_deadline)
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
        if reader.buffer.len() == reader.maximum {
            // A legal pipelined frame may fill the pre-auth reader exactly.
            // Do not ask read_more for zero room and reject ready auth. A
            // bounded probe still observes EOF or additional overload bytes.
            let mut probe = [0; 1];
            let deadline = reader.deadline(until);
            if Instant::now() >= deadline {
                return Err(Error::Timeout);
            }
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(Error::Draining),
                result = &mut auth => {
                    if Instant::now() >= deadline { return Err(Error::Timeout); }
                    if s.ingress.is_draining() { return Err(Error::Draining); }
                    return result;
                }
                _ = tokio::time::sleep_until(deadline) => return Err(Error::Timeout),
                read = stream.read(&mut probe) => {
                    let read = read.map_err(|_| Error::Unavailable)?;
                    return Err(if read == 0 { Error::Unavailable } else { Error::Overloaded });
                }
            }
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(Error::Draining),
            read = reader.read_more(stream, until) => read?,
            result = &mut auth => {
                if Instant::now() >= reader.deadline(until) { return Err(Error::Timeout); }
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
                // Connections observe listener shutdown; live session takeover has
                // its own cancellation owner. No per-connection child tree is needed.
                tasks.spawn(serve_accepted(socket, peer, kind, services.clone(), lease, tls.clone(), stop.clone()));
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
        let (stream, certificate): (BoxStream, Option<Vec<u8>>) = if let Some(tls) = tls {
            // Do not create tasks waiting for handshake capacity. Established TLS
            // connections release this slot before authentication and normal reads.
            let _handshake = services.tls_handshakes.clone().try_acquire_owned()
                .map_err(|_| {
                    services.ingress.metrics.inc(Metric::ConnectionsRejected);
                    Error::Overloaded
                })?;
            tokio::select! {
                biased;
                _ = stop.cancelled() => return Err(Error::Draining),
                result = tokio::time::timeout_at(deadline, tls.accept(socket)) => {
                    let accepted = result.map_err(|_| Error::Timeout)?.map_err(|error| {
                        tracing::debug!(%error, "TLS handshake rejected");
                        Error::Invalid
                    })?;
                    let certificate = if matches!(kind, ListenerKind::Management) {
                        match accepted.get_ref().1.peer_certificates().and_then(|certs| certs.first()) {
                            Some(cert) if cert.as_ref().len() <= 65_536 => Some(cert.as_ref().to_vec()),
                            Some(_) => return Err(Error::Invalid),
                            None => None,
                        }
                    } else { None };
                    (Box::new(accepted), certificate)
                }
            }
        } else {
            (Box::new(socket), None)
        };
        if matches!(kind, ListenerKind::Management) {
            return crate::management_http::connection(stream, peer, services.clone(), lease.into_management()?, stop, certificate).await;
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
#[path = "connection_density_profile.rs"]
mod connection_density_profile;

#[cfg(test)]
#[path = "reader_regressions.rs"]
mod reader_regressions;

#[cfg(test)]
mod audit_deadlines {
    use super::*;
    #[test]
    fn empty_read_buffer_releases_every_allocation() {
        let mut reader = Reader::new(65_536, 30);
        assert_eq!(reader.buffer.capacity(), 0);
        reader.buffer.reserve(32_768);
        reader.buffer.extend_from_slice(&[0; 32_768]);
        reader.buffer.clear();
        reader.consumed();
        assert_eq!(reader.buffer.capacity(), 0);
    }
    #[tokio::test]
    async fn maximum_frame_growth_admits_after_releasing_inline_header_and_alias_keeps_charge() {
        let limits = Arc::new(Limits::default());
        let maximum = limits.max_read_buffer_per_connection;
        let connections = Connections::new(limits, Arc::new(Metrics::default()));
        let mut lease = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Tcp)
            .unwrap();
        let tracking = lease.read_budget();
        let mut reader = Reader::with_budget(maximum, 30_000, lease.read_budget());
        let payload = vec![7; maximum];
        let mut stream = payload.as_slice();
        let idle = Instant::now() + Duration::from_secs(10);
        reader.read_more_to(&mut stream, idle, 4).await.unwrap();
        assert_eq!(reader.buffer.capacity(), 4);
        assert_eq!(tracking.capacity().unwrap(), 4);
        reader
            .read_more_to(&mut stream, idle, maximum)
            .await
            .unwrap();
        assert_eq!(reader.buffer.len(), maximum);
        assert_eq!(reader.buffer.capacity(), maximum);
        assert_eq!(tracking.capacity().unwrap(), maximum);
        let frame = reader.take_frame(maximum).unwrap();
        let alias = frame.slice(5..10);
        reader.consumed();
        assert_eq!(reader.buffer.capacity(), 0);
        drop(reader);
        drop(lease);
        drop(frame);
        assert_eq!(tracking.capacity().unwrap(), maximum);
        assert_eq!(&alias[..], &[7; 5]);
        drop(alias);
        assert_eq!(tracking.capacity().unwrap(), 0);
    }
    #[tokio::test]
    async fn checked_target_rejects_oversize_without_allocation() {
        let mut reader = Reader::new(8, 30_000);
        let mut stream = &[1; 9][..];
        let idle = Instant::now() + Duration::from_secs(10);
        assert!(matches!(
            reader.read_more_to(&mut stream, idle, 9).await,
            Err(Error::Invalid)
        ));
        assert_eq!(reader.buffer.capacity(), 0);
        reader.read_more_to(&mut stream, idle, 8).await.unwrap();
        assert!(matches!(
            reader.read_more_to(&mut stream, idle, 8).await,
            Err(Error::Invalid)
        ));
        assert_eq!(&reader.buffer[..], &[1; 8]);
    }
    #[tokio::test(start_paused = true)]
    async fn cancelled_direct_read_keeps_prefix_and_packet_deadline() {
        let (mut peer, mut stream) = tokio::io::duplex(64);
        let mut reader = Reader::new(64, 30);
        let idle = Instant::now() + Duration::from_secs(10);
        peer.write_all(b"head").await.unwrap();
        reader.read_more_to(&mut stream, idle, 4).await.unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                reader.read_more_to(&mut stream, idle, 32),
            )
            .await
            .is_err()
        );
        assert_eq!(&reader.buffer[..], b"head");
        peer.write_all(b"tail").await.unwrap();
        reader.read_more_to(&mut stream, idle, 32).await.unwrap();
        assert_eq!(&reader.buffer[..], b"headtail");
        tokio::time::advance(Duration::from_millis(21)).await;
        peer.write_all(b"late").await.unwrap();
        assert!(matches!(
            reader.read_more_to(&mut stream, idle, 32).await,
            Err(Error::Timeout)
        ));
        assert_eq!(&reader.buffer[..], b"headtail");
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
