use crate::mqtt::topics::Subscriptions;
use bytes::BytesMut;
use netbaiot_core::Transport;
use netbaiot_runtime::*;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
    time::Instant,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub type BoxStream = Box<dyn Stream>;
pub struct Services {
    pub admin: Option<AdminAccess>,
    pub http_slots: Arc<tokio::sync::Semaphore>,
    pub ingress: Arc<Ingress>,
    pub router: Arc<CommandRouter>,
    pub connections: Arc<Connections>,
    pub rates: RateLimiter,
    pub protocol_admission: Arc<Admission>,
    pub subscriptions: Arc<Subscriptions>,
}
impl Services {
    pub fn new(ingress: Arc<Ingress>) -> Arc<Self> {
        let limits = ingress.limits.clone();
        Arc::new(Self {
            admin: None,
            http_slots: Arc::new(tokio::sync::Semaphore::new(limits.max_ingress)),
            router: Arc::new(CommandRouter {
                ingress: ingress.clone(),
            }),
            connections: Connections::new(limits.clone(), ingress.metrics.clone()),
            rates: RateLimiter::new(limits.clone()),
            protocol_admission: Admission::new(limits.clone()),
            subscriptions: Subscriptions::new(limits),
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
) -> Result<netbaiot_core::AuthenticatedDevice> {
    let auth = s.ingress.authenticate(request);
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
    let mut tasks = JoinSet::new();
    let l = &services.ingress.limits;
    loop {
        tokio::select! {biased;
            _=stop.cancelled()=>break,
            completed=tasks.join_next(),if !tasks.is_empty()=>{if let Some(Err(e))=completed{tracing::warn!(error=%e,"connection task failed");}},
            accepted=listener.accept()=>{
                let (socket,peer)=accepted.map_err(|_|Error::Unavailable)?;
                if tasks.len()>=l.max_connections||services.rates.take(peer.ip()).is_err(){services.ingress.metrics.inc(Metric::ConnectionsRejected);continue;}
                let lease=match services.connections.acquire(peer.ip(),transport){Ok(l)=>l,Err(_)=>{services.ingress.metrics.inc(Metric::ConnectionsRejected);continue;}};
                let svc=services.clone();let cancel=stop.child_token();let tls=tls.clone();
                tasks.spawn(async move{
                    let _=socket.set_nodelay(true);
                    let result=async{
                        let stream:BoxStream=if let Some(tls)=tls{let handshake=tokio::time::timeout(Duration::from_millis(svc.ingress.limits.connect_timeout_ms),tls.accept(socket));tokio::select!{_ = cancel.cancelled()=>return Err(Error::Draining),result=handshake=>Box::new(result.map_err(|_|Error::Timeout)?.map_err(|_|Error::Invalid)?),}}else{Box::new(socket)};
                        match transport{Transport::Http=>crate::http::connection(stream,peer,svc.clone(),lease,cancel).await,Transport::Mqtt=>crate::mqtt::connection(stream,svc.clone(),lease,cancel).await,Transport::Tcp=>crate::tcp::connection(stream,svc.clone(),lease,cancel).await,Transport::Udp=>Err(Error::Invalid)}
                    }.await;
                    if let Err(e)=result{if matches!(e,Error::Timeout){svc.ingress.metrics.inc(Metric::Timeouts);}tracing::debug!(transport=?transport,error=%e,"connection closed");}
                });
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
pub fn local_addr(listener: &TcpListener) -> Result<SocketAddr> {
    listener.local_addr().map_err(|_| Error::Unavailable)
}

#[cfg(test)]
mod audit_deadlines {
    use super::*;
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
