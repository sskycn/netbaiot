//! V3 is a separate binary stream engine. V2 frame parsing stays in the parent module.
use super::*;
use bytes::Bytes;
use netbaiot_core::business_rpc_v3::{
    BUSINESS_RPC_V3_VERSION, V3_END_STREAM, V3Accept, V3Bootstrap, V3EventAck, V3EventStatus,
    V3FrameType, V3GoAway, V3Limits, V3Open, V3Reset, V3ResetCode, V3Response,
};
use netbaiot_v3_mux::{
    DataClass, Frame, FrameReader, Initiator, MuxError, MuxScheduler, ReassemblyBudget, StreamTable,
};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, sync::OnceLock};

const CONNECTION_REASSEMBLY_BYTES: usize = 16 * 1024 * 1024;
const GLOBAL_REASSEMBLY_BYTES: usize = 128 * 1024 * 1024;
const OUTBOUND_BYTES: usize = 16 * 1024 * 1024;
static REASSEMBLY: OnceLock<Arc<ReassemblyBudget>> = OnceLock::new();
struct ActiveV3(Arc<Metrics>);
impl Drop for ActiveV3 {
    fn drop(&mut self) {
        self.0.business_rpc_v3_connection_finished();
    }
}
struct StreamGauges {
    metrics: Arc<Metrics>,
    reported: (usize, usize),
}
impl StreamGauges {
    fn sync(&mut self, streams: &StreamTable) {
        let current = (streams.active(), streams.reserved_bytes());
        self.metrics
            .business_rpc_v3_connection_gauges(self.reported, current);
        self.reported = current;
    }
}
impl Drop for StreamGauges {
    fn drop(&mut self) {
        self.metrics
            .business_rpc_v3_connection_gauges(self.reported, (0, 0));
    }
}
struct WriterGauge {
    metrics: Arc<Metrics>,
    reported: usize,
}
impl WriterGauge {
    fn sync(&mut self, scheduler: &MuxScheduler) {
        let current = scheduler.queued_bytes();
        self.metrics
            .business_rpc_v3_writer_bytes(self.reported, current);
        self.reported = current;
    }
}
impl Drop for WriterGauge {
    fn drop(&mut self) {
        self.metrics.business_rpc_v3_writer_bytes(self.reported, 0);
    }
}

fn decode<T: DeserializeOwned>(frame: &Frame) -> Result<T> {
    if frame.payload.len() > 4096 {
        return Err(Error::Invalid);
    }
    serde_json::from_slice(&frame.payload).map_err(|_| Error::Invalid)
}
fn metadata<T: Serialize>(id: u32, ty: V3FrameType, value: &T, limits: &V3Limits) -> Result<Frame> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Internal)?;
    if bytes.len() > 4096 {
        return Err(Error::Overloaded);
    }
    Frame::new(
        id,
        ty,
        0,
        Bytes::from(bytes),
        limits.max_frame_payload_bytes as usize,
    )
    .map_err(|_| Error::Invalid)
}
fn fixed(id: u32, ty: V3FrameType, bytes: &[u8], limits: &V3Limits) -> Result<Frame> {
    Frame::new(
        id,
        ty,
        0,
        Bytes::copy_from_slice(bytes),
        limits.max_frame_payload_bytes as usize,
    )
    .map_err(|_| Error::Invalid)
}
fn reset(id: u32, code: V3ResetCode, limits: &V3Limits) -> Result<Frame> {
    metadata(
        id,
        V3FrameType::ResetStream,
        &V3Reset {
            code,
            message: String::new(),
        },
        limits,
    )
}

async fn bootstrap_write(
    io: &mut Box<dyn Io>,
    ready: &V3Bootstrap,
    timeout: Duration,
) -> Result<()> {
    let bytes = serde_json::to_vec(ready).map_err(|_| Error::Internal)?;
    if bytes.len() > BUSINESS_RPC_HELLO_MAX_BYTES {
        return Err(Error::Internal);
    }
    let length = u32::try_from(bytes.len()).map_err(|_| Error::Internal)?;
    tokio::time::timeout(timeout, async {
        io.write_all(&length.to_be_bytes())
            .await
            .map_err(|_| Error::Unavailable)?;
        io.write_all(&bytes).await.map_err(|_| Error::Unavailable)
    })
    .await
    .map_err(|_| Error::Timeout)?
}

fn principal_for(
    config: &BusinessRpcTransportConfig,
    certificate: Option<&[u8]>,
    token: Option<&str>,
) -> Result<BusinessPrincipal> {
    let principal = match (&config.identity, certificate) {
        (
            BusinessIdentity::Development {
                token_hash,
                principal,
            },
            None,
        ) => token
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
    }
    .ok_or(Error::Authentication)?;
    if principal
        .expires_at_ms
        .is_some_and(|expiry| expiry <= netbaiot_runtime::now_ms())
    {
        return Err(Error::Forbidden);
    }
    Ok(principal)
}

enum WriterMessage {
    Frame(Frame),
    GoAway {
        frame: Frame,
        done: oneshot::Sender<()>,
    },
    Body {
        id: u32,
        class: DataClass,
        body: Bytes,
        done: Option<oneshot::Sender<Result<()>>>,
        _permit: OwnedSemaphorePermit,
    },
    WindowUpdate {
        id: u32,
        increment: u32,
    },
    Reset {
        id: u32,
    },
}
type PendingWrite = (
    OwnedSemaphorePermit,
    Option<oneshot::Sender<Result<()>>>,
    Option<std::time::Instant>,
);

async fn writer_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut rx: mpsc::Receiver<WriterMessage>,
    limits: V3Limits,
    timeout: Duration,
    stop: CancellationToken,
    credit_tx: mpsc::Sender<(u32, u32)>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut scheduler =
        MuxScheduler::new(&limits, OUTBOUND_BYTES).map_err(|_| Error::Configuration)?;
    let mut gauge = WriterGauge {
        metrics: metrics.clone(),
        reported: 0,
    };
    let mut bodies = HashMap::<u32, PendingWrite>::new();
    let mut goaway: Option<(Frame, oneshot::Sender<()>, tokio::time::Instant)> = None;
    loop {
        // Admit a bounded batch, then write at least one scheduled frame. Continuous control
        // producers cannot starve DATA, and a large body is sliced only when selected.
        for _ in 0..16 {
            let Ok(message) = rx.try_recv() else {
                break;
            };
            let message = match message {
                WriterMessage::GoAway { frame, done } => {
                    goaway = Some((frame, done, tokio::time::Instant::now() + timeout));
                    break;
                }
                other => other,
            };
            apply_writer_message(&mut scheduler, &mut bodies, message)?;
            gauge.sync(&scheduler);
        }
        if let Some(frame) = scheduler.next_frame() {
            gauge.sync(&scheduler);
            let id = frame.header.stream_id;
            if frame.header.frame_type == V3FrameType::Data
                && let Some((_, _, queued_at)) = bodies.get_mut(&id)
                && let Some(queued_at) = queued_at.take()
            {
                metrics.observe(
                    Histogram::BusinessRpcV3SchedulerWait,
                    queued_at.elapsed().as_micros().min(u64::MAX as u128) as u64,
                );
            }
            tracing::debug!(stream_id = id, frame_type = ?frame.header.frame_type, payload_bytes = frame.payload.len(), "business RPC V3 sending frame");
            let completed =
                frame.header.frame_type == V3FrameType::Data && frame.header.flags == V3_END_STREAM;
            let sent = netbaiot_v3_mux::write_frame(&mut writer, &frame, timeout)
                .await
                .map_err(|_| Error::Unavailable);
            if sent.is_ok() {
                metrics.inc(Metric::BusinessRpcV3FramesSent);
                if frame.header.frame_type == V3FrameType::Data {
                    metrics.add(
                        Metric::BusinessRpcV3DataBytesSent,
                        frame.payload.len() as u64,
                    );
                }
            }
            if sent.is_ok() && frame.header.frame_type == V3FrameType::WindowUpdate {
                let increment = u32::from_be_bytes(
                    frame
                        .payload
                        .as_ref()
                        .try_into()
                        .map_err(|_| Error::Invalid)?,
                );
                credit_tx
                    .send((id, increment))
                    .await
                    .map_err(|_| Error::Unavailable)?;
            }
            if completed
                && let Some((_permit, done, _)) = bodies.remove(&id)
                && let Some(done) = done
            {
                let _ = done.send(sent.as_ref().map(|_| ()).map_err(|_| Error::Unavailable));
            }
            if completed {
                scheduler.reset(id);
            }
            sent?;
            continue;
        }
        let (connection_stall, stream_stall) = scheduler.stalled_windows();
        if connection_stall {
            metrics.inc(Metric::BusinessRpcV3ConnectionWindowStalls);
        }
        if stream_stall {
            metrics.inc(Metric::BusinessRpcV3StreamWindowStalls);
        }
        if let Some((frame, done, deadline)) = goaway.take() {
            // The reader has stopped admitting streams. Drain frames already queued by its
            // accepted work, but never wait indefinitely for peer flow credit.
            if scheduler.has_pending_frames() && tokio::time::Instant::now() < deadline {
                goaway = Some((frame, done, deadline));
                tokio::time::sleep_until(deadline).await;
                continue;
            }
            let result = netbaiot_v3_mux::write_frame(&mut writer, &frame, timeout)
                .await
                .map_err(|_| Error::Unavailable);
            let _ = done.send(());
            return result;
        }
        let message = tokio::select! {
            _ = stop.cancelled() => break,
            message = rx.recv() => match message { Some(value) => value, None => break },
        };
        let message = match message {
            WriterMessage::GoAway { frame, done } => {
                goaway = Some((frame, done, tokio::time::Instant::now() + timeout));
                continue;
            }
            other => other,
        };
        apply_writer_message(&mut scheduler, &mut bodies, message)?;
        gauge.sync(&scheduler);
    }
    Ok(())
}
fn apply_writer_message(
    scheduler: &mut MuxScheduler,
    bodies: &mut HashMap<u32, PendingWrite>,
    message: WriterMessage,
) -> Result<()> {
    match message {
        WriterMessage::Frame(frame) => scheduler
            .queue_control(frame)
            .map_err(|_| Error::Overloaded),
        WriterMessage::GoAway { .. } => Err(Error::Internal),
        WriterMessage::Body {
            id,
            class,
            body,
            done,
            _permit,
        } => {
            scheduler
                .queue_body(id, class, body)
                .map_err(|_| Error::Overloaded)?;
            bodies.insert(id, (_permit, done, Some(std::time::Instant::now())));
            Ok(())
        }
        WriterMessage::WindowUpdate { id, increment } => {
            match scheduler.window_update(id, increment) {
                Ok(()) | Err(MuxError::Stream) => Ok(()),
                Err(_) => Err(Error::Invalid),
            }
        }
        WriterMessage::Reset { id } => {
            scheduler.reset(id);
            bodies.remove(&id);
            Ok(())
        }
    }
}

fn send(tx: &mpsc::Sender<WriterMessage>, frame: Frame) -> Result<()> {
    tx.try_send(WriterMessage::Frame(frame))
        .map_err(|_| Error::Overloaded)
}
fn send_body(
    tx: &mpsc::Sender<WriterMessage>,
    budget: &Arc<Semaphore>,
    id: u32,
    class: DataClass,
    body: Bytes,
) -> Result<oneshot::Receiver<Result<()>>> {
    let size = u32::try_from(body.len()).map_err(|_| Error::Overloaded)?;
    let permit = budget
        .clone()
        .try_acquire_many_owned(size.max(1))
        .map_err(|_| Error::Overloaded)?;
    let (done, receive) = oneshot::channel();
    tx.try_send(WriterMessage::Body {
        id,
        class,
        body,
        done: Some(done),
        _permit: permit,
    })
    .map_err(|_| Error::Overloaded)?;
    Ok(receive)
}

pub(super) async fn connection(
    mut io: Box<dyn Io>,
    certificate: Option<Vec<u8>>,
    hello: Vec<u8>,
    config: BusinessRpcTransportConfig,
    services: Arc<BusinessRpcServices>,
    stop: CancellationToken,
) -> Result<()> {
    let server_limits = config.v3.clone().ok_or(Error::Invalid)?;
    let hello: V3Bootstrap = serde_json::from_slice(&hello).map_err(|_| Error::Invalid)?;
    hello.validate().map_err(|_| Error::Invalid)?;
    let V3Bootstrap::Hello {
        version,
        token,
        limits: requested,
    } = hello
    else {
        return Err(Error::Invalid);
    };
    if version != BUSINESS_RPC_V3_VERSION {
        return Err(Error::Invalid);
    }
    let principal = principal_for(&config, certificate.as_deref(), token.as_deref())?;
    let limits = server_limits
        .negotiate(&requested)
        .map_err(|_| Error::Invalid)?;
    let epoch = (Uuid::new_v4().as_u128() as u64).max(1);
    bootstrap_write(
        &mut io,
        &V3Bootstrap::Ready {
            version: 3,
            connection_epoch: epoch,
            limits: limits.clone(),
        },
        config.write_timeout,
    )
    .await?;
    let metrics = services.ingress.metrics.clone();
    metrics.business_rpc_v3_connection_started();
    let _active = ActiveV3(metrics.clone());
    let (reader, writer) = tokio::io::split(io);
    let (tx, rx) = mpsc::channel(256);
    let (credit_tx, credit_rx) = mpsc::channel(256);
    // Shutdown first closes ingress in the reader, then lets its queued work and GOAWAY
    // reach the sole writer. A child of `stop` would cancel the writer too early.
    let writer_stop = CancellationToken::new();
    let writer_closed = CancellationToken::new();
    let writer_task = tokio::spawn({
        let writer_closed = writer_closed.clone();
        let writer_stop = writer_stop.clone();
        let metrics = metrics.clone();
        let limits = limits.clone();
        let timeout = config.write_timeout;
        async move {
            let result =
                writer_loop(writer, rx, limits, timeout, writer_stop, credit_tx, metrics).await;
            writer_closed.cancel();
            result
        }
    });
    let result = reader_loop(
        reader,
        ReaderContext {
            tx,
            credit_rx,
            outbound: Arc::new(Semaphore::new(OUTBOUND_BYTES)),
            limits,
            principal,
            services,
            config,
            stop,
            writer_closed,
        },
    )
    .await;
    writer_stop.cancel();
    let _ = writer_task.await;
    if let Err(error) = &result {
        tracing::warn!(%error, "business RPC V3 connection closed abnormally");
        metrics.inc(Metric::BusinessRpcV3AbnormalClosures);
    }
    match &result {
        Err(Error::Invalid) => metrics.inc(Metric::BusinessRpcV3ProtocolErrors),
        Err(Error::Overloaded) => metrics.inc(Metric::BusinessRpcV3Overloads),
        _ => {}
    }
    result
}

struct PendingEvent {
    id: u32,
    delivery_id: netbaiot_core::DeliveryId,
    event_id: netbaiot_core::EventId,
    result: oneshot::Sender<std::result::Result<SinkAck, SinkError>>,
    written: Option<oneshot::Receiver<Result<()>>>,
    deadline: Option<tokio::time::Instant>,
    started: Option<std::time::Instant>,
}
async fn optional_recv<T>(receiver: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}
async fn event_written(pending: &mut Option<PendingEvent>) -> Option<Result<()>> {
    match pending
        .as_mut()
        .and_then(|pending| pending.written.as_mut())
    {
        Some(written) => written.await.ok(),
        None => std::future::pending().await,
    }
}
fn rpc_error_reply(
    tx: &mpsc::Sender<WriterMessage>,
    id: u32,
    code: RpcErrorCode,
    limits: &V3Limits,
) -> Result<()> {
    let value = V3Response {
        content_length: 0,
        error: Some(RpcError::new(code, "request rejected")),
    };
    let mut frame = metadata(id, V3FrameType::Response, &value, limits)?;
    frame.header.flags = V3_END_STREAM;
    send(tx, frame)
}
fn classify_stream_error(error: MuxError) -> Option<V3ResetCode> {
    match error {
        MuxError::Connection(_) | MuxError::Io | MuxError::Timeout => None,
        MuxError::Overloaded => Some(V3ResetCode::Overloaded),
        MuxError::MessageTooLarge => Some(V3ResetCode::MessageTooLarge),
        MuxError::FlowControl => Some(V3ResetCode::FlowControlError),
        MuxError::Stream => Some(V3ResetCode::ProtocolError),
    }
}
struct CancelWork<'a> {
    tx: &'a mpsc::Sender<WriterMessage>,
    services: &'a BusinessRpcServices,
    incoming_rpc: &'a mut HashMap<u32, (Uuid, String)>,
    control_requests: &'a mut HashMap<Uuid, u32>,
    outbound_auth: &'a mut HashMap<u32, (Uuid, String)>,
    response_meta: &'a mut HashMap<u32, V3Response>,
    pending_event: &'a mut Option<PendingEvent>,
}
fn cancel_stream_work(id: u32, epoch: u64, work: CancelWork<'_>) {
    let CancelWork {
        tx,
        services,
        incoming_rpc,
        control_requests,
        outbound_auth,
        response_meta,
        pending_event,
    } = work;
    let _ = tx.try_send(WriterMessage::Reset { id });
    incoming_rpc.remove(&id);
    control_requests.retain(|_, stream_id| *stream_id != id);
    response_meta.remove(&id);
    if let Some((request_id, method)) = outbound_auth.remove(&id) {
        let _ = services
            .registry
            .complete(epoch, request_id, &method, Err(Error::Unavailable));
    }
    if pending_event.as_ref().is_some_and(|event| event.id == id)
        && let Some(event) = pending_event.take()
    {
        let _ = event.result.send(Err(SinkError::Retryable));
    }
}
fn complete_response(
    id: u32,
    body: &[u8],
    provider: &Option<(u32, Arc<ProviderLease>)>,
    services: &BusinessRpcServices,
    outbound_auth: &mut HashMap<u32, (Uuid, String)>,
    response_meta: &mut HashMap<u32, V3Response>,
    pending_event: &mut Option<PendingEvent>,
) -> Result<()> {
    let response = response_meta.remove(&id).ok_or(Error::Invalid)?;
    if let Some((request_id, method)) = outbound_auth.remove(&id) {
        let result = if let Some(error) = response.error {
            Err(netbaiot_runtime::rpc_error_to_runtime(error.code))
        } else {
            serde_json::from_slice(body).map_err(|_| Error::Invalid)
        };
        let epoch = provider
            .as_ref()
            .map(|(_, lease)| lease.epoch())
            .unwrap_or(0);
        let _ = services
            .registry
            .complete(epoch, request_id, &method, result);
    } else if pending_event.as_ref().is_some_and(|event| event.id == id) {
        let event = pending_event.take().ok_or(Error::Invalid)?;
        let ack = serde_json::from_slice::<V3EventAck>(body).ok();
        let success = response.error.is_none()
            && ack.as_ref().is_some_and(|ack| {
                ack.status == V3EventStatus::Ok
                    && ack.delivery_id == event.delivery_id.0
                    && ack.event_id == event.event_id.0
            });
        if success {
            services.ingress.metrics.inc(Metric::BusinessRpcEventAcks);
            if let Some(started) = event.started {
                services.ingress.metrics.observe(
                    Histogram::BusinessRpcEventAckLatency,
                    started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                );
            }
        }
        let _ = event.result.send(if success {
            Ok(SinkAck)
        } else {
            Err(SinkError::Retryable)
        });
    }
    Ok(())
}

struct ReaderContext {
    tx: mpsc::Sender<WriterMessage>,
    credit_rx: mpsc::Receiver<(u32, u32)>,
    outbound: Arc<Semaphore>,
    limits: V3Limits,
    principal: BusinessPrincipal,
    services: Arc<BusinessRpcServices>,
    config: BusinessRpcTransportConfig,
    stop: CancellationToken,
    writer_closed: CancellationToken,
}
async fn reader_loop<R: AsyncRead + Unpin + Send + 'static>(
    reader: R,
    context: ReaderContext,
) -> Result<()> {
    let ReaderContext {
        tx,
        mut credit_rx,
        outbound,
        limits,
        principal,
        services,
        config,
        stop,
        writer_closed,
    } = context;
    let global = REASSEMBLY
        .get_or_init(|| ReassemblyBudget::new(GLOBAL_REASSEMBLY_BYTES))
        .clone();
    let mut streams = StreamTable::new(
        Initiator::Server,
        limits.clone(),
        CONNECTION_REASSEMBLY_BYTES,
        global,
    )
    .map_err(|_| Error::Configuration)?;
    let mut gauges = StreamGauges {
        metrics: services.ingress.metrics.clone(),
        reported: (0, 0),
    };
    let mut provider: Option<(u32, Arc<ProviderLease>)> = None;
    let mut auth_rx: Option<mpsc::Receiver<BusinessRpcOutbound>> = None;
    let mut control_tx: Option<mpsc::Sender<ControlRequest>> = None;
    let mut control_rx: Option<mpsc::Receiver<Queued>> = None;
    let mut control_worker: Option<tokio::task::JoinHandle<()>> = None;
    let confirmation = Arc::new(AtomicU64::new(0));
    let mut sync_ping: Option<u64> = None;
    let mut sync_written: Option<oneshot::Receiver<Result<()>>> = None;
    let mut subscription: Option<(u32, SubscriptionId, u64)> = None;
    let mut events_rx: Option<mpsc::Receiver<BusinessEventRequest>> = None;
    let mut pending_event: Option<PendingEvent> = None;
    let mut incoming_rpc = HashMap::<u32, (Uuid, String)>::new();
    let mut control_requests = HashMap::<Uuid, u32>::new();
    let mut outbound_auth = HashMap::<u32, (Uuid, String)>::new();
    let mut response_meta = HashMap::<u32, V3Response>::new();
    let mut frames = FrameReader::spawn(
        reader,
        limits.max_frame_payload_bytes as usize,
        config.read_timeout,
    );
    let expiry_deadline = principal.expires_at_ms.and_then(|expiry| {
        tokio::time::Instant::now().checked_add(Duration::from_millis(
            expiry.saturating_sub(netbaiot_runtime::now_ms()).max(0) as u64,
        ))
    });
    let mut draining: Option<tokio::time::Instant> = None;
    macro_rules! close_stream {
        ($target:expr) => {{
            let target = $target;
            let children = streams.reset(target).unwrap_or_default();
            let epoch = provider
                .as_ref()
                .map(|(_, lease)| lease.epoch())
                .unwrap_or(0);
            for child in std::iter::once(target).chain(children) {
                cancel_stream_work(
                    child,
                    epoch,
                    CancelWork {
                        tx: &tx,
                        services: &services,
                        incoming_rpc: &mut incoming_rpc,
                        control_requests: &mut control_requests,
                        outbound_auth: &mut outbound_auth,
                        response_meta: &mut response_meta,
                        pending_event: &mut pending_event,
                    },
                );
            }
            if provider
                .as_ref()
                .is_some_and(|(parent, _)| *parent == target)
            {
                provider = None;
                auth_rx = None;
                control_tx = None;
                control_rx = None;
                sync_ping = None;
                sync_written = None;
                confirmation.store(0, Ordering::Release);
                if let Some(worker) = control_worker.take() {
                    worker.abort();
                }
            }
            if subscription
                .as_ref()
                .is_some_and(|(parent, _, _)| *parent == target)
            {
                if let Some((_, _, generation)) = subscription.take() {
                    let _ = services.sink.release(generation);
                }
                events_rx = None;
            }
        }};
    }
    let result: Result<()> = async {
        loop {
        gauges.sync(&streams);
        // Once the last even ID is consumed, a new connection gets a fresh namespace.
        // The normal exit path sends GOAWAY NO_ERROR before the writer stops.
        if streams.local_ids_exhausted() {
            break Ok(());
        }
        if draining.is_some()
            && outbound_auth.is_empty()
            && pending_event.is_none()
            && control_requests.is_empty()
            && incoming_rpc.is_empty()
            && sync_ping.is_none()
            && sync_written.is_none()
        {
            break Ok(());
        }
        let event_deadline = pending_event.as_ref().and_then(|event| event.deadline);
        tokio::select! {
            biased;
            _ = stop.cancelled() => break Ok(()),
            _ = tokio::time::sleep_until(draining.unwrap_or_else(tokio::time::Instant::now)), if draining.is_some() => break Ok(()),
            _ = writer_closed.cancelled() => break Err(Error::Unavailable),
            credit = credit_rx.recv() => {
                let Some((id, increment)) = credit else { break Err(Error::Unavailable); };
                match streams.grant_credit(id, increment) {
                    Ok(()) | Err(MuxError::Stream) => {}
                    Err(_) => break Err(Error::Invalid),
                }
            }
            _ = tokio::time::sleep_until(expiry_deadline.unwrap_or_else(tokio::time::Instant::now)), if expiry_deadline.is_some() => break Err(Error::Forbidden),
            _ = tokio::time::sleep_until(event_deadline.unwrap_or_else(tokio::time::Instant::now)), if event_deadline.is_some() => {
                if let Some(event) = pending_event.as_ref() { let id = event.id; services.ingress.metrics.inc(Metric::BusinessRpcTimeouts); close_stream!(id); let _ = send(&tx, reset(id, V3ResetCode::Cancel, &limits)?); }
            }
            written = event_written(&mut pending_event), if pending_event.as_ref().is_some_and(|event| event.written.is_some()) => {
                if let Some(event) = pending_event.as_mut() {
                    event.written = None;
                    if written.is_some_and(|result| result.is_ok()) { event.started = Some(std::time::Instant::now()); event.deadline = Some(tokio::time::Instant::now() + config.event_ack_timeout); }
                    else if let Some(event) = pending_event.take() { let _ = event.result.send(Err(SinkError::Retryable)); }
                }
            }
            written = async { match sync_written.as_mut() { Some(written) => written.await.ok(), None => std::future::pending().await } }, if sync_written.is_some() => {
                sync_written = None;
                if written.is_some_and(|result| result.is_ok()) {
                    let nonce = Uuid::new_v4().as_u128() as u64;
                    sync_ping = Some(nonce);
                    send(&tx, fixed(0, V3FrameType::Ping, &nonce.to_be_bytes(), &limits)?)?;
                }
            }
            auth_call = optional_recv(&mut auth_rx) => {
                let Some(auth_call) = auth_call else { auth_rx = None; continue; };
                if matches!(&auth_call.call, BusinessRpcCall::Request { .. }) {
                    services.ingress.metrics.observe(Histogram::BusinessRpcQueueWait,
                        auth_call.queued_at.elapsed().as_micros().min(u64::MAX as u128) as u64);
                }
                match auth_call.call {
                    BusinessRpcCall::Request { request_id, method, deadline_ms, body } => {
                        let Some((parent, lease)) = provider.as_ref() else { continue; };
                        let parent = *parent;
                        let epoch = lease.epoch();
                        let bytes = match serde_json::to_vec(&body) {
                            Ok(bytes) => bytes,
                            Err(_) => { let _ = services.registry.complete(epoch, request_id, method, Err(Error::Invalid)); continue; }
                        };
                        let len = u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?;
                        let open = V3Open::Rpc { parent_stream_id: Some(parent), request_id, method: method.into(), deadline_ms, content_length: len };
                        let id = match streams.open_local(&open) {
                            Ok(id) => id,
                            Err(_) => { let _ = services.registry.complete(epoch, request_id, method, Err(Error::Overloaded)); continue; }
                        };
                        services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsOpened);
                        outbound_auth.insert(id, (request_id, method.into()));
                        send(&tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                        if send_body(&tx, &outbound, id, DataClass::Rpc, Bytes::from(bytes)).is_err() {
                            close_stream!(id);
                            send(&tx, reset(id, V3ResetCode::Overloaded, &limits)?)?;
                            continue;
                        }
                        streams.mark_local_end(id).map_err(|_| Error::Internal)?;
                    }
                    BusinessRpcCall::Cancel { request_id } => {
                        if let Some((&id, _)) = outbound_auth.iter().find(|(_, (id, _))| *id == request_id) {
                            send(&tx, reset(id, V3ResetCode::Cancel, &limits)?)?;
                            close_stream!(id);
                        }
                    }
                }
            }
            reply = optional_recv(&mut control_rx) => {
                let Some(reply) = reply else { control_rx = None; continue; };
                let BusinessRpcFrame::Response { request_id, method, body, error } = reply.frame else { continue; };
                let Some(id) = control_requests.remove(&request_id) else { continue; };
                let bytes = match body { Some(body) => serde_json::to_vec(&body).map_err(|_| Error::Internal)?, None => Vec::new() };
                let len = u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?;
                let meta = V3Response { content_length: len, error: error.clone() };
                let mut response = metadata(id, V3FrameType::Response, &meta, &limits)?;
                if bytes.is_empty() { response.header.flags = V3_END_STREAM; }
                send(&tx, response)?;
                if bytes.is_empty() { streams.mark_local_end(id).map_err(|_| Error::Invalid)?; }
                else {
                    let written = match send_body(&tx, &outbound, id, DataClass::Rpc, Bytes::from(bytes)) {
                        Ok(written) => written,
                        Err(_) => { close_stream!(id); send(&tx, reset(id, V3ResetCode::Overloaded, &limits)?)?; continue; }
                    };
                    streams.mark_local_end(id).map_err(|_| Error::Invalid)?;
                    if method == "auth.sync" && error.is_none() { sync_written = Some(written); }
                }
            }
            delivery = optional_recv(&mut events_rx), if pending_event.is_none() => {
                let Some(delivery) = delivery else { events_rx = None; continue; };
                let Some((parent, subscription_id, _)) = subscription.as_ref() else { let _ = delivery.result.send(Err(SinkError::Retryable)); continue; };
                let delivery_id = netbaiot_core::DeliveryId::generate();
                let event_id = delivery.delivery.event.event_id;
                let dto = EventDelivery { delivery_id, subscription_id: *subscription_id, event: (*delivery.delivery.event).clone(), attempt: delivery.delivery.attempt };
                let bytes = serde_json::to_vec(&dto).map_err(|_| Error::Overloaded)?;
                let len = u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?;
                let open = V3Open::EventDelivery { parent_stream_id: *parent, delivery_id: delivery_id.0, event_id: event_id.0, attempt: dto.attempt, content_length: len };
                let id = match streams.open_local(&open) {
                    Ok(id) => id,
                    Err(_) => { let _ = delivery.result.send(Err(SinkError::Retryable)); continue; }
                };
                services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsOpened);
                send(&tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                let written = match send_body(&tx, &outbound, id, DataClass::Event, Bytes::from(bytes)) {
                    Ok(written) => written,
                    Err(_) => { close_stream!(id); let _ = delivery.result.send(Err(SinkError::Retryable)); send(&tx, reset(id, V3ResetCode::Overloaded, &limits)?)?; continue; }
                };
                streams.mark_local_end(id).map_err(|_| Error::Invalid)?;
                pending_event = Some(PendingEvent { id, delivery_id, event_id, result: delivery.result, written: Some(written), deadline: None, started: None });
            }
            frame = frames.recv() => {
                let frame = match frame { Some(Ok(frame)) => frame, Some(Err(error)) => {
                    tracing::debug!(?error, "business RPC V3 frame read failed");
                    break Err(match error {
                        MuxError::Connection(_) => Error::Invalid,
                        MuxError::Timeout => Error::Timeout,
                        _ => Error::Unavailable,
                    })
                }, None => break Err(Error::Unavailable) };
                tracing::debug!(stream_id = frame.header.stream_id, frame_type = ?frame.header.frame_type, payload_bytes = frame.payload.len(), "business RPC V3 received frame");
                services.ingress.metrics.inc(Metric::BusinessRpcV3FramesReceived);
                if frame.header.frame_type == V3FrameType::Data {
                    services.ingress.metrics.add(Metric::BusinessRpcV3DataBytesReceived, frame.payload.len() as u64);
                }
                let id = frame.header.stream_id;
                match frame.header.frame_type {
                    V3FrameType::Open => {
                        if streams.check_peer_id(id).is_err() { break Err(Error::Invalid); }
                        if draining.is_some() {
                            streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                            send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?;
                            continue;
                        }
                        let open: V3Open = match decode(&frame) {
                            Ok(open) => open,
                            Err(_) => {
                                streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                                send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?;
                                continue;
                            }
                        };
                        if open.validate().is_err() {
                            streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                            send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?;
                            continue;
                        }
                        if let V3Open::Provider { provider_id } = &open {
                            if principal.provider_id.as_deref() != Some(provider_id) || provider.is_some() || !principal.provide_methods.iter().any(|method| method == "device.authenticate") {
                                streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                                send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?; continue;
                            }
                            if let Err(error) = streams.open_peer(id, &open) {
                                if let Some(code) = classify_stream_error(error) { send(&tx, reset(id, code, &limits)?)?; continue; }
                                break Err(Error::Invalid);
                            }
                            services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsOpened);
                            let (auth_tx, rx) = mpsc::channel(config.auth_max_inflight);
                            let lease = match services.registry.register_with_expiry(auth_tx, BusinessProviderScope { global: principal.global, tenants: principal.tenants.clone() }, principal.expires_at_ms) {
                                Ok(lease) => Arc::new(lease), Err(_) => { let _ = streams.reset(id); send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?; continue; }
                            };
                            let (work_tx, work_rx) = mpsc::channel(16);
                            let (reply_tx, reply_rx) = mpsc::channel(16);
                            let worker = tokio::spawn(control_loop(work_rx, reply_tx, Arc::new(Semaphore::new(16 * BUSINESS_RPC_AUTH_MAX_BYTES)), services.clone(), (lease.clone(), principal.clone()), confirmation.clone(), stop.child_token()));
                            send(&tx, metadata(id, V3FrameType::Accept, &V3Accept { provider_epoch: Some(lease.epoch()), sync_required: true }, &limits)?)?;
                            provider = Some((id, lease)); auth_rx = Some(rx); control_tx = Some(work_tx); control_rx = Some(reply_rx); control_worker = Some(worker);
                        } else if let V3Open::EventSubscription { subscription_id, filter } = &open {
                            if principal.sink_id.as_deref() != Some("tcp-rpc") || subscription.is_some() || filter.validate().is_err() || (!principal.global && filter.tenant.as_ref().is_none_or(|tenant| !principal.allows_tenant(tenant))) {
                                streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                                send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?; continue;
                            }
                            if let Err(error) = streams.open_peer(id, &open) {
                                if let Some(code) = classify_stream_error(error) { send(&tx, reset(id, code, &limits)?)?; continue; }
                                break Err(Error::Invalid);
                            }
                            services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsOpened);
                            let (event_tx, event_rx) = mpsc::channel(1);
                            let generation = match services.sink.claim(event_tx, filter.clone()) {
                                Ok(generation) => generation, Err(_) => { let _ = streams.reset(id); send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?; continue; }
                            };
                            send(&tx, metadata(id, V3FrameType::Accept, &V3Accept { provider_epoch: None, sync_required: false }, &limits)?)?;
                            subscription = Some((id, *subscription_id, generation)); events_rx = Some(event_rx);
                        } else if let V3Open::Rpc { parent_stream_id, request_id, method, deadline_ms, .. } = &open {
                            if !matches!(method.as_str(), "auth.sync" | "auth.invalidate") || !principal.call_methods.iter().any(|allowed| allowed == method) || provider.as_ref().map(|(parent, _)| *parent) != *parent_stream_id {
                                streams.refuse_peer(id).map_err(|_| Error::Invalid)?;
                                rpc_error_reply(&tx, id, RpcErrorCode::Forbidden, &limits)?; continue;
                            }
                            if *deadline_ms == 0 { streams.refuse_peer(id).map_err(|_| Error::Invalid)?; send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?; continue; }
                            if let Err(error) = streams.open_peer(id, &open) {
                                if let Some(code) = classify_stream_error(error) { send(&tx, reset(id, code, &limits)?)?; continue; }
                                break Err(Error::Invalid);
                            }
                            services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsOpened);
                            incoming_rpc.insert(id, (*request_id, method.clone()));
                        } else { streams.refuse_peer(id).map_err(|_| Error::Invalid)?; send(&tx, reset(id, V3ResetCode::RefusedStream, &limits)?)?; }
                    }
                    V3FrameType::Data => {
                        let received = match streams.receive_data(id, &frame.payload, frame.header.flags == V3_END_STREAM) {
                            Ok(received) => received,
                            Err(error) => {
                                if let Some(code) = classify_stream_error(error) { close_stream!(id); send(&tx, reset(id, code, &limits)?)?; continue; }
                                break Err(Error::Invalid);
                            }
                        };
                        if received.connection_update > 0 {
                            send(&tx, fixed(0, V3FrameType::WindowUpdate, &received.connection_update.to_be_bytes(), &limits)?)?;
                            if received.stream_update > 0 { send(&tx, fixed(id, V3FrameType::WindowUpdate, &received.stream_update.to_be_bytes(), &limits)?)?; }
                        }
                        if let Some(body) = received.complete {
                            if let Some((request_id, method)) = incoming_rpc.remove(&id) {
                                let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                                if let Some(work) = &control_tx {
                                    if work.try_send(ControlRequest { request_id, method, body }).is_ok() { control_requests.insert(request_id, id); }
                                    else { rpc_error_reply(&tx, id, RpcErrorCode::Overloaded, &limits)?; let _ = streams.reset(id); }
                                }
                            } else if outbound_auth.contains_key(&id) || pending_event.as_ref().is_some_and(|event| event.id == id) {
                                complete_response(id, &body, &provider, &services, &mut outbound_auth, &mut response_meta, &mut pending_event)?;
                            }
                        }
                    }
                    V3FrameType::Response => {
                        let response: V3Response = match decode::<V3Response>(&frame) {
                            Ok(response) if response.validate().is_ok() => response,
                            _ => { close_stream!(id); send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?; continue; }
                        };
                        if !outbound_auth.contains_key(&id) && pending_event.as_ref().is_none_or(|event| event.id != id) {
                            send(&tx, reset(id, V3ResetCode::StreamClosed, &limits)?)?; continue;
                        }
                        if let Err(error) = streams.mark_response(id, response.content_length as usize) {
                            if let Some(code) = classify_stream_error(error) { close_stream!(id); send(&tx, reset(id, code, &limits)?)?; continue; }
                            break Err(Error::Invalid);
                        }
                        response_meta.insert(id, response);
                        if frame.header.flags == V3_END_STREAM {
                            let received = match streams.receive_data(id, &[], true) {
                                Ok(received) => received,
                                Err(_) => { close_stream!(id); send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?; continue; }
                            };
                            if let Some(body) = received.complete {
                                complete_response(id, &body, &provider, &services, &mut outbound_auth, &mut response_meta, &mut pending_event)?;
                            }
                        }
                    }
                    V3FrameType::WindowUpdate => {
                        let increment = u32::from_be_bytes(frame.payload.as_ref().try_into().map_err(|_| Error::Invalid)?);
                        if increment == 0 { break Err(Error::Invalid); }
                        tx.try_send(WriterMessage::WindowUpdate { id, increment }).map_err(|_| Error::Overloaded)?;
                    }
                    V3FrameType::Ping => { send(&tx, fixed(0, V3FrameType::Pong, &frame.payload, &limits)?)?; }
                    V3FrameType::Pong => {
                        let nonce = u64::from_be_bytes(frame.payload.as_ref().try_into().map_err(|_| Error::Invalid)?);
                        if sync_ping == Some(nonce) {
                            sync_ping = None;
                            let revision = confirmation.swap(0, Ordering::AcqRel);
                            if revision != 0 && revision != u64::MAX && let Some((_, lease)) = &provider { lease.mark_serving(revision)?; }
                        }
                    }
                    V3FrameType::ResetStream | V3FrameType::CloseStream => {
                        if frame.header.frame_type == V3FrameType::ResetStream {
                            let reason: V3Reset = decode(&frame)?;
                            reason.validate().map_err(|_| Error::Invalid)?;
                        }
                        if frame.header.frame_type == V3FrameType::ResetStream { services.ingress.metrics.inc(Metric::BusinessRpcV3StreamsReset); }
                        close_stream!(id);
                    }
                    V3FrameType::GoAway => {
                        let away: V3GoAway = decode(&frame)?;
                        away.validate().map_err(|_| Error::Invalid)?;
                        // The peer will accept no new gateway streams above its last_stream_id.
                        // Existing responses and EventAck get a bounded chance to finish.
                        if draining.is_none() {
                            let epoch = provider.as_ref().map(|(_, lease)| lease.epoch()).unwrap_or(0);
                            let mut refused: Vec<u32> = outbound_auth.keys().copied()
                                .filter(|child| *child > away.last_stream_id).collect();
                            if let Some(child) = pending_event.as_ref().map(|event| event.id)
                                .filter(|child| *child > away.last_stream_id) { refused.push(child); }
                            for child in refused {
                                let _ = streams.reset(child);
                                cancel_stream_work(child, epoch, CancelWork {
                                    tx: &tx, services: &services, incoming_rpc: &mut incoming_rpc,
                                    control_requests: &mut control_requests, outbound_auth: &mut outbound_auth,
                                    response_meta: &mut response_meta, pending_event: &mut pending_event,
                                });
                            }
                            draining = Some(tokio::time::Instant::now() + config.read_timeout);
                            auth_rx = None;
                            events_rx = None;
                        }
                    }
                    V3FrameType::Accept => { send(&tx, reset(id, V3ResetCode::ProtocolError, &limits)?)?; }
                }
            }
        }
        }
    }
    .await;
    let last_stream_id = streams.goaway();
    let code = match &result {
        Ok(()) if stop.is_cancelled() => netbaiot_core::business_rpc_v3::V3GoAwayCode::Shutdown,
        Ok(()) => netbaiot_core::business_rpc_v3::V3GoAwayCode::NoError,
        Err(Error::Forbidden) | Err(Error::Authentication) => {
            netbaiot_core::business_rpc_v3::V3GoAwayCode::Unauthorized
        }
        Err(_) => netbaiot_core::business_rpc_v3::V3GoAwayCode::ProtocolError,
    };
    if let Ok(frame) = metadata(
        0,
        V3FrameType::GoAway,
        &V3GoAway {
            last_stream_id,
            code,
            message: String::new(),
        },
        &limits,
    ) {
        let (done, written) = oneshot::channel();
        if tx.try_send(WriterMessage::GoAway { frame, done }).is_ok() {
            let _ = tokio::time::timeout(config.write_timeout, written).await;
        }
    }
    if let Some((_, _, generation)) = subscription {
        let _ = services.sink.release(generation);
    }
    if let Some(worker) = control_worker {
        worker.abort();
        let _ = worker.await;
    }
    drop(provider);
    result
}
