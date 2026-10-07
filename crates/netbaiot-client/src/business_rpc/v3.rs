//! Current Business RPC V3 client with owned bounded drivers.
use super::{
    BorrowedCommandRequest, BoundedCommandBody, BusinessAuthHandler,
    BusinessRpcClientError as Error, BusinessRpcTls, Io, connect_io,
};
use bytes::Bytes;
use netbaiot_protocol::{
    AuthInvalidation, CommandDispatch, CommandId, DeviceCommand, EventDelivery, EventFilter,
    SubscriptionId,
    business_rpc::{
        AuthInvalidateRequest, AuthInvalidateResponse, AuthSyncRequest, AuthSyncResponse,
        DeviceCommandSendResponse, RpcError, RpcErrorCode,
    },
    business_rpc_v3::{
        BUSINESS_RPC_V3_VERSION, V3_END_STREAM, V3Accept, V3Bootstrap, V3EventAck, V3EventStatus,
        V3FrameType, V3GoAway, V3GoAwayCode, V3Limits, V3Open, V3Reset, V3ResetCode, V3Response,
    },
};
use netbaiot_v3_mux::{
    DataClass, Frame, FrameReader, Initiator, MuxError, MuxScheduler, ReassemblyBudget, StreamTable,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const OUTBOUND_BYTES: usize = 16 * 1024 * 1024;
const REASSEMBLY_BYTES: usize = 16 * 1024 * 1024;
type PendingCommands = HashMap<u32, (CommandId, oneshot::Sender<Result<CommandDispatch, Error>>)>;

#[derive(Clone)]
pub struct BusinessRpcV3ClientConfig {
    pub address: SocketAddr,
    pub token: Option<String>,
    pub tls: Option<BusinessRpcTls>,
    pub provider: bool,
    pub events: bool,
    pub authority_incarnation: Uuid,
    pub auth_revision: u64,
    pub filter: EventFilter,
    pub limits: V3Limits,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub auth_max_inflight: usize,
}
impl BusinessRpcV3ClientConfig {
    pub fn development(address: SocketAddr, token: String) -> Self {
        Self {
            address,
            token: Some(token),
            tls: None,
            provider: true,
            events: true,
            authority_incarnation: Uuid::new_v4(),
            auth_revision: 1,
            filter: EventFilter::default(),
            limits: V3Limits::default(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            reconnect_initial: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
            auth_max_inflight: 128,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if self.auth_revision == 0
            || self.authority_incarnation.is_nil()
            || self.limits.validate().is_err()
            || self.filter.validate().is_err()
            || self.connect_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.reconnect_initial.is_zero()
            || self.reconnect_initial > self.reconnect_max
            || self.auth_max_inflight == 0
            || self.auth_max_inflight > self.limits.max_concurrent_streams as usize
            || (self.tls.is_some() == self.token.is_some())
            || (self.tls.is_none()
                && (!self.address.ip().is_loopback()
                    || self
                        .token
                        .as_ref()
                        .is_none_or(|token| token.is_empty() || token.len() > 256)))
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

enum Command {
    Invalidate(
        u64,
        AuthInvalidateRequest,
        oneshot::Sender<Result<AuthInvalidateResponse, Error>>,
    ),
    Dispatch(
        u64,
        CommandId,
        Bytes,
        oneshot::Sender<Result<CommandDispatch, Error>>,
    ),
    EventAck {
        epoch: u64,
        id: u32,
        delivery_id: Uuid,
        event_id: Uuid,
        success: bool,
        done: oneshot::Sender<Result<(), Error>>,
    },
}

pub struct BusinessRpcV3Delivery {
    pub delivery: EventDelivery,
    pub connection_epoch: u64,
    stream_id: u32,
    commands: mpsc::Sender<Command>,
}
impl BusinessRpcV3Delivery {
    pub async fn ack(self) -> Result<(), Error> {
        self.respond(true).await
    }
    pub async fn nack(self) -> Result<(), Error> {
        self.respond(false).await
    }
    async fn respond(self, success: bool) -> Result<(), Error> {
        let (done, receive) = oneshot::channel();
        self.commands
            .try_send(Command::EventAck {
                epoch: self.connection_epoch,
                id: self.stream_id,
                delivery_id: self.delivery.delivery_id.0,
                event_id: self.delivery.event.event_id.0,
                success,
                done,
            })
            .map_err(|_| Error::Overloaded)?;
        receive.await.map_err(|_| Error::Unavailable)?
    }
}

struct Inner {
    commands: mpsc::Sender<Command>,
    ready: watch::Receiver<bool>,
    revision: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,
    incarnation: Uuid,
    timeout: Duration,
    shutdown: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Inner {
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
pub struct BusinessRpcV3Client {
    inner: Arc<Inner>,
}
impl BusinessRpcV3Client {
    pub fn connect(
        config: BusinessRpcV3ClientConfig,
        handler: Option<Arc<dyn BusinessAuthHandler>>,
    ) -> Result<(Self, mpsc::Receiver<BusinessRpcV3Delivery>), Error> {
        config.validate()?;
        if config.provider && handler.is_none() {
            return Err(Error::InvalidConfig);
        }
        let (commands, command_rx) = mpsc::channel(32);
        let (deliveries, delivery_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = watch::channel(false);
        let shutdown = CancellationToken::new();
        let revision = Arc::new(AtomicU64::new(config.auth_revision));
        let epoch = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn(driver(DriverContext {
            config: config.clone(),
            handler,
            commands: command_rx,
            command_tx: commands.clone(),
            deliveries,
            ready: ready_tx,
            revision: revision.clone(),
            epoch: epoch.clone(),
            stop: shutdown.clone(),
        }));
        Ok((
            Self {
                inner: Arc::new(Inner {
                    commands,
                    ready: ready_rx,
                    revision,
                    epoch,
                    incarnation: config.authority_incarnation,
                    timeout: config.request_timeout,
                    shutdown,
                    task: Mutex::new(Some(task)),
                }),
            },
            delivery_rx,
        ))
    }
    pub fn ready(&self) -> bool {
        *self.inner.ready.borrow()
    }
    pub async fn wait_ready(&self) -> Result<(), Error> {
        let mut ready = self.inner.ready.clone();
        loop {
            if *ready.borrow() {
                return Ok(());
            }
            ready.changed().await.map_err(|_| Error::Unavailable)?;
        }
    }
    pub async fn invalidate(
        &self,
        auth_revision: u64,
        invalidation: AuthInvalidation,
    ) -> Result<AuthInvalidateResponse, Error> {
        if !self.ready() || auth_revision == 0 {
            return Err(Error::Unavailable);
        }
        self.inner
            .revision
            .fetch_max(auth_revision, Ordering::SeqCst);
        let request = AuthInvalidateRequest {
            authority_incarnation: self.inner.incarnation,
            auth_revision,
            invalidation,
        };
        let (done, receive) = oneshot::channel();
        self.inner
            .commands
            .try_send(Command::Invalidate(
                self.inner.epoch.load(Ordering::Acquire),
                request,
                done,
            ))
            .map_err(|_| Error::Overloaded)?;
        tokio::time::timeout(self.inner.timeout, receive)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Unavailable)?
    }
    /// A successful response is Queued at the live Gateway session, not device
    /// execution. Connection loss after submission has an unknown outcome; callers
    /// choose whether to retry the same semantic command and command_id.
    pub async fn send_command(&self, command: &DeviceCommand) -> Result<CommandDispatch, Error> {
        if !self.ready() {
            return Err(Error::Unavailable);
        }
        let mut encoded = BoundedCommandBody(Vec::new());
        serde_json::to_writer(&mut encoded, &BorrowedCommandRequest { command })
            .map_err(|_| Error::Protocol)?;
        let (done, receive) = oneshot::channel();
        self.inner
            .commands
            .try_send(Command::Dispatch(
                self.inner.epoch.load(Ordering::Acquire),
                command.command_id,
                Bytes::from(encoded.0),
                done,
            ))
            .map_err(|_| Error::Overloaded)?;
        tokio::time::timeout(self.inner.timeout, receive)
            .await
            .map_err(|_| Error::OutcomeUnknown)?
            .map_err(|_| Error::OutcomeUnknown)?
    }
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let task = self.inner.task.lock().ok().and_then(|mut task| task.take());
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

fn metadata<T: Serialize>(
    id: u32,
    ty: V3FrameType,
    value: &T,
    limits: &V3Limits,
) -> Result<Frame, Error> {
    let bytes = serde_json::to_vec(value).map_err(|_| Error::Protocol)?;
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
    .map_err(|_| Error::Protocol)
}
fn fixed(id: u32, ty: V3FrameType, bytes: &[u8], limits: &V3Limits) -> Result<Frame, Error> {
    Frame::new(
        id,
        ty,
        0,
        Bytes::copy_from_slice(bytes),
        limits.max_frame_payload_bytes as usize,
    )
    .map_err(|_| Error::Protocol)
}
fn decode<T: DeserializeOwned>(frame: &Frame) -> Result<T, Error> {
    serde_json::from_slice(&frame.payload).map_err(|_| Error::Protocol)
}
fn reset(
    tx: &mpsc::Sender<WriterMessage>,
    id: u32,
    code: V3ResetCode,
    limits: &V3Limits,
) -> Result<(), Error> {
    enqueue(
        tx,
        metadata(
            id,
            V3FrameType::ResetStream,
            &V3Reset {
                code,
                message: String::new(),
            },
            limits,
        )?,
    )
}
fn stream_error(error: MuxError) -> Option<V3ResetCode> {
    match error {
        MuxError::Connection(_) | MuxError::Io | MuxError::Timeout => None,
        MuxError::Stream => Some(V3ResetCode::ProtocolError),
        MuxError::FlowControl => Some(V3ResetCode::FlowControlError),
        MuxError::Overloaded => Some(V3ResetCode::Overloaded),
        MuxError::MessageTooLarge => Some(V3ResetCode::MessageTooLarge),
    }
}

enum WriterMessage {
    Frame(Frame),
    Pong(Frame, oneshot::Sender<Result<(), Error>>),
    GoAway(Frame, oneshot::Sender<Result<(), Error>>),
    Body {
        id: u32,
        class: DataClass,
        bytes: Bytes,
        done: Option<oneshot::Sender<Result<(), Error>>>,
        _permit: OwnedSemaphorePermit,
    },
    Update(u32, u32),
    Reset(u32),
}
type PendingWrite = (
    OwnedSemaphorePermit,
    Option<oneshot::Sender<Result<(), Error>>>,
);
struct PendingGoAway {
    frame: Frame,
    done: oneshot::Sender<Result<(), Error>>,
    deadline: tokio::time::Instant,
}
fn enqueue(tx: &mpsc::Sender<WriterMessage>, frame: Frame) -> Result<(), Error> {
    tx.try_send(WriterMessage::Frame(frame))
        .map_err(|_| Error::Overloaded)
}
fn body(
    tx: &mpsc::Sender<WriterMessage>,
    budget: &Arc<Semaphore>,
    id: u32,
    class: DataClass,
    bytes: Bytes,
    done: Option<oneshot::Sender<Result<(), Error>>>,
) -> Result<(), Error> {
    let size = u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?;
    let permit = budget
        .clone()
        .try_acquire_many_owned(size.max(1))
        .map_err(|_| Error::Overloaded)?;
    tx.try_send(WriterMessage::Body {
        id,
        class,
        bytes,
        done,
        _permit: permit,
    })
    .map_err(|_| Error::Overloaded)
}
async fn writer_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut rx: mpsc::Receiver<WriterMessage>,
    limits: V3Limits,
    timeout: Duration,
    stop: CancellationToken,
    credit_tx: mpsc::Sender<(u32, u32)>,
) -> Result<(), Error> {
    let mut scheduler =
        MuxScheduler::new(&limits, OUTBOUND_BYTES).map_err(|_| Error::InvalidConfig)?;
    let mut bodies = HashMap::<u32, PendingWrite>::new();
    let mut goaway: Option<PendingGoAway> = None;
    loop {
        for _ in 0..16 {
            let Ok(message) = rx.try_recv() else {
                break;
            };
            if let WriterMessage::Pong(frame, done) = message {
                let result = netbaiot_v3_mux::write_frame(&mut writer, &frame, timeout)
                    .await
                    .map_err(|_| Error::Unavailable);
                let _ = done.send(result.clone());
                result?;
                continue;
            }
            if let WriterMessage::GoAway(frame, done) = message {
                goaway = Some(PendingGoAway {
                    frame,
                    done,
                    deadline: tokio::time::Instant::now() + timeout,
                });
                break;
            }
            apply_writer(&mut scheduler, &mut bodies, message)?;
        }
        if let Some(frame) = scheduler.next_frame() {
            netbaiot_v3_mux::write_frame(&mut writer, &frame, timeout)
                .await
                .map_err(|_| Error::Unavailable)?;
            if frame.header.frame_type == V3FrameType::WindowUpdate {
                let increment = u32::from_be_bytes(
                    frame
                        .payload
                        .as_ref()
                        .try_into()
                        .map_err(|_| Error::Protocol)?,
                );
                credit_tx
                    .send((frame.header.stream_id, increment))
                    .await
                    .map_err(|_| Error::Unavailable)?;
            }
            if frame.header.frame_type == V3FrameType::Data && frame.header.flags == V3_END_STREAM {
                if let Some((_permit, done)) = bodies.remove(&frame.header.stream_id)
                    && let Some(done) = done
                {
                    let _ = done.send(Ok(()));
                }
                scheduler.reset(frame.header.stream_id);
            }
            continue;
        }
        if let Some(pending) = goaway.take() {
            if scheduler.has_pending_frames() && tokio::time::Instant::now() < pending.deadline {
                let deadline = pending.deadline;
                goaway = Some(pending);
                tokio::time::sleep_until(deadline).await;
                continue;
            }
            let result = netbaiot_v3_mux::write_frame(&mut writer, &pending.frame, timeout)
                .await
                .map_err(|_| Error::Unavailable);
            let _ = pending.done.send(result.clone());
            return result;
        }
        let message = tokio::select! { _ = stop.cancelled() => break, message = rx.recv() => match message { Some(value) => value, None => break } };
        if let WriterMessage::Pong(frame, done) = message {
            let result = netbaiot_v3_mux::write_frame(&mut writer, &frame, timeout)
                .await
                .map_err(|_| Error::Unavailable);
            let _ = done.send(result.clone());
            result?;
            continue;
        }
        if let WriterMessage::GoAway(frame, done) = message {
            goaway = Some(PendingGoAway {
                frame,
                done,
                deadline: tokio::time::Instant::now() + timeout,
            });
            continue;
        }
        apply_writer(&mut scheduler, &mut bodies, message)?;
    }
    Ok(())
}
fn apply_writer(
    scheduler: &mut MuxScheduler,
    bodies: &mut HashMap<u32, PendingWrite>,
    message: WriterMessage,
) -> Result<(), Error> {
    match message {
        WriterMessage::Frame(frame) => scheduler
            .queue_control(frame)
            .map_err(|_| Error::Overloaded),
        WriterMessage::Body {
            id,
            class,
            bytes,
            done,
            _permit,
        } => {
            scheduler
                .queue_body(id, class, bytes)
                .map_err(|_| Error::Overloaded)?;
            bodies.insert(id, (_permit, done));
            Ok(())
        }
        WriterMessage::Update(id, increment) => match scheduler.window_update(id, increment) {
            Ok(()) | Err(netbaiot_v3_mux::MuxError::Stream) => Ok(()),
            Err(_) => Err(Error::Protocol),
        },
        WriterMessage::Reset(id) => {
            scheduler.reset(id);
            bodies.remove(&id);
            Ok(())
        }
        WriterMessage::Pong(_, _) | WriterMessage::GoAway(_, _) => Err(Error::Protocol),
    }
}

async fn bootstrap(
    io: &mut Box<dyn Io>,
    config: &BusinessRpcV3ClientConfig,
) -> Result<(u64, V3Limits), Error> {
    let hello = V3Bootstrap::Hello {
        version: BUSINESS_RPC_V3_VERSION,
        token: config.token.clone(),
        limits: config.limits.clone(),
    };
    let encoded = serde_json::to_vec(&hello).map_err(|_| Error::Protocol)?;
    let length = u32::try_from(encoded.len()).map_err(|_| Error::Protocol)?;
    tokio::time::timeout(config.connect_timeout, async {
        io.write_all(&length.to_be_bytes())
            .await
            .map_err(|_| Error::Unavailable)?;
        io.write_all(&encoded)
            .await
            .map_err(|_| Error::Unavailable)?;
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix)
            .await
            .map_err(|_| Error::Unavailable)?;
        let len = u32::from_be_bytes(prefix) as usize;
        if len == 0 || len > 4096 {
            return Err(Error::Protocol);
        }
        let mut payload = vec![0; len];
        io.read_exact(&mut payload)
            .await
            .map_err(|_| Error::Unavailable)?;
        let ready: V3Bootstrap = serde_json::from_slice(&payload).map_err(|_| Error::Protocol)?;
        ready.validate().map_err(|_| Error::Protocol)?;
        let V3Bootstrap::Ready {
            version,
            connection_epoch,
            limits,
        } = ready
        else {
            return Err(Error::Protocol);
        };
        if version != BUSINESS_RPC_V3_VERSION
            || limits
                != config
                    .limits
                    .negotiate(&limits)
                    .map_err(|_| Error::Protocol)?
        {
            return Err(Error::Protocol);
        }
        Ok((connection_epoch, limits))
    })
    .await
    .map_err(|_| Error::Timeout)?
}

struct DriverContext {
    config: BusinessRpcV3ClientConfig,
    handler: Option<Arc<dyn BusinessAuthHandler>>,
    commands: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    deliveries: mpsc::Sender<BusinessRpcV3Delivery>,
    ready: watch::Sender<bool>,
    revision: Arc<AtomicU64>,
    epoch: Arc<AtomicU64>,
    stop: CancellationToken,
}
async fn driver(mut context: DriverContext) {
    let mut delay = context.config.reconnect_initial;
    loop {
        if context.stop.is_cancelled() {
            break;
        }
        let result = async {
            let mut io = connect_io(&context.config).await?;
            let (epoch, limits) = bootstrap(&mut io, &context.config).await?;
            context.epoch.store(epoch, Ordering::Release);
            connected(io, epoch, limits, &mut context).await
        }
        .await;
        context.ready.send_replace(false);
        context.epoch.store(0, Ordering::Release);
        if context.stop.is_cancelled() {
            break;
        }
        if result.is_ok() {
            delay = context.config.reconnect_initial;
        }
        tokio::select! { _ = context.stop.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
        delay = delay.saturating_mul(2).min(context.config.reconnect_max);
    }
}

struct HandlerReply {
    id: u32,
    body: Result<serde_json::Value, RpcError>,
}
async fn run_handler(
    id: u32,
    method: String,
    body: Bytes,
    handler: Arc<dyn BusinessAuthHandler>,
) -> HandlerReply {
    let value = match method.as_str() {
        "device.authenticate" => match serde_json::from_slice(&body) {
            Ok(request) => handler.authenticate(request).await.and_then(|identity| {
                serde_json::to_value(identity)
                    .map_err(|_| RpcError::new(RpcErrorCode::Internal, "invalid response"))
            }),
            Err(_) => Err(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "invalid request",
            )),
        },
        "device.resolve_verifier" => match serde_json::from_slice(&body) {
            Ok(request) => handler
                .resolve_verifier(request)
                .await
                .and_then(|verifier| {
                    serde_json::to_value(verifier)
                        .map_err(|_| RpcError::new(RpcErrorCode::Internal, "invalid response"))
                }),
            Err(_) => Err(RpcError::new(
                RpcErrorCode::InvalidRequest,
                "invalid request",
            )),
        },
        _ => Err(RpcError::new(RpcErrorCode::UnknownMethod, "unknown method")),
    };
    HandlerReply { id, body: value }
}

fn mark_ready(
    ready: &watch::Sender<bool>,
    config: &BusinessRpcV3ClientConfig,
    provider: bool,
    events: bool,
) {
    ready.send_replace((!config.provider || provider) && (!config.events || events));
}

async fn connected(
    io: Box<dyn Io>,
    epoch: u64,
    limits: V3Limits,
    context: &mut DriverContext,
) -> Result<(), Error> {
    let DriverContext {
        config,
        handler,
        commands,
        command_tx,
        deliveries,
        ready,
        revision,
        epoch: _,
        stop,
    } = context;
    let (reader, writer) = tokio::io::split(io);
    let (writer_tx, writer_rx) = mpsc::channel(256);
    let (credit_tx, mut credit_rx) = mpsc::channel(256);
    let writer_stop = CancellationToken::new();
    let writer_closed = CancellationToken::new();
    let mut frames = FrameReader::spawn(
        reader,
        limits.max_frame_payload_bytes as usize,
        config.request_timeout.saturating_mul(4),
    );
    let outbound = Arc::new(Semaphore::new(OUTBOUND_BYTES));
    let mut streams = StreamTable::new(
        Initiator::Client,
        limits.clone(),
        REASSEMBLY_BYTES,
        ReassemblyBudget::new(REASSEMBLY_BYTES),
    )
    .map_err(|_| Error::InvalidConfig)?;
    let mut provider_id = if config.provider {
        let open = V3Open::Provider {
            provider_id: "primary".into(),
        };
        let id = streams.open_local(&open).map_err(|_| Error::Overloaded)?;
        enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
        Some(id)
    } else {
        None
    };
    let subscription_id = SubscriptionId::generate();
    let mut subscription_stream = if config.events {
        let open = V3Open::EventSubscription {
            subscription_id,
            filter: config.filter.clone(),
        };
        let id = streams.open_local(&open).map_err(|_| Error::Overloaded)?;
        enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
        Some(id)
    } else {
        None
    };
    // No fallible setup may leave an owned writer task behind before the driver can clean it up.
    let writer = tokio::spawn({
        let writer_closed = writer_closed.clone();
        let writer_stop = writer_stop.clone();
        let limits = limits.clone();
        let timeout = config.request_timeout;
        async move {
            let result =
                writer_loop(writer, writer_rx, limits, timeout, writer_stop, credit_tx).await;
            writer_closed.cancel();
            result
        }
    });
    let mut provider_accepted = false;
    let mut provider_ready = false;
    let mut events_ready = false;
    let mut sync_id: Option<u32> = None;
    let mut sync_response_ok = false;
    let mut pong_written: Option<oneshot::Receiver<Result<(), Error>>> = None;
    let mut invalidations =
        HashMap::<u32, oneshot::Sender<Result<AuthInvalidateResponse, Error>>>::new();
    let mut pending_commands = PendingCommands::new();
    let mut response_meta = HashMap::<u32, V3Response>::new();
    let mut inbound_rpc = HashMap::<u32, String>::new();
    let mut inbound_events = HashMap::<u32, (Uuid, Uuid)>::new();
    let mut handlers = JoinSet::<HandlerReply>::new();
    let mut handler_handles = HashMap::<u32, AbortHandle>::new();
    let mut provider_retry: Option<tokio::time::Instant> = None;
    let mut subscription_retry: Option<tokio::time::Instant> = None;
    let mut provider_retry_delay = Duration::from_millis(100);
    let mut subscription_retry_delay = Duration::from_millis(100);
    let heartbeat_period = Duration::from_millis((limits.heartbeat_ms / 2).max(1) as u64);
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_period,
        heartbeat_period,
    );
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut heartbeat_pending: Option<(u64, tokio::time::Instant)> = None;
    let mut draining: Option<tokio::time::Instant> = None;
    macro_rules! release_stream {
        ($target:expr) => {{
            let target = $target;
            let children = streams.reset(target).unwrap_or_default();
            for stream_id in std::iter::once(target).chain(children) {
                let _ = writer_tx.try_send(WriterMessage::Reset(stream_id));
                inbound_rpc.remove(&stream_id);
                inbound_events.remove(&stream_id);
                response_meta.remove(&stream_id);
                invalidations.remove(&stream_id);
                pending_commands.remove(&stream_id);
                if let Some(handle) = handler_handles.remove(&stream_id) {
                    handle.abort();
                }
                if sync_id == Some(stream_id) {
                    sync_id = None;
                    sync_response_ok = false;
                    pong_written = None;
                }
            }
            if Some(target) == provider_id {
                provider_id = None;
                provider_ready = false;
                provider_accepted = false;
                sync_id = None;
                sync_response_ok = false;
                pong_written = None;
                handlers.abort_all();
                inbound_rpc.clear();
                handler_handles.clear();
                mark_ready(ready, config, provider_ready, events_ready);
                provider_retry = Some(tokio::time::Instant::now() + provider_retry_delay);
                provider_retry_delay = provider_retry_delay
                    .saturating_mul(2)
                    .min(Duration::from_secs(5));
            } else if Some(target) == subscription_stream {
                subscription_stream = None;
                events_ready = false;
                mark_ready(ready, config, provider_ready, events_ready);
                subscription_retry = Some(tokio::time::Instant::now() + subscription_retry_delay);
                subscription_retry_delay = subscription_retry_delay
                    .saturating_mul(2)
                    .min(Duration::from_secs(5));
            }
        }};
    }
    macro_rules! fail_stream {
        ($target:expr, $code:expr) => {{
            let target = $target;
            reset(&writer_tx, target, $code, &limits)?;
            let owner = if sync_id == Some(target) {
                provider_id.unwrap_or(target)
            } else {
                target
            };
            if owner != target {
                reset(&writer_tx, owner, V3ResetCode::Cancel, &limits)?;
            }
            release_stream!(owner);
        }};
    }
    macro_rules! finish_response {
        ($id:expr, $bytes:expr) => {{
            match complete_client_response(
                $id,
                $bytes,
                &mut sync_id,
                &mut sync_response_ok,
                &mut invalidations,
                &mut pending_commands,
                &mut response_meta,
            ) {
                // A revision gap demotes the authority. Reopen only its Provider
                // stream and perform reset sync; the event subscription stays owned.
                Err(Error::Remote(RpcErrorCode::StaleRevision)) => {
                    if let Some(parent) = provider_id {
                        fail_stream!(parent, V3ResetCode::Cancel);
                    }
                }
                Err(_) => {
                    fail_stream!($id, V3ResetCode::ProtocolError);
                }
                Ok(()) => {}
            }
        }};
    }
    let result: Result<(), Error> = async {
        if !config.provider && !config.events {
            mark_ready(ready, config, false, false);
        }
        loop {
        if streams.local_ids_exhausted() {
            ready.send_replace(false);
            let away = V3GoAway {
                last_stream_id: streams.last_peer_id(),
                code: V3GoAwayCode::NoError,
                message: String::new(),
            };
            let (done, written) = oneshot::channel();
            let frame = metadata(0, V3FrameType::GoAway, &away, &limits)?;
            if tokio::time::timeout(config.request_timeout,
                writer_tx.send(WriterMessage::GoAway(frame, done))).await.is_ok_and(|sent| sent.is_ok())
            {
                let _ = tokio::time::timeout(config.request_timeout, written).await;
            }
            break Ok(());
        }
        if draining.is_some()
            && invalidations.is_empty()
            && pending_commands.is_empty()
            && handlers.is_empty()
            && streams.active()
                <= usize::from(provider_id.is_some()) + usize::from(subscription_stream.is_some())
        {
            break Ok(());
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => {
                let (done, written) = oneshot::channel();
                let away = V3GoAway { last_stream_id: streams.last_peer_id(), code: V3GoAwayCode::Shutdown, message: String::new() };
                if let Ok(frame) = metadata(0, V3FrameType::GoAway, &away, &limits)
                    && writer_tx.try_send(WriterMessage::GoAway(frame, done)).is_ok()
                {
                    let _ = tokio::time::timeout(config.request_timeout, written).await;
                }
                break Ok(())
            },
            _ = writer_closed.cancelled() => break Err(Error::Unavailable),
            _ = tokio::time::sleep_until(draining.unwrap_or_else(tokio::time::Instant::now)), if draining.is_some() => break Ok(()),
            _ = heartbeat.tick() => {
                if heartbeat_pending.as_ref().is_some_and(|(_, deadline)| tokio::time::Instant::now() >= *deadline) {
                    break Err(Error::Timeout);
                }
                if heartbeat_pending.is_none() {
                    let nonce = Uuid::new_v4().as_u128() as u64;
                    enqueue(&writer_tx, fixed(0, V3FrameType::Ping, &nonce.to_be_bytes(), &limits)?)?;
                    heartbeat_pending = Some((nonce, tokio::time::Instant::now() + heartbeat_period.saturating_mul(4)));
                }
            }
            _ = tokio::time::sleep_until(provider_retry.unwrap_or_else(tokio::time::Instant::now)), if provider_retry.is_some() && draining.is_none() => {
                provider_retry = None;
                let open = V3Open::Provider { provider_id: "primary".into() };
                let id = streams.open_local(&open).map_err(|_| Error::Overloaded)?;
                enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                provider_id = Some(id);
            }
            _ = tokio::time::sleep_until(subscription_retry.unwrap_or_else(tokio::time::Instant::now)), if subscription_retry.is_some() && draining.is_none() => {
                subscription_retry = None;
                let open = V3Open::EventSubscription { subscription_id, filter: config.filter.clone() };
                let id = streams.open_local(&open).map_err(|_| Error::Overloaded)?;
                enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                subscription_stream = Some(id);
            }
            credit = credit_rx.recv() => {
                let Some((id, increment)) = credit else { break Err(Error::Unavailable); };
                match streams.grant_credit(id, increment) {
                    Ok(()) | Err(netbaiot_v3_mux::MuxError::Stream) => {}
                    Err(_) => break Err(Error::Protocol),
                }
            }
            written = async { match pong_written.as_mut() { Some(written) => written.await.ok(), None => std::future::pending().await } }, if pong_written.is_some() => {
                pong_written = None;
                if written.is_some_and(|value| value.is_ok()) {
                    provider_ready = true;
                    mark_ready(ready, config, provider_ready, events_ready);
                }
            }
            finished = handlers.join_next(), if !handlers.is_empty() => {
                let Some(Ok(reply)) = finished else { continue; };
                handler_handles.remove(&reply.id);
                let (body_bytes, error) = match reply.body {
                    Ok(value) => (serde_json::to_vec(&value).map_err(|_| Error::Protocol)?, None),
                    Err(error) => (Vec::new(), Some(error)),
                };
                let response = V3Response { content_length: u32::try_from(body_bytes.len()).map_err(|_| Error::Overloaded)?, error };
                let mut frame = metadata(reply.id, V3FrameType::Response, &response, &limits)?;
                if body_bytes.is_empty() { frame.header.flags = V3_END_STREAM; }
                enqueue(&writer_tx, frame)?;
                if !body_bytes.is_empty()
                    && body(&writer_tx, &outbound, reply.id, DataClass::Rpc, Bytes::from(body_bytes), None).is_err()
                {
                    let _ = streams.reset(reply.id);
                    reset(&writer_tx, reply.id, V3ResetCode::Overloaded, &limits)?;
                    continue;
                }
                if streams.mark_local_end(reply.id).is_err() {
                    reset(&writer_tx, reply.id, V3ResetCode::StreamClosed, &limits)?;
                }
            }
            command = commands.recv() => {
                let Some(command) = command else { break Ok(()); };
                match command {
                    Command::Invalidate(command_epoch, request, done) => {
                        if draining.is_some() || !provider_ready || command_epoch != epoch { let _ = done.send(Err(Error::Unavailable)); continue; }
                        let bytes = serde_json::to_vec(&request).map_err(|_| Error::Protocol)?;
                        let open = V3Open::Rpc { parent_stream_id: provider_id, request_id: Uuid::new_v4(), method: "auth.invalidate".into(), deadline_ms: config.request_timeout.as_millis().min(u32::MAX as u128) as u32, content_length: u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)? };
                        let id = match streams.open_local(&open) {
                            Ok(id) => id,
                            Err(_) => { let _ = done.send(Err(Error::Overloaded)); continue; }
                        };
                        enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                        if body(&writer_tx, &outbound, id, DataClass::Rpc, Bytes::from(bytes), None).is_err() {
                            let _ = streams.reset(id);
                            let _ = done.send(Err(Error::Overloaded));
                            reset(&writer_tx, id, V3ResetCode::Overloaded, &limits)?;
                            continue;
                        }
                        streams.mark_local_end(id).map_err(|_| Error::Protocol)?;
                        invalidations.insert(id, done);
                    }
                    Command::Dispatch(command_epoch, command_id, bytes, done) => {
                        if draining.is_some() || command_epoch != epoch {
                            let _ = done.send(Err(Error::Unavailable));
                            continue;
                        }
                        let open = V3Open::Rpc {
                            parent_stream_id: None,
                            request_id: Uuid::new_v4(),
                            method: "device.command.send".into(),
                            deadline_ms: config.request_timeout.as_millis().clamp(1, 60_000) as u32,
                            content_length: u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?,
                        };
                        let id = match streams.open_local(&open) {
                            Ok(id) => id,
                            Err(_) => { let _ = done.send(Err(Error::Overloaded)); continue; }
                        };
                        enqueue(&writer_tx, metadata(id, V3FrameType::Open, &open, &limits)?)?;
                        if body(&writer_tx, &outbound, id, DataClass::Rpc, bytes, None).is_err() {
                            let _ = streams.reset(id);
                            let _ = done.send(Err(Error::Overloaded));
                            reset(&writer_tx, id, V3ResetCode::Overloaded, &limits)?;
                            continue;
                        }
                        streams.mark_local_end(id).map_err(|_| Error::Protocol)?;
                        pending_commands.insert(id, (command_id, done));
                    }
                    Command::EventAck { epoch: delivery_epoch, id, delivery_id, event_id, success, done } => {
                        if delivery_epoch != epoch { let _ = done.send(Err(Error::Unavailable)); continue; }
                        if streams.mark_local_end(id).is_err() { let _ = done.send(Err(Error::Unavailable)); continue; }
                        let ack = V3EventAck { status: if success { V3EventStatus::Ok } else { V3EventStatus::Error }, delivery_id, event_id };
                        let bytes = serde_json::to_vec(&ack).map_err(|_| Error::Protocol)?;
                        let response = V3Response { content_length: u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)?, error: if success { None } else { Some(RpcError::new(RpcErrorCode::Unavailable, "application did not commit")) } };
                        enqueue(&writer_tx, metadata(id, V3FrameType::Response, &response, &limits)?)?;
                        if body(&writer_tx, &outbound, id, DataClass::Rpc, Bytes::from(bytes), Some(done)).is_err() {
                            let _ = streams.reset(id);
                            reset(&writer_tx, id, V3ResetCode::Overloaded, &limits)?;
                        }
                    }
                }
            }
            frame = frames.recv() => {
                let frame = match frame { Some(Ok(frame)) => frame, Some(Err(error)) => break Err(match error {
                    MuxError::Connection(_) => Error::Protocol,
                    MuxError::Timeout => Error::Timeout,
                    _ => Error::Unavailable,
                }), None => break Err(Error::Unavailable) };
                let id = frame.header.stream_id;
                match frame.header.frame_type {
                    V3FrameType::Accept => {
                        let accept: V3Accept = match decode(&frame) {
                            Ok(accept) => accept,
                            Err(_) => { fail_stream!(id, V3ResetCode::ProtocolError); continue; }
                        };
                        if Some(id) == provider_id {
                            if provider_accepted || !accept.sync_required || accept.provider_epoch.is_none() {
                                fail_stream!(id, V3ResetCode::ProtocolError); continue;
                            }
                            provider_accepted = true;
                            provider_retry_delay = Duration::from_millis(100);
                            let request = AuthSyncRequest { reset: true, authority_incarnation: config.authority_incarnation, auth_revision: revision.load(Ordering::SeqCst) };
                            let bytes = serde_json::to_vec(&request).map_err(|_| Error::Protocol)?;
                            let open = V3Open::Rpc { parent_stream_id: provider_id, request_id: Uuid::new_v4(), method: "auth.sync".into(), deadline_ms: config.request_timeout.as_millis().min(u32::MAX as u128) as u32, content_length: u32::try_from(bytes.len()).map_err(|_| Error::Overloaded)? };
                            let child = streams.open_local(&open).map_err(|_| Error::Overloaded)?;
                            enqueue(&writer_tx, metadata(child, V3FrameType::Open, &open, &limits)?)?;
                            body(&writer_tx, &outbound, child, DataClass::Rpc, Bytes::from(bytes), None)?;
                            streams.mark_local_end(child).map_err(|_| Error::Protocol)?;
                            sync_id = Some(child);
                        } else if Some(id) == subscription_stream {
                            if events_ready || accept.sync_required {
                                fail_stream!(id, V3ResetCode::ProtocolError); continue;
                            }
                            events_ready = true;
                            subscription_retry_delay = Duration::from_millis(100);
                            mark_ready(ready, config, provider_ready, events_ready);
                        } else { break Err(Error::Protocol); }
                    }
                    V3FrameType::Open => {
                        if streams.check_peer_id(id).is_err() { break Err(Error::Protocol); }
                        let open: V3Open = match decode(&frame) {
                            Ok(open) => open,
                            Err(_) => { streams.refuse_peer(id).map_err(|_| Error::Protocol)?; reset(&writer_tx, id, V3ResetCode::ProtocolError, &limits)?; continue; }
                        };
                        if open.validate().is_err() { streams.refuse_peer(id).map_err(|_| Error::Protocol)?; reset(&writer_tx, id, V3ResetCode::ProtocolError, &limits)?; continue; }
                        match &open {
                            V3Open::Rpc { parent_stream_id, method, .. } if *parent_stream_id == provider_id && matches!(method.as_str(), "device.authenticate" | "device.resolve_verifier") => {
                                if let Err(error) = streams.open_peer(id, &open) {
                                    if let Some(code) = stream_error(error) { reset(&writer_tx, id, code, &limits)?; continue; }
                                    break Err(Error::Protocol);
                                }
                                inbound_rpc.insert(id, method.clone());
                            }
                            V3Open::EventDelivery { parent_stream_id, delivery_id, event_id, .. } if Some(*parent_stream_id) == subscription_stream => {
                                if let Err(error) = streams.open_peer(id, &open) {
                                    if let Some(code) = stream_error(error) { reset(&writer_tx, id, code, &limits)?; continue; }
                                    break Err(Error::Protocol);
                                }
                                inbound_events.insert(id, (*delivery_id, *event_id));
                            }
                            _ => { streams.refuse_peer(id).map_err(|_| Error::Protocol)?; reset(&writer_tx, id, V3ResetCode::RefusedStream, &limits)?; }
                        }
                    }
                    V3FrameType::Response => {
                        let response: V3Response = match decode::<V3Response>(&frame) {
                            Ok(response) if response.validate().is_ok() => response,
                            _ => { fail_stream!(id, V3ResetCode::ProtocolError); continue; }
                        };
                        if Some(id) != sync_id && !invalidations.contains_key(&id) && !pending_commands.contains_key(&id) { reset(&writer_tx, id, V3ResetCode::StreamClosed, &limits)?; continue; }
                        if let Err(error) = streams.mark_response(id, response.content_length as usize) {
                            if let Some(code) = stream_error(error) { fail_stream!(id, code); continue; }
                            break Err(Error::Protocol);
                        }
                        response_meta.insert(id, response);
                        if frame.header.flags == V3_END_STREAM {
                            let received = match streams.receive_data(id, &[], true) {
                                Ok(received) => received,
                                Err(error) => {
                                    if let Some(code) = stream_error(error) { fail_stream!(id, code); continue; }
                                    break Err(Error::Protocol);
                                }
                            };
                            if let Some(bytes) = received.complete {
                                finish_response!(id, &bytes);
                            }
                        }
                    }
                    V3FrameType::Data => {
                        let received = match streams.receive_data(id, &frame.payload, frame.header.flags == V3_END_STREAM) {
                            Ok(received) => received,
                            Err(error) => {
                                if let Some(code) = stream_error(error) {
                                    fail_stream!(id, code);
                                    continue;
                                }
                                break Err(Error::Protocol);
                            }
                        };
                        if received.connection_update > 0 {
                            enqueue(&writer_tx, fixed(0, V3FrameType::WindowUpdate, &received.connection_update.to_be_bytes(), &limits)?)?;
                            if received.stream_update > 0 { enqueue(&writer_tx, fixed(id, V3FrameType::WindowUpdate, &received.stream_update.to_be_bytes(), &limits)?)?; }
                        }
                        if let Some(bytes) = received.complete {
                            if Some(id) == sync_id || invalidations.contains_key(&id) || pending_commands.contains_key(&id) {
                                finish_response!(id, &bytes);
                            } else if let Some(method) = inbound_rpc.remove(&id) {
                                if handlers.len() >= config.auth_max_inflight { let _ = streams.reset(id); reset(&writer_tx, id, V3ResetCode::Overloaded, &limits)?; continue; }
                                let Some(handler) = handler.clone() else { break Err(Error::InvalidConfig); };
                                handler_handles.insert(id, handlers.spawn(run_handler(id, method, bytes, handler)));
                            } else if let Some((delivery_id, event_id)) = inbound_events.remove(&id) {
                                let Ok(delivery) = serde_json::from_slice::<EventDelivery>(&bytes) else { let _ = streams.reset(id); reset(&writer_tx, id, V3ResetCode::ProtocolError, &limits)?; continue; };
                                if delivery.delivery_id.0 != delivery_id || delivery.event.event_id.0 != event_id || delivery.subscription_id != subscription_id { let _ = streams.reset(id); reset(&writer_tx, id, V3ResetCode::ProtocolError, &limits)?; continue; }
                                let item = BusinessRpcV3Delivery { delivery, connection_epoch: epoch, stream_id: id, commands: command_tx.clone() };
                                if deliveries.try_send(item).is_err() { reset(&writer_tx, id, V3ResetCode::Overloaded, &limits)?; let _ = streams.reset(id); }
                            } else { break Err(Error::Protocol); }
                        }
                    }
                    V3FrameType::WindowUpdate => {
                        let increment = u32::from_be_bytes(frame.payload.as_ref().try_into().map_err(|_| Error::Protocol)?);
                        writer_tx.try_send(WriterMessage::Update(id, increment)).map_err(|_| Error::Overloaded)?;
                    }
                    V3FrameType::Ping => {
                        let pong = fixed(0, V3FrameType::Pong, &frame.payload, &limits)?;
                        if sync_response_ok && !provider_ready {
                            let (done, receive) = oneshot::channel();
                            writer_tx.try_send(WriterMessage::Pong(pong, done)).map_err(|_| Error::Overloaded)?;
                            pong_written = Some(receive);
                        } else { enqueue(&writer_tx, pong)?; }
                    }
                    V3FrameType::Pong => {
                        let nonce = u64::from_be_bytes(frame.payload.as_ref().try_into().map_err(|_| Error::Protocol)?);
                        if heartbeat_pending.as_ref().is_some_and(|(expected, _)| *expected == nonce) { heartbeat_pending = None; }
                    }
                    V3FrameType::ResetStream | V3FrameType::CloseStream => {
                        if frame.header.frame_type == V3FrameType::ResetStream {
                            let reason = decode::<V3Reset>(&frame);
                            if !matches!(reason, Ok(ref reason) if reason.validate().is_ok()) {
                                reset(&writer_tx, id, V3ResetCode::ProtocolError, &limits)?;
                            }
                        }
                        // Failed initial sync invalidates only the Provider parent. The event
                        // subscription on this same connection remains independently usable.
                        let target = if sync_id == Some(id) { provider_id.unwrap_or(id) } else { id };
                        if target != id { reset(&writer_tx, target, V3ResetCode::Cancel, &limits)?; }
                        release_stream!(target);
                    }
                    V3FrameType::GoAway => {
                        let away: V3GoAway = decode(&frame)?;
                        away.validate().map_err(|_| Error::Protocol)?;
                        // N is the highest client-initiated stream the server considered.
                        // Calls above N have unknown ownership and are not retried implicitly.
                        for id in invalidations.keys().copied().filter(|id| *id > away.last_stream_id).collect::<Vec<_>>() {
                            invalidations.remove(&id);
                            let _ = streams.reset(id);
                            let _ = writer_tx.try_send(WriterMessage::Reset(id));
                        }
                        for id in pending_commands.keys().copied().filter(|id| *id > away.last_stream_id).collect::<Vec<_>>() {
                            pending_commands.remove(&id);
                            let _ = streams.reset(id);
                            let _ = writer_tx.try_send(WriterMessage::Reset(id));
                        }
                        ready.send_replace(false);
                        if draining.is_none() {
                            draining = Some(tokio::time::Instant::now() + config.request_timeout);
                        }
                    }
                }
            }
        }
        }
    }
    .await;
    handlers.abort_all();
    writer_stop.cancel();
    let _ = writer.await;
    result
}

fn complete_client_response(
    id: u32,
    bytes: &[u8],
    sync_id: &mut Option<u32>,
    sync_response_ok: &mut bool,
    invalidations: &mut HashMap<u32, oneshot::Sender<Result<AuthInvalidateResponse, Error>>>,
    pending_commands: &mut PendingCommands,
    meta: &mut HashMap<u32, V3Response>,
) -> Result<(), Error> {
    let response = meta.remove(&id).ok_or(Error::Protocol)?;
    if *sync_id == Some(id) {
        if let Some(error) = response.error {
            return Err(Error::Remote(error.code));
        }
        let _: AuthSyncResponse = serde_json::from_slice(bytes).map_err(|_| Error::Protocol)?;
        *sync_id = None;
        *sync_response_ok = true;
    } else if let Some(done) = invalidations.remove(&id) {
        let result = if let Some(error) = response.error {
            Err(Error::Remote(error.code))
        } else {
            serde_json::from_slice(bytes).map_err(|_| Error::Protocol)
        };
        let reset_provider = matches!(result, Err(Error::Remote(RpcErrorCode::StaleRevision)));
        let _ = done.send(result);
        if reset_provider {
            return Err(Error::Remote(RpcErrorCode::StaleRevision));
        }
    } else if let Some((expected, done)) = pending_commands.remove(&id) {
        let result = if let Some(error) = response.error {
            Err(Error::Remote(error.code))
        } else {
            serde_json::from_slice::<DeviceCommandSendResponse>(bytes)
                .map_err(|_| Error::Protocol)
                .and_then(|value| {
                    if value.dispatch.command_id == expected {
                        Ok(value.dispatch)
                    } else {
                        Err(Error::Protocol)
                    }
                })
        };
        let _ = done.send(result);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use netbaiot_protocol::{
        DeliveryId, DeviceEvent, DeviceEventKind, DeviceId, DeviceKey, EventId, Heartbeat,
        ProductId, SourceMessageId, TenantId,
        business_rpc::{
            AuthenticatedDeviceWire, DeviceAuthenticateRequest, ResolveVerifierRequest,
            ResolveVerifierResponse,
        },
        business_rpc_v3::{V3_HEADER_BYTES, V3FrameHeader},
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::Notify,
    };

    async fn read(socket: &mut TcpStream) -> Frame {
        let mut raw = [0; V3_HEADER_BYTES];
        socket.read_exact(&mut raw).await.unwrap();
        let header = V3FrameHeader::parse(&raw, 8192).unwrap();
        let mut payload = vec![0; header.payload_len as usize];
        socket.read_exact(&mut payload).await.unwrap();
        Frame {
            header,
            payload: payload.into(),
        }
    }
    async fn write(socket: &mut TcpStream, id: u32, ty: V3FrameType, flags: u8, bytes: &[u8]) {
        let header = V3FrameHeader {
            payload_len: bytes.len() as u32,
            stream_id: id,
            frame_type: ty,
            flags,
        };
        socket.write_all(&header.encode()).await.unwrap();
        socket.write_all(bytes).await.unwrap();
    }
    async fn meta(socket: &mut TcpStream, id: u32, ty: V3FrameType, value: &impl Serialize) {
        write(socket, id, ty, 0, &serde_json::to_vec(value).unwrap()).await;
    }
    async fn handshake(socket: &mut TcpStream, epoch: u64) {
        let mut prefix = [0; 4];
        socket.read_exact(&mut prefix).await.unwrap();
        let length = u32::from_be_bytes(prefix) as usize;
        assert!(length <= 4096);
        let mut bytes = vec![0; length];
        socket.read_exact(&mut bytes).await.unwrap();
        let V3Bootstrap::Hello {
            version, limits, ..
        } = serde_json::from_slice(&bytes).unwrap()
        else {
            panic!("Hello required")
        };
        assert_eq!(version, BUSINESS_RPC_V3_VERSION);
        let ready = serde_json::to_vec(&V3Bootstrap::Ready {
            version,
            limits,
            connection_epoch: epoch,
        })
        .unwrap();
        socket
            .write_all(&(ready.len() as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&ready).await.unwrap();
    }
    async fn subscription(socket: &mut TcpStream) -> (u32, SubscriptionId) {
        let frame = read(socket).await;
        assert_eq!(frame.header.frame_type, V3FrameType::Open);
        let V3Open::EventSubscription {
            subscription_id, ..
        } = decode(&frame).unwrap()
        else {
            panic!("subscription required")
        };
        meta(
            socket,
            frame.header.stream_id,
            V3FrameType::Accept,
            &V3Accept {
                provider_epoch: None,
                sync_required: false,
            },
        )
        .await;
        (frame.header.stream_id, subscription_id)
    }
    async fn provider(socket: &mut TcpStream) -> u32 {
        let parent = read(socket).await;
        assert!(matches!(
            decode::<V3Open>(&parent).unwrap(),
            V3Open::Provider { .. }
        ));
        let id = parent.header.stream_id;
        meta(
            socket,
            id,
            V3FrameType::Accept,
            &V3Accept {
                provider_epoch: Some(1),
                sync_required: true,
            },
        )
        .await;
        let sync = read(socket).await;
        assert!(
            matches!(decode::<V3Open>(&sync).unwrap(), V3Open::Rpc { ref method, .. } if method == "auth.sync")
        );
        let data = read(socket).await;
        assert_eq!(data.header.stream_id, sync.header.stream_id);
        assert_eq!(data.header.flags, V3_END_STREAM);
        let body = serde_json::to_vec(&AuthSyncResponse {
            applied_revision: 1,
        })
        .unwrap();
        meta(
            socket,
            sync.header.stream_id,
            V3FrameType::Response,
            &V3Response {
                content_length: body.len() as u32,
                error: None,
            },
        )
        .await;
        write(
            socket,
            sync.header.stream_id,
            V3FrameType::Data,
            V3_END_STREAM,
            &body,
        )
        .await;
        write(socket, 0, V3FrameType::Ping, 0, &1u64.to_be_bytes()).await;
        loop {
            if read(socket).await.header.frame_type == V3FrameType::Pong {
                break;
            }
        }
        id
    }
    struct HeldHandler {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        dropped: Mutex<Option<oneshot::Sender<()>>>,
        release: Notify,
    }
    struct HandlerDrop(Option<oneshot::Sender<()>>);
    impl Drop for HandlerDrop {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }
    #[async_trait]
    impl BusinessAuthHandler for HeldHandler {
        async fn authenticate(
            &self,
            _: DeviceAuthenticateRequest,
        ) -> Result<AuthenticatedDeviceWire, RpcError> {
            let _guard = HandlerDrop(self.dropped.lock().unwrap().take());
            if let Some(tx) = self.entered.lock().unwrap().take() {
                let _ = tx.send(());
            }
            self.release.notified().await;
            Err(RpcError::new(RpcErrorCode::Unavailable, "released"))
        }
        async fn resolve_verifier(
            &self,
            _: ResolveVerifierRequest,
        ) -> Result<ResolveVerifierResponse, RpcError> {
            Err(RpcError::new(RpcErrorCode::Unavailable, "unused"))
        }
    }
    #[tokio::test]
    async fn disconnected_auth_handler_is_cancelled_before_replacement_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (entered, entered_rx) = oneshot::channel();
        let (dropped, dropped_rx) = oneshot::channel();
        let handler = Arc::new(HeldHandler {
            entered: Mutex::new(Some(entered)),
            dropped: Mutex::new(Some(dropped)),
            release: Notify::new(),
        });
        let mut config = BusinessRpcV3ClientConfig::development(
            listener.local_addr().unwrap(),
            "test-token".into(),
        );
        config.events = false;
        config.reconnect_initial = Duration::from_millis(1);
        config.reconnect_max = Duration::from_millis(1);
        let peer = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            handshake(&mut first, 1).await;
            let parent = provider(&mut first).await;
            let bytes =
                br#"{"credential_id":"cred-device","secret_hex":"00","min_auth_revision":1}"#;
            meta(
                &mut first,
                2,
                V3FrameType::Open,
                &V3Open::Rpc {
                    parent_stream_id: Some(parent),
                    request_id: Uuid::new_v4(),
                    method: "device.authenticate".into(),
                    deadline_ms: 5000,
                    content_length: bytes.len() as u32,
                },
            )
            .await;
            write(&mut first, 2, V3FrameType::Data, V3_END_STREAM, bytes).await;
            entered_rx.await.unwrap();
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            dropped_rx.await.unwrap();
            handshake(&mut second, 2).await;
            provider(&mut second).await;
            second
        });
        let (client, _) = BusinessRpcV3Client::connect(config, Some(handler.clone())).unwrap();
        let mut second = tokio::time::timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        handler.release.notify_waiters();
        let until = tokio::time::Instant::now() + Duration::from_millis(100);
        while let Ok(frame) = tokio::time::timeout_at(until, read(&mut second)).await {
            assert_ne!(
                frame.header.frame_type,
                V3FrameType::Response,
                "retired handler response crossed connection epoch"
            );
        }
        client.shutdown().await;
    }
    #[tokio::test]
    async fn one_hundred_reconnect_epochs_and_shutdown_release_driver() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = BusinessRpcV3ClientConfig::development(
            listener.local_addr().unwrap(),
            "test-token".into(),
        );
        config.provider = false;
        config.reconnect_initial = Duration::from_millis(1);
        config.reconnect_max = Duration::from_millis(1);
        let peer = tokio::spawn(async move {
            for epoch in 1..=100 {
                let (mut socket, _) = listener.accept().await.unwrap();
                handshake(&mut socket, epoch).await;
                subscription(&mut socket).await;
            }
        });
        let (client, _) = BusinessRpcV3Client::connect(config, None).unwrap();
        tokio::time::timeout(Duration::from_secs(20), peer)
            .await
            .unwrap()
            .unwrap();
        client.shutdown().await;
        assert!(!client.ready());
        let weak = Arc::downgrade(&client.inner);
        drop(client);
        assert!(weak.upgrade().is_none());
    }
    fn event() -> DeviceEvent {
        DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("pending-before-reset").unwrap(),
            device: DeviceKey {
                tenant_id: TenantId::new("tenant").unwrap(),
                product_id: ProductId::new("product").unwrap(),
                device_id: DeviceId::new("device").unwrap(),
            },
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 2 }),
        }
    }
    #[tokio::test]
    async fn replay_to_full_receive_queue_preserves_old_delivery_and_fences_ack() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = BusinessRpcV3ClientConfig::development(
            listener.local_addr().unwrap(),
            "test-token".into(),
        );
        config.provider = false;
        let (commands, command_rx) = mpsc::channel(32);
        let (deliveries, mut receive) = mpsc::channel(1);
        let event = event();
        let old_id = DeliveryId::generate();
        deliveries
            .try_send(BusinessRpcV3Delivery {
                delivery: EventDelivery {
                    delivery_id: old_id,
                    subscription_id: SubscriptionId::generate(),
                    event: event.clone(),
                    attempt: 1,
                },
                connection_epoch: 1,
                stream_id: 2,
                commands: commands.clone(),
            })
            .ok()
            .unwrap();
        let stop = CancellationToken::new();
        let (ready, _) = watch::channel(false);
        let mut context = DriverContext {
            config,
            handler: None,
            commands: command_rx,
            command_tx: commands,
            deliveries,
            ready,
            revision: Arc::new(AtomicU64::new(1)),
            epoch: Arc::new(AtomicU64::new(2)),
            stop: stop.clone(),
        };
        let peer = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (parent, subscription_id) = subscription(&mut socket).await;
            let delivery = EventDelivery {
                delivery_id: DeliveryId::generate(),
                subscription_id,
                event,
                attempt: 2,
            };
            let body = serde_json::to_vec(&delivery).unwrap();
            meta(
                &mut socket,
                2,
                V3FrameType::Open,
                &V3Open::EventDelivery {
                    parent_stream_id: parent,
                    delivery_id: delivery.delivery_id.0,
                    event_id: delivery.event.event_id.0,
                    attempt: 2,
                    content_length: body.len() as u32,
                },
            )
            .await;
            write(&mut socket, 2, V3FrameType::Data, V3_END_STREAM, &body).await;
            loop {
                let frame = read(&mut socket).await;
                if frame.header.frame_type == V3FrameType::ResetStream {
                    assert_eq!(frame.header.stream_id, 2);
                    assert_eq!(
                        decode::<V3Reset>(&frame).unwrap().code,
                        V3ResetCode::Overloaded
                    );
                    break;
                }
            }
            let old = receive.recv().await.unwrap();
            assert_eq!(old.delivery.delivery_id, old_id);
            assert!(matches!(old.ack().await, Err(Error::Unavailable)));
            assert!(receive.try_recv().is_err());
            stop.cancel();
        };
        let driver = async {
            let socket = TcpStream::connect(context.config.address).await.unwrap();
            let _ = connected(Box::new(socket), 2, V3Limits::default(), &mut context).await;
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(peer, driver);
        })
        .await
        .unwrap();
    }
}
