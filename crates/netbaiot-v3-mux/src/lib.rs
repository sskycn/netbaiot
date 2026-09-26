//! Business RPC V3 stream machinery shared by gateway and SDK.
//! A connection has exactly one writer. That writer obtains frames from `MuxScheduler`;
//! no stream may write directly to the socket.
use bytes::Bytes;
use netbaiot_protocol::business_rpc_v3::{
    V3_END_STREAM, V3_MAX_MESSAGE_BYTES, V3_MAX_STREAM_ID, V3FrameHeader, V3FrameType, V3Limits,
    V3Open, V3WireError,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MuxError {
    Connection(V3WireError),
    Stream,
    FlowControl,
    Overloaded,
    MessageTooLarge,
    Io,
    Timeout,
}

#[derive(Clone)]
pub struct Frame {
    pub header: V3FrameHeader,
    pub payload: Bytes,
}
impl std::fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Frame")
            .field("header", &self.header)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}
impl Frame {
    pub fn new(
        stream_id: u32,
        frame_type: V3FrameType,
        flags: u8,
        payload: Bytes,
        max: usize,
    ) -> Result<Self, MuxError> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| MuxError::MessageTooLarge)?;
        let header = V3FrameHeader {
            payload_len,
            stream_id,
            frame_type,
            flags,
        };
        V3FrameHeader::parse(&header.encode(), max).map_err(MuxError::Connection)?;
        Ok(Self { header, payload })
    }
}

/// Parsing the header and its negotiated bound precedes the only payload allocation.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
    timeout: Duration,
) -> Result<Frame, MuxError> {
    tokio::time::timeout(timeout, async {
        let mut raw = [0; 12];
        reader
            .read_exact(&mut raw)
            .await
            .map_err(|_| MuxError::Io)?;
        let header = V3FrameHeader::parse(&raw, max).map_err(MuxError::Connection)?;
        let mut payload = vec![0; header.payload_len as usize];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| MuxError::Io)?;
        Ok(Frame {
            header,
            payload: Bytes::from(payload),
        })
    })
    .await
    .map_err(|_| MuxError::Timeout)?
}

/// One task owns each inbound byte stream until it has assembled a complete frame. `recv` is
/// cancellation safe for the state machine; cancelling `read_exact` midway is not. At most 32
/// complete negotiated-size frames can wait in this channel. Dropping this owner stops the task.
pub struct FrameReader {
    rx: mpsc::Receiver<Result<Frame, MuxError>>,
    task: JoinHandle<()>,
}
impl FrameReader {
    pub fn spawn<R: AsyncRead + Unpin + Send + 'static>(
        mut reader: R,
        max: usize,
        timeout: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            loop {
                let frame = read_frame(&mut reader, max, timeout).await;
                let failed = frame.is_err();
                if tx.send(frame).await.is_err() || failed {
                    break;
                }
            }
        });
        Self { rx, task }
    }
    pub async fn recv(&mut self) -> Option<Result<Frame, MuxError>> {
        self.rx.recv().await
    }
}
impl Drop for FrameReader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    timeout: Duration,
) -> Result<(), MuxError> {
    tokio::time::timeout(timeout, async {
        writer
            .write_all(&frame.header.encode())
            .await
            .map_err(|_| MuxError::Io)?;
        writer
            .write_all(&frame.payload)
            .await
            .map_err(|_| MuxError::Io)?;
        Ok(())
    })
    .await
    .map_err(|_| MuxError::Timeout)?
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Initiator {
    Client,
    Server,
}
impl Initiator {
    fn owns(self, id: u32) -> bool {
        id != 0 && id <= V3_MAX_STREAM_ID && (id & 1 == if self == Self::Client { 1 } else { 0 })
    }
    fn opposite(self) -> Self {
        if self == Self::Client {
            Self::Server
        } else {
            Self::Client
        }
    }
}

/// The global reservation counts declared, not yet received, body bytes. Drop releases it
/// on completion, RESET, timeout, or connection teardown.
#[derive(Debug)]
pub struct ReassemblyBudget {
    max: usize,
    used: AtomicUsize,
}
impl ReassemblyBudget {
    pub fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            max,
            used: AtomicUsize::new(0),
        })
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation, MuxError> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.max)
            })
            .map_err(|_| MuxError::Overloaded)?;
        Ok(Reservation {
            budget: self.clone(),
            bytes,
        })
    }
}
#[derive(Debug)]
struct Reservation {
    budget: Arc<ReassemblyBudget>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamKind {
    Provider,
    EventSubscription,
    Rpc,
    EventDelivery,
}
impl From<&V3Open> for StreamKind {
    fn from(open: &V3Open) -> Self {
        match open {
            V3Open::Provider { .. } => Self::Provider,
            V3Open::EventSubscription { .. } => Self::EventSubscription,
            V3Open::Rpc { .. } => Self::Rpc,
            V3Open::EventDelivery { .. } => Self::EventDelivery,
        }
    }
}

#[derive(Debug)]
struct ReceiveBody {
    expected: usize,
    bytes: Vec<u8>,
    _reservation: Reservation,
}
#[derive(Debug)]
struct Stream {
    kind: StreamKind,
    parent: Option<u32>,
    remote_closed: bool,
    local_closed: bool,
    response_seen: bool,
    body: Option<ReceiveBody>,
    recv_window: u32,
}

pub struct ReceivedData {
    pub complete: Option<Bytes>,
    pub connection_update: u32,
    pub stream_update: u32,
}

/// Tracks peer stream IDs by a high-water mark. Closed IDs need no unbounded tombstones.
pub struct StreamTable {
    local: Initiator,
    highest_peer: u32,
    next_local: u32,
    limits: V3Limits,
    streams: HashMap<u32, Stream>,
    connection_window: u32,
    connection_reserved: usize,
    connection_budget: usize,
    global_budget: Arc<ReassemblyBudget>,
    accepting: bool,
}
impl StreamTable {
    pub fn new(
        local: Initiator,
        limits: V3Limits,
        connection_budget: usize,
        global_budget: Arc<ReassemblyBudget>,
    ) -> Result<Self, MuxError> {
        limits.validate().map_err(MuxError::Connection)?;
        if connection_budget == 0 || connection_budget > global_budget.max {
            return Err(MuxError::Overloaded);
        }
        Ok(Self {
            local,
            highest_peer: 0,
            next_local: if local == Initiator::Client { 1 } else { 2 },
            connection_window: limits.initial_connection_window_bytes,
            limits,
            streams: HashMap::new(),
            connection_reserved: 0,
            connection_budget,
            global_budget,
            accepting: true,
        })
    }
    pub fn active(&self) -> usize {
        self.streams.len()
    }
    pub fn reserved_bytes(&self) -> usize {
        self.connection_reserved
    }
    pub fn last_peer_id(&self) -> u32 {
        self.highest_peer
    }
    /// The final legal local ID is consumed before GOAWAY; IDs never wrap or restart in place.
    pub fn local_ids_exhausted(&self) -> bool {
        self.next_local > V3_MAX_STREAM_ID
    }
    /// GOAWAY's last ID is the highest accepted peer stream, not an arbitrary future ID.
    pub fn goaway(&mut self) -> u32 {
        self.accepting = false;
        self.highest_peer
    }
    pub fn check_peer_id(&self, id: u32) -> Result<(), MuxError> {
        if !self.local.opposite().owns(id) || id <= self.highest_peer {
            return Err(MuxError::Connection(V3WireError::StreamId));
        }
        if !self.accepting {
            return Err(MuxError::Stream);
        }
        Ok(())
    }
    fn check_parent(&self, open: &V3Open) -> Result<(), MuxError> {
        let Some(parent) = open.parent() else {
            return Ok(());
        };
        let Some(owner) = self.streams.get(&parent) else {
            return Err(MuxError::Stream);
        };
        if owner.remote_closed || owner.local_closed {
            return Err(MuxError::Stream);
        }
        match open {
            V3Open::EventDelivery { .. } if owner.kind == StreamKind::EventSubscription => Ok(()),
            V3Open::Rpc { method, .. }
                if method.starts_with("auth.")
                    || method.starts_with("device.authenticate")
                    || method == "device.resolve_verifier" =>
            {
                if owner.kind == StreamKind::Provider {
                    Ok(())
                } else {
                    Err(MuxError::Stream)
                }
            }
            V3Open::Rpc { .. } => Ok(()),
            _ => Err(MuxError::Stream),
        }
    }
    fn insert(&mut self, id: u32, open: &V3Open, inbound: bool) -> Result<(), MuxError> {
        if self.streams.len() >= self.limits.max_concurrent_streams as usize {
            return Err(MuxError::Overloaded);
        }
        self.check_parent(open)?;
        let kind = StreamKind::from(open);
        let length = if inbound { open.content_length() } else { 0 };
        if length > V3_MAX_MESSAGE_BYTES {
            return Err(MuxError::MessageTooLarge);
        }
        let reserved = self
            .connection_reserved
            .checked_add(length)
            .ok_or(MuxError::Overloaded)?;
        if reserved > self.connection_budget {
            return Err(MuxError::Overloaded);
        }
        let reservation = self.global_budget.reserve(length)?;
        let body = if inbound && matches!(kind, StreamKind::Rpc | StreamKind::EventDelivery) {
            Some(ReceiveBody {
                expected: length,
                bytes: Vec::new(),
                _reservation: reservation,
            })
        } else {
            None
        };
        self.connection_reserved = reserved;
        self.streams.insert(
            id,
            Stream {
                kind,
                parent: open.parent(),
                remote_closed: false,
                local_closed: false,
                response_seen: false,
                body,
                recv_window: self.limits.initial_stream_window_bytes,
            },
        );
        Ok(())
    }
    /// Parity and monotonicity are connection invariants. Even a refused ID is consumed.
    /// `highest_peer` is also GOAWAY's last considered ID; accepted streams are tracked separately.
    pub fn refuse_peer(&mut self, id: u32) -> Result<(), MuxError> {
        self.check_peer_id(id)?;
        self.highest_peer = id;
        Ok(())
    }
    /// Caller authorizes kind, method, and scope before this resource admission.
    pub fn open_peer(&mut self, id: u32, open: &V3Open) -> Result<(), MuxError> {
        self.refuse_peer(id)?;
        self.insert(id, open, true)
    }
    pub fn open_local(&mut self, open: &V3Open) -> Result<u32, MuxError> {
        let id = self.next_local;
        if !self.accepting || id > V3_MAX_STREAM_ID {
            return Err(MuxError::Stream);
        }
        self.insert(id, open, false)?;
        self.next_local = id.checked_add(2).unwrap_or(V3_MAX_STREAM_ID + 1);
        Ok(id)
    }
    pub fn receive_data(
        &mut self,
        id: u32,
        bytes: &[u8],
        end: bool,
    ) -> Result<ReceivedData, MuxError> {
        let stream = self.streams.get_mut(&id).ok_or(MuxError::Stream)?;
        if stream.remote_closed {
            return Err(MuxError::Stream);
        }
        let body = stream.body.as_mut().ok_or(MuxError::Stream)?;
        let length = u32::try_from(bytes.len()).map_err(|_| MuxError::MessageTooLarge)?;
        if length > self.connection_window {
            return Err(MuxError::Connection(V3WireError::Length));
        }
        if length > stream.recv_window {
            return Err(MuxError::FlowControl);
        }
        let next = body
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(MuxError::MessageTooLarge)?;
        if next > body.expected {
            return Err(MuxError::MessageTooLarge);
        }
        if end && next != body.expected {
            return Err(MuxError::Stream);
        }
        self.connection_window -= length;
        stream.recv_window -= length;
        body.bytes.extend_from_slice(bytes);
        let complete = if end {
            stream.remote_closed = true;
            let body = stream.body.take().ok_or(MuxError::Stream)?;
            self.connection_reserved -= body.expected;
            Some(Bytes::from(body.bytes))
        } else {
            None
        };
        if end && stream.local_closed {
            self.remove(id);
        }
        // Receive credit is restored only after the writer actually sends WINDOW_UPDATE.
        // This remains independent of the application's EventAck.
        Ok(ReceivedData {
            complete,
            connection_update: length,
            stream_update: if end { 0 } else { length },
        })
    }
    pub fn grant_credit(&mut self, id: u32, increment: u32) -> Result<(), MuxError> {
        if increment == 0 {
            return Err(MuxError::FlowControl);
        }
        if id == 0 {
            self.connection_window = self
                .connection_window
                .checked_add(increment)
                .filter(|window| *window <= self.limits.initial_connection_window_bytes)
                .ok_or(MuxError::Connection(V3WireError::Length))?;
        } else {
            let stream = self.streams.get_mut(&id).ok_or(MuxError::Stream)?;
            stream.recv_window = stream
                .recv_window
                .checked_add(increment)
                .filter(|window| *window <= self.limits.initial_stream_window_bytes)
                .ok_or(MuxError::FlowControl)?;
        }
        Ok(())
    }
    pub fn mark_response(&mut self, id: u32, expected: usize) -> Result<(), MuxError> {
        let stream = self.streams.get_mut(&id).ok_or(MuxError::Stream)?;
        if stream.response_seen
            || stream.body.is_some()
            || !stream.local_closed
            || expected > V3_MAX_MESSAGE_BYTES
        {
            return Err(MuxError::Stream);
        }
        let reserved = self
            .connection_reserved
            .checked_add(expected)
            .ok_or(MuxError::Overloaded)?;
        if reserved > self.connection_budget {
            return Err(MuxError::Overloaded);
        }
        let reservation = self.global_budget.reserve(expected)?;
        stream.body = Some(ReceiveBody {
            expected,
            bytes: Vec::new(),
            _reservation: reservation,
        });
        stream.response_seen = true;
        self.connection_reserved = reserved;
        Ok(())
    }
    pub fn mark_local_end(&mut self, id: u32) -> Result<(), MuxError> {
        let stream = self.streams.get_mut(&id).ok_or(MuxError::Stream)?;
        if stream.local_closed {
            return Err(MuxError::Stream);
        }
        stream.local_closed = true;
        if stream.remote_closed {
            self.remove(id);
        }
        Ok(())
    }
    pub fn close_parent(&mut self, id: u32) -> Vec<u32> {
        let mut removed = Vec::new();
        if self
            .streams
            .get(&id)
            .is_some_and(|s| matches!(s.kind, StreamKind::Provider | StreamKind::EventSubscription))
        {
            let children: Vec<_> = self
                .streams
                .iter()
                .filter_map(|(&child, stream)| (stream.parent == Some(id)).then_some(child))
                .collect();
            for child in children {
                self.remove(child);
                removed.push(child);
            }
        }
        self.remove(id);
        removed
    }
    pub fn reset(&mut self, id: u32) -> Result<Vec<u32>, MuxError> {
        if !self.streams.contains_key(&id) {
            return Err(MuxError::Stream);
        }
        Ok(self.close_parent(id))
    }
    fn remove(&mut self, id: u32) {
        if let Some(stream) = self.streams.remove(&id)
            && let Some(body) = stream.body
        {
            self.connection_reserved -= body.expected;
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataClass {
    Rpc,
    Event,
}
/// Local sender policy. These counters are independent of the peer's receive windows.
/// A stream can submit at most `stream_bytes` of DATA before stream WINDOW_UPDATE;
/// all streams together can submit at most `connection_bytes` before connection
/// WINDOW_UPDATE. Bytes already submitted to an ordered TCP/TLS stream cannot be
/// preempted by a later RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SendAheadLimits {
    pub stream_bytes: u32,
    pub connection_bytes: u32,
}
impl SendAheadLimits {
    pub fn validate(self, limits: &V3Limits) -> Result<(), MuxError> {
        if self.stream_bytes < limits.max_frame_payload_bytes
            || self.connection_bytes < self.stream_bytes
            || self.connection_bytes > 16 * 1024 * 1024
        {
            return Err(MuxError::FlowControl);
        }
        Ok(())
    }
}
struct OutboundBody {
    bytes: Bytes,
    cursor: usize,
}
/// Control bursts are capped at four. Data scheduling gives RPC four turns and Event one;
/// blocked streams are skipped, so a stalled stream cannot hold another stream's credit.
pub struct MuxScheduler {
    max_frame: usize,
    conn_window: u32,
    conn_max: u32,
    stream_max: u32,
    send_ahead: Option<SendAheadLimits>,
    conn_uncredited: u32,
    stream_uncredited: HashMap<u32, u32>,
    queued_bytes: usize,
    byte_limit: usize,
    control: VecDeque<Frame>,
    control_bytes: usize,
    bodies: HashMap<u32, OutboundBody>,
    windows: HashMap<u32, u32>,
    max_streams: usize,
    rpc: VecDeque<u32>,
    event: VecDeque<u32>,
    class_turn: u8,
    control_burst: u8,
}
impl MuxScheduler {
    pub fn new(limits: &V3Limits, byte_limit: usize) -> Result<Self, MuxError> {
        Self::with_send_ahead(limits, byte_limit, None)
    }
    pub fn with_send_ahead(
        limits: &V3Limits,
        byte_limit: usize,
        send_ahead: Option<SendAheadLimits>,
    ) -> Result<Self, MuxError> {
        limits.validate().map_err(MuxError::Connection)?;
        if let Some(policy) = send_ahead {
            policy.validate(limits)?;
        }
        if byte_limit == 0 {
            return Err(MuxError::Overloaded);
        }
        Ok(Self {
            max_frame: limits.max_frame_payload_bytes as usize,
            conn_window: limits.initial_connection_window_bytes,
            conn_max: limits.initial_connection_window_bytes,
            stream_max: limits.initial_stream_window_bytes,
            send_ahead,
            conn_uncredited: 0,
            stream_uncredited: HashMap::new(),
            queued_bytes: 0,
            byte_limit,
            control: VecDeque::new(),
            control_bytes: 0,
            bodies: HashMap::new(),
            windows: HashMap::new(),
            max_streams: limits.max_concurrent_streams as usize,
            rpc: VecDeque::new(),
            event: VecDeque::new(),
            class_turn: 0,
            control_burst: 0,
        })
    }
    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes + self.control_bytes
    }
    pub fn has_pending_frames(&self) -> bool {
        !self.control.is_empty() || !self.bodies.is_empty()
    }
    pub fn stalled_windows(&self) -> (bool, bool) {
        let mut connection = false;
        let mut stream = false;
        for (&id, body) in &self.bodies {
            if body.cursor == body.bytes.len() {
                continue;
            }
            if self.conn_window == 0 {
                connection = true;
            } else if self.windows.get(&id).is_some_and(|credit| *credit == 0) {
                stream = true;
            }
        }
        (connection, stream)
    }
    pub fn stalled_send_ahead(&self) -> (bool, bool) {
        let Some(policy) = self.send_ahead else {
            return (false, false);
        };
        let mut connection = false;
        let mut stream = false;
        for (&id, body) in &self.bodies {
            if body.cursor == body.bytes.len() {
                continue;
            }
            if self.conn_uncredited >= policy.connection_bytes {
                connection = true;
            } else if self.stream_uncredited.get(&id).copied().unwrap_or(0) >= policy.stream_bytes {
                stream = true;
            }
        }
        (connection, stream)
    }
    pub fn submitted_uncredited(&self) -> (u32, u64) {
        (
            self.conn_uncredited,
            self.stream_uncredited
                .values()
                .map(|value| u64::from(*value))
                .sum(),
        )
    }
    pub fn queue_control(&mut self, frame: Frame) -> Result<(), MuxError> {
        let size = frame.payload.len() + 12;
        if self.control.len() >= 64
            || self
                .control_bytes
                .checked_add(size)
                .is_none_or(|n| n > 64 * 4096)
        {
            return Err(MuxError::Overloaded);
        }
        self.control_bytes += size;
        self.control.push_back(frame);
        Ok(())
    }
    pub fn queue_body(&mut self, id: u32, class: DataClass, bytes: Bytes) -> Result<(), MuxError> {
        if id == 0
            || id > V3_MAX_STREAM_ID
            || self.bodies.contains_key(&id)
            || bytes.len() > V3_MAX_MESSAGE_BYTES
        {
            return Err(MuxError::MessageTooLarge);
        }
        let total = self
            .queued_bytes
            .checked_add(bytes.len())
            .ok_or(MuxError::Overloaded)?;
        if total > self.byte_limit
            || (!self.windows.contains_key(&id) && self.windows.len() >= self.max_streams)
        {
            return Err(MuxError::Overloaded);
        }
        self.queued_bytes = total;
        self.windows.entry(id).or_insert(self.stream_max);
        if self.send_ahead.is_some() {
            self.stream_uncredited.entry(id).or_insert(0);
        }
        self.bodies.insert(id, OutboundBody { bytes, cursor: 0 });
        match class {
            DataClass::Rpc => self.rpc.push_back(id),
            DataClass::Event => self.event.push_back(id),
        }
        Ok(())
    }
    pub fn window_update(&mut self, id: u32, increment: u32) -> Result<(), MuxError> {
        if increment == 0 {
            return Err(MuxError::FlowControl);
        }
        if id == 0 {
            self.conn_window = self
                .conn_window
                .checked_add(increment)
                .filter(|n| *n <= self.conn_max)
                .ok_or(MuxError::Connection(V3WireError::Length))?;
            self.conn_uncredited = self.conn_uncredited.saturating_sub(increment);
        } else {
            let window = self.windows.get_mut(&id).ok_or(MuxError::Stream)?;
            *window = window
                .checked_add(increment)
                .filter(|n| *n <= self.stream_max)
                .ok_or(MuxError::FlowControl)?;
            if let Some(outstanding) = self.stream_uncredited.get_mut(&id) {
                *outstanding = outstanding.saturating_sub(increment);
            }
        }
        Ok(())
    }
    fn take_data(&mut self, class: DataClass) -> Option<Frame> {
        let queue = match class {
            DataClass::Rpc => &mut self.rpc,
            DataClass::Event => &mut self.event,
        };
        for _ in 0..queue.len() {
            let id = queue.pop_front()?;
            let Some(body) = self.bodies.get_mut(&id) else {
                continue;
            };
            // OPEN/RESPONSE is a per-stream barrier. A control burst may yield to DATA,
            // but never let that DATA overtake its own metadata on the wire.
            if self.control.iter().any(|frame| {
                frame.header.stream_id == id
                    && matches!(
                        frame.header.frame_type,
                        V3FrameType::Open | V3FrameType::Response
                    )
            }) {
                queue.push_back(id);
                continue;
            }
            let Some(window) = self.windows.get_mut(&id) else {
                continue;
            };
            let remaining = body.bytes.len().saturating_sub(body.cursor);
            let n = remaining
                .min(self.max_frame)
                .min(*window as usize)
                .min(self.conn_window as usize);
            let outstanding = if self.send_ahead.is_some() {
                *self.stream_uncredited.entry(id).or_insert(0)
            } else {
                0
            };
            let n = if let Some(policy) = self.send_ahead {
                n.min(policy.stream_bytes.saturating_sub(outstanding) as usize)
                    .min(policy.connection_bytes.saturating_sub(self.conn_uncredited) as usize)
            } else {
                n
            };
            if n == 0 && remaining != 0 {
                queue.push_back(id);
                continue;
            }
            let Some(sent) = u32::try_from(n).ok() else {
                queue.push_back(id);
                continue;
            };
            let (next_connection, next_stream) = if self.send_ahead.is_some() {
                let Some(connection) = self.conn_uncredited.checked_add(sent) else {
                    queue.push_back(id);
                    continue;
                };
                let Some(stream) = outstanding.checked_add(sent) else {
                    queue.push_back(id);
                    continue;
                };
                (Some(connection), Some(stream))
            } else {
                (None, None)
            };
            let end = n == remaining;
            let payload = body.bytes.slice(body.cursor..body.cursor + n);
            body.cursor += n;
            *window -= sent;
            self.conn_window -= sent;
            if let (Some(connection), Some(stream)) = (next_connection, next_stream) {
                self.conn_uncredited = connection;
                self.stream_uncredited.insert(id, stream);
            }
            if end {
                let finished = self.bodies.remove(&id)?;
                self.queued_bytes -= finished.bytes.len();
            } else {
                queue.push_back(id);
            }
            return Frame::new(
                id,
                V3FrameType::Data,
                if end { V3_END_STREAM } else { 0 },
                payload,
                self.max_frame,
            )
            .ok();
        }
        None
    }
    pub fn next_frame(&mut self) -> Option<Frame> {
        if self.control_burst < 4 && !self.control.is_empty() {
            self.control_burst += 1;
            let frame = self.control.pop_front()?;
            self.control_bytes -= frame.payload.len() + 12;
            return Some(frame);
        }
        for _ in 0..5 {
            let class = if self.class_turn < 4 {
                DataClass::Rpc
            } else {
                DataClass::Event
            };
            self.class_turn = (self.class_turn + 1) % 5;
            if let Some(frame) = self.take_data(class) {
                self.control_burst = 0;
                return Some(frame);
            }
        }
        if let Some(frame) = self.control.pop_front() {
            self.control_bytes -= frame.payload.len() + 12;
            self.control_burst = 0;
            return Some(frame);
        }
        None
    }
    pub fn reset(&mut self, id: u32) {
        if let Some(body) = self.bodies.remove(&id) {
            self.queued_bytes -= body.bytes.len();
        }
        self.windows.remove(&id);
        self.stream_uncredited.remove(&id);
        self.rpc.retain(|entry| *entry != id);
        self.event.retain(|entry| *entry != id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_protocol::business_rpc_v3::V3Open;
    use uuid::Uuid;
    fn limits() -> V3Limits {
        V3Limits::default()
    }
    #[test]
    fn frame_debug_omits_application_body() {
        let frame = Frame::new(
            2,
            V3FrameType::Data,
            V3_END_STREAM,
            Bytes::from_static(b"private-device-credential"),
            8192,
        )
        .unwrap();
        let debug = format!("{frame:?}");
        assert!(debug.contains("payload_bytes"));
        assert!(!debug.contains("private-device-credential"));
    }
    #[test]
    fn local_id_exhaustion_is_detected_before_reuse() {
        let request = V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.invalidate".into(),
            deadline_ms: 1_000,
            content_length: 1,
        };
        let mut client = StreamTable::new(
            Initiator::Client,
            limits(),
            100_000,
            ReassemblyBudget::new(100_000),
        )
        .unwrap();
        client.next_local = V3_MAX_STREAM_ID;
        assert_eq!(client.open_local(&request), Ok(V3_MAX_STREAM_ID));
        assert!(client.local_ids_exhausted());
        assert!(client.open_local(&request).is_err());

        let mut server = StreamTable::new(
            Initiator::Server,
            limits(),
            100_000,
            ReassemblyBudget::new(100_000),
        )
        .unwrap();
        server.next_local = V3_MAX_STREAM_ID - 1;
        assert_eq!(server.open_local(&request), Ok(V3_MAX_STREAM_ID - 1));
        assert!(server.local_ids_exhausted());
    }
    #[test]
    fn event_fragments_and_rpc_interleaves() {
        let mut s = MuxScheduler::new(&limits(), 128 * 1024).unwrap();
        s.queue_body(2, DataClass::Event, Bytes::from(vec![1; 64 * 1024]))
            .unwrap();
        let first = s.next_frame().unwrap();
        assert_eq!(first.header.stream_id, 2);
        assert_eq!(first.payload.len(), 8192);
        s.queue_body(3, DataClass::Rpc, Bytes::from(vec![2; 100]))
            .unwrap();
        let second = s.next_frame().unwrap();
        assert_eq!(second.header.stream_id, 3);
        assert_eq!(second.header.flags, V3_END_STREAM);
        assert!(s.queued_bytes() > 0);
    }
    #[test]
    fn late_rpc_runs_when_bulk_send_ahead_is_exhausted() {
        let mut scheduler = MuxScheduler::with_send_ahead(
            &limits(),
            2 * 1024 * 1024,
            Some(SendAheadLimits {
                stream_bytes: 16 * 1024,
                connection_bytes: 32 * 1024,
            }),
        )
        .unwrap();
        scheduler
            .queue_body(2, DataClass::Event, Bytes::from(vec![0; 1024 * 1024]))
            .unwrap();
        for _ in 0..2 {
            assert_eq!(scheduler.next_frame().unwrap().header.stream_id, 2);
        }
        assert_eq!(scheduler.stalled_send_ahead(), (false, true));
        assert!(scheduler.next_frame().is_none());
        scheduler
            .queue_body(4, DataClass::Rpc, Bytes::from(vec![1; 100]))
            .unwrap();
        assert_eq!(scheduler.next_frame().unwrap().header.stream_id, 4);
        scheduler.reset(4);
        assert_eq!(
            scheduler.submitted_uncredited(),
            (16 * 1024 + 100, 16 * 1024)
        );
        scheduler.window_update(2, 8192).unwrap();
        assert_eq!(scheduler.next_frame().unwrap().header.stream_id, 2);
        scheduler.reset(2);
        assert_eq!(scheduler.submitted_uncredited().1, 0);
    }
    #[test]
    fn three_bulk_streams_share_connection_send_ahead() {
        let mut scheduler = MuxScheduler::with_send_ahead(
            &limits(),
            4 * 1024 * 1024,
            Some(SendAheadLimits {
                stream_bytes: 16 * 1024,
                connection_bytes: 32 * 1024,
            }),
        )
        .unwrap();
        for id in [2, 4, 6] {
            scheduler
                .queue_body(id, DataClass::Event, Bytes::from(vec![0; 1024 * 1024]))
                .unwrap();
        }
        for _ in 0..4 {
            assert_eq!(scheduler.next_frame().unwrap().payload.len(), 8192);
        }
        assert_eq!(scheduler.submitted_uncredited().0, 32 * 1024);
        assert!(scheduler.stalled_send_ahead().0);
        assert!(scheduler.next_frame().is_none());
        scheduler
            .queue_body(1, DataClass::Rpc, Bytes::from(vec![1; 100]))
            .unwrap();
        assert!(scheduler.next_frame().is_none());
        scheduler.window_update(0, 8192).unwrap();
        assert_eq!(scheduler.next_frame().unwrap().header.stream_id, 1);
    }
    #[test]
    fn reset_keeps_connection_responsibility_until_peer_credit_arrives() {
        let mut scheduler = MuxScheduler::with_send_ahead(
            &limits(),
            100_000,
            Some(SendAheadLimits {
                stream_bytes: 8192,
                connection_bytes: 8192,
            }),
        )
        .unwrap();
        scheduler
            .queue_body(2, DataClass::Event, Bytes::from(vec![0; 20_000]))
            .unwrap();
        assert_eq!(scheduler.next_frame().unwrap().payload.len(), 8192);
        scheduler.reset(2);
        assert_eq!(scheduler.queued_bytes(), 0);
        assert_eq!(scheduler.submitted_uncredited(), (8192, 0));
        scheduler
            .queue_body(3, DataClass::Rpc, Bytes::from_static(b"rpc"))
            .unwrap();
        assert!(scheduler.next_frame().is_none());
        scheduler.window_update(0, 8192).unwrap();
        assert_eq!(scheduler.next_frame().unwrap().header.stream_id, 3);
    }
    #[test]
    fn invalid_send_ahead_never_creates_an_unsendable_frame() {
        let invalid = [
            SendAheadLimits {
                stream_bytes: 1,
                connection_bytes: 8192,
            },
            SendAheadLimits {
                stream_bytes: 4096,
                connection_bytes: 1,
            },
            SendAheadLimits {
                stream_bytes: 8192,
                connection_bytes: 16 * 1024 * 1024 + 1,
            },
        ];
        for policy in invalid {
            assert!(MuxScheduler::with_send_ahead(&limits(), 100_000, Some(policy)).is_err());
        }
    }
    #[test]
    fn eight_kib_send_ahead_progresses_with_large_receive_window() {
        let limits = limits();
        let mut sender = MuxScheduler::with_send_ahead(
            &limits,
            100_000,
            Some(SendAheadLimits {
                stream_bytes: 8192,
                connection_bytes: 8192,
            }),
        )
        .unwrap();
        let mut receiver = StreamTable::new(
            Initiator::Client,
            limits.clone(),
            100_000,
            ReassemblyBudget::new(100_000),
        )
        .unwrap();
        let request = V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: 20_000,
        };
        receiver.open_peer(2, &request).unwrap();
        sender
            .queue_body(2, DataClass::Rpc, Bytes::from(vec![0; 20_000]))
            .unwrap();
        let first = sender.next_frame().unwrap();
        assert_eq!(first.payload.len(), 8192);
        assert!(sender.next_frame().is_none());
        let received = receiver.receive_data(2, &first.payload, false).unwrap();
        assert_eq!(received.stream_update, 8192);
        assert_eq!(received.connection_update, 8192);
        receiver
            .grant_credit(0, received.connection_update)
            .unwrap();
        receiver.grant_credit(2, received.stream_update).unwrap();
        sender.window_update(0, received.connection_update).unwrap();
        sender.window_update(2, received.stream_update).unwrap();
        assert_eq!(sender.next_frame().unwrap().payload.len(), 8192);
    }
    #[tokio::test]
    async fn rpc_frame_reaches_wire_before_large_event_finishes() {
        let mut scheduler = MuxScheduler::new(&limits(), 128 * 1024).unwrap();
        scheduler
            .queue_body(2, DataClass::Event, Bytes::from(vec![1; 64 * 1024]))
            .unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(128 * 1024);
        let first = scheduler.next_frame().unwrap();
        write_frame(&mut writer, &first, Duration::from_secs(1))
            .await
            .unwrap();
        scheduler
            .queue_body(4, DataClass::Rpc, Bytes::from(vec![2; 100]))
            .unwrap();
        while let Some(frame) = scheduler.next_frame() {
            write_frame(&mut writer, &frame, Duration::from_secs(1))
                .await
                .unwrap();
        }
        let mut order = Vec::new();
        for _ in 0..9 {
            let frame = read_frame(&mut reader, 8192, Duration::from_secs(1))
                .await
                .unwrap();
            order.push(frame.header.stream_id);
        }
        assert_eq!(order[0], 2);
        let rpc = order.iter().position(|id| *id == 4).unwrap();
        let last_event = order.iter().rposition(|id| *id == 2).unwrap();
        assert!(
            rpc < last_event,
            "RPC stayed behind the complete Event: {order:?}"
        );
    }
    #[test]
    fn control_burst_cannot_move_data_ahead_of_its_response() {
        let mut s = MuxScheduler::new(&limits(), 128 * 1024).unwrap();
        for id in [1, 3, 5, 7] {
            s.queue_control(
                Frame::new(id, V3FrameType::Accept, 0, Bytes::from_static(b"{}"), 8192).unwrap(),
            )
            .unwrap();
        }
        s.queue_control(
            Frame::new(9, V3FrameType::Response, 0, Bytes::from_static(b"{}"), 8192).unwrap(),
        )
        .unwrap();
        s.queue_body(9, DataClass::Rpc, Bytes::from_static(b"reply"))
            .unwrap();
        let frames: Vec<_> = (0..6)
            .filter_map(|_| s.next_frame())
            .map(|frame| frame.header.frame_type)
            .collect();
        assert_eq!(frames[4], V3FrameType::Response);
        assert_eq!(frames[5], V3FrameType::Data);
    }
    #[test]
    fn blocked_stream_does_not_block_other_stream() {
        let mut l = limits();
        l.initial_stream_window_bytes = 8192;
        let mut s = MuxScheduler::new(&l, 100_000).unwrap();
        s.queue_body(2, DataClass::Event, Bytes::from(vec![1; 16_384]))
            .unwrap();
        assert_eq!(s.next_frame().unwrap().header.stream_id, 2);
        s.queue_body(4, DataClass::Event, Bytes::from(vec![2; 100]))
            .unwrap();
        assert_eq!(s.next_frame().unwrap().header.stream_id, 4);
        assert!(s.next_frame().is_none());
        assert_eq!(s.stalled_windows(), (false, true));
        s.window_update(2, 8192).unwrap();
        assert_eq!(s.next_frame().unwrap().header.stream_id, 2);
    }
    #[test]
    fn reassembly_reservation_and_cleanup() {
        let budget = ReassemblyBudget::new(100);
        let mut table = StreamTable::new(Initiator::Server, limits(), 100, budget.clone()).unwrap();
        let rpc = |len| V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: len,
        };
        table.open_peer(1, &rpc(80)).unwrap();
        assert_eq!(budget.used(), 80);
        assert_eq!(table.open_peer(3, &rpc(80)), Err(MuxError::Overloaded));
        let received = table.receive_data(1, &[1; 80], true).unwrap();
        assert_eq!(received.complete.unwrap().len(), 80);
        assert_eq!(budget.used(), 0);
        table.open_peer(5, &rpc(80)).unwrap();
        table.reset(5).unwrap();
        assert_eq!(budget.used(), 0);
        assert_eq!(
            table.open_peer(1, &rpc(1)),
            Err(MuxError::Connection(V3WireError::StreamId))
        );
    }
    #[test]
    fn body_larger_than_window_resumes_only_after_credit_is_written() {
        let mut limits = limits();
        limits.initial_stream_window_bytes = 8192;
        limits.initial_connection_window_bytes = 8192;
        let budget = ReassemblyBudget::new(100_000);
        let mut table =
            StreamTable::new(Initiator::Server, limits.clone(), 100_000, budget.clone()).unwrap();
        let request = V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: 20_000,
        };
        table.open_peer(1, &request).unwrap();
        let first = table.receive_data(1, &[1; 8192], false).unwrap();
        assert_eq!(first.connection_update, 8192);
        assert_eq!(first.stream_update, 8192);
        assert!(matches!(
            table.receive_data(1, &[1], false),
            Err(MuxError::Connection(_))
        ));
        table.grant_credit(0, 8192).unwrap();
        assert!(matches!(
            table.receive_data(1, &[1], false),
            Err(MuxError::FlowControl)
        ));
        table.grant_credit(1, 8192).unwrap();
        table.receive_data(1, &[2; 8192], false).unwrap();
        table.grant_credit(0, 8192).unwrap();
        table.grant_credit(1, 8192).unwrap();
        let last = table.receive_data(1, &[3; 3616], true).unwrap();
        assert_eq!(last.complete.unwrap().len(), 20_000);
        assert_eq!(last.stream_update, 0);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn parent_reset_cleans_child_reservations_without_touching_other_parent() {
        let budget = ReassemblyBudget::new(100_000);
        let mut table =
            StreamTable::new(Initiator::Server, limits(), 100_000, budget.clone()).unwrap();
        table
            .open_peer(
                1,
                &V3Open::Provider {
                    provider_id: "primary".into(),
                },
            )
            .unwrap();
        table
            .open_peer(
                3,
                &V3Open::EventSubscription {
                    subscription_id: netbaiot_protocol::SubscriptionId::generate(),
                    filter: Default::default(),
                },
            )
            .unwrap();
        let rpc = V3Open::Rpc {
            parent_stream_id: Some(1),
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: 5000,
        };
        table.open_peer(5, &rpc).unwrap();
        assert_eq!(budget.used(), 5000);
        assert_eq!(table.reset(1).unwrap(), vec![5]);
        assert_eq!(budget.used(), 0);
        assert_eq!(table.active(), 1);
        assert!(table.reset(3).is_ok());
        assert_eq!(table.active(), 0);
    }
    #[test]
    fn ids_and_response_state_reject_reuse_and_duplicate_metadata() {
        let budget = ReassemblyBudget::new(100_000);
        let mut table = StreamTable::new(Initiator::Client, limits(), 100_000, budget).unwrap();
        let request = V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: 0,
        };
        assert!(matches!(
            table.open_peer(1, &request),
            Err(MuxError::Connection(_))
        ));
        table.open_peer(2, &request).unwrap();
        assert!(matches!(
            table.open_peer(2, &request),
            Err(MuxError::Connection(_))
        ));
        table.reset(2).unwrap();
        let local = table.open_local(&request).unwrap();
        table.mark_local_end(local).unwrap();
        table.mark_response(local, 3).unwrap();
        assert_eq!(table.mark_response(local, 3), Err(MuxError::Stream));
        assert_eq!(
            table.receive_data(local, b"a", true).err(),
            Some(MuxError::Stream)
        );
        let done = table.receive_data(local, b"abc", true).unwrap();
        assert_eq!(done.complete.unwrap().as_ref(), b"abc");
        assert_eq!(
            table.receive_data(local, b"x", true).err(),
            Some(MuxError::Stream)
        );
        assert_eq!(table.active(), 0);
    }
    #[test]
    fn receive_budget_and_window_overflow_are_bounded() {
        let mut limits = limits();
        limits.initial_stream_window_bytes = 8192;
        limits.initial_connection_window_bytes = 8192;
        let budget = ReassemblyBudget::new(8192);
        let mut table = StreamTable::new(Initiator::Server, limits, 8192, budget.clone()).unwrap();
        let request = V3Open::Rpc {
            parent_stream_id: None,
            request_id: Uuid::new_v4(),
            method: "auth.sync".into(),
            deadline_ms: 1000,
            content_length: 8192,
        };
        table.open_peer(1, &request).unwrap();
        assert_eq!(
            table.grant_credit(0, 1),
            Err(MuxError::Connection(V3WireError::Length))
        );
        assert_eq!(table.grant_credit(1, 1), Err(MuxError::FlowControl));
        assert_eq!(
            table.grant_credit(0, u32::MAX),
            Err(MuxError::Connection(V3WireError::Length))
        );
        table.reset(1).unwrap();
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn data_classes_make_progress_under_mixed_pressure() {
        let mut limits = limits();
        limits.initial_stream_window_bytes = 1024 * 1024;
        let mut scheduler = MuxScheduler::new(&limits, 300_000).unwrap();
        scheduler
            .queue_body(2, DataClass::Event, Bytes::from(vec![0; 64 * 1024]))
            .unwrap();
        scheduler
            .queue_body(4, DataClass::Event, Bytes::from(vec![0; 64 * 1024]))
            .unwrap();
        for id in [1, 3, 5, 7, 9, 11, 13, 15] {
            scheduler
                .queue_body(id, DataClass::Rpc, Bytes::from(vec![0; 100]))
                .unwrap();
        }
        let ids: Vec<_> = (0..12)
            .filter_map(|_| scheduler.next_frame())
            .map(|frame| frame.header.stream_id)
            .collect();
        assert!(ids.contains(&2) && ids.contains(&4));
        assert!(ids.iter().any(|id| id & 1 == 1));
    }
    #[tokio::test]
    async fn split_and_coalesced_binary_frames() {
        use tokio::io::AsyncWriteExt;
        let (mut writer, mut reader) = tokio::io::duplex(100);
        let first =
            Frame::new(1, V3FrameType::Data, 0, Bytes::from_static(b"hello"), 8192).unwrap();
        let second = Frame::new(
            3,
            V3FrameType::Data,
            V3_END_STREAM,
            Bytes::from_static(b"world"),
            8192,
        )
        .unwrap();
        let mut raw = Vec::new();
        raw.extend_from_slice(&first.header.encode());
        raw.extend_from_slice(&first.payload);
        raw.extend_from_slice(&second.header.encode());
        raw.extend_from_slice(&second.payload);
        writer.write_all(&raw[..5]).await.unwrap();
        let task = tokio::spawn(async move {
            let a = read_frame(&mut reader, 8192, Duration::from_secs(1))
                .await
                .unwrap();
            let b = read_frame(&mut reader, 8192, Duration::from_secs(1))
                .await
                .unwrap();
            (a, b)
        });
        writer.write_all(&raw[5..]).await.unwrap();
        let (a, b) = task.await.unwrap();
        assert_eq!(
            (&a.payload[..], &b.payload[..]),
            (&b"hello"[..], &b"world"[..])
        );
        assert_eq!(b.header.flags, V3_END_STREAM);
    }
    #[tokio::test]
    async fn cancelled_state_machine_wait_does_not_discard_partial_data() {
        use tokio::io::AsyncWriteExt;
        let (mut socket, reader) = tokio::io::duplex(20_000);
        let frame = Frame::new(
            2,
            V3FrameType::Data,
            V3_END_STREAM,
            Bytes::from(vec![7; 8192]),
            8192,
        )
        .unwrap();
        let mut wire = frame.header.encode().to_vec();
        wire.extend_from_slice(&frame.payload);
        socket.write_all(&wire[..100]).await.unwrap();
        let mut frames = FrameReader::spawn(reader, 8192, Duration::from_secs(1));
        tokio::select! {
            result = frames.recv() => panic!("partial frame was exposed: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        socket.write_all(&wire[100..]).await.unwrap();
        let complete = frames.recv().await.unwrap().unwrap();
        assert_eq!(complete.payload.len(), 8192);
        assert!(complete.payload.iter().all(|byte| *byte == 7));
    }
}
