//! A bounded Tokio channel whose preallocated block contains only pointers.
//! Count and byte admission remain in the broker, before each frame is enqueued.
use super::BrokerFrame;
use tokio::sync::mpsc;

#[derive(Clone)]
pub(super) struct FrameSender(mpsc::Sender<Box<BrokerFrame>>);

/// Bounded MQTT delivery receiver. Polling and cancellation use Tokio's receiver;
/// a ready frame is unboxed synchronously without adding a task or wait queue.
pub struct FrameReceiver(mpsc::Receiver<Box<BrokerFrame>>);

pub(super) fn channel(capacity: usize) -> (FrameSender, FrameReceiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (FrameSender(sender), FrameReceiver(receiver))
}

impl FrameSender {
    pub(super) fn capacity(&self) -> usize {
        self.0.capacity()
    }
    pub(super) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
    pub(super) fn try_send(
        &self,
        frame: BrokerFrame,
    ) -> std::result::Result<(), mpsc::error::TrySendError<BrokerFrame>> {
        self.0
            .try_send(Box::new(frame))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(frame) => mpsc::error::TrySendError::Full(*frame),
                mpsc::error::TrySendError::Closed(frame) => {
                    mpsc::error::TrySendError::Closed(*frame)
                }
            })
    }
}

impl FrameReceiver {
    pub async fn recv(&mut self) -> Option<BrokerFrame> {
        self.0.recv().await.map(|frame| *frame)
    }
    pub fn try_recv(&mut self) -> std::result::Result<BrokerFrame, mpsc::error::TryRecvError> {
        self.0.try_recv().map(|frame| *frame)
    }
    pub fn close(&mut self) {
        self.0.close();
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
