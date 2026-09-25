use crate::{DeliveryEnvelope, Error, EventSink, Result, SinkAck, SinkError};
use async_trait::async_trait;
use netbaiot_core::EventFilter;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Notify, mpsc, oneshot};

pub struct BusinessEventRequest {
    pub delivery: DeliveryEnvelope,
    pub result: oneshot::Sender<std::result::Result<SinkAck, SinkError>>,
}

/// Shared owner of the stable `tcp-rpc` sink across V1 and V2 subscribers.
pub struct BusinessRpcEventSink {
    active: Mutex<Option<ActiveStream>>,
    availability: Notify,
    generation: AtomicU64,
}
#[derive(Clone)]
struct ActiveStream {
    generation: u64,
    sender: mpsc::Sender<BusinessEventRequest>,
    filter: EventFilter,
}
impl BusinessRpcEventSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(None),
            availability: Notify::new(),
            generation: AtomicU64::new(0),
        })
    }
    pub fn claim(
        &self,
        sender: mpsc::Sender<BusinessEventRequest>,
        filter: EventFilter,
    ) -> Result<u64> {
        let mut active = self.active.lock().map_err(|_| Error::Internal)?;
        if active.is_some() {
            return Err(Error::Conflict);
        }
        let generation = self
            .generation
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1)
            .max(1);
        *active = Some(ActiveStream {
            generation,
            sender,
            filter,
        });
        drop(active);
        self.availability.notify_waiters();
        Ok(generation)
    }
    pub fn has_owner(&self) -> Result<bool> {
        Ok(self.active.lock().map_err(|_| Error::Internal)?.is_some())
    }
    pub fn release(&self, generation: u64) -> Result<()> {
        let mut active = self.active.lock().map_err(|_| Error::Internal)?;
        if active
            .as_ref()
            .is_some_and(|owner| owner.generation == generation)
        {
            *active = None;
            drop(active);
            self.availability.notify_waiters();
        }
        Ok(())
    }
}
#[async_trait]
impl EventSink for BusinessRpcEventSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        let active = loop {
            let available = self.availability.notified();
            let active = self
                .active
                .lock()
                .map_err(|_| SinkError::Permanent)?
                .clone();
            if let Some(active) = active
                && active.filter.matches(&delivery.event)
            {
                break active;
            }
            available.await;
        };
        let (result, receive) = oneshot::channel();
        active
            .sender
            .try_send(BusinessEventRequest { delivery, result })
            .map_err(|_| SinkError::Retryable)?;
        receive.await.map_err(|_| SinkError::Retryable)?
    }
}
