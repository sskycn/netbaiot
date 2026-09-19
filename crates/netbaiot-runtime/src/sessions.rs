use crate::*;
use netbaiot_core::*;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub struct QueuedCommand {
    pub command_id: CommandId,
    pub expires_at: Timestamp,
    pub attempt: u32,
    pub lease_expires_at: Timestamp,
    queued: Arc<AtomicUsize>,
    pub bytes: Vec<u8>,
    _bytes: Vec<BytesPermit>,
    _slot: OwnedSemaphorePermit,
}
impl Drop for QueuedCommand {
    fn drop(&mut self) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
    }
}
#[derive(Clone)]
pub struct SessionEndpoint {
    pub generation: u64,
    pub transport: Transport,
    pub cancel: CancellationToken,
    sender: mpsc::Sender<QueuedCommand>,
    slots: Arc<Semaphore>,
    connection_bytes: ByteBudget,
    tenant_bytes: ByteBudget,
    global_bytes: ByteBudget,
    queued: Arc<AtomicUsize>,
}
impl SessionEndpoint {
    pub fn enqueue(&self, record: CommandRecord, bytes: Vec<u8>) -> Result<()> {
        let lease_expires_at = record.lease_expires_at.ok_or(Error::Invalid)?;
        if record.attempts == 0 {
            return Err(Error::Invalid);
        }
        if self.cancel.is_cancelled() {
            return Err(Error::Unavailable);
        }
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let permits = vec![
            self.connection_bytes.reserve(bytes.len())?,
            self.tenant_bytes.reserve(bytes.len())?,
            self.global_bytes.reserve(bytes.len())?,
        ];
        self.queued.fetch_add(1, Ordering::Relaxed);
        self.sender
            .try_send(QueuedCommand {
                command_id: record.command.command_id,
                expires_at: record.command.expires_at,
                attempt: record.attempts,
                lease_expires_at,
                queued: self.queued.clone(),
                bytes,
                _bytes: permits,
                _slot: slot,
            })
            .map_err(|_| Error::Overloaded)
    }
}
struct SessionState {
    generation: u64,
    sessions: HashMap<DeviceKey, SessionEndpoint>,
    // Weak identity survives while any endpoint OR owned byte permit is alive.
    tenants: HashMap<TenantId, (usize, WeakByteBudget)>,
    presence: HashMap<DeviceKey, Presence>,
}
pub struct Sessions {
    state: Mutex<SessionState>,
    limits: Arc<Limits>,
    global_bytes: ByteBudget,
    queued: Arc<AtomicUsize>,
}
pub struct SessionLease {
    owner: Arc<Sessions>,
    pub device: DeviceKey,
    pub generation: u64,
    pub cancel: CancellationToken,
}
impl Sessions {
    /// Current registries, without cloning device identities for metric sampling.
    pub fn registry_counts(&self) -> Result<(usize, usize, usize)> {
        let state = lock(&self.state)?;
        Ok((
            state.sessions.len(),
            state.tenants.len(),
            state.presence.len(),
        ))
    }

    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SessionState {
                generation: 0,
                sessions: HashMap::new(),
                tenants: HashMap::new(),
                presence: HashMap::new(),
            }),
            global_bytes: ByteBudget::new(limits.max_outbound_bytes),
            queued: Arc::new(AtomicUsize::new(0)),
            limits,
        })
    }
    pub fn register(
        self: &Arc<Self>,
        device: &DeviceKey,
        transport: Transport,
    ) -> Result<(SessionLease, mpsc::Receiver<QueuedCommand>)> {
        if !matches!(transport, Transport::Mqtt | Transport::Tcp) {
            return Err(Error::Invalid);
        }
        let mut s = lock(&self.state)?;
        s.tenants
            .retain(|_, (n, budget)| *n > 0 || budget.upgrade().is_some());
        let replacing = s.sessions.contains_key(device);
        if !replacing && s.sessions.len() >= self.limits.max_connections {
            return Err(Error::Overloaded);
        }
        if !s.presence.contains_key(device) && s.presence.len() >= self.limits.max_devices {
            return Err(Error::Overloaded);
        }
        let generation = s.generation.checked_add(1).ok_or(Error::Overloaded)?;
        if !s.tenants.contains_key(&device.tenant_id) && s.tenants.len() >= self.limits.max_devices
        {
            return Err(Error::Overloaded);
        }
        let tenant = s.tenants.entry(device.tenant_id.clone()).or_default();
        if !replacing && tenant.0 >= self.limits.max_connections_per_tenant {
            return Err(Error::Overloaded);
        }
        if !replacing {
            tenant.0 += 1;
        }
        let tenant_bytes = tenant
            .1
            .get_or_create(self.limits.max_outbound_bytes_per_tenant);
        let (sender, receiver) = mpsc::channel(self.limits.max_outbound_messages_per_connection);
        let cancel = CancellationToken::new();
        let endpoint = SessionEndpoint {
            generation,
            transport,
            cancel: cancel.clone(),
            sender,
            slots: Arc::new(Semaphore::new(
                self.limits.max_outbound_messages_per_connection,
            )),
            connection_bytes: ByteBudget::new(self.limits.max_outbound_bytes_per_connection),
            tenant_bytes,
            global_bytes: self.global_bytes.clone(),
            queued: self.queued.clone(),
        };
        if let Some(old) = s.sessions.insert(device.clone(), endpoint) {
            old.cancel.cancel();
        }
        s.generation = generation;
        s.presence.insert(
            device.clone(),
            Presence {
                connected: true,
                last_seen: now_ms(),
                transport,
                session_generation: Some(generation),
            },
        );
        Ok((
            SessionLease {
                owner: self.clone(),
                device: device.clone(),
                generation,
                cancel,
            },
            receiver,
        ))
    }
    /// Snapshot is bounded by max_connections, includes only node-local streams.
    pub fn active_devices(&self) -> Result<Vec<DeviceKey>> {
        Ok(lock(&self.state)?.sessions.keys().cloned().collect())
    }
    pub fn lookup(&self, device: &DeviceKey) -> Result<Option<SessionEndpoint>> {
        Ok(lock(&self.state)?.sessions.get(device).cloned())
    }
    pub fn touch(&self, device: &DeviceKey, transport: Transport) -> Result<()> {
        let mut s = lock(&self.state)?;
        if !s.presence.contains_key(device) && s.presence.len() >= self.limits.max_devices {
            return Err(Error::Overloaded);
        }
        s.presence
            .entry(device.clone())
            .and_modify(|p| {
                p.last_seen = now_ms();
                if !p.connected {
                    p.transport = transport;
                }
            })
            .or_insert(Presence {
                connected: false,
                last_seen: now_ms(),
                transport,
                session_generation: None,
            });
        Ok(())
    }
    pub fn presence(&self, device: &DeviceKey) -> Result<Option<Presence>> {
        Ok(lock(&self.state)?.presence.get(device).cloned())
    }
    pub fn expire_presence(&self, now: i64) -> Result<()> {
        let mut s = lock(&self.state)?;
        s.presence.retain(|_, p| {
            p.connected || now.saturating_sub(p.last_seen) < self.limits.dedup_ttl_ms as i64
        });
        Ok(())
    }
    pub fn queued_messages(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }
    pub fn queued_bytes(&self) -> usize {
        self.limits.max_outbound_bytes - self.global_bytes.available()
    }
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        if let Ok(mut s) = self.owner.state.lock()
            && s.sessions
                .get(&self.device)
                .is_some_and(|e| e.generation == self.generation)
        {
            s.sessions.remove(&self.device);
            self.cancel.cancel();
            if let Some(t) = s.tenants.get_mut(&self.device.tenant_id) {
                t.0 = t.0.saturating_sub(1);
                if t.0 == 0 && t.1.upgrade().is_none() {
                    s.tenants.remove(&self.device.tenant_id);
                }
            }
            if let Some(p) = s.presence.get_mut(&self.device) {
                p.connected = false;
                p.last_seen = now_ms();
                p.session_generation = None;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn key() -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        }
    }
    #[test]
    fn stale_disconnect_cannot_remove_new_generation() {
        let s = Sessions::new(Arc::new(Limits::default()));
        let d = key();
        let (old, _) = s.register(&d, Transport::Mqtt).unwrap();
        let (new, _) = s.register(&d, Transport::Mqtt).unwrap();
        assert!(old.cancel.is_cancelled());
        assert!(new.generation > old.generation);
        drop(old);
        assert_eq!(s.lookup(&d).unwrap().unwrap().generation, new.generation);
        drop(new);
        assert!(s.lookup(&d).unwrap().is_none());
    }
    #[test]
    fn queue_count_and_bytes_are_bounded() {
        let l = Limits {
            max_outbound_messages_per_connection: 1,
            max_outbound_bytes_per_connection: 16,
            ..Limits::default()
        };
        let s = Sessions::new(Arc::new(l));
        let d = key();
        let (_lease, mut rx) = s.register(&d, Transport::Tcp).unwrap();
        let ep = s.lookup(&d).unwrap().unwrap();
        let c = DeviceCommand {
            command_id: CommandId::generate(),
            device: d,
            expires_at: 100,
            payload: DeviceCommandPayload {
                name: "x".into(),
                arguments: Default::default(),
            },
        };
        let c = CommandRecord {
            command: c,
            delivery: DeliveryState::Dispatching,
            execution: ExecutionState::Unknown,
            attempts: 1,
            lease_expires_at: Some(100),
        };
        assert!(ep.enqueue(c.clone(), vec![0; 17]).is_err());
        ep.enqueue(c.clone(), vec![0; 16]).unwrap();
        assert!(ep.enqueue(c.clone(), vec![0]).is_err());
        let queued = rx.try_recv().unwrap();
        assert!(ep.enqueue(c.clone(), vec![0]).is_err());
        drop(queued);
        ep.enqueue(c, vec![0]).unwrap();
    }

    #[test]
    fn audit_tenant_budget_survives_superseded_queue() {
        let s = Sessions::new(Arc::new(Limits {
            max_outbound_bytes_per_connection: 16,
            max_outbound_bytes_per_tenant: 16,
            max_outbound_bytes: 64,
            ..Limits::default()
        }));
        let d = key();
        let (old, mut rx) = s.register(&d, Transport::Mqtt).unwrap();
        let c = DeviceCommand {
            command_id: CommandId::generate(),
            device: d.clone(),
            expires_at: 100,
            payload: DeviceCommandPayload {
                name: "x".into(),
                arguments: Default::default(),
            },
        };
        let c = CommandRecord {
            command: c,
            delivery: DeliveryState::Dispatching,
            execution: ExecutionState::Unknown,
            attempts: 1,
            lease_expires_at: Some(100),
        };
        s.lookup(&d)
            .unwrap()
            .unwrap()
            .enqueue(c.clone(), vec![0; 16])
            .unwrap();
        let in_flight = rx.try_recv().unwrap();
        let (replacement, replacement_rx) = s.register(&d, Transport::Mqtt).unwrap();
        drop((replacement, replacement_rx));
        let (_new, _new_rx) = s.register(&d, Transport::Mqtt).unwrap();
        let ep = s.lookup(&d).unwrap().unwrap();
        assert!(matches!(
            ep.enqueue(c.clone(), vec![0]),
            Err(Error::Overloaded)
        ));
        drop((in_flight, old, rx));
        ep.enqueue(c, vec![0; 16]).unwrap();
    }
}

#[cfg(test)]
mod audit_cleanup {
    use super::*;
    #[tokio::test]
    async fn cancelled_receiver_and_failed_enqueue_release_all_budgets() {
        let s = Sessions::new(Arc::new(Limits::default()));
        let device = DeviceKey {
            tenant_id: TenantId::new("t").unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new("d").unwrap(),
        };
        let (session, mut rx) = s.register(&device, Transport::Mqtt).unwrap();
        let ep = s.lookup(&device).unwrap().unwrap();
        let record = CommandRecord {
            command: DeviceCommand {
                command_id: CommandId::generate(),
                device,
                expires_at: now_ms() + 60000,
                payload: DeviceCommandPayload {
                    name: "x".into(),
                    arguments: Default::default(),
                },
            },
            delivery: DeliveryState::Dispatching,
            execution: ExecutionState::Unknown,
            attempts: 1,
            lease_expires_at: Some(now_ms() + 30000),
        };
        ep.enqueue(record.clone(), vec![0; 1024]).unwrap();
        let in_flight = rx.recv().await.unwrap();
        ep.enqueue(record.clone(), vec![0; 2048]).unwrap();
        assert_eq!(s.queued_messages(), 2);
        assert_eq!(s.queued_bytes(), 3072);
        drop(rx);
        assert_eq!(s.queued_bytes(), 1024);
        assert!(ep.enqueue(record.clone(), vec![0; 3000]).is_err());
        assert_eq!(s.queued_bytes(), 1024);
        assert_eq!(s.queued_messages(), 1);
        drop(in_flight);
        drop(session);
        assert!(ep.enqueue(record, vec![0; 3000]).is_err());
        assert_eq!(s.queued_messages(), 0);
        assert_eq!(s.queued_bytes(), 0);
    }
}
