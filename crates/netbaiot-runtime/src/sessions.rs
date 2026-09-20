use crate::*;
use netbaiot_core::*;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub struct QueuedCommand {
    pub command_id: CommandId,
    pub expires_at: Timestamp,
    queued: Arc<AtomicUsize>,
    pub bytes: Vec<u8>,
    _bytes: Vec<BytesPermit>,
    _slots: Vec<OwnedSemaphorePermit>,
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
    pub auth: Arc<AuthenticatedDevice>,
    pub cancel: CancellationToken,
    sender: mpsc::Sender<QueuedCommand>,
    connection_slots: Arc<Semaphore>,
    tenant_slots: Arc<Semaphore>,
    global_slots: Arc<Semaphore>,
    connection_bytes: ByteBudget,
    tenant_bytes: ByteBudget,
    global_bytes: ByteBudget,
    queued: Arc<AtomicUsize>,
}

impl SessionEndpoint {
    pub fn enqueue(&self, command: &DeviceCommand, bytes: Vec<u8>) -> Result<()> {
        if self.cancel.is_cancelled() || command.device != self.auth.device_key {
            return Err(Error::Unavailable);
        }
        let slots = vec![
            self.connection_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Overloaded)?,
            self.tenant_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Overloaded)?,
            self.global_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Error::Overloaded)?,
        ];
        let permits = vec![
            self.connection_bytes.reserve(bytes.len())?,
            self.tenant_bytes.reserve(bytes.len())?,
            self.global_bytes.reserve(bytes.len())?,
        ];
        self.queued.fetch_add(1, Ordering::Relaxed);
        self.sender
            .try_send(QueuedCommand {
                command_id: command.command_id,
                expires_at: command.expires_at.ok_or(Error::Invalid)?,
                queued: self.queued.clone(),
                bytes,
                _bytes: permits,
                _slots: slots,
            })
            .map_err(|_| Error::Overloaded)
    }
}

struct TenantState {
    connections: usize,
    bytes: WeakByteBudget,
    command_slots: Weak<Semaphore>,
}

impl Default for TenantState {
    fn default() -> Self {
        Self {
            connections: 0,
            bytes: WeakByteBudget::default(),
            command_slots: Weak::new(),
        }
    }
}

struct SessionState {
    generation: u64,
    sessions: HashMap<DeviceKey, SessionEndpoint>,
    tenants: HashMap<TenantId, TenantState>,
    presence: HashMap<DeviceKey, Presence>,
}

pub struct Sessions {
    state: Mutex<SessionState>,
    limits: Arc<Limits>,
    global_bytes: ByteBudget,
    global_slots: Arc<Semaphore>,
    queued: Arc<AtomicUsize>,
}

pub struct SessionLease {
    owner: Arc<Sessions>,
    pub device: DeviceKey,
    pub generation: u64,
    pub cancel: CancellationToken,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionSummary {
    pub device: DeviceKey,
    pub generation: u64,
    pub transport: Transport,
}

impl Sessions {
    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SessionState {
                generation: 0,
                sessions: HashMap::new(),
                tenants: HashMap::new(),
                presence: HashMap::new(),
            }),
            global_bytes: ByteBudget::new(limits.max_outbound_bytes),
            global_slots: Arc::new(Semaphore::new(limits.max_pending_commands)),
            queued: Arc::new(AtomicUsize::new(0)),
            limits,
        })
    }

    pub fn register(
        self: &Arc<Self>,
        auth: Arc<AuthenticatedDevice>,
        transport: Transport,
    ) -> Result<(SessionLease, mpsc::Receiver<QueuedCommand>)> {
        if !matches!(transport, Transport::Mqtt | Transport::Tcp) {
            return Err(Error::Invalid);
        }
        let device = auth.device_key.clone();
        let mut state = lock(&self.state)?;
        state.tenants.retain(|_, tenant| {
            tenant.connections > 0
                || tenant.bytes.upgrade().is_some()
                || tenant.command_slots.upgrade().is_some()
        });
        let replacing = state.sessions.contains_key(&device);
        if !replacing && state.sessions.len() >= self.limits.max_connections {
            return Err(Error::Overloaded);
        }
        if !state.presence.contains_key(&device) && state.presence.len() >= self.limits.max_devices
        {
            return Err(Error::Overloaded);
        }
        let generation = state.generation.checked_add(1).ok_or(Error::Overloaded)?;
        if !state.tenants.contains_key(&device.tenant_id)
            && state.tenants.len() >= self.limits.max_devices
        {
            return Err(Error::Overloaded);
        }
        let tenant = state.tenants.entry(device.tenant_id.clone()).or_default();
        if !replacing && tenant.connections >= self.limits.max_connections_per_tenant {
            return Err(Error::Overloaded);
        }
        if !replacing {
            tenant.connections += 1;
        }
        let tenant_bytes = tenant
            .bytes
            .get_or_create(self.limits.max_outbound_bytes_per_tenant);
        let tenant_slots = tenant.command_slots.upgrade().unwrap_or_else(|| {
            let slots = Arc::new(Semaphore::new(self.limits.max_pending_commands_per_tenant));
            tenant.command_slots = Arc::downgrade(&slots);
            slots
        });
        let (sender, receiver) = mpsc::channel(self.limits.max_outbound_messages_per_connection);
        let cancel = CancellationToken::new();
        let endpoint = SessionEndpoint {
            generation,
            transport,
            auth,
            cancel: cancel.clone(),
            sender,
            connection_slots: Arc::new(Semaphore::new(self.limits.max_pending_commands_per_device)),
            tenant_slots,
            global_slots: self.global_slots.clone(),
            connection_bytes: ByteBudget::new(self.limits.max_outbound_bytes_per_connection),
            tenant_bytes,
            global_bytes: self.global_bytes.clone(),
            queued: self.queued.clone(),
        };
        if let Some(old) = state.sessions.insert(device.clone(), endpoint) {
            old.cancel.cancel();
        }
        state.generation = generation;
        let connected_at = now_ms();
        state.presence.insert(
            device.clone(),
            Presence {
                connected: true,
                connected_at: Some(connected_at),
                last_seen: connected_at,
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

    pub fn lookup(&self, device: &DeviceKey) -> Result<Option<SessionEndpoint>> {
        Ok(lock(&self.state)?.sessions.get(device).cloned())
    }

    pub fn disconnect(&self, device: &DeviceKey) -> Result<bool> {
        let state = lock(&self.state)?;
        if let Some(endpoint) = state.sessions.get(device) {
            endpoint.cancel.cancel();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn list(&self, offset: usize, limit: usize) -> Result<Vec<ConnectionSummary>> {
        if limit == 0 || limit > 256 {
            return Err(Error::Invalid);
        }
        let state = lock(&self.state)?;
        let mut values = state
            .sessions
            .iter()
            .map(|(device, endpoint)| ConnectionSummary {
                device: device.clone(),
                generation: endpoint.generation,
                transport: endpoint.transport,
            })
            .collect::<Vec<_>>();
        values.sort_by(|a, b| a.device.device_id.cmp(&b.device.device_id));
        Ok(values.into_iter().skip(offset).take(limit).collect())
    }

    pub fn touch(&self, device: &DeviceKey, transport: Transport) -> Result<()> {
        let mut state = lock(&self.state)?;
        if !state.presence.contains_key(device) && state.presence.len() >= self.limits.max_devices {
            return Err(Error::Overloaded);
        }
        state
            .presence
            .entry(device.clone())
            .and_modify(|presence| presence.last_seen = now_ms())
            .or_insert(Presence {
                connected: false,
                connected_at: None,
                last_seen: now_ms(),
                transport,
                session_generation: None,
            });
        Ok(())
    }

    pub fn presence(&self, device: &DeviceKey) -> Result<Option<Presence>> {
        Ok(lock(&self.state)?.presence.get(device).cloned())
    }

    pub fn connection(&self, device: &DeviceKey) -> Result<DeviceConnectionInfo> {
        let state = lock(&self.state)?;
        let presence = state.presence.get(device);
        Ok(DeviceConnectionInfo {
            device: device.clone(),
            connected: presence.is_some_and(|value| value.connected),
            transport: presence.map(|value| value.transport),
            connected_at: presence.and_then(|value| value.connected_at),
            last_seen: presence.map(|value| value.last_seen),
            session_generation: presence.and_then(|value| value.session_generation),
        })
    }

    pub fn registry_counts(&self) -> Result<(usize, usize, usize)> {
        let state = lock(&self.state)?;
        Ok((
            state.sessions.len(),
            state.tenants.len(),
            state.presence.len(),
        ))
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
        if let Ok(mut state) = self.owner.state.lock()
            && state
                .sessions
                .get(&self.device)
                .is_some_and(|endpoint| endpoint.generation == self.generation)
        {
            state.sessions.remove(&self.device);
            self.cancel.cancel();
            if let Some(tenant) = state.tenants.get_mut(&self.device.tenant_id) {
                tenant.connections = tenant.connections.saturating_sub(1);
            }
            if let Some(presence) = state.presence.get_mut(&self.device) {
                presence.connected = false;
                presence.connected_at = None;
                presence.last_seen = now_ms();
                presence.session_generation = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(device: &str) -> Arc<AuthenticatedDevice> {
        Arc::new(AuthenticatedDevice {
            device_key: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new(device).unwrap(),
            },
            credential_version: 1,
            auth_generation: 1,
            codec_id: CodecId::new("json").unwrap(),
            codec_version: 1,
            permissions: Permissions {
                publish: true,
                commands: true,
            },
        })
    }

    #[test]
    fn stale_disconnect_cannot_remove_new_generation() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let auth = auth("d");
        let (old, _) = sessions.register(auth.clone(), Transport::Mqtt).unwrap();
        let (new, _) = sessions.register(auth, Transport::Mqtt).unwrap();
        assert!(old.cancel.is_cancelled());
        drop(old);
        assert_eq!(
            sessions.lookup(&new.device).unwrap().unwrap().generation,
            new.generation
        );
    }

    #[test]
    fn command_count_and_bytes_release_on_drop() {
        let sessions = Sessions::new(Arc::new(Limits {
            max_pending_commands_per_device: 1,
            max_outbound_messages_per_connection: 1,
            max_outbound_bytes_per_connection: 8,
            ..Limits::default()
        }));
        let auth = auth("d");
        let (lease, mut receiver) = sessions.register(auth.clone(), Transport::Tcp).unwrap();
        let endpoint = sessions.lookup(&auth.device_key).unwrap().unwrap();
        let command = DeviceCommand {
            command_id: CommandId::generate(),
            device: auth.device_key.clone(),
            expires_at: Some(now_ms() + 1_000),
            payload: DeviceCommandPayload {
                name: "x".into(),
                arguments: Default::default(),
            },
        };
        assert!(endpoint.enqueue(&command, vec![0; 9]).is_err());
        endpoint.enqueue(&command, vec![0; 8]).unwrap();
        assert!(endpoint.enqueue(&command, vec![0]).is_err());
        drop(receiver.try_recv().unwrap());
        endpoint.enqueue(&command, vec![0]).unwrap();
        drop(lease);
    }
}
