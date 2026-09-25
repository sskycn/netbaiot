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
        let (lease, receiver, ()) = self.register_with(auth, transport, |_, _| Ok(()))?;
        Ok((lease, receiver))
    }

    /// Finalize transport attachment before cancelling the replaced live socket.
    /// Lock order: auth-registration gate (caller), Sessions, transport broker.
    pub fn register_with<T>(
        self: &Arc<Self>,
        auth: Arc<AuthenticatedDevice>,
        transport: Transport,
        finalize: impl FnOnce(&AuthenticatedDevice, u64) -> Result<T>,
    ) -> Result<(SessionLease, mpsc::Receiver<QueuedCommand>, T)> {
        if !matches!(transport, Transport::Mqtt | Transport::Tcp) {
            return Err(Error::Invalid);
        }
        let device = auth.device_key.clone();
        let mut state = lock(&self.state)?;
        self.prune_presence(&mut state);
        state.tenants.retain(|_, tenant| {
            tenant.connections > 0
                || tenant.bytes.upgrade().is_some()
                || tenant.command_slots.upgrade().is_some()
        });
        let replacing = state.sessions.contains_key(&device);
        if !replacing && state.sessions.len() >= self.limits.max_connections {
            return Err(Error::Overloaded);
        }
        self.ensure_presence_slot(&mut state, &device)?;
        let generation = state.generation.checked_add(1).ok_or(Error::Overloaded)?;
        if !state.tenants.contains_key(&device.tenant_id)
            && state.tenants.len() >= self.limits.max_devices
        {
            return Err(Error::Overloaded);
        }
        if !replacing
            && state
                .tenants
                .get(&device.tenant_id)
                .is_some_and(|tenant| tenant.connections >= self.limits.max_connections_per_tenant)
        {
            return Err(Error::Overloaded);
        }
        let finalized = finalize(auth.as_ref(), generation)?;
        let tenant = state.tenants.entry(device.tenant_id.clone()).or_default();
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
            finalized,
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

    /// Cancels active sessions from their bound authenticated identity. This is intentionally
    /// independent of AuthCache contents: an evicted cache entry must not make revocation miss a
    /// live socket.
    pub fn disconnect_matching(&self, invalidation: &AuthInvalidation) -> Result<usize> {
        let state = lock(&self.state)?;
        let mut disconnected = 0usize;
        for endpoint in state.sessions.values() {
            let auth = endpoint.auth.as_ref();
            let matches = match invalidation {
                AuthInvalidation::Device { device } => &auth.device_key == device,
                AuthInvalidation::Product {
                    tenant_id,
                    product_id,
                } => {
                    &auth.device_key.tenant_id == tenant_id
                        && &auth.device_key.product_id == product_id
                }
                AuthInvalidation::Tenant { tenant_id } => &auth.device_key.tenant_id == tenant_id,
                AuthInvalidation::CredentialVersion { version } => {
                    auth.credential_version == *version
                }
                AuthInvalidation::AuthGeneration { generation } => {
                    auth.auth_generation == *generation
                }
                AuthInvalidation::All => true,
            };
            if matches && !endpoint.cancel.is_cancelled() {
                endpoint.cancel.cancel();
                disconnected += 1;
            }
        }
        Ok(disconnected)
    }

    pub fn list(&self, offset: usize, limit: usize) -> Result<Vec<ConnectionSummary>> {
        self.list_filtered(offset, limit, |_| true)
    }

    /// Filters the bounded live registry before applying offset/limit.
    pub fn list_filtered(
        &self,
        offset: usize,
        limit: usize,
        allowed: impl Fn(&DeviceKey) -> bool,
    ) -> Result<Vec<ConnectionSummary>> {
        if limit == 0 || limit > 256 {
            return Err(Error::Invalid);
        }
        let state = lock(&self.state)?;
        let mut values = state
            .sessions
            .iter()
            .filter(|(device, _)| allowed(device))
            .map(|(device, endpoint)| ConnectionSummary {
                device: device.clone(),
                generation: endpoint.generation,
                transport: endpoint.transport,
            })
            .collect::<Vec<_>>();
        values.sort_by(|a, b| {
            (
                &a.device.tenant_id,
                &a.device.product_id,
                &a.device.device_id,
            )
                .cmp(&(
                    &b.device.tenant_id,
                    &b.device.product_id,
                    &b.device.device_id,
                ))
        });
        Ok(values.into_iter().skip(offset).take(limit).collect())
    }

    pub fn touch(&self, device: &DeviceKey, transport: Transport) -> Result<()> {
        let mut state = lock(&self.state)?;
        self.prune_presence(&mut state);
        self.ensure_presence_slot(&mut state, device)?;
        let now = now_ms();
        state
            .presence
            .entry(device.clone())
            .and_modify(|presence| presence.last_seen = now)
            .or_insert(Presence {
                connected: false,
                connected_at: None,
                last_seen: now,
                transport,
                session_generation: None,
            });
        Ok(())
    }

    pub fn presence(&self, device: &DeviceKey) -> Result<Option<Presence>> {
        let mut state = lock(&self.state)?;
        self.prune_presence(&mut state);
        Ok(state.presence.get(device).cloned())
    }

    pub fn connection(&self, device: &DeviceKey) -> Result<DeviceConnectionInfo> {
        let mut state = lock(&self.state)?;
        self.prune_presence(&mut state);
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
        let mut state = lock(&self.state)?;
        self.prune_presence(&mut state);
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

    fn prune_presence(&self, state: &mut SessionState) {
        let cutoff =
            now_ms().saturating_sub(i64::try_from(self.limits.presence_ttl_ms).unwrap_or(i64::MAX));
        state
            .presence
            .retain(|_, presence| presence.connected || presence.last_seen >= cutoff);
    }

    fn ensure_presence_slot(&self, state: &mut SessionState, device: &DeviceKey) -> Result<()> {
        if state.presence.contains_key(device) || state.presence.len() < self.limits.max_devices {
            return Ok(());
        }
        let oldest_offline = state
            .presence
            .iter()
            .filter(|(_, presence)| !presence.connected)
            .min_by_key(|(_, presence)| presence.last_seen)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest_offline {
            state.presence.remove(&oldest);
            Ok(())
        } else {
            Err(Error::Overloaded)
        }
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
    fn active_revocation_uses_bound_identity_not_auth_cache_membership() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let first = auth("one");
        let second = auth("two");
        let (first_lease, _) = sessions.register(first.clone(), Transport::Mqtt).unwrap();
        let (second_lease, _) = sessions.register(second.clone(), Transport::Tcp).unwrap();
        assert_eq!(
            sessions
                .disconnect_matching(&AuthInvalidation::CredentialVersion { version: 1 })
                .unwrap(),
            2
        );
        assert!(first_lease.cancel.is_cancelled());
        assert!(second_lease.cancel.is_cancelled());

        let third = auth("three");
        let (third_lease, _) = sessions.register(third.clone(), Transport::Mqtt).unwrap();
        assert_eq!(
            sessions
                .disconnect_matching(&AuthInvalidation::Device {
                    device: third.device_key.clone(),
                })
                .unwrap(),
            1
        );
        assert!(third_lease.cancel.is_cancelled());
    }

    #[test]
    fn authorized_connection_pagination_filters_before_offset() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let mut other = (*auth("other")).clone();
        other.device_key.tenant_id = TenantId::new("a-other").unwrap();
        let (other_lease, _) = sessions.register(Arc::new(other), Transport::Mqtt).unwrap();
        let (one, _) = sessions.register(auth("one"), Transport::Mqtt).unwrap();
        let (two, _) = sessions.register(auth("two"), Transport::Tcp).unwrap();
        let first = sessions
            .list_filtered(0, 1, |device| device.tenant_id.as_str() == "t")
            .unwrap();
        let second = sessions
            .list_filtered(1, 1, |device| device.tenant_id.as_str() == "t")
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].device.device_id.as_str(), "one");
        assert_eq!(second[0].device.device_id.as_str(), "two");
        assert!(
            sessions
                .list_filtered(2, 1, |device| device.tenant_id.as_str() == "t")
                .unwrap()
                .is_empty()
        );
        drop((other_lease, one, two));
    }

    #[test]
    fn offline_presence_is_lru_bounded_and_active_presence_is_never_evicted() {
        let sessions = Sessions::new(Arc::new(Limits {
            max_devices: 2,
            max_devices_per_tenant: 2,
            max_connections: 2,
            max_connections_per_tenant: 2,
            ..Limits::default()
        }));
        for index in 0..100 {
            let (lease, _) = sessions
                .register(auth(&format!("offline-{index}")), Transport::Mqtt)
                .unwrap();
            drop(lease);
            assert!(sessions.registry_counts().unwrap().2 <= 2);
        }

        let (one, _) = sessions
            .register(auth("active-one"), Transport::Mqtt)
            .unwrap();
        let (two, _) = sessions
            .register(auth("active-two"), Transport::Tcp)
            .unwrap();
        assert!(
            sessions
                .register(auth("cannot-evict-active"), Transport::Mqtt)
                .is_err()
        );
        assert!(sessions.presence(&one.device).unwrap().unwrap().connected);
        assert!(sessions.presence(&two.device).unwrap().unwrap().connected);
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
