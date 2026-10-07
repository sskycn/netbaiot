use crate::*;
use netbaiot_core::*;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub struct QueuedCommand {
    pub command_id: CommandId,
    pub expires_at: Timestamp,
    pub progress: Option<Arc<CommandProgress>>,
    queued: Arc<AtomicUsize>,
    pub bytes: Vec<u8>,
    _bytes: Vec<BytesPermit>,
    _slots: Vec<OwnedSemaphorePermit>,
}

impl Drop for QueuedCommand {
    fn drop(&mut self) {
        self.queued.fetch_sub(1, Ordering::Relaxed);
        if let Some(progress) = &self.progress {
            progress.abandon_unsent();
        }
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
    command_ready: Arc<AtomicBool>,
}

impl SessionEndpoint {
    pub fn command_ready(&self) -> bool {
        self.command_ready.load(Ordering::Acquire)
    }

    pub fn enqueue(&self, command: &DeviceCommand, bytes: Vec<u8>) -> Result<()> {
        self.enqueue_tracked(command, bytes, None)
    }

    pub fn enqueue_tracked(
        &self,
        command: &DeviceCommand,
        bytes: Vec<u8>,
        progress: Option<Arc<CommandProgress>>,
    ) -> Result<()> {
        let expires_at = command.expires_at.ok_or(Error::Invalid)?;
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
                expires_at,
                progress,
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
            command_ready: Arc::new(AtomicBool::new(transport == Transport::Tcp)),
        };
        if let Some(old) = state.sessions.insert(device.clone(), endpoint) {
            tracing::debug!(
                ?device,
                generation,
                previous_generation = old.generation,
                "session takeover diagnostic"
            );
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
                tracing::debug!(device=?auth.device_key, generation=endpoint.generation,
                    auth_generation=auth.auth_generation, "session invalidation diagnostic");
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
        #[cfg(test)]
        let mut timing = crate::hotspot_bench::LockClock::start();
        let mut state = lock(&self.state)?;
        #[cfg(test)]
        timing.acquired();
        let now = now_ms();
        if let Some(presence) = state.presence.get_mut(device) {
            if presence.connected || presence.last_seen >= self.presence_cutoff(now) {
                presence.last_seen = now;
                return Ok(());
            }
            // Expired offline observations are recreated, including transport,
            // just as when the former full-table prune removed this entry.
            state.presence.remove(device);
        }
        self.ensure_presence_slot(&mut state, device)?;
        state.presence.insert(
            device.clone(),
            Presence {
                connected: false,
                connected_at: None,
                last_seen: now,
                transport,
                session_generation: None,
            },
        );
        Ok(())
    }

    pub fn presence(&self, device: &DeviceKey) -> Result<Option<Presence>> {
        #[cfg(test)]
        let mut timing = crate::hotspot_bench::LockClock::start();
        let mut state = lock(&self.state)?;
        #[cfg(test)]
        timing.acquired();
        self.prune_presence_entry(&mut state, device);
        Ok(state.presence.get(device).cloned())
    }

    pub fn connection(&self, device: &DeviceKey) -> Result<DeviceConnectionInfo> {
        #[cfg(test)]
        let mut timing = crate::hotspot_bench::LockClock::start();
        let mut state = lock(&self.state)?;
        #[cfg(test)]
        timing.acquired();
        self.prune_presence_entry(&mut state, device);
        let presence = state.presence.get(device);
        let live = state.sessions.get(device);
        tracing::debug!(
            ?device,
            live_exists = live.is_some(),
            live_generation = ?live.map(|session| session.generation),
            cancelled = ?live.map(|session| session.cancel.is_cancelled()),
            auth_generation = ?live.map(|session| session.auth.auth_generation),
            credential_version = ?live.map(|session| session.auth.credential_version),
            presence_exists = presence.is_some(),
            presence_connected = ?presence.map(|value| value.connected),
            presence_last_seen = ?presence.map(|value| value.last_seen),
            presence_generation = ?presence.and_then(|value| value.session_generation),
            "connection registry diagnostic"
        );
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
        let cutoff = self.presence_cutoff(now_ms());
        state
            .presence
            .retain(|_, presence| presence.connected || presence.last_seen >= cutoff);
    }

    fn presence_cutoff(&self, now: i64) -> i64 {
        now.saturating_sub(i64::try_from(self.limits.presence_ttl_ms).unwrap_or(i64::MAX))
    }

    fn prune_presence_entry(&self, state: &mut SessionState, device: &DeviceKey) {
        let cutoff = self.presence_cutoff(now_ms());
        if state
            .presence
            .get(device)
            .is_some_and(|presence| !presence.connected && presence.last_seen < cutoff)
        {
            state.presence.remove(device);
        }
    }

    fn ensure_presence_slot(&self, state: &mut SessionState, device: &DeviceKey) -> Result<()> {
        if state.presence.contains_key(device) || state.presence.len() < self.limits.max_devices {
            return Ok(());
        }
        self.prune_presence(state);
        if state.presence.len() < self.limits.max_devices {
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

impl SessionLease {
    /// Update only the current transport generation. A replaced connection cannot
    /// mark the replacement's command channel ready.
    pub fn set_command_ready(&self, ready: bool) -> Result<()> {
        let state = lock(&self.owner.state)?;
        let endpoint = state.sessions.get(&self.device).ok_or(Error::Unavailable)?;
        if endpoint.generation != self.generation {
            return Err(Error::Unavailable);
        }
        endpoint.command_ready.store(ready, Ordering::Release);
        Ok(())
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
            tracing::debug!(device=?self.device, generation=self.generation,
                cancelled=self.cancel.is_cancelled(), "current session lease dropped");
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

    #[test]
    fn invalidated_old_generation_cannot_change_replacement_presence() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let identity = auth("one");
        let (old, _) = sessions
            .register(identity.clone(), Transport::Mqtt)
            .unwrap();
        sessions
            .disconnect_matching(&AuthInvalidation::All)
            .unwrap();
        assert!(old.cancel.is_cancelled());
        let (current, _) = sessions.register(identity, Transport::Mqtt).unwrap();
        drop(old);
        sessions
            .state
            .lock()
            .unwrap()
            .presence
            .get_mut(&current.device)
            .unwrap()
            .last_seen = 0;
        sessions.touch(&current.device, Transport::Udp).unwrap();
        let live = sessions.lookup(&current.device).unwrap().unwrap();
        let presence = sessions.presence(&current.device).unwrap().unwrap();
        let connection = sessions.connection(&current.device).unwrap();
        assert_eq!(live.generation, current.generation);
        assert!(!live.cancel.is_cancelled());
        assert!(presence.connected && connection.connected);
        assert_eq!(presence.session_generation, Some(current.generation));
        assert_eq!(connection.session_generation, Some(current.generation));
        assert_eq!(presence.transport, Transport::Mqtt);
        let device = current.device.clone();
        drop(current);
        assert!(sessions.lookup(&device).unwrap().is_none());
        assert!(!sessions.connection(&device).unwrap().connected);
    }

    #[test]
    fn presence_queries_expire_only_the_target_and_keep_active_sessions() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let one = auth("one").device_key.clone();
        let two = auth("two").device_key.clone();
        sessions.touch(&one, Transport::Udp).unwrap();
        sessions.touch(&two, Transport::Udp).unwrap();
        let (lease, _) = sessions.register(auth("active"), Transport::Mqtt).unwrap();
        for presence in sessions.state.lock().unwrap().presence.values_mut() {
            presence.last_seen = 0;
        }
        assert!(sessions.presence(&one).unwrap().is_none());
        assert!(sessions.state.lock().unwrap().presence.contains_key(&two));
        assert!(sessions.connection(&lease.device).unwrap().connected);
        assert!(sessions.presence(&lease.device).unwrap().unwrap().connected);
        assert!(sessions.connection(&two).unwrap().last_seen.is_none());
        assert_eq!(sessions.registry_counts().unwrap().2, 1);
        drop(lease);
        assert!(
            !sessions
                .presence(&auth("active").device_key)
                .unwrap()
                .unwrap()
                .connected
        );
    }

    #[test]
    fn existing_touch_is_local_and_preserves_expired_transport_reset() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let one = auth("one").device_key.clone();
        let two = auth("two").device_key.clone();
        sessions.touch(&one, Transport::Tcp).unwrap();
        sessions.touch(&two, Transport::Tcp).unwrap();
        sessions
            .state
            .lock()
            .unwrap()
            .presence
            .get_mut(&two)
            .unwrap()
            .last_seen = 0;
        sessions.touch(&one, Transport::Udp).unwrap();
        assert_eq!(
            sessions.presence(&one).unwrap().unwrap().transport,
            Transport::Tcp
        );
        assert!(sessions.state.lock().unwrap().presence.contains_key(&two));
        sessions.touch(&two, Transport::Udp).unwrap();
        let presence = sessions.presence(&two).unwrap().unwrap();
        assert_eq!(presence.transport, Transport::Udp);
        assert!(!presence.connected);
        assert_eq!(presence.session_generation, None);
    }

    #[test]
    fn capacity_pressure_prunes_expired_then_evicts_oldest_offline() {
        let sessions = Sessions::new(Arc::new(Limits {
            max_devices: 3,
            ..Limits::default()
        }));
        let keys =
            ["expired", "old", "recent", "new", "newer"].map(|name| auth(name).device_key.clone());
        for key in &keys[..3] {
            sessions.touch(key, Transport::Udp).unwrap();
        }
        {
            let mut state = sessions.state.lock().unwrap();
            state.presence.get_mut(&keys[0]).unwrap().last_seen = 0;
            state.presence.get_mut(&keys[1]).unwrap().last_seen = now_ms() - 100;
        }
        sessions.touch(&keys[3], Transport::Udp).unwrap();
        assert!(
            !sessions
                .state
                .lock()
                .unwrap()
                .presence
                .contains_key(&keys[0])
        );
        assert!(
            sessions
                .state
                .lock()
                .unwrap()
                .presence
                .contains_key(&keys[1])
        );
        sessions.touch(&keys[4], Transport::Udp).unwrap();
        assert!(
            !sessions
                .state
                .lock()
                .unwrap()
                .presence
                .contains_key(&keys[1])
        );
        assert_eq!(sessions.registry_counts().unwrap().2, 3);
    }

    #[test]
    #[ignore = "isolated serial release hotspot benchmark"]
    fn presence_scaling() {
        for count in [1, 64, 256, 1_024] {
            let sessions = Sessions::new(Arc::new(Limits {
                max_devices: count + 1,
                ..Limits::default()
            }));
            let keys = (0..count)
                .map(|index| auth(&format!("bench-{index}")).device_key.clone())
                .collect::<Vec<_>>();
            for key in &keys {
                sessions.touch(key, Transport::Udp).unwrap();
            }
            let target = &keys[0];
            for name in ["touch_existing", "presence_lookup", "connection_lookup"] {
                crate::hotspot_bench::measure(
                    name,
                    count,
                    4_000,
                    || (),
                    |_| match name {
                        "touch_existing" => sessions.touch(target, Transport::Udp).unwrap(),
                        "presence_lookup" => {
                            std::hint::black_box(sessions.presence(target).unwrap());
                        }
                        _ => {
                            std::hint::black_box(sessions.connection(target).unwrap());
                        }
                    },
                    |_| {},
                );
            }
            let new = auth("new").device_key.clone();
            crate::hotspot_bench::measure(
                "touch_new_free",
                count,
                2_000,
                || (),
                |_| sessions.touch(&new, Transport::Udp).unwrap(),
                |_| {
                    sessions.state.lock().unwrap().presence.remove(&new);
                },
            );
            let full = Sessions::new(Arc::new(Limits {
                max_devices: count,
                ..Limits::default()
            }));
            for key in &keys {
                full.touch(key, Transport::Udp).unwrap();
            }
            let oldest = Presence {
                last_seen: now_ms() - 500,
                ..full.presence(target).unwrap().unwrap()
            };
            full.state
                .lock()
                .unwrap()
                .presence
                .insert(target.clone(), oldest.clone());
            crate::hotspot_bench::measure(
                "touch_new_full",
                count,
                2_000,
                || (),
                |_| full.touch(&new, Transport::Udp).unwrap(),
                |_| {
                    let mut state = full.state.lock().unwrap();
                    state.presence.remove(&new);
                    state.presence.insert(target.clone(), oldest.clone());
                },
            );
        }
    }

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
    fn mqtt_command_readiness_is_generation_fenced() {
        let sessions = Sessions::new(Arc::new(Limits::default()));
        let auth = auth("ready");
        let (old, _) = sessions.register(auth.clone(), Transport::Mqtt).unwrap();
        assert!(
            !sessions
                .lookup(&old.device)
                .unwrap()
                .unwrap()
                .command_ready()
        );
        old.set_command_ready(true).unwrap();
        assert!(
            sessions
                .lookup(&old.device)
                .unwrap()
                .unwrap()
                .command_ready()
        );
        let (new, _) = sessions.register(auth, Transport::Mqtt).unwrap();
        assert!(
            !sessions
                .lookup(&new.device)
                .unwrap()
                .unwrap()
                .command_ready()
        );
        assert!(matches!(
            old.set_command_ready(true),
            Err(Error::Unavailable)
        ));
        assert!(
            !sessions
                .lookup(&new.device)
                .unwrap()
                .unwrap()
                .command_ready()
        );
        new.set_command_ready(true).unwrap();
        assert!(
            sessions
                .lookup(&new.device)
                .unwrap()
                .unwrap()
                .command_ready()
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

    #[test]
    fn command_tenant_and_process_slots_reject_and_release_independently() {
        let sessions = Sessions::new(Arc::new(Limits {
            max_pending_commands_per_device: 2,
            max_pending_commands_per_tenant: 1,
            max_pending_commands: 2,
            max_outbound_messages_per_connection: 2,
            ..Limits::default()
        }));
        let first = auth("first");
        let second = auth("second");
        let mut other = (*auth("other")).clone();
        other.device_key.tenant_id = TenantId::new("other").unwrap();
        let other = Arc::new(other);
        let mut third = (*auth("third")).clone();
        third.device_key.tenant_id = TenantId::new("third").unwrap();
        let third = Arc::new(third);
        let (_first_lease, mut first_rx) =
            sessions.register(first.clone(), Transport::Tcp).unwrap();
        let (_second_lease, _second_rx) =
            sessions.register(second.clone(), Transport::Tcp).unwrap();
        let (_other_lease, _other_rx) = sessions.register(other.clone(), Transport::Tcp).unwrap();
        let (_third_lease, _third_rx) = sessions.register(third.clone(), Transport::Tcp).unwrap();
        let command = |device: &DeviceKey| DeviceCommand {
            command_id: CommandId::generate(),
            device: device.clone(),
            expires_at: Some(now_ms() + 1_000),
            payload: DeviceCommandPayload {
                name: "x".into(),
                arguments: Default::default(),
            },
        };
        sessions
            .lookup(&first.device_key)
            .unwrap()
            .unwrap()
            .enqueue(&command(&first.device_key), vec![1])
            .unwrap();
        assert!(matches!(
            sessions
                .lookup(&second.device_key)
                .unwrap()
                .unwrap()
                .enqueue(&command(&second.device_key), vec![2]),
            Err(Error::Overloaded)
        ));
        sessions
            .lookup(&other.device_key)
            .unwrap()
            .unwrap()
            .enqueue(&command(&other.device_key), vec![3])
            .unwrap();
        assert!(matches!(
            sessions
                .lookup(&third.device_key)
                .unwrap()
                .unwrap()
                .enqueue(&command(&third.device_key), vec![4]),
            Err(Error::Overloaded)
        ));
        drop(first_rx.try_recv().unwrap());
        sessions
            .lookup(&second.device_key)
            .unwrap()
            .unwrap()
            .enqueue(&command(&second.device_key), vec![5])
            .unwrap();
    }
    #[test]
    fn enqueue_validation_and_every_reservation_failure_roll_back() {
        for failure in [
            "missing_expiry",
            "channel",
            "connection_bytes",
            "tenant_bytes",
            "global_bytes",
            "connection_slots",
            "tenant_slots",
            "global_slots",
            "closed",
        ] {
            let limits = Arc::new(Limits {
                max_outbound_messages_per_connection: 1,
                max_pending_commands_per_device: 2,
                max_pending_commands_per_tenant: 2,
                max_pending_commands: 2,
                max_outbound_bytes_per_connection: 8,
                max_outbound_bytes_per_tenant: 8,
                max_outbound_bytes: 8,
                ..Limits::default()
            });
            let sessions = Sessions::new(limits);
            let auth = auth("accounting");
            let (_lease, mut receiver) = sessions.register(auth.clone(), Transport::Tcp).unwrap();
            let endpoint = sessions.lookup(&auth.device_key).unwrap().unwrap();
            let mut command = DeviceCommand {
                command_id: CommandId::generate(),
                device: auth.device_key.clone(),
                expires_at: Some(now_ms() + 1000),
                payload: DeviceCommandPayload {
                    name: "test".into(),
                    arguments: Default::default(),
                },
            };
            let slots = match failure {
                "connection_slots" => Some(
                    endpoint
                        .connection_slots
                        .clone()
                        .try_acquire_many_owned(2)
                        .unwrap(),
                ),
                "tenant_slots" => Some(
                    endpoint
                        .tenant_slots
                        .clone()
                        .try_acquire_many_owned(2)
                        .unwrap(),
                ),
                "global_slots" => Some(
                    endpoint
                        .global_slots
                        .clone()
                        .try_acquire_many_owned(2)
                        .unwrap(),
                ),
                _ => None,
            };
            let bytes = match failure {
                "connection_bytes" => Some(endpoint.connection_bytes.reserve(8).unwrap()),
                "tenant_bytes" => Some(endpoint.tenant_bytes.reserve(8).unwrap()),
                "global_bytes" => Some(endpoint.global_bytes.reserve(8).unwrap()),
                _ => None,
            };
            if failure == "missing_expiry" {
                command.expires_at = None;
            }
            if failure == "channel" {
                endpoint.enqueue(&command, vec![1]).unwrap();
            }
            if failure == "closed" {
                receiver.close();
            }
            let before = (
                sessions.queued_messages(),
                sessions.queued_bytes(),
                endpoint.connection_slots.available_permits(),
                endpoint.tenant_slots.available_permits(),
                endpoint.global_slots.available_permits(),
                endpoint.connection_bytes.available(),
                endpoint.tenant_bytes.available(),
                endpoint.global_bytes.available(),
            );
            assert!(endpoint.enqueue(&command, vec![1]).is_err(), "{failure}");
            let after = (
                sessions.queued_messages(),
                sessions.queued_bytes(),
                endpoint.connection_slots.available_permits(),
                endpoint.tenant_slots.available_permits(),
                endpoint.global_slots.available_permits(),
                endpoint.connection_bytes.available(),
                endpoint.tenant_bytes.available(),
                endpoint.global_bytes.available(),
            );
            assert_eq!(before, after, "{failure}");
            drop((slots, bytes, receiver));
            assert_eq!(sessions.queued_messages(), 0, "{failure}");
            assert_eq!(sessions.queued_bytes(), 0, "{failure}");
            assert_eq!(endpoint.connection_slots.available_permits(), 2);
            assert_eq!(endpoint.tenant_slots.available_permits(), 2);
            assert_eq!(endpoint.global_slots.available_permits(), 2);
            assert_eq!(endpoint.connection_bytes.available(), 8);
            assert_eq!(endpoint.tenant_bytes.available(), 8);
            assert_eq!(endpoint.global_bytes.available(), 8);
        }
    }
}
