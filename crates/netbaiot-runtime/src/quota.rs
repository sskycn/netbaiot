use crate::*;
use netbaiot_core::*;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct ByteBudget(Arc<Semaphore>);
#[derive(Default)]
pub struct WeakByteBudget(std::sync::Weak<Semaphore>);
impl WeakByteBudget {
    pub fn upgrade(&self) -> Option<ByteBudget> {
        self.0.upgrade().map(ByteBudget)
    }
    pub fn get_or_create(&mut self, bytes: usize) -> ByteBudget {
        if let Some(budget) = self.upgrade() {
            return budget;
        }
        // The last permit may have disappeared after registry pruning. Replace
        // the weak identity even when the tenant entry itself already exists.
        let budget = ByteBudget::new(bytes);
        *self = budget.downgrade();
        budget
    }
}
#[derive(Debug)]
pub struct BytesPermit {
    _permit: OwnedSemaphorePermit,
}
impl ByteBudget {
    pub fn downgrade(&self) -> WeakByteBudget {
        WeakByteBudget(Arc::downgrade(&self.0))
    }
    pub fn new(bytes: usize) -> Self {
        Self(Arc::new(Semaphore::new(bytes)))
    }
    pub fn reserve(&self, bytes: usize) -> Result<BytesPermit> {
        let n = u32::try_from(bytes).map_err(|_| Error::Overloaded)?;
        Ok(BytesPermit {
            _permit: self
                .0
                .clone()
                .try_acquire_many_owned(n)
                .map_err(|_| Error::Overloaded)?,
        })
    }
    async fn reserve_until(
        &self,
        bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<BytesPermit> {
        let n = u32::try_from(bytes).map_err(|_| Error::Overloaded)?;
        let permit = tokio::time::timeout_at(deadline, self.0.clone().acquire_many_owned(n))
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Draining)?;
        Ok(BytesPermit { _permit: permit })
    }
    pub fn available(&self) -> usize {
        self.0.available_permits()
    }
}
#[derive(Default)]
struct ConnectionCounts {
    ips: HashMap<IpAddr, usize>,
    tenants: HashMap<TenantId, usize>,
    devices: HashMap<DeviceKey, usize>,
    transports: [usize; 3],
    device_transports: [usize; 3],
}
pub struct Connections {
    limits: Arc<Limits>,
    counts: Mutex<ConnectionCounts>,
    slots: Arc<Semaphore>,
    memory: ByteBudget,
    metrics: Arc<Metrics>,
}
pub struct ConnectionLease {
    owner: Arc<Connections>,
    ip: IpAddr,
    device: Option<DeviceKey>,
    transport: Option<Transport>,
    device_ingress: bool,
    connect_deadline: tokio::time::Instant,
    _slot: OwnedSemaphorePermit,
    _bytes: BytesPermit,
}
impl Connections {
    pub fn new(limits: Arc<Limits>, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            counts: Mutex::new(ConnectionCounts::default()),
            slots: Arc::new(Semaphore::new(limits.max_connections)),
            memory: ByteBudget::new(limits.global_connection_logical_bytes),
            limits,
            metrics,
        })
    }
    pub fn acquire(self: &Arc<Self>, ip: IpAddr, transport: Transport) -> Result<ConnectionLease> {
        self.acquire_pending(ip)?.classify(transport)
    }
    /// Reserve count, bytes and peer capacity before TLS or protocol detection.
    pub fn acquire_pending(self: &Arc<Self>, ip: IpAddr) -> Result<PendingConnectionLease> {
        self.reserve_connection(ip, false)
    }
    /// Device listener admission before its protocol is known. No extra semaphore:
    /// pending and classified owners retain the same global count/byte/IP permits.
    pub fn acquire_device_pending(self: &Arc<Self>, ip: IpAddr) -> Result<PendingConnectionLease> {
        self.reserve_connection(ip, true)
    }
    fn reserve_connection(
        self: &Arc<Self>,
        ip: IpAddr,
        device_ingress: bool,
    ) -> Result<PendingConnectionLease> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let bytes = self
            .memory
            .reserve(self.limits.connection_memory_reservation)?;
        let mut counts = lock(&self.counts)?;
        let count = counts.ips.entry(ip).or_default();
        if *count >= self.limits.max_connections_per_ip {
            return Err(Error::Overloaded);
        }
        *count += 1;
        self.metrics.inc(Metric::ConnectionsAccepted);
        Ok(PendingConnectionLease(ConnectionLease {
            owner: self.clone(),
            ip,
            device: None,
            transport: None,
            device_ingress,
            connect_deadline: tokio::time::Instant::now()
                + Duration::from_millis(self.limits.connect_timeout_ms),
            _slot: slot,
            _bytes: bytes,
        }))
    }
    pub fn active(&self) -> Result<[usize; 3]> {
        Ok(lock(&self.counts)?.transports)
    }
}
/// Pending and classified connections own exactly the same permits.
pub struct PendingConnectionLease(ConnectionLease);
impl PendingConnectionLease {
    pub fn connect_deadline(&self) -> tokio::time::Instant {
        self.0.connect_deadline
    }
    /// Management keeps global/IP/byte ownership without entering device counts.
    pub fn into_management(self) -> Result<ConnectionLease> {
        if self.0.device_ingress {
            return Err(Error::Invalid);
        }
        Ok(self.0)
    }
    pub fn classify(mut self, transport: Transport) -> Result<ConnectionLease> {
        {
            let mut counts = lock(&self.0.owner.counts)?;
            if self.0.device_ingress {
                if transport == Transport::Udp {
                    return Err(Error::Invalid);
                }
                if counts.device_transports[transport as usize]
                    >= self.0.owner.limits.max_device_connections_per_protocol
                {
                    return Err(Error::Overloaded);
                }
                counts.device_transports[transport as usize] += 1;
            }
            counts.transports[transport as usize] += 1;
            // The accounting transition and owner state change share one lock.
            self.0.transport = Some(transport);
        }
        Ok(self.0)
    }
}
impl ConnectionLease {
    pub fn connect_deadline(&self) -> tokio::time::Instant {
        self.connect_deadline
    }
    pub fn authenticate(&mut self, device: &DeviceKey) -> Result<()> {
        if self.device.as_ref() == Some(device) {
            return Ok(());
        }
        if self.device.is_some() {
            return Err(Error::Forbidden);
        }
        let mut counts = lock(&self.owner.counts)?;
        if counts.tenants.get(&device.tenant_id).copied().unwrap_or(0)
            >= self.owner.limits.max_connections_per_tenant
            || counts.devices.get(device).copied().unwrap_or(0)
                >= self.owner.limits.max_connections_per_device
        {
            return Err(Error::Overloaded);
        }
        *counts.tenants.entry(device.tenant_id.clone()).or_default() += 1;
        *counts.devices.entry(device.clone()).or_default() += 1;
        self.device = Some(device.clone());
        Ok(())
    }
}
fn decrement<K: Eq + std::hash::Hash>(map: &mut HashMap<K, usize>, key: &K) {
    if let Some(count) = map.get_mut(key) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            map.remove(key);
        }
    }
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        if let Ok(mut counts) = self.owner.counts.lock() {
            decrement(&mut counts.ips, &self.ip);
            if let Some(device) = &self.device {
                decrement(&mut counts.tenants, &device.tenant_id);
                decrement(&mut counts.devices, device);
            }
            if let Some(transport) = self.transport {
                counts.transports[transport as usize] =
                    counts.transports[transport as usize].saturating_sub(1);
                if self.device_ingress {
                    counts.device_transports[transport as usize] =
                        counts.device_transports[transport as usize].saturating_sub(1);
                }
            }
        }
    }
}
struct Window {
    start: Instant,
    count: usize,
}
impl Window {
    fn take(&mut self, limit: usize) -> Result<()> {
        self.take_n(limit, 1)
    }
    fn take_n(&mut self, limit: usize, units: usize) -> Result<()> {
        if self.start.elapsed() >= Duration::from_secs(1) {
            self.start = Instant::now();
            self.count = 0;
        }
        let Some(next) = self.count.checked_add(units) else {
            return Err(Error::Overloaded);
        };
        if next > limit {
            return Err(Error::Overloaded);
        }
        self.count = next;
        Ok(())
    }
}
/// Rate table capacity and one-second expiry prevent unbounded source-IP state.
pub struct RateLimiter {
    limits: Arc<Limits>,
    inner: Mutex<(Window, HashMap<IpAddr, Window>)>,
}
impl RateLimiter {
    pub fn new(limits: Arc<Limits>) -> Self {
        Self {
            limits,
            inner: Mutex::new((
                Window {
                    start: Instant::now(),
                    count: 0,
                },
                HashMap::new(),
            )),
        }
    }
    pub fn take(&self, ip: IpAddr) -> Result<()> {
        let mut inner = lock(&self.inner)?;
        inner.0.take(self.limits.requests_per_second)?;
        inner
            .1
            .retain(|_, w| w.start.elapsed() < Duration::from_secs(1));
        if !inner.1.contains_key(&ip) && inner.1.len() >= self.limits.rate_entries {
            return Err(Error::Overloaded);
        }
        inner
            .1
            .entry(ip)
            .or_insert_with(|| Window {
                start: Instant::now(),
                count: 0,
            })
            .take(self.limits.requests_per_ip_second)
    }
}
struct AdmissionEntry {
    slots: Arc<Semaphore>,
    rate: Window,
}
struct AdmissionState {
    global: Window,
    devices: HashMap<DeviceKey, AdmissionEntry>,
    tenants: HashMap<TenantId, AdmissionEntry>,
}
pub struct Admission {
    limits: Arc<Limits>,
    state: Mutex<AdmissionState>,
    slots: Arc<Semaphore>,
    bytes: ByteBudget,
    waiter_slots: Arc<Semaphore>,
    waiter_bytes: ByteBudget,
}
pub struct AdmissionLease {
    _device_slot: OwnedSemaphorePermit,
    _tenant_slot: OwnedSemaphorePermit,
    _global_slot: OwnedSemaphorePermit,
    _bytes: BytesPermit,
    wait_us: u64,
    lock_wait_us: u64,
    lock_hold_us: u64,
}
impl AdmissionLease {
    pub fn wait_us(&self) -> u64 {
        self.wait_us
    }
    pub fn lock_wait_us(&self) -> u64 {
        self.lock_wait_us
    }
    pub fn lock_hold_us(&self) -> u64 {
        self.lock_hold_us
    }
}
impl Admission {
    /// In-flight permits, not a waiting queue. The two gauges are sampled independently.
    pub fn in_flight(&self) -> (usize, usize) {
        (
            self.limits.max_ingress - self.slots.available_permits(),
            self.limits.max_ingress_bytes - self.bytes.available(),
        )
    }

    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AdmissionState {
                global: Window {
                    start: Instant::now(),
                    count: 0,
                },
                devices: HashMap::new(),
                tenants: HashMap::new(),
            }),
            slots: Arc::new(Semaphore::new(limits.max_ingress)),
            bytes: ByteBudget::new(limits.max_ingress_bytes),
            waiter_slots: Arc::new(Semaphore::new(limits.max_ingress_waiters)),
            waiter_bytes: ByteBudget::new(limits.max_ingress_wait_bytes),
            limits,
        })
    }
    pub fn waiting(&self) -> (usize, usize) {
        (
            self.limits.max_ingress_waiters - self.waiter_slots.available_permits(),
            self.limits.max_ingress_wait_bytes - self.waiter_bytes.available(),
        )
    }
    /// Applies only the bounded rate tables. Callers remain owned by their
    /// connection task, so this does not create a second concurrency lifetime.
    pub fn check_rate(&self, device: &DeviceKey) -> Result<(u64, u64)> {
        self.check_rate_weighted(device, 1)
    }

    /// Charges every protocol packet, including paths that never create a business event.
    /// A caller may charge additional units for a large packet.
    pub fn check_rate_weighted(&self, device: &DeviceKey, units: usize) -> Result<(u64, u64)> {
        if units == 0 {
            return Err(Error::Invalid);
        }
        let lock_started = Instant::now();
        let mut state = lock(&self.state)?;
        let lock_wait_us = lock_started.elapsed().as_micros() as u64;
        state
            .global
            .take_n(self.limits.requests_per_second, units)?;
        if !state.devices.contains_key(device) && state.devices.len() >= self.limits.max_devices {
            let capacity = self.limits.max_ingress_per_device;
            state.devices.retain(|_, entry| {
                entry.slots.available_permits() < capacity
                    || entry.rate.start.elapsed() < Duration::from_secs(1)
            });
        }
        if !state.tenants.contains_key(&device.tenant_id)
            && state.tenants.len() >= self.limits.max_devices
        {
            let capacity = self.limits.max_ingress_per_tenant;
            state.tenants.retain(|_, entry| {
                entry.slots.available_permits() < capacity
                    || entry.rate.start.elapsed() < Duration::from_secs(1)
            });
        }
        if (!state.devices.contains_key(device) && state.devices.len() >= self.limits.max_devices)
            || (!state.tenants.contains_key(&device.tenant_id)
                && state.tenants.len() >= self.limits.max_devices)
        {
            return Err(Error::Overloaded);
        }
        state
            .devices
            .entry(device.clone())
            .or_insert_with(|| AdmissionEntry {
                slots: Arc::new(Semaphore::new(self.limits.max_ingress_per_device)),
                rate: Window {
                    start: Instant::now(),
                    count: 0,
                },
            })
            .rate
            .take_n(self.limits.messages_per_device_second, units)?;
        state
            .tenants
            .entry(device.tenant_id.clone())
            .or_insert_with(|| AdmissionEntry {
                slots: Arc::new(Semaphore::new(self.limits.max_ingress_per_tenant)),
                rate: Window {
                    start: Instant::now(),
                    count: 0,
                },
            })
            .rate
            .take_n(self.limits.messages_per_tenant_second, units)?;
        let lock_hold_us = lock_started
            .elapsed()
            .as_micros()
            .saturating_sub(u128::from(lock_wait_us)) as u64;
        Ok((lock_wait_us, lock_hold_us))
    }
    pub fn acquire(self: &Arc<Self>, device: &DeviceKey, bytes: usize) -> Result<AdmissionLease> {
        let started = Instant::now();
        let (device_slots, tenant_slots) = self.entry_slots(device)?;
        let device_slot = device_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let tenant_slot = tenant_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let global_slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let bytes = self.bytes.reserve(bytes)?;
        self.finish_acquire(
            device,
            device_slot,
            tenant_slot,
            global_slot,
            bytes,
            started,
        )
    }
    pub async fn acquire_wait(
        self: &Arc<Self>,
        device: &DeviceKey,
        bytes: usize,
    ) -> Result<AdmissionLease> {
        let started = Instant::now();
        let _waiter_slot = self
            .waiter_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let _waiter_bytes = self.waiter_bytes.reserve(bytes)?;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.limits.ingress_wait_timeout_ms);
        let (device_slots, tenant_slots) = self.entry_slots(device)?;
        let device_slot = tokio::time::timeout_at(deadline, device_slots.acquire_owned())
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Draining)?;
        let tenant_slot = tokio::time::timeout_at(deadline, tenant_slots.acquire_owned())
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Draining)?;
        let global_slot = tokio::time::timeout_at(deadline, self.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::Overloaded)?
            .map_err(|_| Error::Draining)?;
        let active_bytes = self.bytes.reserve_until(bytes, deadline).await?;
        self.finish_acquire(
            device,
            device_slot,
            tenant_slot,
            global_slot,
            active_bytes,
            started,
        )
    }
    fn entry_slots(&self, device: &DeviceKey) -> Result<(Arc<Semaphore>, Arc<Semaphore>)> {
        let mut state = lock(&self.state)?;
        if !state.devices.contains_key(device) && state.devices.len() >= self.limits.max_devices {
            let capacity = self.limits.max_ingress_per_device;
            state.devices.retain(|_, entry| {
                entry.slots.available_permits() < capacity
                    || entry.rate.start.elapsed() < Duration::from_secs(1)
            });
        }
        if !state.tenants.contains_key(&device.tenant_id)
            && state.tenants.len() >= self.limits.max_devices
        {
            let capacity = self.limits.max_ingress_per_tenant;
            state.tenants.retain(|_, entry| {
                entry.slots.available_permits() < capacity
                    || entry.rate.start.elapsed() < Duration::from_secs(1)
            });
        }
        if (!state.devices.contains_key(device) && state.devices.len() >= self.limits.max_devices)
            || (!state.tenants.contains_key(&device.tenant_id)
                && state.tenants.len() >= self.limits.max_devices)
        {
            return Err(Error::Overloaded);
        }
        let device_slots = state
            .devices
            .entry(device.clone())
            .or_insert_with(|| AdmissionEntry {
                slots: Arc::new(Semaphore::new(self.limits.max_ingress_per_device)),
                rate: Window {
                    start: Instant::now(),
                    count: 0,
                },
            })
            .slots
            .clone();
        let tenant_slots = state
            .tenants
            .entry(device.tenant_id.clone())
            .or_insert_with(|| AdmissionEntry {
                slots: Arc::new(Semaphore::new(self.limits.max_ingress_per_tenant)),
                rate: Window {
                    start: Instant::now(),
                    count: 0,
                },
            })
            .slots
            .clone();
        Ok((device_slots, tenant_slots))
    }
    fn finish_acquire(
        self: &Arc<Self>,
        device: &DeviceKey,
        device_slot: OwnedSemaphorePermit,
        tenant_slot: OwnedSemaphorePermit,
        global_slot: OwnedSemaphorePermit,
        bytes: BytesPermit,
        started: Instant,
    ) -> Result<AdmissionLease> {
        let lock_started = Instant::now();
        let mut state = lock(&self.state)?;
        let lock_wait_us = lock_started.elapsed().as_micros() as u64;
        state.global.take(self.limits.requests_per_second)?;
        state
            .devices
            .get_mut(device)
            .ok_or(Error::Internal)?
            .rate
            .take(self.limits.messages_per_device_second)?;
        state
            .tenants
            .get_mut(&device.tenant_id)
            .ok_or(Error::Internal)?
            .rate
            .take(self.limits.messages_per_tenant_second)?;
        let lock_hold_us = lock_started
            .elapsed()
            .as_micros()
            .saturating_sub(u128::from(lock_wait_us)) as u64;
        Ok(AdmissionLease {
            _device_slot: device_slot,
            _tenant_slot: tenant_slot,
            _global_slot: global_slot,
            _bytes: bytes,
            wait_us: started.elapsed().as_micros() as u64,
            lock_wait_us,
            lock_hold_us,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn pending_connection_owns_count_bytes_ip_and_classifies_once() {
        let limits = Arc::new(Limits {
            max_connections: 2,
            max_connections_per_ip: 1,
            global_connection_logical_bytes: 2 * Limits::default().connection_memory_reservation,
            ..Limits::default()
        });
        let connections = Connections::new(limits.clone(), Arc::new(Metrics::default()));
        let ip = "127.0.0.1".parse().unwrap();
        let other = "127.0.0.2".parse().unwrap();
        let a = connections.acquire_pending(ip).unwrap();
        assert_eq!(connections.active().unwrap(), [0; 3]);
        assert!(connections.acquire_pending(ip).is_err());
        let b = connections.acquire_pending(other).unwrap();
        assert_eq!(connections.slots.available_permits(), 0);
        assert_eq!(connections.memory.available(), 0);
        assert!(
            connections
                .acquire_pending("127.0.0.3".parse().unwrap())
                .is_err()
        );
        let a = a.classify(Transport::Mqtt).unwrap();
        assert_eq!(connections.active().unwrap()[Transport::Mqtt as usize], 1);
        assert_eq!(connections.slots.available_permits(), 0);
        drop(b);
        assert_eq!(connections.slots.available_permits(), 1);
        drop(a);
        assert_eq!(connections.active().unwrap(), [0; 3]);
        assert_eq!(connections.slots.available_permits(), 2);
        assert_eq!(
            connections.memory.available(),
            limits.global_connection_logical_bytes
        );
        assert!(lock(&connections.counts).unwrap().ips.is_empty());
        assert!(connections.acquire(ip, Transport::Tcp).is_ok());
    }
    #[tokio::test]
    async fn device_protocol_ceiling_shares_global_ownership_and_releases_on_failure() {
        let limits = Arc::new(Limits {
            max_connections: 5,
            max_connections_per_ip: 5,
            max_device_connections_per_protocol: 3,
            global_connection_logical_bytes: 5 * Limits::default().connection_memory_reservation,
            ..Limits::default()
        });
        let owner = Connections::new(limits.clone(), Arc::new(Metrics::default()));
        let ip = "127.0.0.1".parse().unwrap();
        let mut mqtt = Vec::new();
        for _ in 0..3 {
            mqtt.push(
                owner
                    .acquire_device_pending(ip)
                    .unwrap()
                    .classify(Transport::Mqtt)
                    .unwrap(),
            );
        }
        // A protocol at its ceiling cannot take the remaining shared capacity.
        let pending = owner.acquire_device_pending(ip).unwrap();
        assert!(matches!(
            pending.classify(Transport::Mqtt),
            Err(Error::Overloaded)
        ));
        assert_eq!(owner.slots.available_permits(), 2);
        let management = owner
            .acquire_pending(ip)
            .unwrap()
            .into_management()
            .unwrap();
        let tcp = owner
            .acquire_device_pending(ip)
            .unwrap()
            .classify(Transport::Tcp)
            .unwrap();
        assert!(owner.acquire_pending(ip).is_err()); // Management still shares global capacity.
        assert_eq!(owner.active().unwrap(), [3, 1, 0]);
        drop((management, tcp, mqtt));
        assert!(matches!(
            owner.acquire_device_pending(ip).unwrap().into_management(),
            Err(Error::Invalid)
        ));
        let first = owner.acquire_device_pending(ip).unwrap();
        let second = owner.acquire_device_pending(ip).unwrap();
        // Known management/standalone listeners retain shared global accounting.
        let management = owner
            .acquire_pending(ip)
            .unwrap()
            .into_management()
            .unwrap();
        assert!(matches!(
            second.classify(Transport::Udp),
            Err(Error::Invalid)
        ));
        drop(first); // TLS/EOF/cancellation before detection must release pending ownership.
        let mqtt = owner
            .acquire_device_pending(ip)
            .unwrap()
            .classify(Transport::Mqtt)
            .unwrap();
        drop((mqtt, management));
        let counts = lock(&owner.counts).unwrap();
        assert_eq!(counts.device_transports, [0; 3]);
        assert_eq!(counts.transports, [0; 3]);
        assert!(counts.ips.is_empty());
        assert_eq!(owner.slots.available_permits(), 5);
        assert_eq!(
            owner.memory.available(),
            limits.global_connection_logical_bytes
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_classification_cannot_exceed_protocol_cap() {
        let owner = Connections::new(
            Arc::new(Limits {
                max_connections: 4,
                max_connections_per_ip: 4,
                max_device_connections_per_protocol: 1,
                ..Limits::default()
            }),
            Arc::new(Metrics::default()),
        );
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let pending = owner
                .acquire_device_pending("127.0.0.1".parse().unwrap())
                .unwrap();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                pending.classify(Transport::Mqtt)
            });
        }
        let mut winners = Vec::new();
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(lease) => winners.push(lease),
                Err(error) => assert!(matches!(error, Error::Overloaded)),
            }
        }
        assert_eq!(winners.len(), 1);
        assert_eq!(owner.active().unwrap()[Transport::Mqtt as usize], 1);
        drop(winners);
        assert_eq!(owner.slots.available_permits(), 4);
        assert_eq!(lock(&owner.counts).unwrap().device_transports, [0; 3]);
    }
    #[test]
    fn expired_tenant_identity_replacement_is_shared() {
        let old = ByteBudget::new(16);
        let mut identity = old.downgrade();
        // register's retain saw a live budget, then the final old owner exited.
        assert!(identity.upgrade().is_some());
        drop(old);
        let replacement = identity.get_or_create(16);
        let held = replacement.reserve(16).unwrap();
        let concurrent = identity.get_or_create(16);
        assert!(matches!(concurrent.reserve(1), Err(Error::Overloaded)));
        drop(replacement);
        assert!(matches!(
            identity.get_or_create(16).reserve(1),
            Err(Error::Overloaded)
        ));
        drop(held);
        assert!(concurrent.reserve(16).is_ok());
    }
    fn key(tenant: &str, device: &str) -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new(tenant).unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new(device).unwrap(),
        }
    }
    #[test]
    fn protocol_packet_budget_charges_count_and_payload_units() {
        let limits = Arc::new(Limits {
            requests_per_second: 4,
            messages_per_device_second: 4,
            messages_per_tenant_second: 4,
            ..Limits::default()
        });
        let data = Admission::new(limits.clone());
        let control = Admission::new(limits);
        let device = key("t", "d");
        data.check_rate_weighted(&device, 3).unwrap();
        data.check_rate(&device).unwrap();
        assert!(matches!(
            data.check_rate_weighted(&device, 1),
            Err(Error::Overloaded)
        ));
        // ACK/PING/DISCONNECT have a separate bounded budget, so data floods
        // cannot prevent a transaction from completing or a peer disconnecting.
        control.check_rate(&device).unwrap();
        assert!(matches!(
            control.check_rate_weighted(&device, 5),
            Err(Error::Overloaded)
        ));
    }
    #[tokio::test]
    async fn ingress_wait_is_count_byte_and_deadline_bounded() {
        let limits = Arc::new(Limits {
            max_ingress: 1,
            max_ingress_per_tenant: 1,
            max_ingress_per_device: 1,
            max_ingress_bytes: 8,
            max_ingress_waiters: 1,
            max_ingress_wait_bytes: 8,
            ingress_wait_timeout_ms: 20,
            requests_per_second: 100,
            messages_per_device_second: 100,
            messages_per_tenant_second: 100,
            ..Limits::default()
        });
        let admission = Admission::new(limits);
        let active = admission.acquire(&key("a", "1"), 8).unwrap();
        let waiting_owner = admission.clone();
        let waiting =
            tokio::spawn(async move { waiting_owner.acquire_wait(&key("b", "2"), 8).await });
        tokio::task::yield_now().await;
        assert_eq!(admission.waiting(), (1, 8));
        assert!(matches!(
            admission.acquire_wait(&key("c", "3"), 1).await,
            Err(Error::Overloaded)
        ));
        drop(active);
        assert!(waiting.await.unwrap().is_ok());

        let active = admission.acquire(&key("a", "1"), 8).unwrap();
        assert!(matches!(
            admission.acquire_wait(&key("b", "2"), 8).await,
            Err(Error::Overloaded)
        ));
        assert_eq!(admission.waiting(), (0, 0));
        drop(active);
    }
    #[test]
    fn ingress_limits_isolate_devices_tenants_and_bytes() {
        let l = Arc::new(Limits {
            max_ingress: 3,
            max_ingress_per_tenant: 2,
            max_ingress_per_device: 1,
            max_ingress_bytes: 30,
            ..Limits::default()
        });
        let a = Admission::new(l);
        assert_eq!(a.in_flight(), (0, 0));
        let first = a.acquire(&key("t", "1"), 10).unwrap();
        assert!(a.acquire(&key("t", "1"), 1).is_err());
        let second = a.acquire(&key("t", "2"), 10).unwrap();
        assert!(a.acquire(&key("t", "3"), 1).is_err());
        assert!(a.acquire(&key("u", "1"), 11).is_err());
        let other = a.acquire(&key("u", "1"), 10).unwrap();
        assert!(a.acquire(&key("v", "1"), 1).is_err());
        assert_eq!(a.in_flight(), (3, 30));
        drop((first, second, other));
        assert_eq!(a.in_flight(), (0, 0));
        assert!(a.acquire(&key("t", "1"), 30).is_ok());
    }
    #[test]
    fn connection_limits_and_raii_cleanup() {
        let l = Arc::new(Limits {
            max_connections: 3,
            max_connections_per_ip: 2,
            max_connections_per_device: 1,
            max_connections_per_tenant: 2,
            ..Limits::default()
        });
        let c = Connections::new(l, Arc::new(Metrics::default()));
        let ip = "127.0.0.1".parse().unwrap();
        let mut first = c.acquire(ip, Transport::Mqtt).unwrap();
        let mut second = c.acquire(ip, Transport::Mqtt).unwrap();
        assert!(c.acquire(ip, Transport::Mqtt).is_err());
        first.authenticate(&key("t", "1")).unwrap();
        assert!(second.authenticate(&key("t", "1")).is_err());
        second.authenticate(&key("t", "2")).unwrap();
        drop((first, second));
        assert_eq!(c.active().unwrap(), [0; 3]);
        assert!(c.acquire(ip, Transport::Mqtt).is_ok());
    }
    #[test]
    fn source_rate_table_has_capacity_and_global_limit() {
        let l = Arc::new(Limits {
            rate_entries: 1,
            requests_per_second: 2,
            requests_per_ip_second: 1,
            ..Limits::default()
        });
        let r = RateLimiter::new(l);
        r.take("127.0.0.1".parse().unwrap()).unwrap();
        assert!(r.take("127.0.0.2".parse().unwrap()).is_err());
        assert!(r.take("127.0.0.1".parse().unwrap()).is_err());
    }
}
