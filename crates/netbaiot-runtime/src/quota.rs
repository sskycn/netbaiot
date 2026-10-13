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
struct TenantConnections {
    connections: usize,
    read_bytes: usize,
}
#[derive(Default)]
struct ConnectionCounts {
    ips: HashMap<IpAddr, usize>,
    tenants: HashMap<TenantId, TenantConnections>,
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
    read_state: Option<Arc<Mutex<ReadAllocationState>>>,
}
/// A lazy, connection-local account for actual read-buffer allocation capacity.
/// Every allocation owns a permit, including allocations shared by decoded
/// packets after their reader has moved on to another frame.
pub struct ConnectionReadBudget {
    owner: Arc<Connections>,
    state: Arc<Mutex<ReadAllocationState>>,
}
#[derive(Default)]
struct ReadAllocationState {
    tenant: Option<TenantId>,
    capacity: usize,
    replacing: bool,
}
/// Keep this permit alive for every alias of the charged allocation. The
/// transport can share it through the owner of an immutable byte buffer.
pub struct ReadAllocationPermit {
    owner: Arc<Connections>,
    state: Arc<Mutex<ReadAllocationState>>,
    capacity: usize,
    _bytes: BytesPermit,
}
impl ReadAllocationPermit {
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}
/// One synchronous, exclusive buffer replacement. Both allocations remain
/// charged globally and to the tenant until their actual owners release them.
/// Ordinary allocations cannot enter this connection account during the copy.
#[must_use]
pub struct ReadAllocationReplacement {
    state: Arc<Mutex<ReadAllocationState>>,
    maximum: usize,
    allocation: Option<ReadAllocationPermit>,
    active: bool,
}
impl ReadAllocationReplacement {
    /// Publish the replacement only after all retained allocations fit the
    /// stable ceiling again. On failure the caller receives this owner back
    /// so it can free the new bytes before dropping their quota reservation.
    pub fn commit(mut self) -> std::result::Result<ReadAllocationPermit, (Error, Self)> {
        let result = (|| {
            let mut state = lock(&self.state)?;
            if state.capacity > self.maximum {
                return Err(Error::Invalid);
            }
            let allocation = self.allocation.take().ok_or(Error::Internal)?;
            state.replacing = false;
            self.active = false;
            Ok(allocation)
        })();
        result.map_err(|error| (error, self))
    }
}
impl Drop for ReadAllocationReplacement {
    fn drop(&mut self) {
        // Never hold the reader lock while dropping a permit: permits acquire
        // counts then reader state. Abort first releases the new reservation.
        drop(self.allocation.take());
        if self.active {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.replacing = false;
            // Preserve poison: cleanup never reopens poisoned admission.
        }
    }
}

#[cfg(test)]
#[path = "quota_reader_tests.rs"]
mod reader_audit;
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
            read_state: None,
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
        if counts
            .tenants
            .get(&device.tenant_id)
            .map_or(0, |tenant| tenant.connections)
            >= self.owner.limits.max_connections_per_tenant
            || counts.devices.get(device).copied().unwrap_or(0)
                >= self.owner.limits.max_connections_per_device
        {
            return Err(Error::Overloaded);
        }
        let mut read_state = self
            .read_state
            .as_ref()
            .map(|state| lock(state))
            .transpose()?;
        let newly_bound_bytes = if let Some(state) = &read_state {
            match &state.tenant {
                Some(id) if id == &device.tenant_id => 0,
                Some(_) => return Err(Error::Forbidden),
                None => state.capacity,
            }
        } else {
            0
        };
        let (tenant_connections, read_bytes) = counts
            .tenants
            .get(&device.tenant_id)
            .map_or((0, 0), |tenant| (tenant.connections, tenant.read_bytes));
        let new_read_bytes = read_bytes
            .checked_add(newly_bound_bytes)
            .ok_or(Error::Overloaded)?;
        let next = tenant_connections
            .checked_add(1)
            .and_then(|count| count.checked_mul(self.owner.limits.connection_memory_reservation))
            .and_then(|fixed| fixed.checked_add(new_read_bytes))
            .ok_or(Error::Overloaded)?;
        if next > self.owner.limits.max_connection_bytes_per_tenant {
            return Err(Error::Overloaded);
        }
        let tenant = counts.tenants.entry(device.tenant_id.clone()).or_default();
        tenant.connections += 1;
        tenant.read_bytes = new_read_bytes;
        if let Some(state) = &mut read_state {
            state.tenant = Some(device.tenant_id.clone());
        }
        *counts.devices.entry(device.clone()).or_default() += 1;
        self.device = Some(device.clone());
        Ok(())
    }
    /// A reader has exactly one owner and no initial allocation or semaphore.
    pub fn read_budget(&mut self) -> ConnectionReadBudget {
        let state = self
            .read_state
            .get_or_insert_with(|| {
                Arc::new(Mutex::new(ReadAllocationState {
                    tenant: self.device.as_ref().map(|device| device.tenant_id.clone()),
                    capacity: 0,
                    replacing: false,
                }))
            })
            .clone();
        ConnectionReadBudget {
            owner: self.owner.clone(),
            state,
        }
    }
}
impl ConnectionReadBudget {
    /// Current charged capacity, including allocations shared by decoded frames.
    pub fn capacity(&self) -> Result<usize> {
        Ok(lock(&self.state)?.capacity)
    }

    /// Bind all live pre-authentication allocations to the authenticated tenant.
    /// A failed bind leaves every allocation charged globally. Call this before
    /// registering the authenticated session.
    pub fn authenticate(&mut self, device: &DeviceKey) -> Result<()> {
        // All paths use the same order: connection counts, then reader state.
        let mut counts = lock(&self.owner.counts)?;
        let mut state = lock(&self.state)?;
        if let Some(tenant) = &state.tenant {
            return if tenant == &device.tenant_id {
                Ok(())
            } else {
                Err(Error::Forbidden)
            };
        }
        let tenant = counts
            .tenants
            .get_mut(&device.tenant_id)
            .ok_or(Error::Invalid)?;
        let bytes = tenant
            .read_bytes
            .checked_add(state.capacity)
            .ok_or(Error::Overloaded)?;
        check_connection_tenant_bytes(&self.owner.limits, tenant.connections, bytes)?;
        tenant.read_bytes = bytes;
        state.tenant = Some(device.tenant_id.clone());
        Ok(())
    }

    /// Fail-fast admission *before* allocation. Replacements must charge both
    /// old and new live allocations; decoded frame aliases keep their charge
    /// until their last owner is dropped. There is no task or quota waiter.
    pub fn reserve_allocation(&mut self, capacity: usize) -> Result<ReadAllocationPermit> {
        self.reserve_allocation_inner(capacity, None)
    }

    /// Replace an exclusively owned allocation without treating its temporary
    /// coexistence as retained frame capacity. The stable capacity still obeys
    /// the configured per-connection ceiling; the copy peak is at most twice
    /// that ceiling and must fit the unchanged global and tenant byte budgets.
    /// The caller must keep this guard until the old bytes and permit are gone,
    /// or drop the new allocation on failure. A shared frame cannot be replaced.
    pub fn reserve_replacement(
        &mut self,
        capacity: usize,
        previous: &mut ReadAllocationPermit,
    ) -> Result<ReadAllocationReplacement> {
        if !Arc::ptr_eq(&self.owner, &previous.owner) || !Arc::ptr_eq(&self.state, &previous.state)
        {
            return Err(Error::Invalid);
        }
        let allocation = self.reserve_allocation_inner(capacity, Some(previous.capacity))?;
        Ok(ReadAllocationReplacement {
            state: self.state.clone(),
            maximum: self.owner.limits.max_read_buffer_per_connection,
            allocation: Some(allocation),
            active: true,
        })
    }

    /// Reserve an exclusive allocation's larger capacity before replacing its
    /// old bytes through a small inline prefix. This has no two-heap copy peak.
    /// The caller must free the old bytes before allocating the larger buffer.
    pub fn expand_allocation(
        &mut self,
        previous: &mut ReadAllocationPermit,
        capacity: usize,
    ) -> Result<()> {
        if !Arc::ptr_eq(&self.owner, &previous.owner) || !Arc::ptr_eq(&self.state, &previous.state)
        {
            return Err(Error::Invalid);
        }
        let additional = capacity
            .checked_sub(previous.capacity)
            .ok_or(Error::Invalid)?;
        let mut counts = lock(&self.owner.counts)?;
        let mut state = lock(&self.state)?;
        let next = state
            .capacity
            .checked_add(additional)
            .ok_or(Error::Overloaded)?;
        if state.replacing || next > self.owner.limits.max_read_buffer_per_connection {
            return Err(Error::Overloaded);
        }
        if let Some(id) = &state.tenant {
            let tenant = counts.tenants.get(id).ok_or(Error::Invalid)?;
            let bytes = tenant
                .read_bytes
                .checked_add(additional)
                .ok_or(Error::Overloaded)?;
            check_connection_tenant_bytes(&self.owner.limits, tenant.connections, bytes)?;
        }
        let bytes = self.owner.memory.reserve(additional)?;
        // Identity checks above ensure these permits own the same semaphore.
        previous._bytes._permit.merge(bytes._permit);
        previous.capacity = capacity;
        state.capacity = next;
        if let Some(id) = &state.tenant {
            counts
                .tenants
                .get_mut(id)
                .ok_or(Error::Internal)?
                .read_bytes += additional;
        }
        Ok(())
    }

    fn reserve_allocation_inner(
        &mut self,
        capacity: usize,
        previous: Option<usize>,
    ) -> Result<ReadAllocationPermit> {
        let shared = self.state.clone();
        let mut counts = lock(&self.owner.counts)?;
        let mut state = lock(&shared)?;
        if state.replacing {
            return Err(Error::Overloaded);
        }
        let next_capacity = state
            .capacity
            .checked_add(capacity)
            .ok_or(Error::Overloaded)?;
        let stable_capacity = next_capacity
            .checked_sub(previous.unwrap_or(0))
            .ok_or(Error::Invalid)?;
        if capacity > self.owner.limits.max_read_buffer_per_connection
            || stable_capacity > self.owner.limits.max_read_buffer_per_connection
        {
            return Err(Error::Overloaded);
        }
        if let Some(id) = &state.tenant {
            let tenant = counts.tenants.get(id).ok_or(Error::Invalid)?;
            let next_bytes = tenant
                .read_bytes
                .checked_add(capacity)
                .ok_or(Error::Overloaded)?;
            check_connection_tenant_bytes(&self.owner.limits, tenant.connections, next_bytes)?;
        }
        // Global admission follows local/tenant preflight, with both accounting
        // commits protected by the counts lock. Failure changes no byte counter.
        let bytes = self.owner.memory.reserve(capacity)?;
        if let Some(id) = &state.tenant {
            counts
                .tenants
                .get_mut(id)
                .ok_or(Error::Internal)?
                .read_bytes += capacity;
        }
        state.capacity = next_capacity;
        state.replacing = previous.is_some();
        drop(state);
        Ok(ReadAllocationPermit {
            owner: self.owner.clone(),
            state: shared,
            capacity,
            _bytes: bytes,
        })
    }
}
fn check_connection_tenant_bytes(
    limits: &Limits,
    connections: usize,
    read_bytes: usize,
) -> Result<()> {
    let total = connections
        .checked_mul(limits.connection_memory_reservation)
        .and_then(|fixed| fixed.checked_add(read_bytes))
        .ok_or(Error::Overloaded)?;
    if total > limits.max_connection_bytes_per_tenant {
        return Err(Error::Overloaded);
    }
    Ok(())
}
impl Drop for ReadAllocationPermit {
    fn drop(&mut self) {
        {
            let mut counts = self
                .owner
                .counts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.capacity = state.capacity.saturating_sub(self.capacity);
            if let Some(id) = &state.tenant {
                if let Some(tenant) = counts.tenants.get_mut(id) {
                    tenant.read_bytes = tenant.read_bytes.saturating_sub(self.capacity);
                }
                // A frame can outlive its reader and connection lease.
                prune_connection_tenant(&mut counts, id);
            }
        }
        // Recover known ownership even after poison, retaining the poison bit
        // so subsequent admissions still fail closed.
    }
}
fn prune_connection_tenant(counts: &mut ConnectionCounts, id: &TenantId) {
    if counts
        .tenants
        .get(id)
        .is_some_and(|tenant| tenant.connections == 0 && tenant.read_bytes == 0)
    {
        counts.tenants.remove(id);
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
        {
            let mut counts = self
                .owner
                .counts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            decrement(&mut counts.ips, &self.ip);
            if let Some(device) = &self.device {
                if let Some(tenant) = counts.tenants.get_mut(&device.tenant_id) {
                    tenant.connections = tenant.connections.saturating_sub(1);
                }
                prune_connection_tenant(&mut counts, &device.tenant_id);
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
        #[cfg(test)]
        let mut clock = crate::hotspot_bench::LockClock::start();
        let mut inner = lock(&self.inner)?;
        #[cfg(test)]
        clock.acquired();
        inner.0.take(self.limits.requests_per_second)?;
        // Existing windows reset themselves. Scan only when a new source needs
        // room; rejected attempts still consume the global rate above.
        if !inner.1.contains_key(&ip) && inner.1.len() >= self.limits.rate_entries {
            inner
                .1
                .retain(|_, w| w.start.elapsed() < Duration::from_secs(1));
        }
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
    #[test]
    fn aggregate_byte_budget_exceeds_u32_without_a_large_allocation() {
        if usize::BITS < 64 {
            return;
        }
        let request = usize::try_from(u32::MAX).unwrap();
        let budget = ByteBudget::new(request + 17);
        let first = budget.reserve(request).unwrap();
        let last = budget.reserve(17).unwrap();
        assert_eq!(budget.available(), 0);
        assert!(matches!(budget.reserve(1), Err(Error::Overloaded)));
        assert!(matches!(
            budget.reserve(request + 1),
            Err(Error::Overloaded)
        ));
        drop(first);
        assert_eq!(budget.available(), request);
        drop(last);
        assert_eq!(budget.available(), request + 17);
    }
    fn key(tenant: &str, device: &str) -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new(tenant).unwrap(),
            product_id: ProductId::new("p").unwrap(),
            device_id: DeviceId::new(device).unwrap(),
        }
    }
    fn reader_limits(global: usize, tenant: usize, per_reader: usize) -> Arc<Limits> {
        Arc::new(Limits {
            max_connections: 4,
            max_connections_per_ip: 4,
            max_connections_per_tenant: 4,
            connection_memory_reservation: 16,
            global_connection_logical_bytes: global,
            max_connection_bytes_per_tenant: tenant,
            max_read_buffer_per_connection: per_reader,
            ..Limits::default()
        })
    }

    #[test]
    fn reader_growth_charges_actual_capacity_and_all_aliases_hold_it() {
        let connections =
            Connections::new(reader_limits(128, 128, 64), Arc::new(Metrics::default()));
        let mut lease = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let mut reader = lease.read_budget();
        let mut other_handle = lease.read_budget();
        assert_eq!(reader.capacity().unwrap(), 0);
        assert_eq!(connections.memory.available(), 112);
        let allocation = Arc::new(reader.reserve_allocation(48).unwrap());
        let alias = allocation.clone();
        assert_eq!(other_handle.capacity().unwrap(), 48);
        assert!(matches!(
            other_handle.reserve_allocation(17),
            Err(Error::Overloaded)
        ));
        lease.authenticate(&key("t", "d")).unwrap();
        assert_eq!(
            lock(&connections.counts).unwrap().tenants[&TenantId::new("t").unwrap()].read_bytes,
            48
        );
        drop((reader, other_handle, allocation, lease));
        // A decoded frame remains live after disconnect. Its allocation still
        // counts globally and against the tenant until its last alias exits.
        assert_eq!(connections.memory.available(), 80);
        assert_eq!(lock(&connections.counts).unwrap().tenants.len(), 1);
        drop(alias);
        assert_eq!(connections.memory.available(), 128);
        assert!(lock(&connections.counts).unwrap().tenants.is_empty());
    }

    #[test]
    fn reader_global_and_tenant_failure_roll_back_every_account() {
        let connections = Connections::new(reader_limits(80, 40, 64), Arc::new(Metrics::default()));
        let mut a = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let mut b = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        a.authenticate(&key("a", "one")).unwrap();
        b.authenticate(&key("b", "two")).unwrap();
        let mut a_reader = a.read_budget();
        let mut b_reader = b.read_budget();
        let held = a_reader.reserve_allocation(24).unwrap();
        assert!(matches!(
            a_reader.reserve_allocation(1),
            Err(Error::Overloaded)
        ));
        assert_eq!(a_reader.capacity().unwrap(), 24);
        let other = b_reader.reserve_allocation(24).unwrap();
        assert_eq!(connections.memory.available(), 0);
        // A different tenant has spare room only after b's first allocation
        // exits; the exhausted global budget cannot grant capacity to a.
        drop(other);
        let mut unauthenticated = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let mut pending_reader = unauthenticated.read_budget();
        assert!(matches!(
            pending_reader.reserve_allocation(9),
            Err(Error::Overloaded)
        ));
        assert_eq!(pending_reader.capacity().unwrap(), 0);
        assert_eq!(connections.memory.available(), 8);
        let tenant_bytes =
            lock(&connections.counts).unwrap().tenants[&TenantId::new("a").unwrap()].read_bytes;
        assert_eq!(tenant_bytes, 24);
        drop((
            held,
            a,
            b,
            unauthenticated,
            a_reader,
            b_reader,
            pending_reader,
        ));
        assert_eq!(connections.memory.available(), 80);
        assert!(lock(&connections.counts).unwrap().tenants.is_empty());
    }

    #[test]
    fn preauth_binding_overload_does_not_register_or_lose_global_ownership() {
        let connections =
            Connections::new(reader_limits(128, 32, 64), Arc::new(Metrics::default()));
        let mut lease = connections
            .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
            .unwrap();
        let mut reader = lease.read_budget();
        let allocation = reader.reserve_allocation(32).unwrap();
        assert!(matches!(
            lease.authenticate(&key("t", "d")),
            Err(Error::Overloaded)
        ));
        let counts = lock(&connections.counts).unwrap();
        assert!(counts.tenants.is_empty());
        assert!(counts.devices.is_empty());
        drop(counts);
        assert_eq!(connections.memory.available(), 80);
        assert_eq!(reader.capacity().unwrap(), 32);
        drop(allocation);
        lease.authenticate(&key("t", "d")).unwrap();
        reader.authenticate(&key("t", "d")).unwrap();
        let allocated = reader.reserve_allocation(16).unwrap();
        assert!(matches!(
            reader.reserve_allocation(1),
            Err(Error::Overloaded)
        ));
        drop((allocated, reader, lease));
        assert_eq!(connections.memory.available(), 128);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_reader_reservations_cannot_exceed_tenant_bytes() {
        let connections =
            Connections::new(reader_limits(256, 64, 64), Arc::new(Metrics::default()));
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = tokio::task::JoinSet::new();
        for device in ["a", "b"] {
            let mut lease = connections
                .acquire("127.0.0.1".parse().unwrap(), Transport::Mqtt)
                .unwrap();
            lease.authenticate(&key("t", device)).unwrap();
            let mut reader = lease.read_budget();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                let allocation = reader.reserve_allocation(32);
                (lease, reader, allocation)
            });
        }
        let mut owners = Vec::new();
        while let Some(owner) = tasks.join_next().await {
            owners.push(owner.unwrap());
        }
        assert_eq!(
            owners
                .iter()
                .filter(|(_, _, allocation)| allocation.is_ok())
                .count(),
            1
        );
        assert_eq!(connections.memory.available(), 192);
        assert_eq!(
            lock(&connections.counts).unwrap().tenants[&TenantId::new("t").unwrap()].read_bytes,
            32
        );
        drop(owners);
        assert_eq!(connections.memory.available(), 256);
        assert!(lock(&connections.counts).unwrap().tenants.is_empty());
    }

    #[test]
    #[ignore = "reports inline ownership sizes; no connection-capacity claim"]
    fn connection_ownership_sizes() {
        println!(
            "connection_ownership_sizes connection_lease={} pending_connection_lease={} read_budget={} read_state={} read_state_mutex={} read_allocation_permit={} bytes_permit={}",
            std::mem::size_of::<ConnectionLease>(),
            std::mem::size_of::<PendingConnectionLease>(),
            std::mem::size_of::<ConnectionReadBudget>(),
            std::mem::size_of::<ReadAllocationState>(),
            std::mem::size_of::<Mutex<ReadAllocationState>>(),
            std::mem::size_of::<ReadAllocationPermit>(),
            std::mem::size_of::<BytesPermit>(),
        );
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
    #[test]
    fn rate_limits_charge_global_even_when_ip_or_capacity_rejects() {
        let limiter = RateLimiter::new(Arc::new(Limits {
            rate_entries: 1,
            requests_per_second: 3,
            requests_per_ip_second: 1,
            ..Limits::default()
        }));
        let ip = IpAddr::from([10, 0, 0, 1]);
        limiter.take(ip).unwrap();
        assert!(matches!(limiter.take(ip), Err(Error::Overloaded)));
        assert!(matches!(
            limiter.take(IpAddr::from([10, 0, 0, 2])),
            Err(Error::Overloaded)
        ));
        let mut inner = limiter.inner.lock().unwrap();
        assert_eq!(inner.0.count, 3);
        inner.1.get_mut(&ip).unwrap().start = Instant::now() - Duration::from_secs(2);
        drop(inner);
        assert!(matches!(limiter.take(ip), Err(Error::Overloaded)));
    }

    #[test]
    fn rate_table_reclaims_expired_capacity_and_existing_windows_reset() {
        let limiter = RateLimiter::new(Arc::new(Limits {
            rate_entries: 1024,
            requests_per_second: 10000,
            requests_per_ip_second: 2,
            ..Limits::default()
        }));
        let first = IpAddr::from([10, 0, 0, 0]);
        for n in 0..1024 {
            limiter
                .take(IpAddr::from([10, 0, (n / 256) as u8, (n % 256) as u8]))
                .unwrap();
        }
        limiter.take(first).unwrap();
        assert!(matches!(limiter.take(first), Err(Error::Overloaded)));
        let new = IpAddr::from([11, 0, 0, 0]);
        assert!(matches!(limiter.take(new), Err(Error::Overloaded)));
        {
            let mut inner = limiter.inner.lock().unwrap();
            inner.1.get_mut(&first).unwrap().start = Instant::now() - Duration::from_secs(2);
        }
        limiter.take(first).unwrap();
        limiter.take(first).unwrap();
        assert!(matches!(limiter.take(first), Err(Error::Overloaded)));
        {
            let mut inner = limiter.inner.lock().unwrap();
            inner.1.get_mut(&first).unwrap().start = Instant::now() - Duration::from_secs(2);
        }
        limiter.take(new).unwrap();
        let inner = limiter.inner.lock().unwrap();
        assert_eq!(inner.1.len(), 1024);
        assert!(!inner.1.contains_key(&first));
        assert!(inner.1.contains_key(&new));
    }

    #[test]
    #[ignore = "serial release benchmark"]
    fn rate_table_scaling() {
        use crate::hotspot_bench::measure;
        for count in [1, 64, 256, 1024] {
            let limiter = RateLimiter::new(Arc::new(Limits {
                rate_entries: count,
                requests_per_second: usize::MAX,
                requests_per_ip_second: usize::MAX,
                ..Limits::default()
            }));
            let target = IpAddr::from([10, 0, 0, 0]);
            {
                let mut inner = limiter.inner.lock().unwrap();
                for i in 0..count {
                    let ip = IpAddr::from([10, 0, (i / 256) as u8, (i % 256) as u8]);
                    inner.1.insert(
                        ip,
                        Window {
                            start: Instant::now(),
                            count: 0,
                        },
                    );
                }
            }
            measure(
                "rate_existing",
                count,
                2000,
                || (),
                |_| limiter.take(target).unwrap(),
                |_| {},
            );
        }
    }
}
