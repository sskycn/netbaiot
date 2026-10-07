use crate::{Error, EventBusProbe, Histogram, Limits, Metric, Metrics, Result, lock, now_ms};
use async_trait::async_trait;
use futures_util::FutureExt;
use netbaiot_core::{DeviceEvent, EventAccepted, EventId, RouteDefinition, SinkId, TenantId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkDeliveryMode {
    ConfirmedRequired,
    BestEffort,
}

#[derive(Clone, Debug)]
pub struct DeliveryEnvelope {
    pub event: Arc<DeviceEvent>,
    pub sink_id: SinkId,
    pub attempt: u32,
    pub accepted_at: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct SinkAck;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SinkError {
    Retryable,
    Permanent,
}

#[async_trait]
pub trait EventSink: Send + Sync {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError>;
}

#[derive(Clone)]
pub struct SinkDefinition {
    pub id: SinkId,
    pub mode: SinkDeliveryMode,
    pub sink: Arc<dyn EventSink>,
    pub max_count: usize,
    pub max_bytes: usize,
    pub concurrency: usize,
    pub timeout: Duration,
    pub max_attempts: u32,
    pub max_age: Duration,
}

impl SinkDefinition {
    pub fn bounded(
        id: SinkId,
        mode: SinkDeliveryMode,
        sink: Arc<dyn EventSink>,
        limits: &Limits,
    ) -> Self {
        Self {
            id,
            mode,
            sink,
            max_count: limits.sink_queue_max_count,
            max_bytes: limits.sink_queue_max_bytes,
            concurrency: limits.sink_delivery_concurrency,
            timeout: Duration::from_millis(limits.sink_timeout_ms),
            max_attempts: limits.sink_max_attempts,
            max_age: Duration::from_millis(limits.sink_max_age_ms),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpoolRecord {
    pub event: DeviceEvent,
    pub pending_sinks: Vec<SinkId>,
    pub routing_revision: u64,
    pub accepted_at: i64,
    pub attempts: BTreeMap<SinkId, u32>,
}

impl SpoolRecord {
    /// JSON payload size without another payload-sized allocation.
    pub fn encoded_len(&self) -> Result<usize> {
        bounded_json_bytes(self, usize::MAX)
    }
}

pub type EventAcceptance = EventAccepted;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventBusUsage {
    pub events: usize,
    pub bytes: usize,
    pub pending_required: usize,
}

#[derive(Clone)]
struct DeliveryRecord {
    event: Arc<DeviceEvent>,
    bytes: usize,
    accepted_at: i64,
    attempt: u32,
    next_attempt: Instant,
}

struct SinkState {
    definition: SinkDefinition,
    ready: VecDeque<DeliveryRecord>,
    delayed: BTreeMap<Instant, VecDeque<DeliveryRecord>>,
    used_count: usize,
    used_bytes: usize,
    inflight: usize,
    notify: Arc<tokio::sync::Notify>,
}

struct ActiveEvent {
    event: Arc<DeviceEvent>,
    bytes: usize,
    remaining: BTreeSet<SinkId>,
    required: BTreeSet<SinkId>,
    attempts: BTreeMap<SinkId, u32>,
    accepted_at: i64,
    routing_revision: u64,
}

struct State {
    sinks: BTreeMap<SinkId, SinkState>,
    routes: CompiledRoutes,
    routing_revision: u64,
    active: HashMap<EventId, ActiveEvent>,
    active_bytes: usize,
    accepting: bool,
}

/// Immutable, sorted effective fanout. The slices also own safe snapshots across
/// replacement; publishing never builds or deduplicates a routing set.
struct CompiledRoutes {
    global: Arc<[SinkId]>,
    tenants: HashMap<TenantId, Arc<[SinkId]>>,
}

impl CompiledRoutes {
    fn for_tenant(&self, tenant: &TenantId) -> &Arc<[SinkId]> {
        self.tenants.get(tenant).unwrap_or(&self.global)
    }
}

// Field drop order releases the state mutex before updating probe histograms.
struct ProbedState<'a> {
    guard: std::sync::MutexGuard<'a, State>,
    _timing: StateTiming<'a>,
}
impl std::ops::Deref for ProbedState<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        &self.guard
    }
}
impl std::ops::DerefMut for ProbedState<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.guard
    }
}
struct StateTiming<'a> {
    metrics: &'a Metrics,
    site: EventBusProbe,
    wait: Option<Duration>,
    held: Option<Instant>,
    dequeue: Option<(usize, usize, u64)>,
}
impl Drop for StateTiming<'_> {
    fn drop(&mut self) {
        if let (Some(wait), Some(held)) = (self.wait, self.held) {
            // Includes mutex release, excludes histogram recording.
            let hold = held.elapsed();
            self.metrics
                .observe(Histogram::EventBusStateWait, wait.as_micros() as u64);
            self.metrics
                .observe(Histogram::EventBusStateHold, hold.as_micros() as u64);
            self.metrics.event_bus_state_timing(
                self.site,
                wait.as_nanos() as u64,
                hold.as_nanos() as u64,
            );
            if let Some((queue_len, records, selection_ns)) = self.dequeue {
                self.metrics
                    .event_bus_dequeue(queue_len, records, selection_ns);
            }
        }
    }
}

pub struct EventBus {
    limits: Arc<Limits>,
    metrics: Arc<Metrics>,
    state: Mutex<State>,
    changed: tokio::sync::Notify,
    stop: CancellationToken,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// Composition owner: normal shutdown joins workers; Drop cancels and aborts as
/// the fallback when the startup/run future itself is dropped.
pub struct EventBusWorkers {
    bus: Arc<EventBus>,
}
impl EventBusWorkers {
    pub async fn shutdown(&self) -> Result<()> {
        self.bus.stop_workers().await
    }
}
impl Drop for EventBusWorkers {
    fn drop(&mut self) {
        self.bus.stop.cancel();
        let mut workers = match self.bus.workers.lock() {
            Ok(workers) => workers,
            Err(poisoned) => poisoned.into_inner(),
        };
        for worker in workers.drain(..) {
            worker.abort();
        }
    }
}

impl EventBus {
    fn lock_state(&self, operation: EventBusProbe) -> Result<ProbedState<'_>> {
        let started = self.metrics.lock_timing_enabled().then(Instant::now);
        let guard = lock(&self.state)?;
        let wait = started.map(|t| t.elapsed());
        let held = started.map(|_| Instant::now());
        self.metrics.event_bus_probe(operation);
        Ok(ProbedState {
            guard,
            _timing: StateTiming {
                metrics: &self.metrics,
                site: operation,
                wait,
                held,
                dequeue: None,
            },
        })
    }

    pub fn new(
        limits: Arc<Limits>,
        metrics: Arc<Metrics>,
        definitions: Vec<SinkDefinition>,
        routes: Vec<RouteDefinition>,
        routing_revision: u64,
    ) -> Result<Arc<Self>> {
        let bus = Self::new_paused(limits, metrics, definitions, routes, routing_revision)?;
        bus.start_workers()?;
        Ok(bus)
    }

    /// Prepare/restore without delivering anything before startup has succeeded.
    pub fn new_paused(
        limits: Arc<Limits>,
        metrics: Arc<Metrics>,
        definitions: Vec<SinkDefinition>,
        routes: Vec<RouteDefinition>,
        routing_revision: u64,
    ) -> Result<Arc<Self>> {
        if definitions.is_empty() || definitions.len() > limits.max_sinks {
            return Err(Error::Configuration);
        }
        let mut sinks = BTreeMap::new();
        for definition in definitions {
            if definition.max_count == 0
                || definition.max_bytes == 0
                || definition.concurrency == 0
                || definition.concurrency > definition.max_count
                || definition.max_attempts == 0
                || sinks.contains_key(&definition.id)
            {
                return Err(Error::Configuration);
            }
            sinks.insert(
                definition.id.clone(),
                SinkState {
                    definition,
                    ready: VecDeque::new(),
                    delayed: BTreeMap::new(),
                    used_count: 0,
                    used_bytes: 0,
                    inflight: 0,
                    notify: Arc::new(tokio::sync::Notify::new()),
                },
            );
        }
        let routes = compile_routes(&routes, &sinks, &limits)?;
        let bus = Arc::new(Self {
            limits,
            metrics,
            state: Mutex::new(State {
                sinks,
                routes,
                routing_revision,
                active: HashMap::new(),
                active_bytes: 0,
                accepting: true,
            }),
            changed: tokio::sync::Notify::new(),
            stop: CancellationToken::new(),
            workers: Mutex::new(Vec::new()),
        });
        Ok(bus)
    }

    pub fn start_owned_workers(self: &Arc<Self>) -> Result<EventBusWorkers> {
        self.start_workers()?;
        Ok(EventBusWorkers { bus: self.clone() })
    }

    fn start_workers(self: &Arc<Self>) -> Result<()> {
        let ids = self
            .lock_state(EventBusProbe::Other)?
            .sinks
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut workers = lock(&self.workers)?;
        if self.stop.is_cancelled() {
            return Err(Error::Draining);
        }
        if !workers.is_empty() {
            return Err(Error::Conflict);
        }
        for id in ids {
            let bus = self.clone();
            workers.push(tokio::spawn(async move { bus.run_sink(id).await }));
        }
        Ok(())
    }

    pub fn replace_routes(&self, revision: u64, routes: Vec<RouteDefinition>) -> Result<()> {
        let mut state = self.lock_state(EventBusProbe::Other)?;
        let routes = compile_routes(&routes, &state.sinks, &self.limits)?;
        if revision <= state.routing_revision {
            return Err(Error::Conflict);
        }
        state.routes = routes;
        state.routing_revision = revision;
        Ok(())
    }

    pub fn validate_route_update(&self, revision: u64, routes: &[RouteDefinition]) -> Result<()> {
        let state = self.lock_state(EventBusProbe::Other)?;
        if revision <= state.routing_revision {
            return Err(Error::Conflict);
        }
        compile_routes(routes, &state.sinks, &self.limits).map(|_| ())
    }

    pub fn close_admission(&self) -> Result<()> {
        self.lock_state(EventBusProbe::Other)?.accepting = false;
        Ok(())
    }

    pub fn usage(&self) -> Result<EventBusUsage> {
        let state = self.lock_state(EventBusProbe::Other)?;
        Ok(EventBusUsage {
            events: state.active.len(),
            bytes: state.active_bytes,
            pending_required: state
                .active
                .values()
                .map(|event| event.required.len())
                .sum(),
        })
    }

    pub fn publish(&self, event: DeviceEvent) -> Result<EventAcceptance> {
        let bytes = bounded_json_bytes(&event, self.limits.global_event_max_bytes)?;
        let event = Arc::new(event);
        let lock_started = self.metrics.lock_timing_enabled().then(Instant::now);
        let mut state = self.lock_state(EventBusProbe::Publish)?;
        let lock_wait_us = lock_started.map(|started| started.elapsed().as_micros() as u64);
        let hold_started = lock_started.map(|_| Instant::now());
        if !state.accepting {
            return Err(Error::Draining);
        }
        // The active map owns delivery responsibility, not permanent history.
        // Fence duplicates under the same mutex before any fanout admission.
        if state.active.contains_key(&event.event_id) {
            return Err(Error::Conflict);
        }
        // Split borrows of immutable routing and mutable sink/accounting fields.
        // Only callers retaining a snapshot need to increment its Arc count.
        let state_ref = &mut *state.guard;
        let targets = state_ref.routes.for_tenant(&event.device.tenant_id);
        if targets.is_empty() {
            return Err(Error::Unavailable);
        }
        if state_ref.active.len() >= self.limits.global_event_max_count
            || state_ref.active_bytes.saturating_add(bytes) > self.limits.global_event_max_bytes
        {
            return Err(Error::Overloaded);
        }
        // Required capacity is checked for every sink before the first mutation.
        for id in targets.iter() {
            let sink = state_ref.sinks.get(id).ok_or(Error::Configuration)?;
            if sink.definition.mode == SinkDeliveryMode::ConfirmedRequired
                && (sink.used_count >= sink.definition.max_count
                    || sink.used_bytes.saturating_add(bytes) > sink.definition.max_bytes)
            {
                return Err(Error::Overloaded);
            }
        }
        let now = now_ms();
        let mut remaining = BTreeSet::new();
        let mut required = BTreeSet::new();
        let mut accepted_notifies = Vec::new();
        for id in targets.iter() {
            let sink = state_ref.sinks.get_mut(id).ok_or(Error::Configuration)?;
            let available = sink.used_count < sink.definition.max_count
                && sink.used_bytes.saturating_add(bytes) <= sink.definition.max_bytes;
            if !available {
                self.metrics.inc(Metric::SinkDrops);
                continue;
            }
            sink.used_count += 1;
            sink.used_bytes += bytes;
            sink.ready.push_back(DeliveryRecord {
                event: event.clone(),
                bytes,
                accepted_at: now,
                attempt: 0,
                next_attempt: Instant::now(),
            });
            remaining.insert(id.clone());
            if sink.definition.mode == SinkDeliveryMode::ConfirmedRequired {
                required.insert(id.clone());
            }
            accepted_notifies.push(sink.notify.clone());
        }
        let required_deliveries = required.len();
        let best_effort_deliveries = remaining.len().saturating_sub(required_deliveries);
        if remaining.is_empty() {
            return Err(Error::Overloaded);
        }
        let event_id = event.event_id;
        let revision = state_ref.routing_revision;
        state_ref.active_bytes += bytes;
        state_ref.active.insert(
            event_id,
            ActiveEvent {
                event,
                bytes,
                remaining,
                required,
                attempts: BTreeMap::new(),
                accepted_at: now,
                routing_revision: revision,
            },
        );
        let lock_hold_us = hold_started.map(|started| started.elapsed().as_micros() as u64);
        drop(state);
        if let (Some(wait), Some(hold)) = (lock_wait_us, lock_hold_us) {
            self.metrics.observe(Histogram::EventBusLockWait, wait);
            self.metrics.observe(Histogram::EventBusLockHold, hold);
        }
        for notify in accepted_notifies {
            self.metrics.event_bus_probe(EventBusProbe::NotifyWorker);
            notify.notify_one();
        }
        self.metrics.inc(Metric::EventsAccepted);
        self.metrics.add(Metric::EventBytes, bytes as u64);
        Ok(EventAcceptance {
            event_id,
            accepted_at: now,
            required_deliveries,
            best_effort_deliveries,
        })
    }

    pub fn restore(&self, records: Vec<SpoolRecord>) -> Result<usize> {
        if records.len() > self.limits.global_event_max_count {
            return Err(Error::Overloaded);
        }
        // Move payload ownership once; preflight retains only sizes and sink projections.
        let prepared = records
            .into_iter()
            .map(|record| {
                let bytes = bounded_json_bytes(&record.event, self.limits.global_event_max_bytes)?;
                Ok((record, bytes))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut state = self.lock_state(EventBusProbe::Other)?;
        let mut ids = BTreeSet::new();
        let mut total_bytes = state.active_bytes;
        let total_count = state
            .active
            .len()
            .checked_add(prepared.len())
            .ok_or(Error::Overloaded)?;
        if total_count > self.limits.global_event_max_count {
            return Err(Error::Overloaded);
        }
        let mut projections = BTreeMap::<SinkId, (usize, usize)>::new();
        for (record, bytes) in &prepared {
            if record.pending_sinks.is_empty() || record.accepted_at < 0 {
                return Err(Error::Invalid);
            }
            if record.pending_sinks.len() > self.limits.max_fanout_per_event
                || record.attempts.len() > self.limits.max_sinks
            {
                return Err(Error::Overloaded);
            }
            if !ids.insert(record.event.event_id)
                || state.active.contains_key(&record.event.event_id)
            {
                return Err(Error::Conflict);
            }
            total_bytes = total_bytes.checked_add(*bytes).ok_or(Error::Overloaded)?;
            if total_bytes > self.limits.global_event_max_bytes {
                return Err(Error::Overloaded);
            }
            let mut pending = BTreeSet::new();
            for id in &record.pending_sinks {
                if !pending.insert(id) {
                    return Err(Error::Invalid);
                }
                let sink = state.sinks.get(id).ok_or(Error::Configuration)?;
                if sink.definition.mode != SinkDeliveryMode::ConfirmedRequired {
                    return Err(Error::Invalid);
                }
                let projection = projections
                    .entry(id.clone())
                    .or_insert((sink.used_count, sink.used_bytes));
                projection.0 = projection.0.checked_add(1).ok_or(Error::Overloaded)?;
                projection.1 = projection.1.checked_add(*bytes).ok_or(Error::Overloaded)?;
                if projection.0 > sink.definition.max_count
                    || projection.1 > sink.definition.max_bytes
                {
                    return Err(Error::Overloaded);
                }
            }
            // Historical attempts may include completed/removed sinks; revision
            // need not match the current routes. u32 attempts may be saturated.
        }
        let restored = prepared.len();
        for (record, bytes) in prepared {
            tracing::debug!(event_id=%record.event.event_id, attempts=?record.attempts,
                routing_revision=record.routing_revision, "restored required event diagnostic");
            let event = Arc::new(record.event);
            let pending = record.pending_sinks.into_iter().collect::<BTreeSet<_>>();
            for id in &pending {
                // All lookups and arithmetic were validated with this mutex held.
                if let Some(sink) = state.sinks.get_mut(id) {
                    sink.used_count += 1;
                    sink.used_bytes += bytes;
                    sink.ready.push_back(DeliveryRecord {
                        event: event.clone(),
                        bytes,
                        accepted_at: record.accepted_at,
                        attempt: *record.attempts.get(id).unwrap_or(&0),
                        next_attempt: Instant::now(),
                    });
                }
            }
            state.active.insert(
                event.event_id,
                ActiveEvent {
                    event,
                    bytes,
                    remaining: pending.clone(),
                    required: pending,
                    attempts: record.attempts,
                    accepted_at: record.accepted_at,
                    routing_revision: record.routing_revision,
                },
            );
        }
        state.active_bytes = total_bytes;
        let notifies = projections
            .keys()
            .filter_map(|id| state.sinks.get(id).map(|sink| sink.notify.clone()))
            .collect::<Vec<_>>();
        drop(state);
        for notify in notifies {
            self.metrics.event_bus_probe(EventBusProbe::NotifyWorker);
            notify.notify_one();
        }
        Ok(restored)
    }

    pub fn spool_records(&self) -> Result<Vec<SpoolRecord>> {
        let state = self.lock_state(EventBusProbe::Other)?;
        let mut records = Vec::new();
        for active in state.active.values() {
            if !active.required.is_empty() {
                records.push(SpoolRecord {
                    event: (*active.event).clone(),
                    pending_sinks: active.required.iter().cloned().collect(),
                    routing_revision: active.routing_revision,
                    accepted_at: active.accepted_at,
                    attempts: active.attempts.clone(),
                });
            }
        }
        records.sort_by_key(|record| record.event.event_id.0);
        Ok(records)
    }

    pub async fn wait_required_drained(&self, timeout: Duration) -> Result<bool> {
        match tokio::time::timeout(timeout, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                if self.usage()?.pending_required == 0 {
                    return Ok::<(), Error>(());
                }
                notified.await;
            }
        })
        .await
        {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        }
    }

    pub async fn stop_workers(&self) -> Result<()> {
        self.stop.cancel();
        let handles = {
            let mut workers = lock(&self.workers)?;
            std::mem::take(&mut *workers)
        };
        for handle in handles {
            let _ = handle.await;
        }
        Ok(())
    }

    async fn run_sink(self: Arc<Self>, id: SinkId) {
        let (definition, notify) = match self.lock_state(EventBusProbe::Other) {
            Ok(state) => match state.sinks.get(&id) {
                Some(sink) => (sink.definition.clone(), sink.notify.clone()),
                None => {
                    tracing::error!(sink_id=%id, "EventBus sink worker lost its definition");
                    return;
                }
            },
            Err(error) => {
                tracing::error!(%error, sink_id=%id, "EventBus sink worker could not read state");
                return;
            }
        };
        let mut inflight = JoinSet::new();
        let mut woke = false;
        loop {
            if self.stop.is_cancelled() {
                inflight.abort_all();
                while inflight.join_next().await.is_some() {}
                break;
            }
            let mut took_work = false;
            while inflight.len() < definition.concurrency {
                let next = self.take_ready(&id);
                let record = match next {
                    Ok(Some(record)) => record,
                    Ok(None) => break,
                    Err(error) => {
                        tracing::error!(%error, sink_id=%id, "EventBus failed to take ready delivery");
                        break;
                    }
                };
                took_work = true;
                let sink = definition.sink.clone();
                let sink_id = id.clone();
                let timeout = definition.timeout;
                inflight.spawn(async move {
                    let envelope = DeliveryEnvelope {
                        event: record.event.clone(),
                        sink_id,
                        attempt: record.attempt.saturating_add(1),
                        accepted_at: record.accepted_at,
                    };
                    let result = std::panic::AssertUnwindSafe(tokio::time::timeout(
                        timeout,
                        sink.deliver(envelope),
                    ))
                    .catch_unwind()
                    .await;
                    let result = match result {
                        Ok(Ok(result)) => result,
                        Ok(Err(_)) | Err(_) => Err(SinkError::Retryable),
                    };
                    (record, result)
                });
            }
            if woke && !took_work {
                self.metrics.event_bus_probe(EventBusProbe::EmptyWake);
            }
            woke = true;
            if self.stop.is_cancelled() {
                inflight.abort_all();
                while inflight.join_next().await.is_some() {}
                break;
            }
            if inflight.len() >= definition.concurrency {
                // No queue slot can be consumed until an in-flight delivery completes. A ready
                // queued record would otherwise produce a zero-duration timer and spin this task.
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => continue,
                    completed = inflight.join_next() => {
                        self.metrics.event_bus_probe(EventBusProbe::WakeJoin);
                        match completed {
                            Some(Ok((record, result))) => {
                                if let Err(error) = self.complete(&id, record, result, &definition) {
                                    tracing::error!(%error, sink_id=%id, "EventBus failed to complete delivery");
                                }
                            }
                            Some(Err(error)) => tracing::error!(%error, sink_id=%id, "EventBus delivery task failed"),
                            None => {}
                        }
                    }
                }
                continue;
            }
            if inflight.is_empty() {
                let delay = match self.next_ready_delay(&id) {
                    Ok(Some(delay)) => delay,
                    Ok(None) => Duration::from_secs(3_600),
                    Err(error) => {
                        tracing::error!(%error, sink_id=%id, "EventBus failed to schedule next delivery");
                        Duration::from_secs(3_600)
                    }
                };
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => continue,
                    _ = notify.notified() => { self.metrics.event_bus_probe(EventBusProbe::WakeNotify); },
                    _ = tokio::time::sleep(delay) => { self.metrics.event_bus_probe(EventBusProbe::WakeTimer); },
                }
            } else {
                let delay = match self.next_ready_delay(&id) {
                    Ok(Some(delay)) => delay,
                    Ok(None) => Duration::from_secs(3_600),
                    Err(error) => {
                        tracing::error!(%error, sink_id=%id, "EventBus failed to schedule next delivery");
                        Duration::from_secs(3_600)
                    }
                };
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => continue,
                    completed = inflight.join_next() => {
                        self.metrics.event_bus_probe(EventBusProbe::WakeJoin);
                        match completed {
                            Some(Ok((record, result))) => {
                                if let Err(error) = self.complete(&id, record, result, &definition) {
                                    tracing::error!(%error, sink_id=%id, "EventBus failed to complete delivery");
                                }
                            }
                            Some(Err(error)) => tracing::error!(%error, sink_id=%id, "EventBus delivery task failed"),
                            None => {}
                        }
                    }
                    _ = notify.notified() => { self.metrics.event_bus_probe(EventBusProbe::WakeNotify); },
                    _ = tokio::time::sleep(delay) => { self.metrics.event_bus_probe(EventBusProbe::WakeTimer); },
                }
            }
        }
    }

    fn take_ready(&self, id: &SinkId) -> Result<Option<DeliveryRecord>> {
        let mut state = self.lock_state(EventBusProbe::TakeReady)?;
        let sink = state.sinks.get_mut(id).ok_or(Error::Internal)?;
        let queue_len = sink.used_count.saturating_sub(sink.inflight);
        let now = Instant::now();
        let selection_started = self.metrics.lock_timing_enabled().then(Instant::now);
        // Ready work remains FIFO. Due retries join the tail; later admissions
        // cannot overtake them. Both queues remain covered by the same quotas.
        while sink
            .delayed
            .first_key_value()
            .is_some_and(|(deadline, _)| *deadline <= now)
        {
            let Some((_, mut records)) = sink.delayed.pop_first() else {
                break;
            };
            sink.ready.append(&mut records);
        }
        let record = sink.ready.pop_front();
        if let Some(record) = &record {
            sink.inflight += 1;
            tracing::debug!(sink_id=%id, event_id=%record.event.event_id,
                "ready delivery diagnostic");
        }
        let selection_ns = selection_started.map(|started| started.elapsed().as_nanos() as u64);
        if let Some(selection_ns) = selection_ns {
            state._timing.dequeue = Some((queue_len, usize::from(record.is_some()), selection_ns));
        }
        Ok(record)
    }

    fn next_ready_delay(&self, id: &SinkId) -> Result<Option<Duration>> {
        let state = self.lock_state(EventBusProbe::NextDelay)?;
        let sink = state.sinks.get(id).ok_or(Error::Internal)?;
        let now = Instant::now();
        if !sink.ready.is_empty() {
            return Ok(Some(Duration::ZERO));
        }
        Ok(sink
            .delayed
            .first_key_value()
            .map(|(deadline, _)| deadline.saturating_duration_since(now)))
    }

    fn complete(
        &self,
        id: &SinkId,
        mut record: DeliveryRecord,
        result: std::result::Result<SinkAck, SinkError>,
        definition: &SinkDefinition,
    ) -> Result<()> {
        let mut state = self.lock_state(EventBusProbe::Complete)?;
        {
            let sink = state.sinks.get_mut(id).ok_or(Error::Internal)?;
            sink.inflight = sink.inflight.saturating_sub(1);
        }
        record.attempt = record.attempt.saturating_add(1);
        tracing::debug!(sink_id=%id, event_id=%record.event.event_id, attempt=record.attempt,
            ?result, "delivery completion diagnostic");
        if let Some(active) = state.active.get_mut(&record.event.event_id) {
            active.attempts.insert(id.clone(), record.attempt);
        }
        let age = now_ms().saturating_sub(record.accepted_at);
        let retry = matches!(result, Err(SinkError::Retryable))
            && record.attempt < definition.max_attempts
            && age < i64::try_from(definition.max_age.as_millis()).unwrap_or(i64::MAX);
        if retry {
            let exponent = record.attempt.min(30);
            let cap = self
                .limits
                .retry_base_ms
                .saturating_mul(1u64.checked_shl(exponent).unwrap_or(u64::MAX))
                .min(self.limits.retry_max_ms);
            let seed = record.event.event_id.0.as_u128() as u64 ^ u64::from(record.attempt);
            record.next_attempt = Instant::now() + Duration::from_millis(1 + seed % cap.max(1));
            let sink = state.sinks.get_mut(id).ok_or(Error::Internal)?;
            sink.delayed
                .entry(record.next_attempt)
                .or_default()
                .push_back(record);
            self.metrics.event_bus_probe(EventBusProbe::NotifyWorker);
            sink.notify.notify_one();
            self.metrics.inc(Metric::SinkRetries);
            return Ok(());
        }
        let required = definition.mode == SinkDeliveryMode::ConfirmedRequired;
        if result.is_err() && required {
            record.next_attempt =
                Instant::now() + Duration::from_millis(self.limits.retry_max_ms.max(1));
            let sink = state.sinks.get_mut(id).ok_or(Error::Internal)?;
            sink.delayed
                .entry(record.next_attempt)
                .or_default()
                .push_back(record);
            self.metrics.event_bus_probe(EventBusProbe::NotifyWorker);
            sink.notify.notify_one();
            self.metrics.inc(Metric::SinkFailures);
            return Ok(());
        }
        {
            let sink = state.sinks.get_mut(id).ok_or(Error::Internal)?;
            sink.used_count = sink.used_count.saturating_sub(1);
            sink.used_bytes = sink.used_bytes.saturating_sub(record.bytes);
        }
        if result.is_ok() {
            self.metrics.inc(Metric::SinkAcks);
            self.metrics.observe(
                Histogram::EventAcceptedToSinkAck,
                u64::try_from(age).unwrap_or(u64::MAX).saturating_mul(1_000),
            );
        } else {
            self.metrics.inc(Metric::SinkDrops);
        }
        let mut remove_event = false;
        if let Some(active) = state.active.get_mut(&record.event.event_id) {
            active.remaining.remove(id);
            active.required.remove(id);
            remove_event = active.remaining.is_empty();
        }
        if remove_event && let Some(active) = state.active.remove(&record.event.event_id) {
            state.active_bytes = state.active_bytes.saturating_sub(active.bytes);
        }
        drop(state);
        self.metrics.event_bus_probe(EventBusProbe::NotifyDrain);
        self.changed.notify_waiters();
        Ok(())
    }
}

pub(crate) fn bounded_json_bytes(event: &impl Serialize, maximum: usize) -> Result<usize> {
    // Count JSON without allocating another event-sized buffer just to admit it.
    struct Size {
        bytes: usize,
        maximum: usize,
        overloaded: bool,
    }
    impl std::io::Write for Size {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let Some(size) = self
                .bytes
                .checked_add(bytes.len())
                .filter(|size| *size <= self.maximum)
            else {
                self.overloaded = true;
                return Err(std::io::ErrorKind::OutOfMemory.into());
            };
            self.bytes = size;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut size = Size {
        bytes: 0,
        maximum,
        overloaded: false,
    };
    serde_json::to_writer(&mut size, event).map_err(|_| {
        if size.overloaded {
            Error::Overloaded
        } else {
            Error::Invalid
        }
    })?;
    Ok(size.bytes)
}

fn compile_routes(
    routes: &[RouteDefinition],
    sinks: &BTreeMap<SinkId, SinkState>,
    limits: &Limits,
) -> Result<CompiledRoutes> {
    if routes.is_empty() || routes.len() > limits.max_routing_filters {
        return Err(Error::Configuration);
    }
    let mut global = BTreeSet::new();
    let mut tenants = HashMap::<TenantId, BTreeSet<SinkId>>::new();
    for route in routes {
        if route.sinks.len() > limits.max_fanout_per_event
            || route.sinks.len() > limits.max_sinks_per_tenant
        {
            return Err(Error::Configuration);
        }
        let unique = route.sinks.iter().collect::<BTreeSet<_>>();
        if unique.is_empty()
            || unique.len() != route.sinks.len()
            || unique.len() > limits.max_fanout_per_event
            || unique.len() > limits.max_sinks_per_tenant
            || unique.iter().any(|id| !sinks.contains_key(*id))
        {
            return Err(Error::Configuration);
        }
        let target = match &route.tenant {
            Some(tenant) => tenants.entry(tenant.clone()).or_default(),
            None => &mut global,
        };
        target.extend(route.sinks.iter().cloned());
        if target.len() > limits.max_fanout_per_event || target.len() > limits.max_sinks_per_tenant
        {
            return Err(Error::Configuration);
        }
    }
    let tenants = tenants
        .into_iter()
        .map(|(tenant, mut effective)| {
            effective.extend(global.iter().cloned());
            if effective.len() > limits.max_fanout_per_event
                || effective.len() > limits.max_sinks_per_tenant
            {
                return Err(Error::Configuration);
            }
            Ok((tenant, Arc::from(effective.into_iter().collect::<Vec<_>>())))
        })
        .collect::<Result<_>>()?;
    Ok(CompiledRoutes {
        global: Arc::from(global.into_iter().collect::<Vec<_>>()),
        tenants,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Ack;
    #[async_trait]
    impl EventSink for Ack {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            Ok(SinkAck)
        }
    }

    #[tokio::test]
    async fn restore_before_worker_start_preserves_id_and_application_ack_ownership() {
        assert_restored_business_delivery(false).await;
    }

    #[tokio::test]
    async fn restore_wakes_parked_worker_before_business_sink_claim() {
        assert_restored_business_delivery(true).await;
    }

    async fn assert_restored_business_delivery(park_worker: bool) {
        let limits = Arc::new(Limits::default());
        let sink = crate::BusinessRpcEventSink::new();
        let id = SinkId::new("tcp-rpc").unwrap();
        let bus = EventBus::new_paused(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id.clone()],
            }],
            7,
        )
        .unwrap();
        let worker = bus.clone().run_sink(id.clone());
        tokio::pin!(worker);
        if park_worker {
            // Poll through the empty-queue check and into the notification wait.
            assert!(futures_util::poll!(worker.as_mut()).is_pending());
        }
        let event = event(8);
        let event_id = event.event_id;
        bus.restore(vec![SpoolRecord {
            event,
            pending_sinks: vec![id.clone()],
            routing_revision: 3,
            accepted_at: now_ms(),
            attempts: BTreeMap::from([(id, 2)]),
        }])
        .unwrap();
        // Own the restored delivery even when no business stream exists yet.
        assert!(futures_util::poll!(worker.as_mut()).is_pending());
        assert_eq!(bus.usage().unwrap().pending_required, 1);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let generation = sink.claim(sender, EventFilter::default()).unwrap();
        let consume = async {
            let request = receiver.recv().await.unwrap();
            assert_eq!(request.delivery.event.event_id, event_id);
            assert_eq!(request.delivery.attempt, 3);
            assert_eq!(bus.spool_records().unwrap()[0].routing_revision, 3);
            assert_eq!(bus.usage().unwrap().pending_required, 1);
            request.result.send(Ok(SinkAck)).unwrap();
            assert!(
                bus.wait_required_drained(Duration::from_secs(1))
                    .await
                    .unwrap()
            );
            sink.release(generation).unwrap();
            bus.stop.cancel();
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(worker, consume);
        })
        .await
        .unwrap();
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
    }

    struct Signal {
        calls: AtomicUsize,
        block: Option<Arc<tokio::sync::Notify>>,
    }

    struct FailFirst {
        calls: AtomicUsize,
        delivered: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl EventSink for FailFirst {
        async fn deliver(
            &self,
            delivery: DeliveryEnvelope,
        ) -> std::result::Result<SinkAck, SinkError> {
            let source = delivery.event.source_message_id.as_str().to_owned();
            self.delivered.lock().unwrap().push(source.clone());
            self.calls.fetch_add(1, Ordering::SeqCst);
            if source == "a" && delivery.attempt == 1 {
                Err(SinkError::Retryable)
            } else {
                Ok(SinkAck)
            }
        }
    }

    struct Recovering {
        fail: std::sync::atomic::AtomicBool,
        calls: AtomicUsize,
    }

    struct PanicOnce {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl EventSink for PanicOnce {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("test sink panic");
            }
            Ok(SinkAck)
        }
    }

    struct RetryBesideBlocked {
        delivered: Mutex<Vec<String>>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl EventSink for RetryBesideBlocked {
        async fn deliver(
            &self,
            delivery: DeliveryEnvelope,
        ) -> std::result::Result<SinkAck, SinkError> {
            let source = delivery.event.source_message_id.as_str().to_owned();
            self.delivered.lock().unwrap().push(source.clone());
            if source == "a" && delivery.attempt == 1 {
                return Err(SinkError::Retryable);
            }
            if source == "blocked" {
                self.release.notified().await;
            }
            Ok(SinkAck)
        }
    }

    #[async_trait]
    impl EventSink for Recovering {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                Err(SinkError::Retryable)
            } else {
                Ok(SinkAck)
            }
        }
    }
    #[async_trait]
    impl EventSink for Signal {
        async fn deliver(&self, _: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some(block) = &self.block {
                block.notified().await;
            }
            Ok(SinkAck)
        }
    }

    fn event(payload: usize) -> DeviceEvent {
        DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new("1").unwrap(),
            device: DeviceKey {
                tenant_id: TenantId::new("t").unwrap(),
                product_id: ProductId::new("p").unwrap(),
                device_id: DeviceId::new("d").unwrap(),
            },
            received_at: 1,
            occurred_at: None,
            kind: DeviceEventKind::DeviceEvent(DeviceEventPayload {
                name: "x".repeat(payload),
                value: None,
            }),
        }
    }

    fn named_event(source: &str) -> DeviceEvent {
        let mut event = event(1);
        event.source_message_id = SourceMessageId::new(source).unwrap();
        event
    }

    fn route(tenant: Option<&str>, sinks: &[&str]) -> RouteDefinition {
        RouteDefinition {
            tenant: tenant.map(|tenant| TenantId::new(tenant).unwrap()),
            sinks: sinks
                .iter()
                .map(|sink| SinkId::new(*sink).unwrap())
                .collect(),
        }
    }

    fn route_bus(limits: Limits, routes: Vec<RouteDefinition>) -> Result<Arc<EventBus>> {
        let limits = Arc::new(limits);
        let definitions = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
            .map(|id| {
                SinkDefinition::bounded(
                    SinkId::new(id).unwrap(),
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(Ack),
                    &limits,
                )
            })
            .into();
        EventBus::new_paused(limits, Arc::new(Metrics::default()), definitions, routes, 7)
    }

    #[test]
    fn effective_fanout_rejects_global_and_tenant_aggregate_overflow() {
        for limits in [
            Limits {
                max_fanout_per_event: 8,
                max_sinks_per_tenant: 9,
                ..Limits::default()
            },
            Limits {
                max_fanout_per_event: 9,
                max_sinks_per_tenant: 8,
                ..Limits::default()
            },
        ] {
            for routes in [
                vec![
                    route(None, &["a", "b", "c", "d", "e", "f", "g", "h"]),
                    route(Some("t"), &["i"]),
                ],
                vec![
                    route(None, &["a", "b", "c", "d", "e", "f", "g", "h"]),
                    route(None, &["i"]),
                ],
                vec![
                    route(Some("t"), &["a", "b", "c", "d", "e", "f", "g", "h"]),
                    route(Some("t"), &["i"]),
                ],
            ] {
                assert!(matches!(
                    route_bus(limits.clone(), routes),
                    Err(Error::Configuration)
                ));
            }
        }
    }

    #[test]
    fn effective_routes_deduplicate_sort_and_isolate_tenants() {
        let bus = route_bus(
            Limits {
                max_fanout_per_event: 4,
                max_sinks_per_tenant: 4,
                ..Limits::default()
            },
            vec![
                route(None, &["c", "a", "b"]),
                route(Some("t"), &["d", "c"]),
                route(Some("other"), &["e", "b"]),
            ],
        )
        .unwrap();
        for (tenant, expected) in [
            ("t", vec!["a", "b", "c", "d"]),
            ("other", vec!["a", "b", "c", "e"]),
            ("unlisted", vec!["a", "b", "c"]),
        ] {
            let mut event = event(8);
            event.device.tenant_id = TenantId::new(tenant).unwrap();
            assert_eq!(
                bus.publish(event).unwrap().required_deliveries,
                expected.len()
            );
            let state = bus.state.lock().unwrap();
            let targets = state.routes.for_tenant(&TenantId::new(tenant).unwrap());
            assert_eq!(
                targets.iter().map(SinkId::as_str).collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn rejected_route_replacement_preserves_revision_and_delivery() {
        let bus = route_bus(Limits::default(), vec![route(None, &["a"])]).unwrap();
        for routes in [
            vec![
                route(None, &["a", "b", "c", "d", "e", "f", "g", "h"]),
                route(Some("t"), &["i"]),
            ],
            vec![route(None, &["missing"])],
            vec![route(None, &["a", "a"])],
        ] {
            assert!(matches!(
                bus.validate_route_update(8, &routes),
                Err(Error::Configuration)
            ));
            assert!(matches!(
                bus.replace_routes(8, routes),
                Err(Error::Configuration)
            ));
            assert_eq!(bus.state.lock().unwrap().routing_revision, 7);
            let accepted = bus.publish(event(8)).unwrap();
            assert_eq!(accepted.required_deliveries, 1);
            let record = bus
                .spool_records()
                .unwrap()
                .into_iter()
                .find(|record| record.event.event_id == accepted.event_id)
                .unwrap();
            assert_eq!(record.routing_revision, 7);
            assert_eq!(record.pending_sinks, vec![SinkId::new("a").unwrap()]);
        }
        assert!(matches!(
            bus.replace_routes(7, vec![route(None, &["b"])]),
            Err(Error::Conflict)
        ));
        assert_eq!(bus.publish(event(8)).unwrap().required_deliveries, 1);
    }

    #[test]
    fn compiled_route_snapshot_survives_replacement() {
        let bus = route_bus(Limits::default(), vec![route(None, &["a", "b"])]).unwrap();
        let tenant = TenantId::new("t").unwrap();
        let old = bus.state.lock().unwrap().routes.for_tenant(&tenant).clone();
        bus.replace_routes(8, vec![route(None, &["c"])]).unwrap();
        assert_eq!(
            old.iter().map(SinkId::as_str).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        let new = bus.state.lock().unwrap().routes.for_tenant(&tenant).clone();
        assert_eq!(
            new.iter().map(SinkId::as_str).collect::<Vec<_>>(),
            vec!["c"]
        );
        assert_eq!(bus.publish(event(8)).unwrap().required_deliveries, 1);
    }

    #[test]
    #[ignore = "isolated serial release hotspot benchmark"]
    fn route_publish_scaling() {
        for count in [1, 64, 256, 1_024] {
            let limits = Arc::new(Limits {
                max_routing_filters: 1_024,
                ..Limits::default()
            });
            let id = SinkId::new("route-bench").unwrap();
            let definition = SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(Ack),
                &limits,
            );
            let mut routes = vec![RouteDefinition {
                tenant: None,
                sinks: vec![id.clone()],
            }];
            routes.extend((1..count).map(|index| RouteDefinition {
                tenant: Some(TenantId::new(format!("tenant-{index}")).unwrap()),
                sinks: vec![id.clone()],
            }));
            let bus = EventBus::new_paused(
                limits,
                Arc::new(Metrics::default()),
                vec![definition.clone()],
                routes,
                1,
            )
            .unwrap();
            crate::hotspot_bench::measure(
                "route_publish",
                count,
                4_000,
                || event(64),
                |event| bus.publish(event).unwrap(),
                |_| {
                    let record = bus.take_ready(&id).unwrap().unwrap();
                    bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
                },
            );
        }
    }

    #[tokio::test]
    async fn paused_restore_does_not_deliver_and_owned_worker_drop_cleans_up() {
        let limits = Arc::new(Limits::default());
        let sink = Arc::new(Signal {
            calls: AtomicUsize::new(0),
            block: Some(Arc::new(tokio::sync::Notify::new())),
        });
        let id = SinkId::new("a").unwrap();
        let bus = EventBus::new_paused(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.restore(vec![audit_record()]).unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(sink.calls.load(Ordering::Relaxed), 0);
        assert!(bus.workers.lock().unwrap().is_empty());
        let weak = Arc::downgrade(&bus);
        let owner = bus.start_owned_workers().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.calls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(owner);
        drop(bus);
        tokio::time::timeout(Duration::from_secs(1), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn audit_bus(limits: Limits) -> Arc<EventBus> {
        let limits = Arc::new(limits);
        let ids = ["a", "b", "best"].map(|id| SinkId::new(id).unwrap());
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            ids.iter()
                .map(|id| {
                    SinkDefinition::bounded(
                        id.clone(),
                        if id.as_str() == "best" {
                            SinkDeliveryMode::BestEffort
                        } else {
                            SinkDeliveryMode::ConfirmedRequired
                        },
                        Arc::new(Ack),
                        &limits,
                    )
                })
                .collect(),
            vec![RouteDefinition {
                tenant: None,
                sinks: ids[..2].to_vec(),
            }],
            9,
        )
        .unwrap();
        bus.stop_workers().await.unwrap();
        bus
    }

    fn audit_record() -> SpoolRecord {
        SpoolRecord {
            event: event(8),
            pending_sinks: vec![SinkId::new("a").unwrap()],
            routing_revision: 3,
            accepted_at: 1,
            attempts: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn active_event_id_conflict_preserves_original_fanout_and_acks() {
        let bus = audit_bus(Limits::default()).await;
        let original = event(8);
        bus.publish(original.clone()).unwrap();
        let before = bus.usage().unwrap();
        for payload in [8, 100] {
            let mut duplicate = event(payload);
            duplicate.event_id = original.event_id;
            assert!(matches!(bus.publish(duplicate), Err(Error::Conflict)));
            assert_eq!(bus.usage().unwrap(), before);
            assert_eq!(
                bus.spool_records().unwrap()[0].event.event_id,
                original.event_id
            );
            let state = bus.state.lock().unwrap();
            for id in ["a", "b"] {
                let sink = &state.sinks[&SinkId::new(id).unwrap()];
                assert_eq!((sink.used_count, sink.ready.len()), (1, 1));
                assert_eq!(sink.used_bytes, before.bytes);
            }
        }
        for (index, id) in ["a", "b"].into_iter().enumerate() {
            let id = SinkId::new(id).unwrap();
            let delivery = bus.take_ready(&id).unwrap().unwrap();
            let definition = bus.state.lock().unwrap().sinks[&id].definition.clone();
            bus.complete(&id, delivery, Ok(SinkAck), &definition)
                .unwrap();
            assert_eq!(bus.usage().unwrap().pending_required, 1 - index);
            if index == 0 {
                assert!(
                    !bus.wait_required_drained(Duration::from_millis(1))
                        .await
                        .unwrap()
                );
                assert_eq!(
                    bus.spool_records().unwrap()[0].pending_sinks,
                    vec![SinkId::new("b").unwrap()]
                );
            }
        }
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        // Only active responsibility is guarded: after ACK completion reuse is legal.
        bus.publish(original).unwrap();
    }

    #[tokio::test]
    async fn simultaneous_active_ids_have_one_owner_and_one_conflict() {
        let bus = audit_bus(Limits::default()).await;
        let original = event(8);
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let bus = bus.clone();
                let barrier = barrier.clone();
                let event = original.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    bus.publish(event)
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let mut accepted = 0;
        let mut conflicts = 0;
        for handle in handles {
            match handle.join().unwrap() {
                Ok(_) => accepted += 1,
                Err(Error::Conflict) => conflicts += 1,
                other => panic!("unexpected admission: {other:?}"),
            }
        }
        assert_eq!((accepted, conflicts), (1, 1));
        assert_eq!(bus.usage().unwrap().events, 1);
        assert_eq!(bus.usage().unwrap().pending_required, 2);
        assert_eq!(bus.spool_records().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn restore_validates_the_entire_batch_before_any_state_changes() {
        let good = audit_record();
        let mut invalid = Vec::new();
        for pending in [vec![], vec!["a", "a"], vec!["missing"], vec!["best"]] {
            let mut record = audit_record();
            record.pending_sinks = pending
                .into_iter()
                .map(|id| SinkId::new(id).unwrap())
                .collect();
            invalid.push(record);
        }
        let mut duplicate = good.clone();
        duplicate.accepted_at = 1;
        invalid.push(duplicate);
        let mut bad_time = audit_record();
        bad_time.accepted_at = -1;
        invalid.push(bad_time);
        let mut accepted = Vec::new();
        for (index, bad) in invalid.into_iter().enumerate() {
            let bus = audit_bus(Limits::default()).await;
            if bus.restore(vec![good.clone(), bad]).is_ok() {
                accepted.push(index);
            }
            if bus.usage().unwrap() != EventBusUsage::default() {
                accepted.push(index + 100);
            }
            for sink in bus.state.lock().unwrap().sinks.values() {
                if sink.used_count != 0
                    || sink.used_bytes != 0
                    || !sink.ready.is_empty()
                    || !sink.delayed.is_empty()
                {
                    accepted.push(index + 200);
                }
            }
        }
        for limits in [
            Limits {
                global_event_max_count: 1,
                ..Limits::default()
            },
            Limits {
                global_event_max_bytes: serde_json::to_vec(&good.event).unwrap().len(),
                ..Limits::default()
            },
            Limits {
                sink_queue_max_count: 1,
                sink_delivery_concurrency: 1,
                ..Limits::default()
            },
            Limits {
                sink_queue_max_bytes: serde_json::to_vec(&good.event).unwrap().len(),
                ..Limits::default()
            },
        ] {
            let bus = audit_bus(limits).await;
            assert!(bus.restore(vec![good.clone(), audit_record()]).is_err());
            if bus.usage().unwrap() != EventBusUsage::default() {
                accepted.push(999);
            }
        }
        assert!(
            accepted.is_empty(),
            "invalid or partially restored cases: {accepted:?}"
        );
        let bus = audit_bus(Limits::default()).await;
        bus.restore(vec![good.clone()]).unwrap();
        let before = bus.usage().unwrap();
        assert!(matches!(bus.restore(vec![good]), Err(Error::Conflict)));
        assert_eq!(bus.usage().unwrap(), before);
        let mut historical = audit_record();
        historical
            .attempts
            .insert(SinkId::new("removed-completed").unwrap(), u32::MAX);
        historical.routing_revision = 0;
        assert_eq!(bus.restore(vec![historical]).unwrap(), 1);
    }

    #[tokio::test]
    async fn fanout_restore_inflight_spool_preserves_shared_event_and_accounting() {
        for count in [1, 4, 8] {
            let limits = Arc::new(Limits::default());
            let ids = (0..count)
                .map(|i| SinkId::new(format!("sink{i}")).unwrap())
                .collect::<Vec<_>>();
            let definitions = || {
                ids.iter()
                    .map(|id| {
                        SinkDefinition::bounded(
                            id.clone(),
                            SinkDeliveryMode::ConfirmedRequired,
                            Arc::new(Ack),
                            &limits,
                        )
                    })
                    .collect()
            };
            let routes = || {
                vec![RouteDefinition {
                    tenant: None,
                    sinks: ids.clone(),
                }]
            };
            let bus = EventBus::new(
                limits.clone(),
                Arc::new(Metrics::default()),
                definitions(),
                routes(),
                7,
            )
            .unwrap();
            bus.stop_workers().await.unwrap();
            let original = event(256);
            let bytes = serde_json::to_vec(&original).unwrap().len();
            let acceptance = bus.publish(original.clone()).unwrap();
            assert_eq!(acceptance.required_deliveries, count);
            assert_eq!(
                bus.usage().unwrap(),
                EventBusUsage {
                    events: 1,
                    bytes,
                    pending_required: count
                }
            );
            let mut records = bus.spool_records().unwrap();
            records[0].attempts = ids.iter().map(|id| (id.clone(), 3)).collect();
            let restored = EventBus::new(
                limits.clone(),
                Arc::new(Metrics::default()),
                definitions(),
                routes(),
                99,
            )
            .unwrap();
            restored.stop_workers().await.unwrap();
            assert_eq!(restored.restore(records).unwrap(), 1);
            let deliveries = ids
                .iter()
                .map(|id| restored.take_ready(id).unwrap().unwrap())
                .collect::<Vec<_>>();
            for delivery in &deliveries {
                assert!(Arc::ptr_eq(&delivery.event, &deliveries[0].event));
                assert_eq!(delivery.attempt, 3);
                assert_eq!(delivery.accepted_at, acceptance.accepted_at);
            }
            // All entries are inflight, but every required responsibility is still spooled.
            let snapshot = restored.spool_records().unwrap();
            assert_eq!(snapshot[0].pending_sinks, ids);
            assert_eq!(snapshot[0].routing_revision, 7);
            assert_eq!(snapshot[0].accepted_at, acceptance.accepted_at);
            assert_eq!(snapshot[0].event.event_id, original.event_id);
            assert!(snapshot[0].attempts.values().all(|attempt| *attempt == 3));
            for (index, (id, record)) in ids.iter().zip(deliveries).enumerate() {
                let definition = lock(&restored.state).unwrap().sinks[id].definition.clone();
                restored
                    .complete(id, record, Ok(SinkAck), &definition)
                    .unwrap();
                if index + 1 < count {
                    let pending = restored.spool_records().unwrap();
                    assert_eq!(pending[0].attempts[id], 4);
                    assert_eq!(pending[0].pending_sinks, ids[index + 1..]);
                }
            }
            assert_eq!(restored.usage().unwrap(), EventBusUsage::default());
            let state = lock(&restored.state).unwrap();
            assert!(state.sinks.values().all(|sink| sink.used_count == 0
                && sink.used_bytes == 0
                && sink.inflight == 0
                && sink.ready.is_empty()
                && sink.delayed.is_empty()));
        }
    }

    #[tokio::test]
    #[ignore = "release-only queue scan measurement"]
    async fn eventbus_queue_depth_probe() {
        for depth in [0, 1_000, 10_000, 16_383] {
            let limits = Arc::new(Limits {
                sink_queue_max_count: 16_384,
                sink_queue_max_bytes: 64 * 1024 * 1024,
                ..Limits::default()
            });
            let id = SinkId::new("depth").unwrap();
            let definition = SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(Ack),
                &limits,
            );
            let bus = EventBus::new(
                limits,
                Arc::new(Metrics::with_lock_timing()),
                vec![definition.clone()],
                vec![RouteDefinition {
                    tenant: None,
                    sinks: vec![id.clone()],
                }],
                1,
            )
            .unwrap();
            bus.stop_workers().await.unwrap();
            for _ in 0..depth {
                bus.publish(event(8)).unwrap();
            }
            {
                let mut state = bus.state.lock().unwrap();
                let sink = state.sinks.get_mut(&id).unwrap();
                for mut record in sink.ready.drain(..) {
                    record.next_attempt = Instant::now() + Duration::from_secs(3600);
                    sink.delayed
                        .entry(record.next_attempt)
                        .or_default()
                        .push_back(record);
                }
            }
            let mut take_ns = Vec::new();
            let mut delay_ns = Vec::new();
            let mut complete_ns = Vec::new();
            for _ in 0..1_000 {
                let start = Instant::now();
                assert!(bus.take_ready(&id).unwrap().is_none());
                take_ns.push(start.elapsed().as_nanos());
                let start = Instant::now();
                let _ = bus.next_ready_delay(&id).unwrap();
                delay_ns.push(start.elapsed().as_nanos());
                bus.publish(event(8)).unwrap();
                let record = bus.take_ready(&id).unwrap().unwrap();
                let start = Instant::now();
                bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
                complete_ns.push(start.elapsed().as_nanos());
            }
            take_ns.sort_unstable();
            delay_ns.sort_unstable();
            complete_ns.sort_unstable();
            println!(
                "depth={depth} take_ns={}/{}/{} delay_ns={}/{}/{} complete_ns={}/{}/{}",
                take_ns[500],
                take_ns[950],
                take_ns[990],
                delay_ns[500],
                delay_ns[950],
                delay_ns[990],
                complete_ns[500],
                complete_ns[950],
                complete_ns[990]
            );
        }
    }

    #[tokio::test]
    async fn required_admission_is_atomic_and_count_byte_bounded() {
        let limits = Arc::new(Limits {
            sink_queue_max_count: 1,
            sink_queue_max_bytes: 1_024,
            sink_delivery_concurrency: 1,
            ..Limits::default()
        });
        let first = SinkId::new("a").unwrap();
        let second = SinkId::new("b").unwrap();
        let mut a = SinkDefinition::bounded(
            first.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(Ack),
            &limits,
        );
        let mut b = SinkDefinition::bounded(
            second.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(Ack),
            &limits,
        );
        a.max_bytes = 1;
        b.max_bytes = 1_024;
        let bus = EventBus::new(
            limits,
            Arc::new(Metrics::default()),
            vec![a, b],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![second.clone(), first],
            }],
            1,
        )
        .unwrap();
        assert!(matches!(bus.publish(event(8)), Err(Error::Overloaded)));
        let state = bus.state.lock().unwrap();
        assert!(state.sinks.get(&second).unwrap().ready.is_empty());
        assert!(state.active.is_empty());
    }

    #[tokio::test]
    async fn retries_and_required_fanout_preserve_exact_accepted_event_id() {
        struct RetryTwice(Mutex<Vec<EventId>>);
        #[async_trait]
        impl EventSink for RetryTwice {
            async fn deliver(
                &self,
                delivery: DeliveryEnvelope,
            ) -> std::result::Result<SinkAck, SinkError> {
                let mut seen = self.0.lock().unwrap();
                assert!(seen.len() < 3, "unexpected extra delivery");
                seen.push(delivery.event.event_id);
                if delivery.attempt < 3 {
                    Err(SinkError::Retryable)
                } else {
                    Ok(SinkAck)
                }
            }
        }
        let limits = Arc::new(Limits {
            retry_base_ms: 1,
            retry_max_ms: 1,
            ..Limits::default()
        });
        let sinks = [
            Arc::new(RetryTwice(Mutex::new(Vec::with_capacity(3)))),
            Arc::new(RetryTwice(Mutex::new(Vec::with_capacity(3)))),
        ];
        let ids = [
            SinkId::new("first").unwrap(),
            SinkId::new("second").unwrap(),
        ];
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            ids.iter()
                .zip(&sinks)
                .map(|(id, sink)| {
                    SinkDefinition::bounded(
                        id.clone(),
                        SinkDeliveryMode::ConfirmedRequired,
                        sink.clone(),
                        &limits,
                    )
                })
                .collect(),
            vec![RouteDefinition {
                tenant: None,
                sinks: ids.to_vec(),
            }],
            1,
        )
        .unwrap();
        let event = event(8);
        let event_id = event.event_id;
        assert_eq!(bus.publish(event).unwrap().event_id, event_id);
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        for sink in sinks {
            assert_eq!(*sink.0.lock().unwrap(), vec![event_id; 3]);
        }
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn required_last_target_full_preserves_every_queue_and_counter() {
        for count in [1, 4, 8] {
            for byte_limit in [false, true] {
                let limits = Arc::new(Limits::default());
                let ids = (0..count)
                    .map(|i| SinkId::new(format!("sink{i}")).unwrap())
                    .collect::<Vec<_>>();
                let last = ids.last().unwrap().clone();
                let bytes = serde_json::to_vec(&event(8)).unwrap().len();
                let definitions = ids
                    .iter()
                    .map(|id| {
                        let mut definition = SinkDefinition::bounded(
                            id.clone(),
                            SinkDeliveryMode::ConfirmedRequired,
                            Arc::new(Ack),
                            &limits,
                        );
                        definition.concurrency = 1;
                        if id == &last {
                            if byte_limit {
                                definition.max_bytes = bytes;
                            } else {
                                definition.max_count = 1;
                            }
                        }
                        definition
                    })
                    .collect();
                let bus = EventBus::new(
                    limits,
                    Arc::new(Metrics::default()),
                    definitions,
                    vec![RouteDefinition {
                        tenant: None,
                        sinks: vec![last],
                    }],
                    1,
                )
                .unwrap();
                bus.stop_workers().await.unwrap();
                bus.publish(event(8)).unwrap();
                bus.replace_routes(
                    2,
                    vec![RouteDefinition {
                        tenant: None,
                        sinks: ids,
                    }],
                )
                .unwrap();
                let before = bus.usage().unwrap();
                let accounting = || {
                    let state = lock(&bus.state).unwrap();
                    state
                        .sinks
                        .values()
                        .map(|sink| {
                            (
                                sink.used_count,
                                sink.used_bytes,
                                sink.inflight,
                                sink.ready.len(),
                            )
                        })
                        .collect::<Vec<_>>()
                };
                let counters = accounting();
                assert!(matches!(bus.publish(event(8)), Err(Error::Overloaded)));
                assert_eq!(bus.usage().unwrap(), before);
                assert_eq!(accounting(), counters);
                assert_eq!(bus.spool_records().unwrap().len(), 1);
            }
        }
    }

    #[tokio::test]
    async fn cancelled_inflight_required_delivery_remains_owned_for_spool() {
        let limits = Arc::new(Limits::default());
        let id = SinkId::new("blocked").unwrap();
        let sink = Arc::new(Signal {
            calls: AtomicUsize::new(0),
            block: Some(Arc::new(tokio::sync::Notify::new())),
        });
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id.clone()],
            }],
            9,
        )
        .unwrap();
        let acceptance = bus.publish(event(8)).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        bus.close_admission().unwrap();
        let before = bus.usage().unwrap();
        bus.stop_workers().await.unwrap();
        assert_eq!(bus.usage().unwrap(), before);
        let records = bus.spool_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event.event_id, acceptance.event_id);
        assert_eq!(records[0].routing_revision, 9);
        assert_eq!(records[0].pending_sinks, vec![id]);
    }

    #[tokio::test]
    async fn eventbus_timeout_retries_owned_event_and_drains() {
        struct TimeoutOnce;
        #[async_trait]
        impl EventSink for TimeoutOnce {
            async fn deliver(
                &self,
                delivery: DeliveryEnvelope,
            ) -> std::result::Result<SinkAck, SinkError> {
                if delivery.attempt == 1 {
                    std::future::pending::<()>().await;
                }
                Ok(SinkAck)
            }
        }
        let limits = Arc::new(Limits {
            sink_timeout_ms: 10,
            retry_base_ms: 1,
            retry_max_ms: 1,
            ..Limits::default()
        });
        let metrics = Arc::new(Metrics::default());
        let id = SinkId::new("timeout").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            metrics.clone(),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                Arc::new(TimeoutOnce),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        let event = event(8);
        let expected = event.event_id;
        assert_eq!(bus.publish(event).unwrap().event_id, expected);
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(metrics.get(Metric::SinkRetries), 1);
        assert_eq!(metrics.get(Metric::SinkAcks), 1);
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn eventbus_sink_panic_recovery_001() {
        let limits = Arc::new(Limits {
            retry_base_ms: 1,
            retry_max_ms: 1,
            ..Limits::default()
        });
        let sink = Arc::new(PanicOnce {
            calls: AtomicUsize::new(0),
        });
        let id = SinkId::new("panic-recovery").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.publish(named_event("panic-owned")).unwrap();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(sink.calls.load(Ordering::SeqCst), 2);
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn slow_required_sink_does_not_block_fast_sink_and_accounting_returns_to_zero() {
        let limits = Arc::new(Limits::default());
        let release = Arc::new(tokio::sync::Notify::new());
        let fast = Arc::new(Signal {
            calls: AtomicUsize::new(0),
            block: None,
        });
        let slow = Arc::new(Signal {
            calls: AtomicUsize::new(0),
            block: Some(release.clone()),
        });
        let fast_id = SinkId::new("fast").unwrap();
        let slow_id = SinkId::new("slow").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![
                SinkDefinition::bounded(
                    fast_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    fast.clone(),
                    &limits,
                ),
                SinkDefinition::bounded(
                    slow_id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    slow.clone(),
                    &limits,
                ),
            ],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![slow_id, fast_id],
            }],
            1,
        )
        .unwrap();
        bus.publish(event(8)).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while fast.calls.load(Ordering::Relaxed) == 0 || slow.calls.load(Ordering::Relaxed) == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(slow.calls.load(Ordering::Relaxed), 1);
        assert_eq!(bus.usage().unwrap().pending_required, 1);
        release.notify_one();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn event_bus_full_concurrency_waits_for_completion_without_ready_timer_spin() {
        let limits = Arc::new(Limits {
            sink_delivery_concurrency: 1,
            ..Limits::default()
        });
        let release = Arc::new(tokio::sync::Notify::new());
        let sink = Arc::new(Signal {
            calls: AtomicUsize::new(0),
            block: Some(release.clone()),
        });
        let id = SinkId::new("spin-guard").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.publish(named_event("blocked-first")).unwrap();
        bus.publish(named_event("ready-second")).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.calls.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release.notify_one();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn best_effort_overload_drops_without_blocking_required_acceptance() {
        let limits = Arc::new(Limits::default());
        let required_id = SinkId::new("required").unwrap();
        let best_id = SinkId::new("best").unwrap();
        let required = SinkDefinition::bounded(
            required_id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            Arc::new(Ack),
            &limits,
        );
        let mut best = SinkDefinition::bounded(
            best_id.clone(),
            SinkDeliveryMode::BestEffort,
            Arc::new(Ack),
            &limits,
        );
        best.max_bytes = 1;
        let bus = EventBus::new(
            limits,
            Arc::new(Metrics::default()),
            vec![required, best],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![best_id, required_id],
            }],
            1,
        )
        .unwrap();
        let accepted = bus.publish(event(8)).unwrap();
        assert_eq!(accepted.required_deliveries, 1);
        assert_eq!(accepted.best_effort_deliveries, 0);
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn delayed_retry_does_not_block_later_ready_delivery() {
        let limits = Arc::new(Limits {
            sink_delivery_concurrency: 1,
            retry_base_ms: 500,
            retry_max_ms: 500,
            ..Limits::default()
        });
        let sink = Arc::new(FailFirst {
            calls: AtomicUsize::new(0),
            delivered: Mutex::new(Vec::new()),
        });
        let id = SinkId::new("required").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.publish(named_event("a")).unwrap();
        while sink.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        bus.publish(named_event("b")).unwrap();
        tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if sink
                    .delivered
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|value| value == "b")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn delayed_retry_wakes_with_another_delivery_inflight() {
        let limits = Arc::new(Limits {
            sink_delivery_concurrency: 2,
            retry_base_ms: 100,
            retry_max_ms: 100,
            ..Limits::default()
        });
        let release = Arc::new(tokio::sync::Notify::new());
        let sink = Arc::new(RetryBesideBlocked {
            delivered: Mutex::new(Vec::new()),
            release: release.clone(),
        });
        let id = SinkId::new("required").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.publish(named_event("a")).unwrap();
        while sink.delivered.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        bus.publish(named_event("blocked")).unwrap();
        tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if sink
                    .delivered
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|source| source.as_str() == "a")
                    .count()
                    >= 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release.notify_one();
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        bus.stop_workers().await.unwrap();
    }

    #[tokio::test]
    async fn required_delivery_recovers_after_normal_retry_exhaustion() {
        let limits = Arc::new(Limits {
            sink_delivery_concurrency: 1,
            sink_max_attempts: 1,
            retry_base_ms: 5,
            retry_max_ms: 10,
            ..Limits::default()
        });
        let sink = Arc::new(Recovering {
            fail: std::sync::atomic::AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        let id = SinkId::new("required").unwrap();
        let bus = EventBus::new(
            limits.clone(),
            Arc::new(Metrics::default()),
            vec![SinkDefinition::bounded(
                id.clone(),
                SinkDeliveryMode::ConfirmedRequired,
                sink.clone(),
                &limits,
            )],
            vec![RouteDefinition {
                tenant: None,
                sinks: vec![id],
            }],
            1,
        )
        .unwrap();
        bus.publish(named_event("parked")).unwrap();
        tokio::time::sleep(Duration::from_millis(35)).await;
        assert_eq!(bus.usage().unwrap().pending_required, 1);
        assert!(sink.calls.load(Ordering::SeqCst) < 10);
        sink.fail.store(false, Ordering::SeqCst);
        assert!(
            bus.wait_required_drained(Duration::from_secs(1))
                .await
                .unwrap()
        );
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        bus.stop_workers().await.unwrap();
    }
    #[test]
    fn ready_deliveries_are_fifo_and_release_all_accounting() {
        let bus = route_bus(Limits::default(), vec![route(None, &["a"])]).unwrap();
        let id = SinkId::new("a").unwrap();
        let definition = bus.state.lock().unwrap().sinks[&id].definition.clone();
        let events = [
            named_event("first"),
            named_event("second"),
            named_event("third"),
        ];
        let ids = events.each_ref().map(|event| event.event_id);
        for event in events {
            bus.publish(event).unwrap();
        }
        for expected in ids {
            let record = bus.take_ready(&id).unwrap().unwrap();
            assert_eq!(record.event.event_id, expected);
            bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
        }
        assert!(bus.take_ready(&id).unwrap().is_none());
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
    }

    #[tokio::test]
    async fn delayed_same_deadline_fifo_survives_shutdown_and_spooling() {
        let bus = route_bus(Limits::default(), vec![route(None, &["a"])]).unwrap();
        let id = SinkId::new("a").unwrap();
        let definition = bus.state.lock().unwrap().sinks[&id].definition.clone();
        let events = [named_event("retry-first"), named_event("retry-second")];
        let ids = events.each_ref().map(|event| event.event_id);
        for event in events {
            bus.publish(event).unwrap();
        }
        for _ in 0..2 {
            let record = bus.take_ready(&id).unwrap().unwrap();
            bus.complete(&id, record, Err(SinkError::Retryable), &definition)
                .unwrap();
        }
        let bytes = bus.usage().unwrap().bytes;
        {
            let mut state = bus.state.lock().unwrap();
            let sink = state.sinks.get_mut(&id).unwrap();
            let mut records = std::mem::take(&mut sink.delayed)
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            let future = Instant::now() + Duration::from_secs(60);
            for expected in ids {
                let position = records
                    .iter()
                    .position(|record| record.event.event_id == expected)
                    .unwrap();
                let mut record = records.remove(position);
                record.next_attempt = future;
                sink.delayed.entry(future).or_default().push_back(record);
            }
            assert!(sink.ready.is_empty());
            assert_eq!(sink.delayed.len(), 1);
            assert_eq!(sink.delayed[&future].len(), 2);
            assert_eq!(sink.used_count, 2);
            assert_eq!(
                sink.used_bytes,
                sink.delayed[&future].iter().map(|r| r.bytes).sum::<usize>()
            );
        }
        assert!(bus.take_ready(&id).unwrap().is_none());
        assert!(bus.next_ready_delay(&id).unwrap().unwrap() > Duration::ZERO);
        let ready = named_event("later-ready");
        let ready_id = ready.event_id;
        bus.publish(ready).unwrap();
        let record = bus.take_ready(&id).unwrap().unwrap();
        assert_eq!(record.event.event_id, ready_id);
        bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
        bus.close_admission().unwrap();
        bus.stop_workers().await.unwrap();
        let spooled = bus.spool_records().unwrap();
        assert_eq!(spooled.len(), 2);
        assert!(spooled.iter().all(|record| record.pending_sinks == vec![id.clone()] && record.attempts[&id] == 1));
        assert_eq!(bus.usage().unwrap().bytes, bytes);
        {
            let mut state = bus.state.lock().unwrap();
            let sink = state.sinks.get_mut(&id).unwrap();
            let (_, mut records) = sink.delayed.pop_first().unwrap();
            let due = Instant::now();
            for record in &mut records {
                record.next_attempt = due;
            }
            sink.delayed.insert(due, records);
        }
        assert_eq!(bus.next_ready_delay(&id).unwrap(), Some(Duration::ZERO));
        for expected in ids {
            let record = bus.take_ready(&id).unwrap().unwrap();
            assert_eq!(record.event.event_id, expected);
            assert_eq!(record.attempt, 1);
            bus.complete(&id, record, Ok(SinkAck), &definition).unwrap();
        }
        assert_eq!(bus.next_ready_delay(&id).unwrap(), None);
        assert_eq!(bus.usage().unwrap(), EventBusUsage::default());
        assert!(bus.spool_records().unwrap().is_empty());
    }

    #[test]
    #[ignore = "serial release queue benchmark"]
    fn ready_retry_queue_scaling() {
        use crate::hotspot_bench::measure;
        fn metric(text: &str, name: &str) -> f64 {
            text.lines()
                .find_map(|line| {
                    line.strip_prefix(name)
                        .and_then(|value| value.trim().parse().ok())
                })
                .unwrap()
        }
        for depth in [0, 100, 1000, 4096] {
            for retry_percent in [0, 10, 50] {
                let memory = stats_alloc::Region::new(&stats_alloc::INSTRUMENTED_SYSTEM);
                let limits = Arc::new(Limits {
                    sink_queue_max_count: 5000,
                    ..Limits::default()
                });
                let id = SinkId::new("queue").unwrap();
                let metrics = Arc::new(Metrics::with_lock_timing());
                let definition = SinkDefinition::bounded(
                    id.clone(),
                    SinkDeliveryMode::ConfirmedRequired,
                    Arc::new(Ack),
                    &limits,
                );
                let bus = EventBus::new_paused(
                    limits,
                    metrics.clone(),
                    vec![definition],
                    vec![RouteDefinition {
                        tenant: None,
                        sinks: vec![id.clone()],
                    }],
                    1,
                )
                .unwrap();
                for _ in 0..depth {
                    bus.publish(event(8)).unwrap();
                }
                {
                    let mut state = bus.state.lock().unwrap();
                    let sink = state.sinks.get_mut(&id).unwrap();
                    for _ in 0..depth * retry_percent / 100 {
                        let mut record = sink.ready.pop_front().unwrap();
                        record.next_attempt = Instant::now() + Duration::from_secs(3600);
                        sink.delayed
                            .entry(record.next_attempt)
                            .or_default()
                            .push_back(record);
                    }
                }
                let heap = memory.change();
                println!(
                    "EVENT_QUEUE_MEMORY,{depth},{retry_percent},{}",
                    heap.bytes_allocated as i128 - heap.bytes_deallocated as i128
                );
                let name = format!("dequeue_retry{retry_percent}");
                let before = metrics.render();
                measure(
                    &name,
                    depth,
                    2000,
                    || (),
                    |_| bus.take_ready(&id).unwrap(),
                    |record| {
                        if let Some(mut record) = record {
                            record.next_attempt = Instant::now();
                            let mut state = bus.state.lock().unwrap();
                            let sink = state.sinks.get_mut(&id).unwrap();
                            sink.inflight -= 1;
                            sink.ready.push_back(record);
                        }
                    },
                );
                let after = metrics.render();
                for (site, label) in [
                    ("take_ready", name),
                    ("next_ready_delay", format!("deadline_retry{retry_percent}")),
                ] {
                    let (start, finish) = if site == "next_ready_delay" {
                        let before = metrics.render();
                        measure(
                            &label,
                            depth,
                            2000,
                            || (),
                            |_| bus.next_ready_delay(&id).unwrap(),
                            |_| {},
                        );
                        (before, metrics.render())
                    } else {
                        (before.clone(), after.clone())
                    };
                    let count_name = format!("netbaiot_event_bus_site_{site}_hold_ns_count");
                    let count = metric(&finish, &count_name) - metric(&start, &count_name);
                    let wait_name = format!("netbaiot_event_bus_site_{site}_wait_ns_sum");
                    let hold_name = format!("netbaiot_event_bus_site_{site}_hold_ns_sum");
                    let wait = metric(&finish, &wait_name) - metric(&start, &wait_name);
                    let hold = metric(&finish, &hold_name) - metric(&start, &hold_name);
                    println!(
                        "EVENT_QUEUE_LOCK,{label},{depth},{count},{:.3},{:.3}",
                        wait / count,
                        hold / count
                    );
                }
            }
        }
    }
}
