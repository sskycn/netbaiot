use crate::{Error, EventBusProbe, Histogram, Limits, Metric, Metrics, Result, lock, now_ms};
use async_trait::async_trait;
use futures_util::FutureExt;
use netbaiot_core::{DeviceEvent, EventAccepted, EventId, RouteDefinition, SinkId};
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
    queue: VecDeque<DeliveryRecord>,
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
    routes: Vec<RouteDefinition>,
    routing_revision: u64,
    active: HashMap<EventId, ActiveEvent>,
    active_bytes: usize,
    accepting: bool,
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
    wait: Option<Duration>,
    held: Option<Instant>,
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
                wait,
                held,
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
                    queue: VecDeque::new(),
                    used_count: 0,
                    used_bytes: 0,
                    inflight: 0,
                    notify: Arc::new(tokio::sync::Notify::new()),
                },
            );
        }
        validate_routes(&routes, &sinks, &limits)?;
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
        bus.start_workers()?;
        Ok(bus)
    }

    fn start_workers(self: &Arc<Self>) -> Result<()> {
        let ids = self
            .lock_state(EventBusProbe::Other)?
            .sinks
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut workers = lock(&self.workers)?;
        for id in ids {
            let bus = self.clone();
            workers.push(tokio::spawn(async move { bus.run_sink(id).await }));
        }
        Ok(())
    }

    pub fn replace_routes(&self, revision: u64, routes: Vec<RouteDefinition>) -> Result<()> {
        let mut state = self.lock_state(EventBusProbe::Other)?;
        validate_routes(&routes, &state.sinks, &self.limits)?;
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
        validate_routes(routes, &state.sinks, &self.limits)
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
        let bytes = serde_json::to_vec(&event)
            .map_err(|_| Error::Invalid)?
            .len();
        let event = Arc::new(event);
        let lock_started = self.metrics.lock_timing_enabled().then(Instant::now);
        let mut state = self.lock_state(EventBusProbe::Publish)?;
        let lock_wait_us = lock_started.map(|started| started.elapsed().as_micros() as u64);
        let hold_started = lock_started.map(|_| Instant::now());
        if !state.accepting {
            return Err(Error::Draining);
        }
        let mut targets = BTreeSet::new();
        for route in &state.routes {
            if route
                .tenant
                .as_ref()
                .is_none_or(|tenant| tenant == &event.device.tenant_id)
            {
                targets.extend(route.sinks.iter().cloned());
            }
        }
        if targets.is_empty() {
            return Err(Error::Unavailable);
        }
        if targets.len() > self.limits.max_fanout_per_event {
            return Err(Error::Overloaded);
        }
        if state.active.len() >= self.limits.global_event_max_count
            || state.active_bytes.saturating_add(bytes) > self.limits.global_event_max_bytes
        {
            return Err(Error::Overloaded);
        }
        // Required capacity is checked for every sink before the first mutation.
        for id in &targets {
            let sink = state.sinks.get(id).ok_or(Error::Configuration)?;
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
        for id in targets {
            let sink = state.sinks.get_mut(&id).ok_or(Error::Configuration)?;
            let available = sink.used_count < sink.definition.max_count
                && sink.used_bytes.saturating_add(bytes) <= sink.definition.max_bytes;
            if !available {
                self.metrics.inc(Metric::SinkDrops);
                continue;
            }
            sink.used_count += 1;
            sink.used_bytes += bytes;
            sink.queue.push_back(DeliveryRecord {
                event: event.clone(),
                bytes,
                accepted_at: now,
                attempt: 0,
                next_attempt: Instant::now(),
            });
            remaining.insert(id.clone());
            if sink.definition.mode == SinkDeliveryMode::ConfirmedRequired {
                required.insert(id);
            }
            accepted_notifies.push(sink.notify.clone());
        }
        let required_deliveries = required.len();
        let best_effort_deliveries = remaining.len().saturating_sub(required_deliveries);
        if remaining.is_empty() {
            return Err(Error::Overloaded);
        }
        let event_id = event.event_id;
        let revision = state.routing_revision;
        state.active_bytes += bytes;
        state.active.insert(
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
        let mut restored = 0;
        for record in records {
            let bytes = serde_json::to_vec(&record.event)
                .map_err(|_| Error::Invalid)?
                .len();
            let event = Arc::new(record.event);
            let mut state = self.lock_state(EventBusProbe::Other)?;
            if state.active.contains_key(&event.event_id)
                || state.active.len() >= self.limits.global_event_max_count
                || state.active_bytes.saturating_add(bytes) > self.limits.global_event_max_bytes
                || record.pending_sinks.len() > self.limits.max_fanout_per_event
            {
                return Err(Error::Overloaded);
            }
            for id in &record.pending_sinks {
                let sink = state.sinks.get(id).ok_or(Error::Configuration)?;
                if sink.definition.mode != SinkDeliveryMode::ConfirmedRequired
                    || sink.used_count >= sink.definition.max_count
                    || sink.used_bytes.saturating_add(bytes) > sink.definition.max_bytes
                {
                    return Err(Error::Overloaded);
                }
            }
            let mut pending = BTreeSet::new();
            let mut notifies = Vec::new();
            for id in &record.pending_sinks {
                let sink = state.sinks.get_mut(id).ok_or(Error::Configuration)?;
                sink.used_count += 1;
                sink.used_bytes += bytes;
                sink.queue.push_back(DeliveryRecord {
                    event: event.clone(),
                    bytes,
                    accepted_at: record.accepted_at,
                    attempt: *record.attempts.get(id).unwrap_or(&0),
                    next_attempt: Instant::now(),
                });
                pending.insert(id.clone());
                notifies.push(sink.notify.clone());
            }
            state.active_bytes += bytes;
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
            drop(state);
            for notify in notifies {
                self.metrics.event_bus_probe(EventBusProbe::NotifyWorker);
                notify.notify_one();
            }
            restored += 1;
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
        let (definition, notify) =
            match self
                .lock_state(EventBusProbe::Other)
                .ok()
                .and_then(|state| {
                    state
                        .sinks
                        .get(&id)
                        .map(|sink| (sink.definition.clone(), sink.notify.clone()))
                }) {
                Some(value) => value,
                None => return,
            };
        let mut inflight = JoinSet::new();
        let mut woke = false;
        loop {
            let mut took_work = false;
            while inflight.len() < definition.concurrency {
                let next = self.take_ready(&id);
                let Ok(Some(record)) = next else { break };
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
                        if let Some(Ok((record, result))) = completed {
                            let _ = self.complete(&id, record, result, &definition);
                        }
                    }
                }
                continue;
            }
            if inflight.is_empty() {
                let delay = self
                    .next_ready_delay(&id)
                    .ok()
                    .flatten()
                    .unwrap_or(Duration::from_secs(3_600));
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => continue,
                    _ = notify.notified() => { self.metrics.event_bus_probe(EventBusProbe::WakeNotify); },
                    _ = tokio::time::sleep(delay) => { self.metrics.event_bus_probe(EventBusProbe::WakeTimer); },
                }
            } else {
                let delay = self
                    .next_ready_delay(&id)
                    .ok()
                    .flatten()
                    .unwrap_or(Duration::from_secs(3_600));
                tokio::select! {
                    biased;
                    _ = self.stop.cancelled() => continue,
                    completed = inflight.join_next() => {
                        self.metrics.event_bus_probe(EventBusProbe::WakeJoin);
                        if let Some(Ok((record, result))) = completed {
                            let _ = self.complete(&id, record, result, &definition);
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
        let now = Instant::now();
        let Some(position) = sink
            .queue
            .iter()
            .enumerate()
            .filter(|(_, record)| record.next_attempt <= now)
            .min_by_key(|(_, record)| record.next_attempt)
            .map(|(position, _)| position)
        else {
            return Ok(None);
        };
        let record = sink.queue.remove(position).ok_or(Error::Internal)?;
        sink.inflight += 1;
        Ok(Some(record))
    }

    fn next_ready_delay(&self, id: &SinkId) -> Result<Option<Duration>> {
        let state = self.lock_state(EventBusProbe::NextDelay)?;
        let sink = state.sinks.get(id).ok_or(Error::Internal)?;
        let now = Instant::now();
        Ok(sink
            .queue
            .iter()
            .map(|record| record.next_attempt.saturating_duration_since(now))
            .min())
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
            sink.queue.push_back(record);
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
            sink.queue.push_back(record);
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

fn validate_routes(
    routes: &[RouteDefinition],
    sinks: &BTreeMap<SinkId, SinkState>,
    limits: &Limits,
) -> Result<()> {
    if routes.is_empty() || routes.len() > limits.max_routing_filters {
        return Err(Error::Configuration);
    }
    for route in routes {
        let unique = route.sinks.iter().collect::<BTreeSet<_>>();
        if unique.is_empty()
            || unique.len() != route.sinks.len()
            || unique.len() > limits.max_fanout_per_event
            || unique.len() > limits.max_sinks_per_tenant
            || unique.iter().any(|id| !sinks.contains_key(*id))
        {
            return Err(Error::Configuration);
        }
    }
    Ok(())
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
                for record in &mut state.sinks.get_mut(&id).unwrap().queue {
                    record.next_attempt = Instant::now() + Duration::from_secs(3_600);
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
        assert!(state.sinks.get(&second).unwrap().queue.is_empty());
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
}
