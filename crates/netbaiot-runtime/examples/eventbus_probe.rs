//! Bounded lifecycle experiment. Run in release; not a network capacity benchmark.
use async_trait::async_trait;
use netbaiot_core::*;
use netbaiot_runtime::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct ProbeSink {
    delay: Duration,
    retry: bool,
    starts: Arc<Vec<OnceLock<Instant>>>,
    samples: Mutex<Vec<u64>>,
    gate: Option<tokio::sync::watch::Receiver<bool>>,
}
#[async_trait]
impl EventSink for ProbeSink {
    async fn deliver(&self, delivery: DeliveryEnvelope) -> std::result::Result<SinkAck, SinkError> {
        if let Some(mut gate) = self.gate.clone() {
            gate.wait_for(|open| *open)
                .await
                .map_err(|_| SinkError::Retryable)?;
        }
        if self.retry && delivery.attempt == 1 {
            return Err(SinkError::Retryable);
        }
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let index: usize = delivery
            .event
            .source_message_id
            .as_str()
            .parse()
            .map_err(|_| SinkError::Permanent)?;
        let start = self
            .starts
            .get(index)
            .and_then(OnceLock::get)
            .ok_or(SinkError::Permanent)?;
        self.samples
            .lock()
            .map_err(|_| SinkError::Permanent)?
            .push(start.elapsed().as_nanos() as u64);
        Ok(SinkAck)
    }
}
fn quantiles(mut values: Vec<u64>) -> serde_json::Value {
    values.sort_unstable();
    json!({"p50_ns":values[values.len()/2],"p95_ns":values[values.len()*95/100],"p99_ns":values[values.len()*99/100]})
}
async fn run(
    sinks: usize,
    delay_ms: u64,
    retry: bool,
    window: usize,
    total: usize,
) -> BenchResult<()> {
    let limits = Arc::new(Limits {
        sink_queue_max_count: 16384,
        sink_queue_max_bytes: 64 * 1024 * 1024,
        global_event_max_count: 16384,
        global_event_max_bytes: 64 * 1024 * 1024,
        retry_base_ms: 5,
        retry_max_ms: 10,
        ..Limits::default()
    });
    let metrics = Arc::new(Metrics::with_lock_timing());
    let starts = Arc::new((0..total).map(|_| OnceLock::new()).collect::<Vec<_>>());
    let mut definitions = Vec::new();
    let mut implementations = Vec::new();
    let mut ids = Vec::new();
    for i in 0..sinks {
        let id = SinkId::new(format!("probe{i}"))?;
        let sink = Arc::new(ProbeSink {
            delay: Duration::from_millis(delay_ms),
            retry,
            starts: starts.clone(),
            samples: Mutex::new(Vec::with_capacity(total)),
            gate: None,
        });
        definitions.push(SinkDefinition::bounded(
            id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink.clone(),
            &limits,
        ));
        implementations.push(sink);
        ids.push(id);
    }
    let bus = EventBus::new(
        limits,
        metrics.clone(),
        definitions,
        vec![RouteDefinition {
            tenant: None,
            sinks: ids,
        }],
        1,
    )?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let before = metrics.render();
    let start = Instant::now();
    let mut publish = Vec::with_capacity(total);
    let mut max_depth = 0;
    let mut max_bytes = 0;
    for i in 0..total {
        // Bound outstanding deliveries to window * sinks. Uneven sink progress
        // can leave up to that many active events; sample storage is fixed.
        while i as u64 >= metrics.get(Metric::SinkAcks) / sinks as u64 + window as u64 {
            if start.elapsed() > Duration::from_secs(60) {
                return Err("probe deadline".into());
            }
            tokio::task::yield_now().await;
        }
        let event = DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new(i.to_string())?,
            device: DeviceKey {
                tenant_id: TenantId::new("t")?,
                product_id: ProductId::new("p")?,
                device_id: DeviceId::new("d")?,
            },
            received_at: now_ms(),
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: i as u64 }),
        };
        let t = Instant::now();
        starts[i].set(t).map_err(|_| "duplicate start")?;
        bus.publish(event)?;
        publish.push(t.elapsed().as_nanos() as u64);
        if i % 128 == 0 {
            let usage = bus.usage()?;
            max_depth = max_depth.max(usage.events);
            max_bytes = max_bytes.max(usage.bytes);
        }
    }
    let published_seconds = start.elapsed().as_secs_f64();
    while metrics.get(Metric::SinkAcks) < (total * sinks) as u64 {
        if start.elapsed() > Duration::from_secs(60) {
            return Err("drain deadline".into());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    if !bus.wait_required_drained(Duration::from_secs(1)).await? {
        return Err("not drained".into());
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let after = metrics.render();
    bus.stop_workers().await?;
    let completion = implementations
        .iter()
        .map(|sink| quantiles(sink.samples.lock().unwrap().clone()))
        .collect::<Vec<_>>();
    println!(
        "{}",
        json!({"sinks":sinks,"delay_ms":delay_ms,"retry":retry,"window":window,"events":total,
        "events_per_sec":total as f64/elapsed,"publish_per_sec":total as f64/published_seconds,
        "accepted":quantiles(publish),"completion":completion,"sampled_max_depth":max_depth,"sampled_max_bytes":max_bytes,
        "before_metrics":before,"metrics":after})
    );
    Ok(())
}
async fn overload() -> BenchResult<()> {
    let capacity = 1_024;
    let limits = Arc::new(Limits {
        sink_queue_max_count: capacity,
        sink_queue_max_bytes: 1024 * 1024,
        global_event_max_count: capacity,
        global_event_max_bytes: 1024 * 1024,
        ..Limits::default()
    });
    let metrics = Arc::new(Metrics::with_lock_timing());
    let starts = Arc::new(
        (0..capacity * 2)
            .map(|_| OnceLock::new())
            .collect::<Vec<_>>(),
    );
    let (release, gate) = tokio::sync::watch::channel(false);
    let mut definitions = Vec::new();
    let mut sinks = Vec::new();
    let mut ids = Vec::new();
    for i in 0..2 {
        let id = SinkId::new(format!("isolation{i}"))?;
        let sink = Arc::new(ProbeSink {
            delay: Duration::ZERO,
            retry: false,
            starts: starts.clone(),
            samples: Mutex::new(Vec::with_capacity(capacity)),
            gate: if i == 1 { Some(gate.clone()) } else { None },
        });
        definitions.push(SinkDefinition::bounded(
            id.clone(),
            SinkDeliveryMode::ConfirmedRequired,
            sink.clone(),
            &limits,
        ));
        sinks.push(sink);
        ids.push(id);
    }
    let bus = EventBus::new(
        limits,
        metrics.clone(),
        definitions,
        vec![RouteDefinition {
            tenant: None,
            sinks: ids,
        }],
        1,
    )?;
    let start = Instant::now();
    let mut accepted = 0;
    let mut rejected = 0;
    for i in 0..capacity * 2 {
        starts[i]
            .set(Instant::now())
            .map_err(|_| "duplicate start")?;
        let event = DeviceEvent {
            event_id: EventId::generate(),
            source_message_id: SourceMessageId::new(i.to_string())?,
            device: DeviceKey {
                tenant_id: TenantId::new("t")?,
                product_id: ProductId::new("p")?,
                device_id: DeviceId::new("d")?,
            },
            received_at: now_ms(),
            occurred_at: None,
            kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: i as u64 }),
        };
        match bus.publish(event) {
            Ok(_) => accepted += 1,
            Err(Error::Overloaded) => rejected += 1,
            Err(e) => return Err(e.into()),
        }
    }
    while metrics.get(Metric::SinkAcks) < accepted {
        if start.elapsed() > Duration::from_secs(10) {
            return Err("fast sink starved".into());
        }
        tokio::task::yield_now().await;
    }
    let fast_seconds = start.elapsed().as_secs_f64();
    let usage = bus.usage()?;
    let before_idle = metrics.render();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_idle = metrics.render();
    assert_eq!(accepted, capacity as u64);
    assert_eq!(rejected, capacity as u64);
    assert_eq!(usage.pending_required, capacity);
    assert!(usage.bytes <= 1024 * 1024);
    release.send(true)?;
    if !bus.wait_required_drained(Duration::from_secs(5)).await? {
        return Err("isolation drain failed".into());
    }
    assert_eq!(bus.usage()?, EventBusUsage::default());
    bus.stop_workers().await?;
    println!(
        "{}",
        json!({"scenario":"overload-isolation","accepted":accepted,"rejections":rejected,
        "slow_backlog":usage.pending_required,"pending_bytes":usage.bytes,"fast_events_per_sec":accepted as f64/fast_seconds,
        "fast_completion":quantiles(sinks[0].samples.lock().unwrap().clone()),"before_idle":before_idle,"after_idle":after_idle,"metrics":metrics.render()})
    );
    Ok(())
}
#[tokio::main]
async fn main() -> BenchResult<()> {
    if std::env::args().any(|arg| arg == "--overload") {
        return overload().await;
    }
    for sinks in [1, 4, 8] {
        run(sinks, 0, false, 128, 10000).await?;
    }
    for delay in [1, 10] {
        run(1, delay, false, 128, 2000).await?;
    }
    run(1, 0, true, 128, 2000).await?;
    // Single event isolates an otherwise idle worker's complete lifecycle.
    run(1, 0, false, 1, 1).await?;
    run(1, 0, true, 1, 1).await?;
    Ok(())
}
