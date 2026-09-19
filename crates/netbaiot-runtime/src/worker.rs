use crate::*;
use async_trait::async_trait;
use netbaiot_core::*;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
#[derive(Debug)]
pub enum DeliveryError {
    Retryable,
    Permanent,
}
#[async_trait]
pub trait DeliverySink: Send + Sync {
    async fn deliver(&self, message: &DeviceMessage) -> std::result::Result<(), DeliveryError>;
}
struct DependencyRecovery {
    degraded: bool,
    failures: u32,
    seed: MessageId,
}
impl DependencyRecovery {
    fn new() -> Self {
        Self {
            degraded: false,
            failures: 0,
            seed: MessageId(Uuid::new_v4()),
        }
    }
    fn succeeded(&mut self, metrics: &Metrics) {
        self.failures = 0;
        if self.degraded {
            self.degraded = false;
            metrics.inc(Metric::DependencyRecovered);
            tracing::info!("database dependency recovered");
        }
    }
    async fn failed(
        &mut self,
        error: Error,
        limits: &Limits,
        metrics: &Metrics,
        stop: &CancellationToken,
    ) -> Result<bool> {
        if !matches!(error, Error::Storage | Error::Timeout) {
            return Err(error);
        }
        if !self.degraded {
            self.degraded = true;
            metrics.inc(Metric::DependencyDegraded);
            tracing::warn!(error=%error,"database dependency degraded");
        }
        self.failures = self.failures.saturating_add(1);
        let delay = retry_delay(limits, self.seed, self.failures);
        tokio::select! {
            _ = stop.cancelled() => Ok(false),
            _ = tokio::time::sleep(Duration::from_millis(delay)) => Ok(true),
        }
    }
}
/// Full jitter is deterministic per message/attempt, reproducible in tests.
pub fn retry_delay(l: &Limits, id: MessageId, attempt: u32) -> u64 {
    jitter(l, id.0, attempt)
}
pub fn command_retry_delay(l: &Limits, id: CommandId, attempt: u32) -> u64 {
    jitter(l, id.0, attempt)
}
fn jitter(l: &Limits, id: Uuid, attempt: u32) -> u64 {
    let cap = l
        .retry_base_ms
        .saturating_mul(1u64.checked_shl(attempt.min(30)).unwrap_or(u64::MAX))
        .min(l.retry_max_ms);
    let seed = (id.as_u128() as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(u64::from(attempt).wrapping_mul(1442695040888963407));
    1 + seed % cap.max(1)
}
pub async fn delivery_worker(
    ingress: Arc<Ingress>,
    sink: Arc<dyn DeliverySink>,
    stop: CancellationToken,
) -> Result<()> {
    let owner = Uuid::new_v4();
    let mut tick = tokio::time::interval(Duration::from_millis(
        ingress.limits.worker_poll_interval_ms,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut idle = true;
    let mut last_maintenance = Instant::now();
    let mut recovery = DependencyRecovery::new();
    'worker: loop {
        if idle {
            tokio::select! {biased;_ = stop.cancelled()=>break,_=tick.tick()=>{}}
        } else {
            tokio::select! {biased;_ = stop.cancelled()=>break,_=tokio::task::yield_now()=>{}}
        }
        let jobs = match deadline(
            ingress.limits.external_timeout_ms,
            ingress.store.claim_jobs(owner, now_ms(), 1),
        )
        .await
        {
            Ok(jobs) => {
                recovery.succeeded(&ingress.metrics);
                jobs
            }
            Err(error) => {
                if recovery
                    .failed(error, &ingress.limits, &ingress.metrics, &stop)
                    .await?
                {
                    continue;
                }
                break;
            }
        };
        idle = jobs.is_empty();
        for job in jobs {
            if stop.is_cancelled() {
                break;
            }
            let start = Instant::now();
            let result = tokio::time::timeout(
                Duration::from_millis(ingress.limits.external_timeout_ms),
                sink.deliver(&job.message),
            )
            .await;
            let (success, retryable) = match result {
                Ok(Ok(())) => (true, false),
                Ok(Err(DeliveryError::Permanent)) => (false, false),
                _ => (false, true),
            };
            ingress.metrics.inc(if success {
                Metric::DeliverySuccess
            } else {
                Metric::DeliveryFailed
            });
            ingress.metrics.add(
                Metric::DeliveryLatencyMs,
                start.elapsed().as_millis() as u64,
            );
            let now = now_ms();
            let next = now.saturating_add(retry_delay(
                &ingress.limits,
                job.message.message_id,
                job.attempts,
            ) as i64);
            if let Err(error) = deadline(
                ingress.limits.external_timeout_ms,
                ingress
                    .store
                    .finish_job(&job, success, retryable, now, next),
            )
            .await
            {
                if recovery
                    .failed(error, &ingress.limits, &ingress.metrics, &stop)
                    .await?
                {
                    continue 'worker;
                }
                break 'worker;
            }
            recovery.succeeded(&ingress.metrics);
        }
        if idle
            || last_maintenance.elapsed()
                >= Duration::from_millis(ingress.limits.worker_poll_interval_ms)
        {
            let cleanup_started = Instant::now();
            let maintenance = match deadline(
                ingress.limits.external_timeout_ms,
                ingress
                    .store
                    .maintain(now_ms(), ingress.limits.delivery_batch),
            )
            .await
            {
                Ok(stats) => stats,
                Err(error) => {
                    if recovery
                        .failed(error, &ingress.limits, &ingress.metrics, &stop)
                        .await?
                    {
                        continue;
                    }
                    break;
                }
            };
            recovery.succeeded(&ingress.metrics);
            ingress.metrics.inc(Metric::CleanupRuns);
            ingress
                .metrics
                .add(Metric::CleanupIngressRows, maintenance.ingress_deleted);
            ingress.metrics.add(
                Metric::CleanupCommandRows,
                maintenance
                    .commands_deleted
                    .saturating_add(maintenance.commands_expired),
            );
            ingress
                .metrics
                .add(Metric::CleanupJobs, maintenance.jobs_terminal);
            ingress.metrics.observe(
                Histogram::Cleanup,
                cleanup_started.elapsed().as_micros() as u64,
            );
            ingress.sessions.expire_presence(now_ms())?;
            last_maintenance = Instant::now();
        }
    }
    Ok(())
}
pub async fn command_worker(
    router: Arc<CommandRouter>,
    identities: HashMap<DeviceKey, AuthenticatedDevice>,
    stop: CancellationToken,
) -> Result<()> {
    let ingress = &router.ingress;
    let mut tick = tokio::time::interval(Duration::from_millis(
        ingress.limits.worker_poll_interval_ms,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut recovery = DependencyRecovery::new();
    'worker: loop {
        tokio::select! {biased;_=stop.cancelled()=>break,_=tick.tick()=>{}}
        let active = ingress.sessions.active_devices()?;
        for batch in active.chunks(ingress.limits.delivery_batch) {
            if stop.is_cancelled() {
                break;
            }
            let commands = match deadline(
                ingress.limits.external_timeout_ms,
                ingress
                    .store
                    .claim_command_batch(batch, now_ms(), ingress.limits.delivery_batch),
            )
            .await
            {
                Ok(commands) => {
                    recovery.succeeded(&ingress.metrics);
                    commands
                }
                Err(error) => {
                    if recovery
                        .failed(error, &ingress.limits, &ingress.metrics, &stop)
                        .await?
                    {
                        continue 'worker;
                    }
                    break 'worker;
                }
            };
            for record in commands {
                if let Some(auth) = identities.get(&record.command.device)
                    && let Err(e) = router.dispatch(record, auth).await
                {
                    tracing::debug!(error=%e,"command remains leased for bounded retry");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod audit {
    use super::*;
    #[tokio::test]
    async fn dependency_recovery_is_bounded_observable_and_cancelable() {
        let limits = Limits {
            retry_base_ms: 1,
            retry_max_ms: 1,
            ..Limits::default()
        };
        let metrics = Metrics::default();
        let stop = CancellationToken::new();
        let mut recovery = DependencyRecovery::new();

        assert!(
            recovery
                .failed(Error::Storage, &limits, &metrics, &stop)
                .await
                .unwrap()
        );
        assert_eq!(metrics.get(Metric::DependencyDegraded), 1);
        recovery.succeeded(&metrics);
        assert_eq!(metrics.get(Metric::DependencyRecovered), 1);

        assert!(matches!(
            recovery
                .failed(Error::Invalid, &limits, &metrics, &stop)
                .await,
            Err(Error::Invalid)
        ));
        stop.cancel();
        assert!(
            !recovery
                .failed(Error::Timeout, &limits, &metrics, &stop)
                .await
                .unwrap()
        );
    }

    #[test]
    fn command_backoff_is_positive_bounded_and_spread_across_ids() {
        let l = Limits {
            retry_base_ms: 3,
            retry_max_ms: 20,
            ..Limits::default()
        };
        let mut first = std::collections::HashSet::new();
        for id in 1..=64 {
            for attempt in 1..=100 {
                let delay = command_retry_delay(&l, CommandId(Uuid::from_u128(id)), attempt);
                let cap = 3u64.saturating_mul(1u64 << attempt.min(30)).min(20);
                assert!((1..=cap).contains(&delay));
                if attempt == 1 {
                    first.insert(delay);
                }
            }
        }
        assert!(first.len() > 1);
    }
}
