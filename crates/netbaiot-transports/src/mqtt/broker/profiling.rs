//! Optional broker timing covers acquisition and every post-lock return path.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::MutexGuard,
    time::Duration,
};

// Rust field drop order releases the mutex before recording histograms.
pub(super) struct BrokerGuard<'a> {
    guard: MutexGuard<'a, BrokerState>,
    timing: GuardTiming<'a>,
}
impl Deref for BrokerGuard<'_> {
    type Target = BrokerState;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}
impl DerefMut for BrokerGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}
impl BrokerGuard<'_> {
    pub(super) fn classify(&mut self, site: BrokerProbe) {
        self.timing.site = site;
    }
}
struct GuardTiming<'a> {
    metrics: Option<&'a Metrics>,
    site: BrokerProbe,
    wait: Option<Duration>,
    held: Option<Instant>,
}
impl Drop for GuardTiming<'_> {
    fn drop(&mut self) {
        if let (Some(metrics), Some(wait), Some(held)) = (self.metrics, self.wait, self.held) {
            let hold = held.elapsed();
            metrics.observe(Histogram::BrokerLockWait, wait.as_micros() as u64);
            metrics.observe(Histogram::BrokerLockHold, hold.as_micros() as u64);
            metrics.broker_state_timing(self.site, wait.as_nanos() as u64, hold.as_nanos() as u64);
        }
    }
}
impl MqttBroker {
    pub(super) fn lock_state(&self, site: BrokerProbe) -> Result<BrokerGuard<'_>> {
        let started = self
            .metrics
            .as_ref()
            .filter(|metrics| metrics.lock_timing_enabled())
            .map(|_| Instant::now());
        let guard = lock(&self.state)?;
        let acquired = started.map(|_| Instant::now());
        Ok(BrokerGuard {
            guard,
            timing: GuardTiming {
                metrics: self.metrics.as_deref(),
                site,
                wait: started.zip(acquired).map(|(a, b)| b.duration_since(a)),
                held: acquired,
            },
        })
    }
}
