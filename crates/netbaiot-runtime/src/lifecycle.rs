use crate::{Error, Result};
use serde::Serialize;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum LifecycleState {
    Starting,
    Running,
    Quiescing,
    Draining,
    Spooling,
    Drained,
}

pub struct Lifecycle {
    state: AtomicU8,
    active_admissions: AtomicUsize,
    changed: tokio::sync::Notify,
}

pub struct AdmissionGuard<'a> {
    lifecycle: &'a Lifecycle,
}

impl Lifecycle {
    pub fn starting() -> Self {
        Self {
            state: AtomicU8::new(LifecycleState::Starting as u8),
            active_admissions: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }

    pub fn state(&self) -> LifecycleState {
        match self.state.load(Ordering::Acquire) {
            0 => LifecycleState::Starting,
            1 => LifecycleState::Running,
            2 => LifecycleState::Quiescing,
            3 => LifecycleState::Draining,
            4 => LifecycleState::Spooling,
            _ => LifecycleState::Drained,
        }
    }

    pub fn ready(&self) -> bool {
        self.state() == LifecycleState::Running
    }

    pub fn mark_running(&self) -> Result<()> {
        self.transition(LifecycleState::Starting, LifecycleState::Running)
    }

    pub fn begin_admission(&self) -> Result<AdmissionGuard<'_>> {
        if self.state() != LifecycleState::Running {
            return Err(Error::Draining);
        }
        self.active_admissions.fetch_add(1, Ordering::AcqRel);
        if self.state() != LifecycleState::Running {
            self.release_admission();
            return Err(Error::Draining);
        }
        Ok(AdmissionGuard { lifecycle: self })
    }

    pub async fn begin_quiesce(&self) -> Result<()> {
        self.transition(LifecycleState::Running, LifecycleState::Quiescing)?;
        while self.active_admissions.load(Ordering::Acquire) != 0 {
            self.changed.notified().await;
        }
        self.transition(LifecycleState::Quiescing, LifecycleState::Draining)
    }

    pub fn mark_spooling(&self) -> Result<()> {
        self.transition(LifecycleState::Draining, LifecycleState::Spooling)
    }

    pub fn mark_drained(&self) -> Result<()> {
        let current = self.state();
        if !matches!(current, LifecycleState::Draining | LifecycleState::Spooling) {
            return Err(Error::Conflict);
        }
        self.state
            .compare_exchange(
                current as u8,
                LifecycleState::Drained as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Conflict)?;
        self.changed.notify_waiters();
        Ok(())
    }

    fn transition(&self, from: LifecycleState, to: LifecycleState) -> Result<()> {
        self.state
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Conflict)?;
        self.changed.notify_waiters();
        Ok(())
    }

    fn release_admission(&self) {
        if self.active_admissions.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.changed.notify_waiters();
        }
    }
}

impl Drop for AdmissionGuard<'_> {
    fn drop(&mut self) {
        self.lifecycle.release_admission();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn quiesce_waits_for_active_and_closes_gate() {
        let lifecycle = Arc::new(Lifecycle::starting());
        lifecycle.mark_running().unwrap();
        let guard = lifecycle.begin_admission().unwrap();
        let owner = lifecycle.clone();
        let task = tokio::spawn(async move { owner.begin_quiesce().await });
        tokio::task::yield_now().await;
        assert_eq!(lifecycle.state(), LifecycleState::Quiescing);
        assert!(matches!(lifecycle.begin_admission(), Err(Error::Draining)));
        drop(guard);
        task.await.unwrap().unwrap();
        assert_eq!(lifecycle.state(), LifecycleState::Draining);
    }
}
