use crate::*;
use netbaiot_core::*;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::{self, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

struct AcceptedCommand {
    fingerprint: [u8; 32],
    dispatch: CommandDispatch,
    retained_until: Instant,
}

enum CommandEntry {
    InFlight {
        fingerprint: [u8; 32],
        done: watch::Sender<bool>,
    },
    Accepted(AcceptedCommand),
}

struct BoundedCommandJson {
    bytes: Vec<u8>,
    maximum: usize,
}
impl Write for BoundedCommandJson {
    fn write(&mut self, chunk: &[u8]) -> io::Result<usize> {
        if chunk.len() > self.maximum.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("command exceeds byte limit"));
        }
        self.bytes.extend_from_slice(chunk);
        Ok(chunk.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct CommandDedup {
    entries: HashMap<(TenantId, CommandId), CommandEntry>,
    expirations: VecDeque<(Instant, TenantId, CommandId)>,
}

pub struct CommandRouter {
    pub ingress: Arc<Ingress>,
}

impl CommandRouter {
    pub fn new(ingress: Arc<Ingress>) -> Self {
        Self { ingress }
    }

    /// Actual local-session dispatch authority. Callers use CommandService for
    /// semantic validation and process-local idempotency before reaching this path.
    pub fn send(&self, mut command: DeviceCommand) -> Result<CommandDispatch> {
        let _gate = self.ingress.lifecycle.begin_admission()?;
        let now = now_ms();
        let maximum = now.saturating_add(self.ingress.limits.command_ttl_ms as i64);
        let expires_at = command.expires_at.unwrap_or(maximum);
        if expires_at <= now || expires_at > maximum {
            return Err(Error::Invalid);
        }
        command.expires_at = Some(expires_at);
        let endpoint = self
            .ingress
            .sessions
            .lookup(&command.device)?
            .ok_or(Error::Unavailable)?;
        if !endpoint.command_ready() {
            return Err(Error::Unavailable);
        }
        if !endpoint.auth.permissions.commands {
            return Err(Error::Forbidden);
        }
        let bytes = self
            .ingress
            .codecs
            .get(&endpoint.auth)?
            .encode(
                &EncodeContext {
                    device: &command.device,
                },
                &command,
            )
            .map_err(|_| Error::Codec)?;
        if bytes.len() > self.ingress.limits.max_command_bytes {
            return Err(Error::Invalid);
        }
        endpoint.enqueue(&command, bytes).inspect_err(|_| {
            self.ingress.metrics.inc(Metric::QueueRejects);
        })?;
        self.ingress.metrics.inc(Metric::CommandQueued);
        Ok(CommandDispatch {
            command_id: command.command_id,
            state: DeliveryState::Queued,
        })
    }

    pub fn transport_state(&self, state: DeliveryState) {
        match state {
            DeliveryState::Sent => self.ingress.metrics.inc(Metric::CommandSent),
            DeliveryState::Received => self.ingress.metrics.inc(Metric::CommandReceived),
            DeliveryState::Failed | DeliveryState::Expired => {
                self.ingress.metrics.inc(Metric::CommandFailed);
            }
            _ => {}
        }
    }
}

/// Shared semantic authority for HTTP and Business RPC. There is no persistent
/// command history: deduplication applies only within this process and retention window.
pub struct CommandService {
    router: Arc<CommandRouter>,
    dedup: Mutex<CommandDedup>,
}

impl CommandService {
    pub fn new(router: Arc<CommandRouter>) -> Self {
        Self {
            router,
            dedup: Mutex::new(CommandDedup::default()),
        }
    }

    fn prune(&self, state: &mut CommandDedup, now: Instant) {
        while state
            .expirations
            .front()
            .is_some_and(|(expiry, _, _)| *expiry <= now)
        {
            if let Some((expiry, tenant, id)) = state.expirations.pop_front()
                && matches!(state.entries.get(&(tenant.clone(), id)), Some(CommandEntry::Accepted(accepted)) if accepted.retained_until == expiry)
            {
                state.entries.remove(&(tenant, id));
                self.router
                    .ingress
                    .metrics
                    .inc(Metric::CommandDedupEvictions);
            }
        }
    }

    /// A duplicate waits on its one in-flight owner; no registry mutex crosses an
    /// await. The first successful Router::send establishes responsibility before
    /// response production. Failed dispatch removes the reservation for retry.
    pub async fn send(&self, command: DeviceCommand) -> Result<CommandDispatch> {
        if command.command_id.0.is_nil() {
            return Err(Error::Invalid);
        }
        // The caller-supplied expiry, including None, is part of the stable input.
        let mut canonical = BoundedCommandJson {
            bytes: Vec::new(),
            maximum: self.router.ingress.limits.max_command_bytes,
        };
        serde_json::to_writer(&mut canonical, &command).map_err(|_| Error::Invalid)?;
        let fingerprint: [u8; 32] = Sha256::digest(&canonical.bytes).into();
        let key = (command.device.tenant_id.clone(), command.command_id);
        let limits = &self.router.ingress.limits;
        let retention = Duration::from_millis(limits.command_dedup_ttl_ms);
        // Validate the monotonic deadline before side effects; a later checked_add
        // may use this valid fallback if the platform clock is at its limit.
        let fallback_deadline = Instant::now()
            .checked_add(retention)
            .ok_or(Error::Internal)?;
        loop {
            let (wait, reserved) = {
                let mut state = lock(&self.dedup)?;
                self.prune(&mut state, Instant::now());
                match state.entries.get(&key) {
                    Some(CommandEntry::Accepted(accepted)) => {
                        if accepted.fingerprint != fingerprint {
                            self.router
                                .ingress
                                .metrics
                                .inc(Metric::CommandDedupConflicts);
                            return Err(Error::Conflict);
                        }
                        self.router.ingress.metrics.inc(Metric::CommandDedupHits);
                        return Ok(accepted.dispatch.clone());
                    }
                    Some(CommandEntry::InFlight {
                        fingerprint: existing,
                        done,
                    }) => {
                        if *existing != fingerprint {
                            self.router
                                .ingress
                                .metrics
                                .inc(Metric::CommandDedupConflicts);
                            return Err(Error::Conflict);
                        }
                        (Some(done.subscribe()), None)
                    }
                    None => {
                        if state.entries.len() >= limits.command_dedup_max_entries {
                            self.router
                                .ingress
                                .metrics
                                .inc(Metric::CommandDedupOverloads);
                            return Err(Error::Overloaded);
                        }
                        let now = now_ms();
                        let maximum = now.saturating_add(limits.command_ttl_ms as i64);
                        let expires_at = command.expires_at.unwrap_or(maximum);
                        if expires_at <= now || expires_at > maximum {
                            return Err(Error::Invalid);
                        }
                        let (done, _) = watch::channel(false);
                        state.entries.insert(
                            key.clone(),
                            CommandEntry::InFlight {
                                fingerprint,
                                done: done.clone(),
                            },
                        );
                        self.router
                            .ingress
                            .metrics
                            .inc(Metric::CommandDedupReservations);
                        (None, Some((done, expires_at)))
                    }
                }
            };
            if let Some(mut wait) = wait {
                let _ = wait.changed().await;
                continue;
            }
            let Some((done, expires_at)) = reserved else {
                return Err(Error::Internal);
            };

            let mut prepared = command.clone();
            prepared.expires_at = Some(expires_at);
            // Synchronous, bounded admission has no cancellation point between
            // Router::send and recording its Accepted receipt.
            let result = self.router.send(prepared);
            // Once Router::send succeeds, a receipt must be recorded even if a
            // previous panicking task poisoned this mutex.
            let mut state = self
                .dedup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &result {
                Ok(dispatch) => {
                    let retained_until = Instant::now()
                        .checked_add(retention)
                        .unwrap_or(fallback_deadline);
                    state.entries.insert(
                        key.clone(),
                        CommandEntry::Accepted(AcceptedCommand {
                            fingerprint,
                            dispatch: dispatch.clone(),
                            retained_until,
                        }),
                    );
                    state
                        .expirations
                        .push_back((retained_until, key.0.clone(), key.1));
                }
                Err(_) => {
                    state.entries.remove(&key);
                }
            }
            done.send_replace(true);
            return result;
        }
    }

    /// Current bounded registry occupancy for shutdown/resource assertions.
    pub fn usage(&self) -> Result<(usize, usize)> {
        let state = lock(&self.dedup)?;
        let inflight = state
            .entries
            .values()
            .filter(|entry| matches!(entry, CommandEntry::InFlight { .. }))
            .count();
        Ok((state.entries.len(), inflight))
    }
}
