use crate::*;
use netbaiot_core::*;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    io::{self, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct AcceptedCommand {
    fingerprint: [u8; 32],
    dispatch: CommandDispatch,
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
    accepted: HashMap<(TenantId, CommandId), AcceptedCommand>,
    expirations: VecDeque<(Instant, TenantId, CommandId)>,
}

pub struct CommandRouter {
    pub ingress: Arc<Ingress>,
    dedup: Mutex<CommandDedup>,
}

impl CommandRouter {
    pub fn new(ingress: Arc<Ingress>) -> Self {
        Self {
            ingress,
            dedup: Mutex::new(CommandDedup::default()),
        }
    }

    /// Commands are admitted only to a currently live local session. There is no
    /// offline queue and no persistence fallback. The registry lock precedes session
    /// locks; session code never calls back into this registry. Holding it through the
    /// synchronous admission prevents two identical IDs from reaching a device twice.
    pub fn send(&self, command: DeviceCommand) -> Result<CommandDispatch> {
        if command.command_id.0.is_nil() {
            return Err(Error::Invalid);
        }
        // BTreeMap arguments give deterministic JSON bytes. Hash the caller's expiry,
        // before send_inner fills in its default TTL.
        let mut canonical = BoundedCommandJson {
            bytes: Vec::new(),
            maximum: self.ingress.limits.max_command_bytes,
        };
        serde_json::to_writer(&mut canonical, &command).map_err(|_| Error::Invalid)?;
        let fingerprint: [u8; 32] = Sha256::digest(&canonical.bytes).into();
        let key = (command.device.tenant_id.clone(), command.command_id);
        let mut dedup = self.dedup.lock().map_err(|_| Error::Internal)?;
        let now = Instant::now();
        while dedup
            .expirations
            .front()
            .is_some_and(|(expiry, _, _)| *expiry <= now)
        {
            if let Some((_, tenant, id)) = dedup.expirations.pop_front() {
                dedup.accepted.remove(&(tenant, id));
            }
        }
        if let Some(accepted) = dedup.accepted.get(&key) {
            return if accepted.fingerprint == fingerprint {
                Ok(accepted.dispatch.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        if dedup.accepted.len() >= self.ingress.limits.max_pending_commands {
            return Err(Error::Overloaded);
        }
        let dispatch = self.send_inner(command)?;
        let ttl = Duration::from_millis(self.ingress.limits.command_ttl_ms);
        dedup.accepted.insert(
            key.clone(),
            AcceptedCommand {
                fingerprint,
                dispatch: dispatch.clone(),
            },
        );
        dedup.expirations.push_back((now + ttl, key.0, key.1));
        Ok(dispatch)
    }

    fn send_inner(&self, mut command: DeviceCommand) -> Result<CommandDispatch> {
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
