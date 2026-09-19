use async_trait::async_trait;
use netbaiot_core::*;
use netbaiot_runtime::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;
struct Entry {
    input: StoredIngress,
    receipt: IngressReceipt,
    expires: i64,
    job: JobState,
    charge: usize,
}
struct JobState {
    attempts: u32,
    next: i64,
    expires: i64,
    lease: Option<(Uuid, i64)>,
    done: bool,
    last_error: Option<&'static str>,
}
struct CommandEntry {
    record: CommandRecord,
    next: i64,
    retain_until: i64,
}
#[derive(Default)]
struct State {
    messages: HashMap<(DeviceKey, SourceMessageId), Entry>,
    commands: HashMap<CommandId, CommandEntry>,
    bytes: usize,
}
/// Test/development adapter. Receipts explicitly report `volatile`.
pub struct MemoryStore {
    limits: Arc<Limits>,
    state: Mutex<State>,
}
impl MemoryStore {
    pub fn new(limits: Arc<Limits>) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(State::default()),
        })
    }
    pub fn message_count(&self) -> Result<usize> {
        Ok(lock(&self.state)?.messages.len())
    }
    pub fn job_count(&self) -> Result<usize> {
        Ok(lock(&self.state)?
            .messages
            .values()
            .filter(|e| !e.job.done)
            .count())
    }
    pub fn messages(&self, limit: usize) -> Result<Vec<DeviceMessage>> {
        Ok(lock(&self.state)?
            .messages
            .values()
            .take(limit.min(self.limits.delivery_batch))
            .map(|e| e.input.message.clone())
            .collect())
    }
}
#[async_trait]
impl Store for MemoryStore {
    async fn accept(&self, input: StoredIngress) -> Result<StoreAcceptance> {
        let now = now_ms();
        let mut s = lock(&self.state)?;
        let key = (
            input.message.device.clone(),
            input.message.source_message_id.clone(),
        );
        if let Some(previous) = s.messages.get(&key)
            && previous.expires > now
        {
            if previous.input.canonical != input.canonical {
                return Err(Error::Conflict);
            }
            let mut r = previous.receipt.clone();
            r.duplicate = true;
            return Ok(StoreAcceptance {
                receipt: r,
                timings: StoreTimings::default(),
            });
        }
        if let Some(old) = s.messages.remove(&key) {
            s.bytes = s.bytes.saturating_sub(old.charge);
        }
        let charge = input
            .canonical
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(8192))
            .ok_or(Error::Overloaded)?;
        let d = &input.message.device;
        if s.messages.len() >= self.limits.max_stored_messages
            || s.bytes.saturating_add(charge) > self.limits.max_stored_bytes
            || s.messages
                .values()
                .filter(|e| e.input.message.device.tenant_id == d.tenant_id)
                .count()
                >= self.limits.max_stored_messages_per_tenant
            || s.messages
                .values()
                .filter(|e| e.input.message.device == *d)
                .count()
                >= self.limits.max_stored_messages_per_device
        {
            return Err(Error::Overloaded);
        }
        let device_bytes = s
            .messages
            .values()
            .filter(|e| e.input.message.device == *d)
            .fold(0usize, |n, e| n.saturating_add(e.charge));
        let tenant_bytes = s
            .messages
            .values()
            .filter(|e| e.input.message.device.tenant_id == d.tenant_id)
            .fold(0usize, |n, e| n.saturating_add(e.charge));
        if device_bytes.saturating_add(charge) > self.limits.max_stored_bytes_per_device
            || tenant_bytes.saturating_add(charge) > self.limits.max_stored_bytes_per_tenant
        {
            return Err(Error::Overloaded);
        }
        if let DevicePayload::CommandAck(ack) = &input.message.payload {
            let command = s.commands.get_mut(&ack.command_id).ok_or(Error::Invalid)?;
            if command.record.command.device != *d {
                return Err(Error::Forbidden);
            }
            if command.record.command.expires_at <= now {
                return Err(Error::Invalid);
            }
            command.record.execution = apply_execution(command.record.execution, ack.execution)?;
            command.record.delivery = DeliveryState::Received;
        }
        let receipt = IngressReceipt {
            message_id: input.message.message_id,
            source_message_id: input.message.source_message_id.clone(),
            accepted_at: now,
            boundary: ReceiptBoundary::Volatile,
            duplicate: false,
        };
        let job = JobState {
            attempts: 0,
            next: now,
            expires: now.saturating_add(self.limits.delivery_ttl_ms as i64),
            lease: None,
            done: false,
            last_error: None,
        };
        s.bytes += charge;
        s.messages.insert(
            key,
            Entry {
                input,
                receipt: receipt.clone(),
                expires: now.saturating_add(self.limits.dedup_ttl_ms as i64),
                job,
                charge,
            },
        );
        Ok(StoreAcceptance {
            receipt,
            timings: StoreTimings::default(),
        })
    }
    async fn claim_jobs(&self, owner: Uuid, now: i64, limit: usize) -> Result<Vec<DeliveryJob>> {
        let mut s = lock(&self.state)?;
        let mut jobs = Vec::new();
        for e in s.messages.values_mut() {
            if jobs.len() >= limit.min(self.limits.delivery_batch) {
                break;
            }
            if e.job.done
                || e.job.next > now
                || e.job.expires <= now
                || e.job.attempts >= self.limits.max_attempts
                || e.job.lease.is_some_and(|(_, until)| until > now)
            {
                continue;
            }
            e.job.attempts += 1;
            e.job.lease = Some((owner, now.saturating_add(self.limits.lease_ms as i64)));
            jobs.push(DeliveryJob {
                message: e.input.message.clone(),
                attempts: e.job.attempts,
                lease_owner: owner,
                expires_at: e.job.expires,
            });
        }
        Ok(jobs)
    }
    async fn finish_job(
        &self,
        job: &DeliveryJob,
        success: bool,
        retryable: bool,
        now: i64,
        next: i64,
    ) -> Result<()> {
        let mut s = lock(&self.state)?;
        let key = (
            job.message.device.clone(),
            job.message.source_message_id.clone(),
        );
        let e = s.messages.get_mut(&key).ok_or(Error::Invalid)?;
        if e.job
            .lease
            .is_none_or(|(owner, until)| owner != job.lease_owner || until <= now)
            || e.job.attempts != job.attempts
        {
            return Err(Error::Conflict);
        }
        e.job.done = success
            || !retryable
            || e.job.attempts >= self.limits.max_attempts
            || e.job.expires <= now;
        e.job.next = next;
        e.job.lease = None;
        e.job.last_error = if success {
            None
        } else {
            Some("delivery_failed")
        };
        Ok(())
    }
    async fn insert_command(&self, command: DeviceCommand) -> Result<CommandRecord> {
        let mut s = lock(&self.state)?;
        if let Some(old) = s.commands.get(&command.command_id) {
            if old.record.command != command {
                return Err(Error::Conflict);
            }
            return Ok(old.record.clone());
        }
        if serde_json::to_vec(&command)
            .map_err(|_| Error::Invalid)?
            .len()
            > self.limits.max_command_bytes
        {
            return Err(Error::Invalid);
        }
        if command.expires_at <= now_ms()
            || command.expires_at.saturating_sub(now_ms()) > self.limits.command_ttl_ms as i64
        {
            return Err(Error::Invalid);
        }
        if s.commands.len() >= self.limits.max_pending_commands
            || s.commands
                .values()
                .filter(|e| e.record.command.device.tenant_id == command.device.tenant_id)
                .count()
                >= self.limits.max_pending_commands_per_tenant
            || s.commands
                .values()
                .filter(|e| e.record.command.device == command.device)
                .count()
                >= self.limits.max_pending_commands_per_device
        {
            return Err(Error::Overloaded);
        }
        let retain_until = command
            .expires_at
            .saturating_add(self.limits.command_ttl_ms as i64);
        let record = CommandRecord {
            command,
            delivery: DeliveryState::Queued,
            execution: ExecutionState::Unknown,
            attempts: 0,
            lease_expires_at: None,
        };
        s.commands.insert(
            record.command.command_id,
            CommandEntry {
                record: record.clone(),
                next: now_ms(),
                retain_until,
            },
        );
        Ok(record)
    }
    async fn claim_commands(
        &self,
        device: Option<&DeviceKey>,
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>> {
        let mut s = lock(&self.state)?;
        let mut out = Vec::new();
        for c in s.commands.values_mut() {
            if out.len() >= limit.min(self.limits.delivery_batch) {
                break;
            }
            if device.is_some_and(|d| *d != c.record.command.device)
                || c.next > now
                || c.record.command.expires_at <= now
                || c.record.attempts >= self.limits.max_attempts
                || matches!(
                    c.record.execution,
                    ExecutionState::Succeeded | ExecutionState::Failed
                )
                || matches!(
                    c.record.delivery,
                    DeliveryState::Expired | DeliveryState::Failed
                )
            {
                continue;
            }
            c.record.attempts += 1;
            c.record.delivery = advance_delivery(c.record.delivery, DeliveryState::Dispatching);
            let lease_until = now.saturating_add(self.limits.lease_ms as i64);
            c.record.lease_expires_at = Some(lease_until);
            c.next = lease_until.saturating_add(worker::command_retry_delay(
                &self.limits,
                c.record.command.command_id,
                c.record.attempts,
            ) as i64);
            out.push(c.record.clone());
        }
        Ok(out)
    }
    async fn claim_command_batch(
        &self,
        devices: &[DeviceKey],
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>> {
        if devices.len() > self.limits.delivery_batch {
            return Err(Error::Invalid);
        }
        let maximum = limit.min(self.limits.delivery_batch);
        let mut out = Vec::new();
        for device in devices {
            if out.len() >= maximum {
                break;
            }
            out.extend(
                self.claim_commands(Some(device), now, maximum - out.len())
                    .await?,
            );
        }
        Ok(out)
    }
    async fn command_state(
        &self,
        device: &DeviceKey,
        id: CommandId,
        attempt: u32,
        state: DeliveryState,
    ) -> Result<bool> {
        let mut s = lock(&self.state)?;
        let c = s.commands.get_mut(&id).ok_or(Error::Invalid)?;
        if &c.record.command.device != device {
            return Err(Error::Forbidden);
        }
        if c.record.command.expires_at <= now_ms() {
            return Err(Error::Invalid);
        }
        if !matches!(state, DeliveryState::Sent | DeliveryState::Received) {
            return Err(Error::Invalid);
        }
        if attempt == 0
            || c.record.attempts != attempt
            || c.record
                .lease_expires_at
                .is_none_or(|until| until <= now_ms())
        {
            return Ok(false);
        }
        c.record.delivery = advance_delivery(c.record.delivery, state);
        Ok(true)
    }
    async fn get_command(
        &self,
        device: &DeviceKey,
        id: CommandId,
    ) -> Result<Option<CommandRecord>> {
        Ok(lock(&self.state)?
            .commands
            .get(&id)
            .filter(|c| c.record.command.device == *device)
            .map(|c| c.record.clone()))
    }
    async fn maintain(&self, now: i64, batch: usize) -> Result<MaintenanceStats> {
        let mut s = lock(&self.state)?;
        let mut stats = MaintenanceStats::default();
        let keys: Vec<_> = s
            .messages
            .iter()
            .filter(|(_, e)| e.expires <= now)
            .take(batch.min(self.limits.delivery_batch))
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys {
            if let Some(e) = s.messages.remove(&key) {
                s.bytes = s.bytes.saturating_sub(e.charge);
                stats.ingress_deleted += 1;
            }
        }
        for e in s.messages.values_mut() {
            if !e.job.done
                && (e.job.expires <= now
                    || (e.job.attempts >= self.limits.max_attempts
                        && e.job.lease.is_none_or(|(_, until)| until <= now)))
            {
                e.job.done = true;
                stats.jobs_terminal += 1;
            }
        }
        let keys: Vec<_> = s
            .commands
            .iter()
            .filter(|(_, c)| c.retain_until <= now)
            .take(batch.min(self.limits.delivery_batch))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            stats.commands_deleted += u64::from(s.commands.remove(&key).is_some());
        }
        for c in s.commands.values_mut() {
            if c.record.command.expires_at <= now
                && !matches!(
                    c.record.delivery,
                    DeliveryState::Expired | DeliveryState::Failed
                )
                && !matches!(
                    c.record.execution,
                    ExecutionState::Succeeded | ExecutionState::Failed
                )
            {
                c.record.delivery = DeliveryState::Expired;
                stats.commands_expired += 1;
            } else if c.record.attempts >= self.limits.max_attempts
                && c.next <= now
                && !matches!(
                    c.record.execution,
                    ExecutionState::Succeeded | ExecutionState::Failed
                )
            {
                c.record.delivery = DeliveryState::Failed;
            }
        }
        Ok(stats)
    }
}
