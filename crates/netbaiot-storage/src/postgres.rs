use async_trait::async_trait;
use netbaiot_core::*;
use netbaiot_runtime::*;
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
pub struct PgStore {
    pool: PgPool,
    limits: Arc<Limits>,
    pool_waiters: AtomicUsize,
}
struct PoolWaiter<'a>(&'a AtomicUsize);
impl Drop for PoolWaiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
fn db(_: sqlx::Error) -> Error {
    Error::Storage
}
fn json<T: serde::Serialize>(value: &T) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|_| Error::Invalid)
}
fn record(row: &sqlx::postgres::PgRow) -> Result<CommandRecord> {
    serde_json::from_value(row.try_get("record").map_err(db)?).map_err(|_| Error::Storage)
}
impl PgStore {
    pub async fn connect(url: &str, limits: Arc<Limits>) -> Result<Arc<Self>> {
        let pool = PgPoolOptions::new()
            .max_connections(limits.max_database_connections)
            .acquire_timeout(Duration::from_millis(limits.external_timeout_ms))
            // SQLx emits acquisition timing only when its debug target is enabled.
            // This includes connection establishment/health checks, not just waiting.
            .acquire_time_level("debug".parse().map_err(|_| Error::Configuration)?)
            .connect(url)
            .await
            .map_err(db)?;
        Ok(Arc::new(Self {
            pool,
            limits,
            pool_waiters: AtomicUsize::new(0),
        }))
    }
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("../../migrations")
            .run(&self.pool)
            .await
            .map_err(|_| Error::Storage)
    }
    async fn transaction(&self) -> Result<Transaction<'_, Postgres>> {
        self.pool_waiters.fetch_add(1, Ordering::Relaxed);
        let waiter = PoolWaiter(&self.pool_waiters);
        let mut tx = self.pool.begin().await.map_err(db)?;
        drop(waiter);
        sqlx::query(
            "SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",
        )
        .bind(format!("{}ms", self.limits.external_timeout_ms))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        Ok(tx)
    }
    /// Serializes quota admission across every process sharing this database.
    async fn admission(&self) -> Result<Transaction<'_, Postgres>> {
        let mut tx = self.transaction().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(782634291)")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        Ok(tx)
    }
    pub async fn provision(&self, credentials: &[Credential]) -> Result<()> {
        if credentials.len() > self.limits.max_devices {
            return Err(Error::Configuration);
        }
        let mut tx = self.admission().await?;
        for c in credentials {
            let k = &c.identity.device_key;
            sqlx::query("INSERT INTO tenants VALUES ($1) ON CONFLICT DO NOTHING")
                .bind(k.tenant_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("INSERT INTO products VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,product_id) DO UPDATE SET codec_id=EXCLUDED.codec_id,codec_version=EXCLUDED.codec_version").bind(k.tenant_id.as_str()).bind(k.product_id.as_str()).bind(c.identity.codec_id.as_str()).bind(i32::from(c.identity.codec_version)).execute(&mut *tx).await.map_err(db)?;
            sqlx::query("INSERT INTO devices VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
                .bind(k.tenant_id.as_str())
                .bind(k.product_id.as_str())
                .bind(k.device_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            // HMAC keys stay in the bounded configuration provider; database stores a verifier only.
            use sha2::{Digest, Sha256};
            let verifier = Sha256::digest(c.secret_hex.as_bytes()).to_vec();
            sqlx::query("INSERT INTO device_credentials VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (credential_id) DO UPDATE SET credential_version=EXCLUDED.credential_version,verifier=EXCLUDED.verifier,tenant_id=EXCLUDED.tenant_id,product_id=EXCLUDED.product_id,device_id=EXCLUDED.device_id").bind(&c.credential_id).bind(k.tenant_id.as_str()).bind(k.product_id.as_str()).bind(k.device_id.as_str()).bind(i64::from(c.identity.credential_version)).bind(verifier).execute(&mut *tx).await.map_err(db)?;
        }
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM devices")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let largest: Option<i64> = sqlx::query_scalar(
            "SELECT max(n) FROM (SELECT count(*) n FROM devices GROUP BY tenant_id) d",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let creds: i64 = sqlx::query_scalar("SELECT count(*) FROM device_credentials")
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if total > self.limits.max_devices as i64
            || creds > self.limits.max_devices as i64
            || largest.unwrap_or(0) > self.limits.max_devices_per_tenant as i64
        {
            return Err(Error::Overloaded);
        }
        tx.commit().await.map_err(db)
    }
    async fn save_command(tx: &mut Transaction<'_, Postgres>, r: &CommandRecord) -> Result<()> {
        let terminal = matches!(
            r.execution,
            ExecutionState::Succeeded | ExecutionState::Failed
        ) || matches!(r.delivery, DeliveryState::Expired | DeliveryState::Failed);
        sqlx::query("UPDATE commands SET record=$2,terminal=$3 WHERE command_id=$1")
            .bind(r.command.command_id.0)
            .bind(json(r)?)
            .bind(terminal)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        Ok(())
    }

    async fn adjust_ingress_quota(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        device: &DeviceKey,
        message_delta: i64,
        byte_delta: i64,
    ) -> Result<()> {
        // Keep all three exact counters in one server round trip. The explicit
        // CTE dependencies also give every writer the same global -> tenant ->
        // device lock order, matching retention cleanup and avoiding deadlocks.
        let counts = sqlx::query("WITH global_update AS (UPDATE ingress_quota_global SET messages=messages+$1,bytes=bytes+$2 WHERE singleton AND messages+$1 BETWEEN 0 AND $3 AND bytes+$2 BETWEEN 0 AND $4 RETURNING 1), tenant_update AS (INSERT INTO ingress_quota_tenants(tenant_id,messages,bytes) SELECT $5,$1,$2 FROM global_update WHERE $1 BETWEEN 0 AND $6 AND $2 BETWEEN 0 AND $7 ON CONFLICT (tenant_id) DO UPDATE SET messages=ingress_quota_tenants.messages+$1,bytes=ingress_quota_tenants.bytes+$2 WHERE ingress_quota_tenants.messages+$1 BETWEEN 0 AND $6 AND ingress_quota_tenants.bytes+$2 BETWEEN 0 AND $7 RETURNING 1), device_update AS (INSERT INTO ingress_quota_devices(tenant_id,product_id,device_id,messages,bytes) SELECT $5,$8,$9,$1,$2 FROM tenant_update WHERE $1 BETWEEN 0 AND $10 AND $2 BETWEEN 0 AND $11 ON CONFLICT (tenant_id,product_id,device_id) DO UPDATE SET messages=ingress_quota_devices.messages+$1,bytes=ingress_quota_devices.bytes+$2 WHERE ingress_quota_devices.messages+$1 BETWEEN 0 AND $10 AND ingress_quota_devices.bytes+$2 BETWEEN 0 AND $11 RETURNING 1) SELECT (SELECT count(*) FROM global_update) global_count,(SELECT count(*) FROM tenant_update) tenant_count,(SELECT count(*) FROM device_update) device_count")
            .bind(message_delta)
            .bind(byte_delta)
            .bind(self.limits.max_stored_messages as i64)
            .bind(self.limits.max_stored_bytes as i64)
            .bind(device.tenant_id.as_str())
            .bind(self.limits.max_stored_messages_per_tenant as i64)
            .bind(self.limits.max_stored_bytes_per_tenant as i64)
            .bind(device.product_id.as_str())
            .bind(device.device_id.as_str())
            .bind(self.limits.max_stored_messages_per_device as i64)
            .bind(self.limits.max_stored_bytes_per_device as i64)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        if counts.try_get::<i64, _>("global_count").map_err(db)? != 1
            || counts.try_get::<i64, _>("tenant_count").map_err(db)? != 1
            || counts.try_get::<i64, _>("device_count").map_err(db)? != 1
        {
            return Err(Error::Overloaded);
        }
        Ok(())
    }

    async fn reserve_command_quota(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        device: &DeviceKey,
    ) -> Result<()> {
        let counts = sqlx::query("WITH global_update AS (UPDATE command_quota_global SET commands=commands+1 WHERE singleton AND commands<$1 RETURNING 1), tenant_update AS (INSERT INTO command_quota_tenants(tenant_id,commands) SELECT $2,1 FROM global_update ON CONFLICT (tenant_id) DO UPDATE SET commands=command_quota_tenants.commands+1 WHERE command_quota_tenants.commands<$3 RETURNING 1), device_update AS (INSERT INTO command_quota_devices(tenant_id,product_id,device_id,commands) SELECT $2,$4,$5,1 FROM tenant_update ON CONFLICT (tenant_id,product_id,device_id) DO UPDATE SET commands=command_quota_devices.commands+1 WHERE command_quota_devices.commands<$6 RETURNING 1) SELECT (SELECT count(*) FROM global_update) global_count,(SELECT count(*) FROM tenant_update) tenant_count,(SELECT count(*) FROM device_update) device_count")
            .bind(self.limits.max_pending_commands as i64)
            .bind(device.tenant_id.as_str())
            .bind(self.limits.max_pending_commands_per_tenant as i64)
            .bind(device.product_id.as_str())
            .bind(device.device_id.as_str())
            .bind(self.limits.max_pending_commands_per_device as i64)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
        if counts.try_get::<i64, _>("global_count").map_err(db)? != 1
            || counts.try_get::<i64, _>("tenant_count").map_err(db)? != 1
            || counts.try_get::<i64, _>("device_count").map_err(db)? != 1
        {
            return Err(Error::Overloaded);
        }
        Ok(())
    }
}
#[async_trait]
impl Store for PgStore {
    fn health(&self) -> StoreHealth {
        let size = self.pool.size() as usize;
        let idle = self.pool.num_idle();
        StoreHealth {
            pool_active: size.saturating_sub(idle),
            pool_idle: idle,
            pool_waiters: self.pool_waiters.load(Ordering::Relaxed),
        }
    }

    async fn accept(&self, input: StoredIngress) -> Result<StoreAcceptance> {
        let transaction_started = Instant::now();
        let mut timings = StoreTimings::default();
        let now = now_ms();
        self.pool_waiters.fetch_add(1, Ordering::Relaxed);
        let waiter = PoolWaiter(&self.pool_waiters);
        let pool_started = Instant::now();
        timings.call_to_pool_us = transaction_started.elapsed().as_micros() as u64;
        let mut connection = self.pool.acquire().await.map_err(db)?;
        timings.pool_wait_us = pool_started.elapsed().as_micros() as u64;
        drop(waiter);
        let begin_started = Instant::now();
        let mut tx = connection.begin().await.map_err(db)?;
        sqlx::query(
            "SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",
        )
        .bind(format!("{}ms", self.limits.external_timeout_ms))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        timings.transaction_start_us = begin_started.elapsed().as_micros() as u64;
        let k = &input.message.device;
        let dedup_started = Instant::now();
        let expired = sqlx::query("DELETE FROM ingress_messages WHERE tenant_id=$1 AND product_id=$2 AND device_id=$3 AND source_message_id=$4 AND expires_at<=$5 RETURNING charge").bind(k.tenant_id.as_str()).bind(k.product_id.as_str()).bind(k.device_id.as_str()).bind(input.message.source_message_id.as_str()).bind(now).fetch_optional(&mut *tx).await.map_err(db)?;
        let old=sqlx::query("SELECT message_id,canonical,accepted_at FROM ingress_messages WHERE tenant_id=$1 AND product_id=$2 AND device_id=$3 AND source_message_id=$4").bind(k.tenant_id.as_str()).bind(k.product_id.as_str()).bind(k.device_id.as_str()).bind(input.message.source_message_id.as_str()).fetch_optional(&mut *tx).await.map_err(db)?;
        timings.dedup_us = dedup_started.elapsed().as_micros() as u64;
        if let Some(row) = old {
            let data: Vec<u8> = row.try_get("canonical").map_err(db)?;
            if data != input.canonical {
                return Err(Error::Conflict);
            }
            let receipt = IngressReceipt {
                message_id: MessageId(row.try_get("message_id").map_err(db)?),
                source_message_id: input.message.source_message_id,
                accepted_at: row.try_get("accepted_at").map_err(db)?,
                boundary: ReceiptBoundary::Durable,
                duplicate: true,
            };
            let commit_started = Instant::now();
            tx.commit().await.map_err(db)?;
            timings.commit_us = commit_started.elapsed().as_micros() as u64;
            timings.transaction_us = transaction_started.elapsed().as_micros() as u64;
            return Ok(StoreAcceptance { receipt, timings });
        }
        let charge = input
            .canonical
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(8192))
            .ok_or(Error::Overloaded)? as i64;
        let writes_started = Instant::now();
        let inserted = sqlx::query("INSERT INTO ingress_messages VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (tenant_id,product_id,device_id,source_message_id) DO NOTHING RETURNING message_id")
            .bind(input.message.message_id.0)
            .bind(k.tenant_id.as_str())
            .bind(k.product_id.as_str())
            .bind(k.device_id.as_str())
            .bind(input.message.source_message_id.as_str())
            .bind(json(&input.message)?)
            .bind(&input.canonical)
            .bind(charge)
            .bind(now)
            .bind(now.saturating_add(self.limits.dedup_ttl_ms as i64))
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        if inserted.is_none() {
            let row=sqlx::query("SELECT message_id,canonical,accepted_at FROM ingress_messages WHERE tenant_id=$1 AND product_id=$2 AND device_id=$3 AND source_message_id=$4").bind(k.tenant_id.as_str()).bind(k.product_id.as_str()).bind(k.device_id.as_str()).bind(input.message.source_message_id.as_str()).fetch_one(&mut *tx).await.map_err(db)?;
            let data: Vec<u8> = row.try_get("canonical").map_err(db)?;
            if data != input.canonical {
                return Err(Error::Conflict);
            }
            let receipt = IngressReceipt {
                message_id: MessageId(row.try_get("message_id").map_err(db)?),
                source_message_id: input.message.source_message_id,
                accepted_at: row.try_get("accepted_at").map_err(db)?,
                boundary: ReceiptBoundary::Durable,
                duplicate: true,
            };
            timings.writes_us = writes_started.elapsed().as_micros() as u64;
            let commit_started = Instant::now();
            tx.commit().await.map_err(db)?;
            timings.commit_us = commit_started.elapsed().as_micros() as u64;
            timings.transaction_us = transaction_started.elapsed().as_micros() as u64;
            return Ok(StoreAcceptance { receipt, timings });
        }
        let expired_charge = expired
            .as_ref()
            .map(|row| row.try_get::<i64, _>("charge").map_err(db))
            .transpose()?
            .unwrap_or(0);
        if let DevicePayload::CommandAck(ack) = &input.message.payload {
            let row = sqlx::query("SELECT record FROM commands WHERE command_id=$1 FOR UPDATE")
                .bind(ack.command_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?
                .ok_or(Error::Invalid)?;
            let mut r = record(&row)?;
            if r.command.device != *k {
                return Err(Error::Forbidden);
            }
            if r.command.expires_at <= now {
                return Err(Error::Invalid);
            }
            r.execution = apply_execution(r.execution, ack.execution)?;
            r.delivery = DeliveryState::Received;
            Self::save_command(&mut tx, &r).await?;
        }
        sqlx::query(
            "INSERT INTO delivery_jobs(message_id,next_attempt_at,expires_at) VALUES ($1,$2,$3)",
        )
        .bind(input.message.message_id.0)
        .bind(now)
        .bind(now.saturating_add(self.limits.delivery_ttl_ms as i64))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        timings.writes_us = writes_started.elapsed().as_micros() as u64;
        // Take the globally contended quota row only after all independent
        // persistence work. PostgreSQL holds an UPDATE row lock until commit;
        // charging earlier serialized the command-ack/outbox writes behind it.
        // The charge remains in this transaction, so rollback still releases
        // both the durable rows and their accounting exactly once.
        let quota_started = Instant::now();
        self.adjust_ingress_quota(
            &mut tx,
            k,
            i64::from(expired.is_none()),
            charge.saturating_sub(expired_charge),
        )
        .await?;
        timings.quota_accounting_us = quota_started.elapsed().as_micros() as u64;
        timings.quota_wait_us = timings.quota_accounting_us;
        let commit_started = Instant::now();
        tx.commit().await.map_err(db)?;
        timings.commit_us = commit_started.elapsed().as_micros() as u64;
        // PostgreSQL does not expose the instant at which the row lock was
        // granted to SQLx. Statement-start through COMMIT is a conservative
        // upper bound for the quota critical section; QuotaAccounting retains
        // the statement's wait + execution time separately.
        timings.quota_lock_hold_us = quota_started.elapsed().as_micros() as u64;
        timings.transaction_us = transaction_started.elapsed().as_micros() as u64;
        Ok(StoreAcceptance {
            receipt: IngressReceipt {
                message_id: input.message.message_id,
                source_message_id: input.message.source_message_id,
                accepted_at: now,
                boundary: ReceiptBoundary::Durable,
                duplicate: false,
            },
            timings,
        })
    }
    async fn claim_jobs(&self, owner: Uuid, now: i64, limit: usize) -> Result<Vec<DeliveryJob>> {
        let mut tx = self.transaction().await?;
        let rows=sqlx::query("WITH selected AS (SELECT message_id FROM delivery_jobs WHERE NOT done AND next_attempt_at<=$1 AND expires_at>$1 AND attempts<$2 AND (lease_expiry IS NULL OR lease_expiry<=$1) ORDER BY next_attempt_at LIMIT $3 FOR UPDATE SKIP LOCKED), claimed AS (UPDATE delivery_jobs j SET attempts=attempts+1,lease_owner=$4,lease_expiry=$5 FROM selected s WHERE j.message_id=s.message_id RETURNING j.message_id,j.attempts,j.expires_at) SELECT c.attempts,c.expires_at,m.message FROM claimed c JOIN ingress_messages m USING(message_id)")
            .bind(now).bind(self.limits.max_attempts as i32).bind(limit.min(self.limits.delivery_batch) as i64).bind(owner).bind(now.saturating_add(self.limits.lease_ms as i64)).fetch_all(&mut *tx).await.map_err(db)?;
        let jobs = rows
            .iter()
            .map(|r| {
                Ok(DeliveryJob {
                    message: serde_json::from_value(r.try_get("message").map_err(db)?)
                        .map_err(|_| Error::Storage)?,
                    attempts: r.try_get::<i32, _>("attempts").map_err(db)? as u32,
                    lease_owner: owner,
                    expires_at: r.try_get("expires_at").map_err(db)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await.map_err(db)?;
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
        let done = success
            || !retryable
            || job.attempts >= self.limits.max_attempts
            || job.expires_at <= now;
        let mut tx = self.transaction().await?;
        let result=sqlx::query("UPDATE delivery_jobs SET done=$1,next_attempt_at=$2,lease_owner=NULL,lease_expiry=NULL,last_error=$3 WHERE message_id=$4 AND lease_owner=$5 AND attempts=$6 AND lease_expiry>$7").bind(done).bind(next).bind(if success{None}else{Some("delivery_failed")}).bind(job.message.message_id.0).bind(job.lease_owner).bind(job.attempts as i32).bind(now).execute(&mut *tx).await.map_err(db)?;
        if result.rows_affected() != 1 {
            return Err(Error::Conflict);
        }
        tx.commit().await.map_err(db)
    }
    async fn insert_command(&self, command: DeviceCommand) -> Result<CommandRecord> {
        let now = now_ms();
        if command.expires_at <= now
            || command.expires_at.saturating_sub(now) > self.limits.command_ttl_ms as i64
            || serde_json::to_vec(&command)
                .map_err(|_| Error::Invalid)?
                .len()
                > self.limits.max_command_bytes
        {
            return Err(Error::Invalid);
        }
        let mut tx = self.transaction().await?;
        let r = CommandRecord {
            command,
            delivery: DeliveryState::Queued,
            execution: ExecutionState::Unknown,
            attempts: 0,
            lease_expires_at: None,
        };
        let k = &r.command.device;
        let inserted = sqlx::query("INSERT INTO commands VALUES ($1,$2,$3,$4,$5,$6,$7,$8,false) ON CONFLICT (command_id) DO NOTHING RETURNING command_id")
            .bind(r.command.command_id.0)
            .bind(k.tenant_id.as_str())
            .bind(k.product_id.as_str())
            .bind(k.device_id.as_str())
            .bind(json(&r)?)
            .bind(now)
            .bind(r.command.expires_at)
            .bind(
                r.command
                    .expires_at
                    .saturating_add(self.limits.command_ttl_ms as i64),
            )
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        if inserted.is_none() {
            let row = sqlx::query("SELECT record FROM commands WHERE command_id=$1")
                .bind(r.command.command_id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            let old = record(&row)?;
            if old.command != r.command {
                return Err(Error::Conflict);
            }
            return Ok(old);
        }
        self.reserve_command_quota(&mut tx, k).await?;
        tx.commit().await.map_err(db)?;
        Ok(r)
    }
    async fn claim_commands(
        &self,
        device: Option<&DeviceKey>,
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>> {
        let device = device.ok_or(Error::Invalid)?;
        self.claim_command_batch(std::slice::from_ref(device), now, limit)
            .await
    }
    async fn claim_command_batch(
        &self,
        devices: &[DeviceKey],
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>> {
        if devices.is_empty() {
            return Ok(Vec::new());
        }
        if devices.len() > self.limits.delivery_batch {
            return Err(Error::Invalid);
        }
        let mut tx = self.transaction().await?;
        let rows=sqlx::query("SELECT record FROM commands WHERE NOT terminal AND expires_at>$1 AND next_attempt_at<=$1 AND (tenant_id,product_id,device_id) IN (SELECT d.tenant_id,d.product_id,d.device_id FROM jsonb_to_recordset($2) AS d(tenant_id text,product_id text,device_id text)) ORDER BY next_attempt_at LIMIT $3 FOR UPDATE SKIP LOCKED").bind(now).bind(json(&devices)?).bind(limit.min(self.limits.delivery_batch) as i64).fetch_all(&mut *tx).await.map_err(db)?;
        let mut out = Vec::new();
        for row in rows {
            let mut r = record(&row)?;
            if r.attempts >= self.limits.max_attempts {
                r.delivery = DeliveryState::Failed;
                Self::save_command(&mut tx, &r).await?;
                continue;
            }
            r.attempts += 1;
            r.lease_expires_at = Some(now.saturating_add(self.limits.lease_ms as i64));
            r.delivery = advance_delivery(r.delivery, DeliveryState::Dispatching);
            Self::save_command(&mut tx, &r).await?;
            sqlx::query("UPDATE commands SET next_attempt_at=$2 WHERE command_id=$1")
                .bind(r.command.command_id.0)
                .bind(
                    now.saturating_add(self.limits.lease_ms as i64)
                        .saturating_add(worker::command_retry_delay(
                            &self.limits,
                            r.command.command_id,
                            r.attempts,
                        ) as i64),
                )
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            sqlx::query("INSERT INTO command_attempts VALUES ($1,$2,$3,'dispatching')")
                .bind(r.command.command_id.0)
                .bind(r.attempts as i32)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            out.push(r);
        }
        tx.commit().await.map_err(db)?;
        Ok(out)
    }
    async fn command_state(
        &self,
        device: &DeviceKey,
        id: CommandId,
        attempt: u32,
        state: DeliveryState,
    ) -> Result<bool> {
        let mut tx = self.transaction().await?;
        let row = sqlx::query("SELECT record FROM commands WHERE command_id=$1 FOR UPDATE")
            .bind(id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(Error::Invalid)?;
        let mut r = record(&row)?;
        if &r.command.device != device {
            return Err(Error::Forbidden);
        }
        if r.command.expires_at <= now_ms() {
            return Err(Error::Invalid);
        }
        if !matches!(state, DeliveryState::Sent | DeliveryState::Received) {
            return Err(Error::Invalid);
        }
        if attempt == 0
            || r.attempts != attempt
            || r.lease_expires_at.is_none_or(|until| until <= now_ms())
        {
            return Ok(false);
        }
        r.delivery = advance_delivery(r.delivery, state);
        Self::save_command(&mut tx, &r).await?;
        sqlx::query("UPDATE command_attempts SET state=$3 WHERE command_id=$1 AND attempt=$2")
            .bind(id.0)
            .bind(r.attempts as i32)
            .bind(
                serde_json::to_value(r.delivery)
                    .map_err(|_| Error::Invalid)?
                    .as_str()
                    .ok_or(Error::Invalid)?,
            )
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    async fn get_command(
        &self,
        device: &DeviceKey,
        id: CommandId,
    ) -> Result<Option<CommandRecord>> {
        let mut tx = self.transaction().await?;
        let row=sqlx::query("SELECT record FROM commands WHERE command_id=$1 AND tenant_id=$2 AND product_id=$3 AND device_id=$4").bind(id.0).bind(device.tenant_id.as_str()).bind(device.product_id.as_str()).bind(device.device_id.as_str()).fetch_optional(&mut *tx).await.map_err(db)?;
        row.as_ref().map(record).transpose()
    }
    async fn maintain(&self, now: i64, batch: usize) -> Result<MaintenanceStats> {
        let mut tx = self.transaction().await?;
        let batch = batch.min(self.limits.delivery_batch) as i64;
        let ingress_deleted: i64 = sqlx::query_scalar("WITH deleted AS (DELETE FROM ingress_messages WHERE message_id IN (SELECT message_id FROM ingress_messages WHERE expires_at<=$1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) RETURNING tenant_id,product_id,device_id,charge), global_delta AS (SELECT count(*) messages,coalesce(sum(charge),0) bytes FROM deleted HAVING count(*)>0), global_update AS (UPDATE ingress_quota_global q SET messages=q.messages-d.messages,bytes=q.bytes-d.bytes FROM global_delta d WHERE q.singleton RETURNING 1), tenant_delta AS (SELECT tenant_id,count(*) messages,sum(charge) bytes FROM deleted GROUP BY tenant_id), tenant_update AS (UPDATE ingress_quota_tenants q SET messages=q.messages-d.messages,bytes=q.bytes-d.bytes FROM tenant_delta d CROSS JOIN global_update g WHERE q.tenant_id=d.tenant_id RETURNING q.tenant_id), device_delta AS (SELECT tenant_id,product_id,device_id,count(*) messages,sum(charge) bytes FROM deleted GROUP BY tenant_id,product_id,device_id), device_update AS (UPDATE ingress_quota_devices q SET messages=q.messages-d.messages,bytes=q.bytes-d.bytes FROM device_delta d JOIN tenant_update t USING(tenant_id) WHERE q.tenant_id=d.tenant_id AND q.product_id=d.product_id AND q.device_id=d.device_id RETURNING 1) SELECT count(*) FROM deleted")
            .bind(now)
            .bind(batch)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let commands_deleted: i64 = sqlx::query_scalar("WITH deleted AS (DELETE FROM commands WHERE command_id IN (SELECT command_id FROM commands WHERE retain_until<=$1 ORDER BY retain_until LIMIT $2 FOR UPDATE SKIP LOCKED) RETURNING tenant_id,product_id,device_id), global_delta AS (SELECT count(*) commands FROM deleted HAVING count(*)>0), global_update AS (UPDATE command_quota_global q SET commands=q.commands-d.commands FROM global_delta d WHERE q.singleton RETURNING 1), tenant_delta AS (SELECT tenant_id,count(*) commands FROM deleted GROUP BY tenant_id), tenant_update AS (UPDATE command_quota_tenants q SET commands=q.commands-d.commands FROM tenant_delta d CROSS JOIN global_update g WHERE q.tenant_id=d.tenant_id RETURNING q.tenant_id), device_delta AS (SELECT tenant_id,product_id,device_id,count(*) commands FROM deleted GROUP BY tenant_id,product_id,device_id), device_update AS (UPDATE command_quota_devices q SET commands=q.commands-d.commands FROM device_delta d JOIN tenant_update t USING(tenant_id) WHERE q.tenant_id=d.tenant_id AND q.product_id=d.product_id AND q.device_id=d.device_id RETURNING 1) SELECT count(*) FROM deleted").bind(now).bind(batch).fetch_one(&mut *tx).await.map_err(db)?;
        let rows=sqlx::query("SELECT record FROM commands WHERE NOT terminal AND expires_at<=$1 LIMIT $2 FOR UPDATE SKIP LOCKED").bind(now).bind(batch).fetch_all(&mut *tx).await.map_err(db)?;
        let commands_expired = rows.len() as u64;
        for row in rows {
            let mut r = record(&row)?;
            r.delivery = DeliveryState::Expired;
            Self::save_command(&mut tx, &r).await?;
        }
        let jobs_terminal = sqlx::query("UPDATE delivery_jobs SET done=true,last_error='expired_or_exhausted' WHERE message_id IN (SELECT message_id FROM delivery_jobs WHERE NOT done AND (expires_at<=$1 OR (attempts>=$2 AND (lease_expiry IS NULL OR lease_expiry<=$1))) LIMIT $3 FOR UPDATE SKIP LOCKED)").bind(now).bind(self.limits.max_attempts as i32).bind(batch).execute(&mut *tx).await.map_err(db)?.rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(MaintenanceStats {
            ingress_deleted: ingress_deleted as u64,
            commands_deleted: commands_deleted as u64,
            commands_expired,
            jobs_terminal,
        })
    }
}
