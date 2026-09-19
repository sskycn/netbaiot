use netbaiot_core::*;
use netbaiot_runtime::*;
use netbaiot_storage::{MemoryStore, PgStore};
use std::sync::Arc;
use uuid::Uuid;
fn key(name: &str) -> DeviceKey {
    DeviceKey {
        tenant_id: TenantId::new("tenant").unwrap(),
        product_id: ProductId::new("product").unwrap(),
        device_id: DeviceId::new(name).unwrap(),
    }
}
fn message(source: &str) -> StoredIngress {
    let message = DeviceMessage {
        message_id: MessageId::generate(),
        source_message_id: SourceMessageId::new(source).unwrap(),
        device: key("a"),
        received_at: now_ms(),
        occurred_at: None,
        payload: DevicePayload::Heartbeat(Heartbeat { sequence: 1 }),
    };
    StoredIngress {
        canonical: canonical(&message).unwrap(),
        message,
    }
}
fn command() -> DeviceCommand {
    DeviceCommand {
        command_id: CommandId::generate(),
        device: key("a"),
        expires_at: now_ms() + 60000,
        payload: DeviceCommandPayload {
            name: "reboot".into(),
            arguments: Default::default(),
        },
    }
}
async fn stale_command_attempt_contract(store: Arc<dyn Store>) {
    let mut c = command();
    c.expires_at = now_ms() + 180000;
    store.insert_command(c.clone()).await.unwrap();
    let now = now_ms();
    let first = store.claim_commands(Some(&c.device), now, 1).await.unwrap();
    assert_eq!(first[0].attempts, 1);
    let next = store
        .claim_commands(Some(&c.device), now + 60_001, 1)
        .await
        .unwrap();
    assert_eq!(next[0].attempts, 2);
    // Delayed PUBACK from attempt 1 must not mark attempt 2 as received.
    let _ = store
        .command_state(&c.device, c.command_id, 1, DeliveryState::Received)
        .await;
    let actual = store
        .get_command(&c.device, c.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.delivery, DeliveryState::Dispatching);
}
#[tokio::test]
async fn audit_stale_command_attempt_is_fenced() {
    stale_command_attempt_contract(MemoryStore::new(Arc::new(Limits::default()))).await;
}
async fn acceptance_contract(store: Arc<dyn Store>) {
    let input = message("source:1");
    let receipt = store.accept(input.clone()).await.unwrap();
    let mut again = input.clone();
    again.message.message_id = MessageId::generate();
    again.message.received_at += 10;
    let duplicate = store.accept(again).await.unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(receipt.message_id, duplicate.message_id);
    let mut conflict = input;
    conflict.message.payload = DevicePayload::Heartbeat(Heartbeat { sequence: 2 });
    conflict.canonical = canonical(&conflict.message).unwrap();
    assert!(matches!(store.accept(conflict).await, Err(Error::Conflict)));
    let owner = Uuid::new_v4();
    let now = now_ms();
    let jobs = store.claim_jobs(owner, now, 16).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].message.message_id, receipt.message_id);
    assert!(
        store
            .claim_jobs(Uuid::new_v4(), now, 16)
            .await
            .unwrap()
            .is_empty()
    );
    let mut stale = jobs[0].clone();
    stale.lease_owner = Uuid::new_v4();
    assert!(matches!(
        store.finish_job(&stale, true, false, now, now).await,
        Err(Error::Conflict)
    ));
    store
        .finish_job(&jobs[0], false, true, now, now + 10)
        .await
        .unwrap();
    assert!(store.claim_jobs(owner, now, 16).await.unwrap().is_empty());
    let jobs = store.claim_jobs(owner, now + 11, 16).await.unwrap();
    assert_eq!(jobs[0].attempts, 2);
    store
        .finish_job(&jobs[0], true, false, now + 12, now + 12)
        .await
        .unwrap();
    assert!(
        store
            .claim_jobs(owner, now + 13, 16)
            .await
            .unwrap()
            .is_empty()
    );
    store.maintain(now + 86_400_100, 16).await.unwrap();
    let new = store.accept(message("source:1")).await.unwrap();
    assert!(!new.duplicate);
    assert_ne!(new.message_id, receipt.message_id);
}
async fn command_contract(store: Arc<dyn Store>) {
    let c = command();
    let inserted = store.insert_command(c.clone()).await.unwrap();
    assert_eq!(inserted.delivery, DeliveryState::Queued);
    assert_eq!(
        store
            .insert_command(c.clone())
            .await
            .unwrap()
            .command
            .command_id,
        c.command_id
    );
    let mut conflict = c.clone();
    conflict.payload.name = "other".into();
    assert!(matches!(
        store.insert_command(conflict).await,
        Err(Error::Conflict)
    ));
    let claimed = store
        .claim_commands(Some(&key("a")), now_ms(), 1)
        .await
        .unwrap();
    assert_eq!(claimed[0].attempts, 1);
    assert!(
        store
            .claim_commands(Some(&key("a")), now_ms(), 1)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .command_state(&key("a"), c.command_id, 1, DeliveryState::Sent)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_command(&key("a"), c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Unknown
    );
    store
        .command_state(&key("a"), c.command_id, 1, DeliveryState::Received)
        .await
        .unwrap();
    store
        .command_state(&key("a"), c.command_id, 1, DeliveryState::Sent)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_command(&key("a"), c.command_id)
            .await
            .unwrap()
            .unwrap()
            .delivery,
        DeliveryState::Received
    );
    let mut ack = message("ack:1");
    ack.message.payload = DevicePayload::CommandAck(CommandAck {
        command_id: c.command_id,
        execution: ExecutionState::Succeeded,
    });
    ack.canonical = canonical(&ack.message).unwrap();
    let mut cross = ack.clone();
    cross.message.device = key("b");
    cross.canonical = canonical(&cross.message).unwrap();
    assert!(matches!(store.accept(cross).await, Err(Error::Forbidden)));
    store.accept(ack).await.unwrap();
    assert_eq!(
        store
            .get_command(&key("a"), c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Succeeded
    );
    let mut late = message("ack:2");
    late.message.payload = DevicePayload::CommandAck(CommandAck {
        command_id: c.command_id,
        execution: ExecutionState::Running,
    });
    late.canonical = canonical(&late.message).unwrap();
    assert!(matches!(store.accept(late).await, Err(Error::Conflict)));
    assert!(
        store
            .claim_commands(Some(&key("a")), now_ms() + 60001, 1)
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn memory_acceptance_outbox_dedup_lease_and_retention() {
    acceptance_contract(MemoryStore::new(Arc::new(Limits::default()))).await;
}
#[tokio::test]
async fn memory_command_lifecycle_and_atomic_ack() {
    command_contract(MemoryStore::new(Arc::new(Limits::default()))).await;
}
#[tokio::test]
async fn capacity_failures_do_not_create_partial_jobs_or_commands() {
    let limits = Limits {
        max_stored_messages_per_device: 1,
        max_pending_commands_per_device: 1,
        ..Limits::default()
    };
    let store = MemoryStore::new(Arc::new(limits));
    store.accept(message("1")).await.unwrap();
    assert!(matches!(
        store.accept(message("2")).await,
        Err(Error::Overloaded)
    ));
    assert_eq!(store.message_count().unwrap(), 1);
    assert_eq!(store.job_count().unwrap(), 1);
    store.insert_command(command()).await.unwrap();
    assert!(matches!(
        store.insert_command(command()).await,
        Err(Error::Overloaded)
    ));
}
#[tokio::test]
async fn delivery_attempts_are_bounded_and_stale_leases_cannot_finish() {
    let limits = Limits {
        max_attempts: 2,
        ..Limits::default()
    };
    let store = MemoryStore::new(Arc::new(limits));
    store.accept(message("1")).await.unwrap();
    let now = now_ms();
    let job = store
        .claim_jobs(Uuid::new_v4(), now, 1)
        .await
        .unwrap()
        .remove(0);
    let reclaimed = store
        .claim_jobs(Uuid::new_v4(), now + 30001, 1)
        .await
        .unwrap()
        .remove(0);
    assert!(matches!(
        store
            .finish_job(&job, true, false, now + 30002, now + 30002)
            .await,
        Err(Error::Conflict)
    ));
    store
        .finish_job(&reclaimed, false, true, now + 30002, now + 30003)
        .await
        .unwrap();
    assert!(
        store
            .claim_jobs(Uuid::new_v4(), now + 30004, 1)
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
#[ignore = "requires NETBAIOT_TEST_DATABASE_URL pointing at a fresh disposable PostgreSQL database"]
async fn postgres_transaction_and_command_contract() {
    let url = std::env::var("NETBAIOT_TEST_DATABASE_URL")
        .expect("dedicated PostgreSQL test database required");
    let store = PgStore::connect(&url, Arc::new(Limits::default()))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    acceptance_contract(store.clone()).await;
    command_contract(store.clone()).await;
    stale_command_attempt_contract(store.clone()).await;
    command_retry_backoff_contract(store.clone()).await;
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_test_job() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER reject_test_job BEFORE INSERT ON delivery_jobs FOR EACH ROW EXECUTE FUNCTION reject_test_job();").execute(&pool).await.unwrap();
    let c = command();
    store.insert_command(c.clone()).await.unwrap();
    let mut ack = message("rollback");
    ack.message.payload = DevicePayload::CommandAck(CommandAck {
        command_id: c.command_id,
        execution: ExecutionState::Succeeded,
    });
    ack.canonical = canonical(&ack.message).unwrap();
    assert!(matches!(store.accept(ack).await, Err(Error::Storage)));
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingress_messages WHERE source_message_id='rollback'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    assert_eq!(
        store
            .get_command(&key("a"), c.command_id)
            .await
            .unwrap()
            .unwrap()
            .execution,
        ExecutionState::Unknown
    );
    sqlx::raw_sql(
        "DROP TRIGGER reject_test_job ON delivery_jobs; DROP FUNCTION reject_test_job();",
    )
    .execute(&pool)
    .await
    .unwrap();
    let input = message("concurrent");
    let (left, right) = tokio::join!(store.accept(input.clone()), store.accept(input));
    let left = left.unwrap();
    let right = right.unwrap();
    assert_eq!(left.message_id, right.message_id);
    assert_ne!(left.duplicate, right.duplicate);
    pool.close().await;
}

#[tokio::test]
async fn byte_quota_isolates_device_from_other_devices() {
    let sample = message("bytes:1");
    let charge = sample.canonical.len() * 16 + 8192;
    let l = Limits {
        max_stored_bytes_per_device: charge,
        max_stored_bytes_per_tenant: charge * 4,
        max_stored_bytes: charge * 8,
        ..Limits::default()
    };
    let store = MemoryStore::new(Arc::new(l));
    store.accept(sample).await.unwrap();
    assert!(matches!(
        store.accept(message("bytes:2")).await,
        Err(Error::Overloaded)
    ));
    let mut other = message("bytes:2");
    other.message.device = key("b");
    other.canonical = canonical(&other.message).unwrap();
    store.accept(other).await.unwrap();
    assert_eq!(store.message_count().unwrap(), 2);
}

#[tokio::test]
#[ignore = "requires fresh NETBAIOT_TEST_DATABASE_URL"]
async fn audit_postgres_concurrency_leases_pool_pressure_and_cleanup() {
    let url = std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap();
    let limits = Arc::new(Limits {
        max_database_connections: 2,
        external_timeout_ms: 100,
        max_stored_messages_per_device: 40,
        ..Limits::default()
    });
    let store = PgStore::connect(&url, limits.clone()).await.unwrap();
    store.migrate().await.unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let input = message("audit-concurrent");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..32 {
        let store = store.clone();
        let input = input.clone();
        tasks.spawn(async move { store.accept(input).await });
    }
    let mut first = 0;
    while let Some(result) = tasks.join_next().await {
        let receipt = result.unwrap().unwrap();
        assert_eq!(receipt.message_id, input.message.message_id);
        first += usize::from(!receipt.duplicate);
    }
    assert_eq!(first, 1);
    let mut alternate = input.clone();
    alternate.message.payload = DevicePayload::Heartbeat(Heartbeat { sequence: 2 });
    alternate.canonical = canonical(&alternate.message).unwrap();
    for i in 0..32 {
        let store = store.clone();
        let input = if i % 2 == 0 {
            input.clone()
        } else {
            alternate.clone()
        };
        tasks.spawn(async move { store.accept(input).await });
    }
    let (mut duplicates, mut conflicts) = (0, 0);
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(r) => {
                assert!(r.duplicate);
                duplicates += 1;
            }
            Err(Error::Conflict) => conflicts += 1,
            other => panic!("unexpected result: {}", other.is_ok()),
        }
    }
    assert_eq!((duplicates, conflicts), (16, 16));
    let counts: (i64,i64) = sqlx::query_as("SELECT count(*),count(j.message_id) FROM ingress_messages m LEFT JOIN delivery_jobs j USING(message_id)").fetch_one(&pool).await.unwrap();
    assert_eq!(counts, (1, 1));
    // Crash after claim / successful external side effect before finish: reclaim same ID.
    let now = now_ms();
    let old = store
        .claim_jobs(Uuid::new_v4(), now, 1)
        .await
        .unwrap()
        .remove(0);
    let new = store
        .claim_jobs(Uuid::new_v4(), now + 30_001, 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(old.message.message_id, new.message.message_id);
    assert!(matches!(
        store.finish_job(&old, true, false, now + 30_002, now).await,
        Err(Error::Conflict)
    ));
    store
        .finish_job(&new, true, false, now + 30_002, now)
        .await
        .unwrap();
    // Block admission, saturating a two-connection pool with a fixed set of callers.
    let mut lock = pool.begin().await.unwrap();
    sqlx::query("UPDATE ingress_quota_global SET messages=messages WHERE singleton")
        .execute(&mut *lock)
        .await
        .unwrap();
    for i in 0..16 {
        let store = store.clone();
        tasks.spawn(
            async move { deadline(60, store.accept(message(&format!("pressure-{i}")))).await },
        );
    }
    while let Some(result) = tasks.join_next().await {
        assert!(result.unwrap().is_err());
    }
    lock.rollback().await.unwrap();
    // Cancelled transactions/pool waiters must release ownership and allow recovery.
    deadline(1000, store.accept(message("recovered")))
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingress_messages WHERE source_message_id LIKE 'pressure-%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    for i in 0..20 {
        store
            .accept(message(&format!("cleanup-{i}")))
            .await
            .unwrap();
    }
    store.maintain(now_ms() + 86_400_001, 4).await.unwrap();
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM ingress_messages")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        left, 18,
        "cleanup deletes exactly the requested bounded batch"
    );
    let accounted: (i64, i64) =
        sqlx::query_as("SELECT messages,bytes FROM ingress_quota_global WHERE singleton")
            .fetch_one(&pool)
            .await
            .unwrap();
    let actual: (i64, i64) =
        sqlx::query_as("SELECT count(*),coalesce(sum(charge),0)::bigint FROM ingress_messages")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        accounted, actual,
        "cleanup updates quota accounting atomically"
    );
    pool.close().await;
}

#[tokio::test]
async fn audit_late_transport_update_preserves_completed_command() {
    let store = MemoryStore::new(Arc::new(Limits::default()));
    let mut c = command();
    c.expires_at = now_ms() + 50;
    store.insert_command(c.clone()).await.unwrap();
    store
        .claim_commands(Some(&c.device), now_ms(), 1)
        .await
        .unwrap();
    let mut ack = message("completed");
    ack.message.payload = DevicePayload::CommandAck(CommandAck {
        command_id: c.command_id,
        execution: ExecutionState::Succeeded,
    });
    ack.canonical = canonical(&ack.message).unwrap();
    store.accept(ack).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(55)).await;
    assert!(
        store
            .command_state(&c.device, c.command_id, 1, DeliveryState::Sent)
            .await
            .is_err()
    );
    let actual = store
        .get_command(&c.device, c.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.delivery, DeliveryState::Received);
    assert_eq!(actual.execution, ExecutionState::Succeeded);
}
#[tokio::test]
#[ignore = "requires fresh NETBAIOT_TEST_DATABASE_URL"]
async fn audit_postgres_attempt_history_cannot_regress() {
    let url = std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap();
    let store = PgStore::connect(&url, Arc::new(Limits::default()))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    let c = command();
    store.insert_command(c.clone()).await.unwrap();
    store
        .claim_commands(Some(&c.device), now_ms(), 1)
        .await
        .unwrap();
    store
        .command_state(&c.device, c.command_id, 1, DeliveryState::Received)
        .await
        .unwrap();
    store
        .command_state(&c.device, c.command_id, 1, DeliveryState::Sent)
        .await
        .unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let state: String =
        sqlx::query_scalar("SELECT state FROM command_attempts WHERE command_id=$1 AND attempt=1")
            .bind(c.command_id.0)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "received");
    pool.close().await;
}

#[tokio::test]
#[ignore = "owned subprocess helper for audit_outbox_process_crash_recovery"]
async fn audit_outbox_child() {
    let path = std::path::PathBuf::from(std::env::var("NETBAIOT_OUTBOX_PATH").unwrap());
    let store = PgStore::connect(
        &std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap(),
        Arc::new(Limits::default()),
    )
    .await
    .unwrap();
    let job = store
        .claim_jobs(Uuid::new_v4(), now_ms(), 1)
        .await
        .unwrap()
        .remove(0);
    if std::env::var("NETBAIOT_OUTBOX_AFTER_SEND").unwrap() == "yes" {
        tokio::fs::write(
            path.with_extension("sink"),
            format!("{}\n", job.message.message_id.0),
        )
        .await
        .unwrap();
    }
    tokio::fs::write(
        path.with_extension("claimed"),
        job.message.message_id.0.to_string(),
    )
    .await
    .unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
#[ignore = "requires fresh NETBAIOT_TEST_DATABASE_URL; kills only owned children"]
async fn audit_outbox_process_crash_recovery() {
    use tokio::io::AsyncWriteExt;
    let store = PgStore::connect(
        &std::env::var("NETBAIOT_TEST_DATABASE_URL").unwrap(),
        Arc::new(Limits::default()),
    )
    .await
    .unwrap();
    store.migrate().await.unwrap();
    let directory = std::env::temp_dir().join(format!("netbaiot-outbox-crash-{}", Uuid::new_v4()));
    tokio::fs::create_dir(&directory).await.unwrap();
    for after in [false, true] {
        let path = directory.join(if after { "after" } else { "before" });
        let input = message(if after {
            "outbox-after-send"
        } else {
            "outbox-before-send"
        });
        let receipt = store.accept(input).await.unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "audit_outbox_child"])
            .env("NETBAIOT_OUTBOX_PATH", &path)
            .env(
                "NETBAIOT_OUTBOX_AFTER_SEND",
                if after { "yes" } else { "no" },
            )
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while tokio::fs::metadata(path.with_extension("claimed"))
                .await
                .is_err()
            {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        child.kill().await.unwrap();
        child.wait().await.unwrap();
        let recovery_time = now_ms() + 30_001;
        let reclaimed = store
            .claim_jobs(Uuid::new_v4(), recovery_time, 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(reclaimed.message.message_id, receipt.message_id);
        assert_eq!(reclaimed.attempts, 2);
        // Test business sink: stable identity allows dedup even after success-before-finish crash.
        let mut sink = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.with_extension("sink"))
            .await
            .unwrap();
        sink.write_all(format!("{}\n", reclaimed.message.message_id.0).as_bytes())
            .await
            .unwrap();
        drop(sink);
        store
            .finish_job(&reclaimed, true, false, recovery_time, recovery_time)
            .await
            .unwrap();
        let data = tokio::fs::read_to_string(path.with_extension("sink"))
            .await
            .unwrap();
        let ids: Vec<_> = data.lines().collect();
        assert_eq!(ids.len(), if after { 2 } else { 1 });
        assert_eq!(
            ids.into_iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            1
        );
    }
    tokio::fs::remove_dir_all(directory).await.unwrap();
}

async fn command_retry_backoff_contract(store: Arc<dyn Store>) {
    let mut c = command();
    c.expires_at = now_ms() + 180000;
    store.insert_command(c.clone()).await.unwrap();
    let now = now_ms();
    let claimed = store.claim_commands(Some(&c.device), now, 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(
        store
            .claim_commands(Some(&c.device), now + 30000, 1)
            .await
            .unwrap()
            .is_empty(),
        "lease expiry alone must not synchronize every retry"
    );
    let next = store
        .claim_commands(Some(&c.device), now + 60001, 1)
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].attempts, 2);
}
#[tokio::test]
async fn audit_command_retry_uses_bounded_backoff() {
    command_retry_backoff_contract(MemoryStore::new(Arc::new(Limits::default()))).await;
}
