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
        .command_state(&key("a"), c.command_id, DeliveryState::Sent)
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
        .command_state(&key("a"), c.command_id, DeliveryState::Received)
        .await
        .unwrap();
    store
        .command_state(&key("a"), c.command_id, DeliveryState::Sent)
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
