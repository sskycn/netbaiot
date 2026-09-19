use crate::*;
use async_trait::async_trait;
use netbaiot_core::*;
use uuid::Uuid;
#[derive(Clone)]
pub struct StoredIngress {
    pub message: DeviceMessage,
    pub canonical: Vec<u8>,
}
#[derive(Clone, Default)]
pub struct StoreTimings {
    pub call_to_pool_us: u64,
    pub pool_wait_us: u64,
    pub transaction_start_us: u64,
    pub quota_wait_us: u64,
    pub quota_accounting_us: u64,
    pub quota_lock_hold_us: u64,
    pub dedup_us: u64,
    pub writes_us: u64,
    pub commit_us: u64,
    pub transaction_us: u64,
}
pub struct StoreAcceptance {
    pub receipt: IngressReceipt,
    pub timings: StoreTimings,
}
impl std::ops::Deref for StoreAcceptance {
    type Target = IngressReceipt;
    fn deref(&self) -> &Self::Target {
        &self.receipt
    }
}
#[derive(Clone, Copy, Default)]
pub struct StoreHealth {
    pub pool_active: usize,
    pub pool_idle: usize,
    pub pool_waiters: usize,
}
#[derive(Clone, Copy, Default)]
pub struct MaintenanceStats {
    pub ingress_deleted: u64,
    pub commands_deleted: u64,
    pub commands_expired: u64,
    pub jobs_terminal: u64,
}
#[derive(Clone)]
pub struct DeliveryJob {
    pub message: DeviceMessage,
    pub attempts: u32,
    pub lease_owner: Uuid,
    pub expires_at: i64,
}
#[async_trait]
pub trait Store: Send + Sync {
    async fn accept(&self, input: StoredIngress) -> Result<StoreAcceptance>;
    fn health(&self) -> StoreHealth {
        StoreHealth::default()
    }
    async fn claim_jobs(&self, owner: Uuid, now: i64, limit: usize) -> Result<Vec<DeliveryJob>>;
    async fn finish_job(
        &self,
        job: &DeliveryJob,
        success: bool,
        retryable: bool,
        now: i64,
        next: i64,
    ) -> Result<()>;
    async fn insert_command(&self, command: DeviceCommand) -> Result<CommandRecord>;
    async fn claim_commands(
        &self,
        device: Option<&DeviceKey>,
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>>;
    async fn claim_command_batch(
        &self,
        devices: &[DeviceKey],
        now: i64,
        limit: usize,
    ) -> Result<Vec<CommandRecord>>;
    async fn command_state(
        &self,
        device: &DeviceKey,
        id: CommandId,
        attempt: u32,
        state: DeliveryState,
    ) -> Result<bool>;
    async fn get_command(&self, device: &DeviceKey, id: CommandId)
    -> Result<Option<CommandRecord>>;
    async fn maintain(&self, now: i64, batch: usize) -> Result<MaintenanceStats>;
}
/// Canonical identity excludes generated message ID and arrival timestamp.
pub fn canonical(message: &DeviceMessage) -> Result<Vec<u8>> {
    serde_json::to_vec(&(
        &message.device,
        &message.source_message_id,
        message.occurred_at,
        &message.payload,
    ))
    .map_err(|_| Error::Invalid)
}
pub fn apply_execution(current: ExecutionState, new: ExecutionState) -> Result<ExecutionState> {
    if new == ExecutionState::Unknown {
        return Err(Error::Invalid);
    }
    if matches!(current, ExecutionState::Succeeded | ExecutionState::Failed) && current != new {
        return Err(Error::Conflict);
    }
    Ok(new)
}
pub fn advance_delivery(current: DeliveryState, new: DeliveryState) -> DeliveryState {
    if matches!(current, DeliveryState::Expired | DeliveryState::Failed) {
        return current;
    }
    if current == DeliveryState::Received
        && matches!(new, DeliveryState::Sent | DeliveryState::Dispatching)
    {
        return current;
    }
    new
}
