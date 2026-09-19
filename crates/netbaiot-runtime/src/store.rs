use crate::*;
use async_trait::async_trait;
use netbaiot_core::*;
use uuid::Uuid;
#[derive(Clone)]
pub struct StoredIngress {
    pub message: DeviceMessage,
    pub canonical: Vec<u8>,
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
    async fn accept(&self, input: StoredIngress) -> Result<IngressReceipt>;
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
    async fn maintain(&self, now: i64, batch: usize) -> Result<()>;
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
