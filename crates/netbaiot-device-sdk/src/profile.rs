use crate::mqtt;
use futures_core::Stream;
use netbaiot_protocol::*;
use std::{
    collections::BTreeMap,
    fmt,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use url::Url;

pub(super) const DEFAULT_MAX_PAYLOAD_BYTES: usize = 65_536;
const DEFAULT_MQTT_QUEUE_ITEMS: usize = 16;
const DEFAULT_COMMAND_ITEMS: usize = 16;

pub struct DeviceCredentials {
    pub(super) credential_id: String,
    pub(super) secret: String,
}
impl DeviceCredentials {
    pub fn new(
        credential_id: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, DeviceSdkError> {
        let credential_id = credential_id.into();
        let secret = secret.into();
        if DeviceId::new(&credential_id).is_err() || secret.is_empty() || secret.len() > 256 {
            return Err(DeviceSdkError::InvalidConfiguration(
                "invalid credential identifier or secret length".into(),
            ));
        }
        Ok(Self {
            credential_id,
            secret,
        })
    }
}
impl fmt::Debug for DeviceCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceCredentials")
            .field("credential_id", &"[REDACTED]")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfflinePublishPolicy {
    Reject,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceReconnectPolicy {
    pub initial_backoff: Duration,
    pub maximum_backoff: Duration,
}
impl Default for DeviceReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            maximum_backoff: Duration::from_secs(5),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishQos {
    AtMostOnce,
    AtLeastOnce,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MqttProtocolVersion {
    #[default]
    V311,
    V5,
}

#[derive(Debug, Error, Clone)]
pub enum DeviceSdkError {
    #[error("invalid device SDK configuration: {0}")]
    InvalidConfiguration(String),
    #[error("device is not connected; offline buffering is disabled")]
    Offline,
    #[error("bounded device SDK queue is full")]
    Overloaded,
    #[error("authentication failed")]
    Unauthenticated,
    #[error("operation is forbidden")]
    Forbidden,
    #[error("operation timed out")]
    Timeout,
    #[error("server is unavailable")]
    ServerUnavailable,
    #[error("broker session state does not match this client instance")]
    SessionStateMismatch,
    #[error("invalid protocol response: {0}")]
    Protocol(String),
    #[error("transport failure: {0}")]
    Transport(String),
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeviceMetrics {
    pub mqtt_reconnects: u64,
    pub publishes_accepted: u64,
    pub commands_received: u64,
}
#[derive(Default)]
pub(super) struct Metrics {
    pub mqtt_reconnects: AtomicU64,
    pub publishes_accepted: AtomicU64,
    pub commands_received: AtomicU64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishResult {
    Written,
    Puback,
    Rejected(u8),
    SessionLost,
    Expired,
    Uncertain,
}
#[derive(Clone, Debug)]
pub struct PublishReceipt {
    pub source_message_id: SourceMessageId,
    pub result: PublishResult,
}
#[derive(Clone, Debug)]
pub(super) enum MqttConnectionState {
    Connecting,
    Connected,
    Terminal(DeviceSdkError),
    Closed,
}
pub(super) struct Outbound {
    pub payload: Vec<u8>,
    pub qos: PublishQos,
    pub suffix: &'static str,
    pub source_message_id: SourceMessageId,
    pub expiry: Option<u32>,
    pub admitted: tokio::time::Instant,
    pub _permit: OwnedSemaphorePermit,
}
pub(super) struct CommandEnvelope {
    pub command: DeviceCommand,
    pub _permit: OwnedSemaphorePermit,
}
pub(super) struct Admission {
    pub ready: bool,
    pub closing: bool,
    pub sender: mpsc::Sender<Outbound>,
}
struct DeviceInner {
    device: DeviceKey,
    admission: Arc<Mutex<Admission>>,
    connection_state: watch::Receiver<MqttConnectionState>,
    command_receiver: Mutex<Option<mpsc::Receiver<CommandEnvelope>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
    receipts: broadcast::Sender<PublishReceipt>,
    max_payload_bytes: usize,
    message_expiry_interval: Option<u32>,
    publish_bytes: Arc<Semaphore>,
    total_publish_bytes: usize,
}
impl Drop for DeviceInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Ok(mut task) = self.task.lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
    }
}
#[derive(Clone)]
pub struct DeviceClient {
    inner: Arc<DeviceInner>,
}
impl fmt::Debug for DeviceClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceClient")
            .field("device", &self.inner.device)
            .field("mqtt_connected", &self.mqtt_connected())
            .finish_non_exhaustive()
    }
}

pub struct DeviceClientBuilder {
    device: Option<DeviceKey>,
    credentials: Option<DeviceCredentials>,
    mqtt_endpoint: Option<String>,
    client_id: Option<String>,
    mqtt_connect_timeout: Duration,
    max_payload_bytes: usize,
    max_packet_bytes: usize,
    mqtt_queue_items: usize,
    command_items: usize,
    command_bytes: usize,
    inflight_items: usize,
    offline_policy: OfflinePublishPolicy,
    reconnect: DeviceReconnectPolicy,
    protocol_version: MqttProtocolVersion,
    session_expiry_interval: u32,
    message_expiry_interval: Option<u32>,
    ca_pem: Option<PathBuf>,
}
impl fmt::Debug for DeviceClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceClientBuilder")
            .field("device", &self.device)
            .field("mqtt_endpoint", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}
impl Default for DeviceClientBuilder {
    fn default() -> Self {
        Self {
            device: None,
            credentials: None,
            mqtt_endpoint: None,
            client_id: None,
            mqtt_connect_timeout: Duration::from_secs(10),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            max_packet_bytes: 131_072,
            mqtt_queue_items: DEFAULT_MQTT_QUEUE_ITEMS,
            command_items: DEFAULT_COMMAND_ITEMS,
            command_bytes: 1_048_576,
            inflight_items: 16,
            offline_policy: OfflinePublishPolicy::Reject,
            reconnect: DeviceReconnectPolicy::default(),
            protocol_version: MqttProtocolVersion::V311,
            session_expiry_interval: 3_600,
            message_expiry_interval: None,
            ca_pem: None,
        }
    }
}
impl DeviceClientBuilder {
    pub fn protocol_version(mut self, v: MqttProtocolVersion) -> Self {
        self.protocol_version = v;
        self
    }
    pub fn session_expiry_interval(mut self, v: u32) -> Self {
        self.session_expiry_interval = v;
        self
    }
    pub fn message_expiry_interval(mut self, v: Option<u32>) -> Self {
        self.message_expiry_interval = v;
        self
    }
    pub fn device(mut self, v: DeviceKey) -> Self {
        self.device = Some(v);
        self
    }
    pub fn credentials(mut self, v: DeviceCredentials) -> Self {
        self.credentials = Some(v);
        self
    }
    pub fn mqtt_endpoint(mut self, v: impl Into<String>) -> Self {
        self.mqtt_endpoint = Some(v.into());
        self
    }
    pub fn client_id(mut self, v: impl Into<String>) -> Self {
        self.client_id = Some(v.into());
        self
    }
    pub fn mqtt_connect_timeout(mut self, v: Duration) -> Self {
        self.mqtt_connect_timeout = v;
        self
    }
    pub fn max_payload_bytes(mut self, v: usize) -> Self {
        self.max_payload_bytes = v;
        self
    }
    pub fn max_packet_bytes(mut self, v: usize) -> Self {
        self.max_packet_bytes = v;
        self
    }
    pub fn mqtt_queue_items(mut self, v: usize) -> Self {
        self.mqtt_queue_items = v;
        self
    }
    pub fn command_buffer_items(mut self, v: usize) -> Self {
        self.command_items = v;
        self
    }
    pub fn command_buffer_bytes(mut self, v: usize) -> Self {
        self.command_bytes = v;
        self
    }
    pub fn qos1_inflight_items(mut self, v: usize) -> Self {
        self.inflight_items = v;
        self
    }
    pub fn offline_publish_policy(mut self, v: OfflinePublishPolicy) -> Self {
        self.offline_policy = v;
        self
    }
    pub fn reconnect_policy(mut self, v: DeviceReconnectPolicy) -> Self {
        self.reconnect = v;
        self
    }
    pub fn mqtt_ca_pem(mut self, v: impl Into<PathBuf>) -> Self {
        self.ca_pem = Some(v.into());
        self
    }

    pub async fn connect(self) -> Result<DeviceClient, DeviceSdkError> {
        let device = self.device.clone().ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("device identity is required".into())
        })?;
        let credentials = self.credentials.as_ref().ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("device credentials are required".into())
        })?;
        let endpoint = self.mqtt_endpoint.as_ref().ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("mqtt_endpoint is required".into())
        })?;
        let url = Url::parse(endpoint)
            .map_err(|_| DeviceSdkError::InvalidConfiguration("invalid MQTT endpoint".into()))?;
        if !matches!(url.scheme(), "mqtt" | "mqtts")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || !matches!(url.path(), "" | "/")
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT endpoint must be mqtt[s]://host[:port] without credentials, path or query"
                    .into(),
            ));
        }
        let client_id = self
            .client_id
            .clone()
            .unwrap_or_else(|| format!("netbaiot-sdk-{}", device.device_id.as_str()));
        if client_id.is_empty()
            || client_id.len() > 64
            || self.mqtt_connect_timeout.is_zero()
            || self.max_payload_bytes == 0
            || self.max_packet_bytes < self.max_payload_bytes.saturating_add(512)
            || self.max_packet_bytes > 8_388_608
            || self.mqtt_queue_items == 0
            || self.command_items == 0
            || self.command_bytes == 0
            || self.inflight_items == 0
            || self.inflight_items > 65_535
            || self.reconnect.initial_backoff.is_zero()
            || self.reconnect.initial_backoff > self.reconnect.maximum_backoff
            || self.message_expiry_interval == Some(0)
        {
            return Err(DeviceSdkError::InvalidConfiguration(
                "invalid MQTT timeout or resource limits".into(),
            ));
        }
        let total_publish_bytes = self
            .max_packet_bytes
            .checked_mul(
                self.mqtt_queue_items
                    .checked_add(self.inflight_items)
                    .ok_or(DeviceSdkError::Overloaded)?,
            )
            .filter(|n| *n <= u32::MAX as usize)
            .ok_or_else(|| {
                DeviceSdkError::InvalidConfiguration("MQTT queue byte budget is too large".into())
            })?;
        let (sender, receiver) = mpsc::channel(self.mqtt_queue_items);
        let (command_sender, command_receiver) = mpsc::channel(self.command_items);
        let (state_sender, connection_state) = watch::channel(MqttConnectionState::Connecting);
        let (receipts, _) = broadcast::channel(self.mqtt_queue_items + self.inflight_items);
        let shutdown = CancellationToken::new();
        let admission = Arc::new(Mutex::new(Admission {
            ready: false,
            closing: false,
            sender,
        }));
        let metrics = Arc::new(Metrics::default());
        let publish_bytes = Arc::new(Semaphore::new(total_publish_bytes));
        let config = mqtt::DriverConfig {
            host: url.host_str().unwrap_or_default().to_owned(),
            port: url
                .port()
                .unwrap_or(if url.scheme() == "mqtts" { 8883 } else { 1883 }),
            tls: url.scheme() == "mqtts",
            ca_pem: self.ca_pem,
            device: device.clone(),
            client_id,
            username: credentials.credential_id.clone(),
            password: credentials.secret.as_bytes().to_vec(),
            protocol_version: self.protocol_version,
            session_expiry: self.session_expiry_interval,
            connect_timeout: self.mqtt_connect_timeout,
            max_packet_bytes: self.max_packet_bytes,
            max_payload_bytes: self.max_payload_bytes,
            command_bytes: self.command_bytes,
            inflight_items: self.inflight_items,
            reconnect: self.reconnect,
        };
        let task = tokio::spawn(mqtt::run(
            config,
            receiver,
            command_sender,
            state_sender,
            admission.clone(),
            shutdown.clone(),
            metrics.clone(),
            receipts.clone(),
        ));
        let client = DeviceClient {
            inner: Arc::new(DeviceInner {
                device,
                admission,
                connection_state,
                command_receiver: Mutex::new(Some(command_receiver)),
                task: Mutex::new(Some(task)),
                shutdown,
                metrics,
                receipts,
                max_payload_bytes: self.max_payload_bytes,
                message_expiry_interval: self.message_expiry_interval,
                publish_bytes,
                total_publish_bytes,
            }),
        };
        client
            .wait_until_connected(self.mqtt_connect_timeout)
            .await?;
        Ok(client)
    }
}
impl DeviceClient {
    pub fn builder() -> DeviceClientBuilder {
        DeviceClientBuilder::default()
    }
    pub fn device(&self) -> &DeviceKey {
        &self.inner.device
    }
    pub fn metrics(&self) -> DeviceMetrics {
        DeviceMetrics {
            mqtt_reconnects: self.inner.metrics.mqtt_reconnects.load(Ordering::Relaxed),
            publishes_accepted: self
                .inner
                .metrics
                .publishes_accepted
                .load(Ordering::Relaxed),
            commands_received: self.inner.metrics.commands_received.load(Ordering::Relaxed),
        }
    }
    pub fn mqtt_connected(&self) -> bool {
        self.inner.admission.lock().is_ok_and(|a| a.ready)
    }
    pub fn publish_receipts(&self) -> broadcast::Receiver<PublishReceipt> {
        self.inner.receipts.subscribe()
    }
    pub async fn wait_until_connected(&self, timeout: Duration) -> Result<(), DeviceSdkError> {
        if timeout.is_zero() {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT connection wait timeout must be nonzero".into(),
            ));
        }
        let mut state = self.inner.connection_state.clone();
        tokio::time::timeout(timeout, async move {
            loop {
                match state.borrow().clone() {
                    MqttConnectionState::Connected => return Ok(()),
                    MqttConnectionState::Terminal(error) => return Err(error),
                    MqttConnectionState::Closed => return Err(DeviceSdkError::ServerUnavailable),
                    MqttConnectionState::Connecting => {}
                }
                state
                    .changed()
                    .await
                    .map_err(|_| DeviceSdkError::ServerUnavailable)?;
            }
        })
        .await
        .map_err(|_| DeviceSdkError::Timeout)?
    }
    pub async fn publish_telemetry(
        &self,
        telemetry: BTreeMap<String, Scalar>,
    ) -> Result<(), DeviceSdkError> {
        self.publish(
            DeviceUplink::new(next_source_id()?, DeviceUplinkKind::Telemetry(telemetry)),
            PublishQos::AtLeastOnce,
        )
        .await
    }
    pub async fn publish(
        &self,
        event: DeviceUplink,
        qos: PublishQos,
    ) -> Result<(), DeviceSdkError> {
        self.enqueue(event, qos, "up")
    }
    fn enqueue(
        &self,
        event: DeviceUplink,
        qos: PublishQos,
        suffix: &'static str,
    ) -> Result<(), DeviceSdkError> {
        let mut writer = BoundedJson::new(self.inner.max_payload_bytes);
        serde_json::to_writer(&mut writer, &event).map_err(|_| DeviceSdkError::Overloaded)?;
        let payload = writer.into_inner();
        let charged = u32::try_from(
            payload
                .len()
                .checked_add(512)
                .ok_or(DeviceSdkError::Overloaded)?,
        )
        .map_err(|_| DeviceSdkError::Overloaded)?;
        let permit = self
            .inner
            .publish_bytes
            .clone()
            .try_acquire_many_owned(charged)
            .map_err(|_| DeviceSdkError::Overloaded)?;
        let outbound = Outbound {
            payload,
            qos,
            suffix,
            source_message_id: event.source_message_id.clone(),
            expiry: self.inner.message_expiry_interval,
            admitted: tokio::time::Instant::now(),
            _permit: permit,
        };
        let admission = self
            .inner
            .admission
            .lock()
            .map_err(|_| DeviceSdkError::ServerUnavailable)?;
        if !admission.ready {
            return Err(DeviceSdkError::Offline);
        }
        admission
            .sender
            .try_send(outbound)
            .map_err(|_| DeviceSdkError::Overloaded)?;
        self.inner
            .metrics
            .publishes_accepted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    pub fn commands(&self) -> Result<DeviceCommandStream, DeviceSdkError> {
        let receiver = self
            .inner
            .command_receiver
            .lock()
            .map_err(|_| DeviceSdkError::ServerUnavailable)?
            .take()
            .ok_or_else(|| {
                DeviceSdkError::InvalidConfiguration(
                    "the device command stream has already been taken".into(),
                )
            })?;
        Ok(DeviceCommandStream { receiver })
    }
    pub async fn ack_command(
        &self,
        command_id: CommandId,
        execution: ExecutionState,
    ) -> Result<(), DeviceSdkError> {
        self.enqueue(
            DeviceUplink::new(
                next_source_id()?,
                DeviceUplinkKind::CommandAck(CommandAck {
                    command_id,
                    execution,
                }),
            ),
            PublishQos::AtLeastOnce,
            "down_ack",
        )
    }
    pub fn shutdown(&self) {
        if let Ok(mut admission) = self.inner.admission.lock() {
            admission.ready = false;
            admission.closing = true;
        }
        self.inner.shutdown.cancel();
    }
    pub async fn shutdown_with_timeout(&self, timeout: Duration) -> Result<(), DeviceSdkError> {
        if timeout.is_zero() {
            self.shutdown();
            return Err(DeviceSdkError::Timeout);
        }
        if let Ok(mut admission) = self.inner.admission.lock() {
            admission.ready = false;
            admission.closing = true;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        while self.inner.publish_bytes.available_permits() < self.inner.total_publish_bytes {
            if tokio::time::Instant::now() >= deadline {
                self.shutdown();
                return Err(DeviceSdkError::Timeout);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.shutdown();
        let task = self
            .inner
            .task
            .lock()
            .map_err(|_| DeviceSdkError::ServerUnavailable)?
            .take();
        if let Some(mut task) = task {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(_) => Ok(()),
                Err(_) => {
                    task.abort();
                    Err(DeviceSdkError::Timeout)
                }
            }
        } else {
            Ok(())
        }
    }
}
pub struct DeviceCommandStream {
    pub(super) receiver: mpsc::Receiver<CommandEnvelope>,
}
impl DeviceCommandStream {
    pub async fn recv(&mut self) -> Option<DeviceCommand> {
        self.receiver.recv().await.map(|env| env.command)
    }
}
impl Stream for DeviceCommandStream {
    type Item = DeviceCommand;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver
            .poll_recv(cx)
            .map(|item| item.map(|env| env.command))
    }
}
struct BoundedJson {
    bytes: Vec<u8>,
    maximum: usize,
}
impl BoundedJson {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}
impl std::io::Write for BoundedJson {
    fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(input.len())
            .is_none_or(|n| n > self.maximum)
        {
            return Err(std::io::Error::other("JSON payload limit"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn topic(device: &DeviceKey, suffix: &str) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/{suffix}",
        device.tenant_id, device.product_id, device.device_id
    )
}
fn next_source_id() -> Result<SourceMessageId, DeviceSdkError> {
    SourceMessageId::new(EventId::generate().to_string())
        .map_err(|_| DeviceSdkError::Protocol("failed to generate source message ID".into()))
}
pub(super) fn device_reconnect_delay(
    policy: DeviceReconnectPolicy,
    attempt: u32,
    seed: u64,
) -> Duration {
    let exponent = attempt.saturating_sub(1).min(20);
    let maximum = policy
        .initial_backoff
        .saturating_mul(1u32.checked_shl(exponent).unwrap_or(u32::MAX))
        .min(policy.maximum_backoff);
    let maximum_ms = u64::try_from(maximum.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let mixed = seed
        .wrapping_add(u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15))
        .wrapping_mul(0xbf58_476d_1ce4_e5b9);
    Duration::from_millis(1 + mixed % maximum_ms)
}
pub(super) fn terminal_connect_error(
    code: u8,
    version: MqttProtocolVersion,
) -> Option<DeviceSdkError> {
    match (version, code) {
        (_, 0) | (MqttProtocolVersion::V311, 3) => None,
        (MqttProtocolVersion::V311, 4 | 5) | (MqttProtocolVersion::V5, 0x86 | 0x87 | 0x8c) => {
            Some(DeviceSdkError::Unauthenticated)
        }
        (MqttProtocolVersion::V311, 2) | (MqttProtocolVersion::V5, 0x85) => Some(
            DeviceSdkError::InvalidConfiguration("MQTT broker rejected client ID".into()),
        ),
        (MqttProtocolVersion::V311, 1) | (MqttProtocolVersion::V5, 0x84) => {
            Some(DeviceSdkError::Protocol("MQTT version unsupported".into()))
        }
        (MqttProtocolVersion::V5, 0x88 | 0x89 | 0x97 | 0x9f) => None,
        _ => Some(DeviceSdkError::Protocol("MQTT CONNACK rejected".into())),
    }
}
