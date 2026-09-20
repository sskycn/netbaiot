//! Optional Rust convenience SDK for standard NetbaIoT MQTT 3.1.1 and device HTTP.
//!
//! The SDK does not create a runtime or local database. MQTT publishes are rejected
//! while disconnected by default; it never creates an unbounded offline queue.

use futures_core::Stream;
use netbaiot_protocol::*;
use reqwest::{StatusCode, Url};
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event, Incoming, MqttOptions, QoS,
    SubscribeReasonCode, Transport as MqttTransport,
};
use std::{
    collections::BTreeMap,
    fmt,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const DEFAULT_MAX_PAYLOAD_BYTES: usize = 65_536;
const DEFAULT_RESPONSE_BYTES: usize = 1_048_576;
const DEFAULT_MQTT_QUEUE_ITEMS: usize = 16;
const DEFAULT_COMMAND_ITEMS: usize = 16;

pub struct DeviceCredentials {
    credential_id: String,
    secret: String,
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceCredentials")
            .field("credential_id", &self.credential_id)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfflinePublishPolicy {
    /// Reject immediately while no MQTT connection is established. This is the default.
    Reject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Bounded exponential retry limits for the MQTT event loop.
pub struct DeviceReconnectPolicy {
    /// First reconnect delay ceiling. Full jitter chooses a smaller positive delay.
    pub initial_backoff: Duration,
    /// Hard ceiling for every reconnect delay.
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

impl From<PublishQos> for QoS {
    fn from(value: PublishQos) -> Self {
        match value {
            PublishQos::AtMostOnce => QoS::AtMostOnce,
            PublishQos::AtLeastOnce => QoS::AtLeastOnce,
        }
    }
}

#[derive(Debug, Error, Clone)]
pub enum DeviceSdkError {
    #[error("invalid device SDK configuration: {0}")]
    InvalidConfiguration(String),
    #[error("requested transport is not configured")]
    TransportNotConfigured,
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
    pub http_requests: u64,
}

#[derive(Default)]
struct Metrics {
    mqtt_reconnects: AtomicU64,
    publishes_accepted: AtomicU64,
    commands_received: AtomicU64,
    http_requests: AtomicU64,
}

struct MqttState {
    client: AsyncClient,
    connected: Arc<AtomicBool>,
    connection_state: watch::Receiver<MqttConnectionState>,
    command_receiver: Mutex<Option<mpsc::Receiver<DeviceCommand>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone, Debug)]
enum MqttConnectionState {
    Connecting,
    Connected,
    Terminal(DeviceSdkError),
}

struct DeviceInner {
    device: DeviceKey,
    credentials: Arc<DeviceCredentials>,
    http_endpoint: Option<Url>,
    http: Option<reqwest::Client>,
    mqtt: Option<MqttState>,
    max_payload_bytes: usize,
    max_response_bytes: usize,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
}

impl Drop for DeviceInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(mqtt) = &self.mqtt
            && let Ok(mut task) = mqtt.task.lock()
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceClient")
            .field("device", &self.inner.device)
            .field("credentials", &self.inner.credentials)
            .field("http_endpoint", &self.inner.http_endpoint)
            .field("mqtt_configured", &self.inner.mqtt.is_some())
            .finish_non_exhaustive()
    }
}

pub struct DeviceClientBuilder {
    device: Option<DeviceKey>,
    credentials: Option<DeviceCredentials>,
    mqtt_endpoint: Option<String>,
    http_endpoint: Option<String>,
    client_id: Option<String>,
    request_timeout: Duration,
    mqtt_connect_timeout: Duration,
    max_payload_bytes: usize,
    max_response_bytes: usize,
    mqtt_queue_items: usize,
    command_items: usize,
    offline_policy: OfflinePublishPolicy,
    reconnect: DeviceReconnectPolicy,
}

impl fmt::Debug for DeviceClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceClientBuilder")
            .field("device", &self.device)
            .field("credentials", &self.credentials)
            .field("mqtt_endpoint", &self.mqtt_endpoint)
            .field("http_endpoint", &self.http_endpoint)
            .finish_non_exhaustive()
    }
}

impl Default for DeviceClientBuilder {
    fn default() -> Self {
        Self {
            device: None,
            credentials: None,
            mqtt_endpoint: None,
            http_endpoint: None,
            client_id: None,
            request_timeout: Duration::from_secs(10),
            mqtt_connect_timeout: Duration::from_secs(10),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            max_response_bytes: DEFAULT_RESPONSE_BYTES,
            mqtt_queue_items: DEFAULT_MQTT_QUEUE_ITEMS,
            command_items: DEFAULT_COMMAND_ITEMS,
            offline_policy: OfflinePublishPolicy::Reject,
            reconnect: DeviceReconnectPolicy::default(),
        }
    }
}

impl DeviceClientBuilder {
    pub fn device(mut self, device: DeviceKey) -> Self {
        self.device = Some(device);
        self
    }

    pub fn credentials(mut self, credentials: DeviceCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn mqtt_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.mqtt_endpoint = Some(endpoint.into());
        self
    }

    pub fn http_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.http_endpoint = Some(endpoint.into());
        self
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets how long `connect()` may wait for the initial MQTT CONNACK.
    pub fn mqtt_connect_timeout(mut self, timeout: Duration) -> Self {
        self.mqtt_connect_timeout = timeout;
        self
    }

    pub fn mqtt_queue_items(mut self, items: usize) -> Self {
        self.mqtt_queue_items = items;
        self
    }

    pub fn command_buffer_items(mut self, items: usize) -> Self {
        self.command_items = items;
        self
    }

    pub fn offline_publish_policy(mut self, policy: OfflinePublishPolicy) -> Self {
        self.offline_policy = policy;
        self
    }

    /// Configures cancellation-aware bounded MQTT reconnect delays.
    pub fn reconnect_policy(mut self, policy: DeviceReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    pub async fn connect(self) -> Result<DeviceClient, DeviceSdkError> {
        let device = self.device.ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("device identity is required".into())
        })?;
        let credentials = Arc::new(self.credentials.ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("device credentials are required".into())
        })?);
        if self.mqtt_endpoint.is_none() && self.http_endpoint.is_none() {
            return Err(DeviceSdkError::InvalidConfiguration(
                "at least one transport endpoint is required".into(),
            ));
        }
        if self.request_timeout.is_zero()
            || self.mqtt_connect_timeout.is_zero()
            || self.max_payload_bytes == 0
            || self.max_response_bytes == 0
            || self.mqtt_queue_items == 0
            || self.command_items == 0
            || self.max_payload_bytes > u32::MAX as usize
            || self.reconnect.initial_backoff.is_zero()
            || self.reconnect.initial_backoff > self.reconnect.maximum_backoff
        {
            return Err(DeviceSdkError::InvalidConfiguration(
                "timeouts and bounds must be nonzero".into(),
            ));
        }
        let http_endpoint = match self.http_endpoint {
            Some(endpoint) => {
                let mut url = Url::parse(&endpoint).map_err(|error| {
                    DeviceSdkError::InvalidConfiguration(format!("invalid HTTP endpoint: {error}"))
                })?;
                if !matches!(url.scheme(), "http" | "https") || url.cannot_be_a_base() {
                    return Err(DeviceSdkError::InvalidConfiguration(
                        "HTTP endpoint must use http or https".into(),
                    ));
                }
                if !url.path().ends_with('/') {
                    url.set_path(&format!("{}/", url.path()));
                }
                Some(url)
            }
            None => None,
        };
        let http = if http_endpoint.is_some() {
            Some(
                reqwest::Client::builder()
                    .no_proxy()
                    .timeout(self.request_timeout)
                    .connect_timeout(self.request_timeout)
                    .user_agent(concat!("netbaiot-device-sdk/", env!("CARGO_PKG_VERSION")))
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .map_err(|error| DeviceSdkError::Transport(error.to_string()))?,
            )
        } else {
            None
        };
        let shutdown = CancellationToken::new();
        let metrics = Arc::new(Metrics::default());
        let mqtt = if let Some(endpoint) = self.mqtt_endpoint {
            let url = Url::parse(&endpoint).map_err(|error| {
                DeviceSdkError::InvalidConfiguration(format!("invalid MQTT endpoint: {error}"))
            })?;
            if !matches!(url.scheme(), "mqtt" | "mqtts") {
                return Err(DeviceSdkError::InvalidConfiguration(
                    "MQTT endpoint must use mqtt or mqtts".into(),
                ));
            }
            let host = url.host_str().ok_or_else(|| {
                DeviceSdkError::InvalidConfiguration("MQTT endpoint has no host".into())
            })?;
            let port = url
                .port()
                .unwrap_or(if url.scheme() == "mqtts" { 8883 } else { 1883 });
            let client_id = self
                .client_id
                .unwrap_or_else(|| format!("netbaiot-sdk-{}", device.device_id.as_str()));
            if client_id.is_empty() || client_id.len() > 64 {
                return Err(DeviceSdkError::InvalidConfiguration(
                    "MQTT client ID must contain 1..=64 bytes".into(),
                ));
            }
            let mut options = MqttOptions::new(client_id, host, port);
            options
                .set_credentials(&credentials.credential_id, &credentials.secret)
                .set_keep_alive(Duration::from_secs(15))
                .set_clean_session(false)
                .set_manual_acks(true)
                .set_max_packet_size(self.max_payload_bytes, self.max_payload_bytes);
            if url.scheme() == "mqtts" {
                options.set_transport(MqttTransport::tls_with_default_config());
            }
            let (client, mut eventloop) = AsyncClient::new(options, self.mqtt_queue_items);
            eventloop
                .network_options
                .set_connection_timeout(self.mqtt_connect_timeout.as_secs().max(1));
            let down_topic = topic(&device, "down");
            let (command_sender, command_receiver) = mpsc::channel(self.command_items);
            let connected = Arc::new(AtomicBool::new(false));
            let (connection_sender, connection_state) =
                watch::channel(MqttConnectionState::Connecting);
            let task_connected = connected.clone();
            let task_client = client.clone();
            let task_shutdown = shutdown.child_token();
            let task_metrics = metrics.clone();
            let task_device = device.clone();
            let reconnect = self.reconnect;
            let reconnect_seed = EventId::generate().0.as_u128() as u64;
            let task = tokio::spawn(async move {
                let mut connected_once = false;
                let mut reconnect_attempt = 0u32;
                let mut awaiting_suback = false;
                loop {
                    let event = tokio::select! {
                        _ = task_shutdown.cancelled() => break,
                        event = eventloop.poll() => event,
                    };
                    match event {
                        Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                            task_connected.store(false, Ordering::Release);
                            let _ = connection_sender.send(MqttConnectionState::Connecting);
                            if task_client
                                .try_subscribe(down_topic.clone(), QoS::AtLeastOnce)
                                .is_err()
                            {
                                eventloop.clean();
                                task_connected.store(false, Ordering::Release);
                                let _ = connection_sender.send(MqttConnectionState::Connecting);
                                reconnect_attempt = reconnect_attempt.saturating_add(1);
                                let delay = device_reconnect_delay(
                                    reconnect,
                                    reconnect_attempt,
                                    reconnect_seed,
                                );
                                tokio::select! {
                                    _ = task_shutdown.cancelled() => break,
                                    _ = tokio::time::sleep(delay) => {}
                                }
                                continue;
                            }
                            awaiting_suback = true;
                        }
                        Ok(Event::Incoming(Incoming::SubAck(suback))) => {
                            if !awaiting_suback
                                || !matches!(
                                    suback.return_codes.as_slice(),
                                    [SubscribeReasonCode::Success(
                                        QoS::AtMostOnce | QoS::AtLeastOnce
                                    )]
                                )
                            {
                                task_connected.store(false, Ordering::Release);
                                let _ = connection_sender
                                    .send(MqttConnectionState::Terminal(DeviceSdkError::Forbidden));
                                let _ = task_client.try_disconnect();
                                return;
                            }
                            awaiting_suback = false;
                            if connected_once {
                                task_metrics.mqtt_reconnects.fetch_add(1, Ordering::Relaxed);
                            }
                            connected_once = true;
                            reconnect_attempt = 0;
                            task_connected.store(true, Ordering::Release);
                            let _ = connection_sender.send(MqttConnectionState::Connected);
                        }
                        Ok(Event::Incoming(Incoming::Publish(publish)))
                            if publish.topic == down_topic =>
                        {
                            let command = serde_json::from_slice::<DeviceCommand>(&publish.payload);
                            match command {
                                Ok(command) if command.device == task_device => {
                                    if command_sender.try_send(command).is_ok() {
                                        task_metrics
                                            .commands_received
                                            .fetch_add(1, Ordering::Relaxed);
                                        if task_client.try_ack(&publish).is_err() {
                                            task_connected.store(false, Ordering::Release);
                                            let _ = connection_sender
                                                .send(MqttConnectionState::Connecting);
                                            let _ = task_client.try_disconnect();
                                        }
                                    } else {
                                        task_connected.store(false, Ordering::Release);
                                        let _ =
                                            connection_sender.send(MqttConnectionState::Connecting);
                                        let _ = task_client.try_disconnect();
                                    }
                                }
                                _ => {
                                    task_connected.store(false, Ordering::Release);
                                    let _ = connection_sender.send(MqttConnectionState::Connecting);
                                    let _ = task_client.try_disconnect();
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            awaiting_suback = false;
                            task_connected.store(false, Ordering::Release);
                            if let ConnectionError::ConnectionRefused(code) = error
                                && let Some(error) = terminal_connect_error(code)
                            {
                                let _ =
                                    connection_sender.send(MqttConnectionState::Terminal(error));
                                return;
                            }
                            let _ = connection_sender.send(MqttConnectionState::Connecting);
                            reconnect_attempt = reconnect_attempt.saturating_add(1);
                            let delay = device_reconnect_delay(
                                reconnect,
                                reconnect_attempt,
                                reconnect_seed,
                            );
                            tokio::select! {
                                _ = task_shutdown.cancelled() => break,
                                _ = tokio::time::sleep(delay) => {}
                            }
                        }
                    }
                }
                task_connected.store(false, Ordering::Release);
                let _ = connection_sender.send(MqttConnectionState::Connecting);
            });
            Some(MqttState {
                client,
                connected,
                connection_state,
                command_receiver: Mutex::new(Some(command_receiver)),
                task: Mutex::new(Some(task)),
            })
        } else {
            None
        };
        let _ = self.offline_policy;
        let client = DeviceClient {
            inner: Arc::new(DeviceInner {
                device,
                credentials,
                http_endpoint,
                http,
                mqtt,
                max_payload_bytes: self.max_payload_bytes,
                max_response_bytes: self.max_response_bytes,
                shutdown,
                metrics,
            }),
        };
        if client.inner.mqtt.is_some() {
            client
                .wait_until_connected(self.mqtt_connect_timeout)
                .await?;
        }
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
            http_requests: self.inner.metrics.http_requests.load(Ordering::Relaxed),
        }
    }

    /// Returns whether the MQTT event loop currently has an authenticated connection.
    pub fn mqtt_connected(&self) -> bool {
        self.inner
            .mqtt
            .as_ref()
            .is_some_and(|mqtt| mqtt.connected.load(Ordering::Acquire))
    }

    /// Waits for initial or recovered MQTT connectivity without creating a task.
    pub async fn wait_until_connected(&self, timeout: Duration) -> Result<(), DeviceSdkError> {
        if timeout.is_zero() {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT connection wait timeout must be nonzero".into(),
            ));
        }
        let mqtt = self
            .inner
            .mqtt
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        let mut state = mqtt.connection_state.clone();
        tokio::time::timeout(timeout, async move {
            loop {
                match state.borrow().clone() {
                    MqttConnectionState::Connected => return Ok(()),
                    MqttConnectionState::Terminal(error) => return Err(error),
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

    /// Enqueues telemetry to the bounded MQTT client at the requested QoS.
    /// Success means accepted by the local MQTT client, not business persistence.
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
        let mqtt = self
            .inner
            .mqtt
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        if !mqtt.connected.load(Ordering::Acquire) {
            return Err(DeviceSdkError::Offline);
        }
        let payload = serde_json::to_vec(&event)
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        if payload.len() > self.inner.max_payload_bytes {
            return Err(DeviceSdkError::Overloaded);
        }
        mqtt.client
            .try_publish(topic(&self.inner.device, "up"), qos.into(), false, payload)
            .map_err(|_| DeviceSdkError::Overloaded)?;
        self.inner
            .metrics
            .publishes_accepted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Uploads one JSON-v1 device envelope. HTTP 202 means `EventAccepted` only.
    pub async fn upload_data(&self, event: &DeviceUplink) -> Result<EventAccepted, DeviceSdkError> {
        self.http_json(reqwest::Method::POST, paths::DEVICE_DATA, Some(event))
            .await
    }

    pub fn config(&self) -> DeviceConfigApi {
        DeviceConfigApi(self.clone())
    }

    pub fn commands(&self) -> Result<DeviceCommandStream, DeviceSdkError> {
        let mqtt = self
            .inner
            .mqtt
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        let receiver = mqtt
            .command_receiver
            .lock()
            .map_err(|_| DeviceSdkError::Transport("command receiver lock poisoned".into()))?
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
        let mqtt = self
            .inner
            .mqtt
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        if !mqtt.connected.load(Ordering::Acquire) {
            return Err(DeviceSdkError::Offline);
        }
        let event = DeviceUplink::new(
            next_source_id()?,
            DeviceUplinkKind::CommandAck(CommandAck {
                command_id,
                execution,
            }),
        );
        let payload = serde_json::to_vec(&event)
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        if payload.len() > self.inner.max_payload_bytes {
            return Err(DeviceSdkError::Overloaded);
        }
        mqtt.client
            .try_publish(
                topic(&self.inner.device, "down_ack"),
                QoS::AtLeastOnce,
                false,
                payload,
            )
            .map_err(|_| DeviceSdkError::Overloaded)
    }

    pub async fn heartbeat(&self, sequence: u64) -> Result<EventAccepted, DeviceSdkError> {
        let event = DeviceUplink::new(
            next_source_id()?,
            DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
        );
        self.http_json(reqwest::Method::POST, paths::DEVICE_HEARTBEAT, Some(&event))
            .await
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
        if let Some(mqtt) = &self.inner.mqtt {
            let _ = mqtt.client.try_disconnect();
        }
    }

    async fn http_json<B: serde::Serialize + ?Sized, R: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<R, DeviceSdkError> {
        let http = self
            .inner
            .http
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        let url = self
            .inner
            .http_endpoint
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?
            .join(path.trim_start_matches('/'))
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        let mut request = http.request(method, url).bearer_auth(format!(
            "{}:{}",
            self.inner.credentials.credential_id, self.inner.credentials.secret
        ));
        if let Some(body) = body {
            request = request.json(body);
        }
        self.inner
            .metrics
            .http_requests
            .fetch_add(1, Ordering::Relaxed);
        let response = request.send().await.map_err(map_http_error)?;
        let status = response.status();
        let bytes = read_response(response, self.inner.max_response_bytes).await?;
        if !status.is_success() {
            return Err(status_error(status));
        }
        serde_json::from_slice(&bytes).map_err(|error| DeviceSdkError::Protocol(error.to_string()))
    }
}

pub struct DeviceCommandStream {
    receiver: mpsc::Receiver<DeviceCommand>,
}

impl DeviceCommandStream {
    pub async fn recv(&mut self) -> Option<DeviceCommand> {
        self.receiver.recv().await
    }
}

impl Stream for DeviceCommandStream {
    type Item = DeviceCommand;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(context)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigUpdate {
    Unchanged,
    Updated(DeviceConfig),
}

#[derive(Clone)]
pub struct DeviceConfigApi(DeviceClient);

impl DeviceConfigApi {
    pub async fn check(
        &self,
        current_revision: Option<ConfigRevision>,
    ) -> Result<ConfigUpdate, DeviceSdkError> {
        let http = self
            .0
            .inner
            .http
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?;
        let url = self
            .0
            .inner
            .http_endpoint
            .as_ref()
            .ok_or(DeviceSdkError::TransportNotConfigured)?
            .join(paths::DEVICE_CONFIG.trim_start_matches('/'))
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        let mut request = http.get(url).bearer_auth(format!(
            "{}:{}",
            self.0.inner.credentials.credential_id, self.0.inner.credentials.secret
        ));
        if let Some(revision) = current_revision {
            request = request.header(reqwest::header::IF_NONE_MATCH, format!("\"{revision}\""));
        }
        self.0
            .inner
            .metrics
            .http_requests
            .fetch_add(1, Ordering::Relaxed);
        let response = request.send().await.map_err(map_http_error)?;
        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(ConfigUpdate::Unchanged);
        }
        let status = response.status();
        let bytes = read_response(response, self.0.inner.max_response_bytes).await?;
        if !status.is_success() {
            return Err(status_error(status));
        }
        let config = serde_json::from_slice(&bytes)
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        Ok(ConfigUpdate::Updated(config))
    }

    /// Reports application outcome separately from configuration download.
    pub async fn ack(
        &self,
        revision: ConfigRevision,
        status: ConfigApplyStatus,
        error: Option<String>,
    ) -> Result<EventAccepted, DeviceSdkError> {
        if error.as_ref().is_some_and(|value| value.len() > 256) {
            return Err(DeviceSdkError::InvalidConfiguration(
                "configuration failure detail exceeds 256 bytes".into(),
            ));
        }
        let event = DeviceUplink::new(
            next_source_id()?,
            DeviceUplinkKind::ConfigAck(ConfigAck {
                revision,
                status,
                error,
            }),
        );
        self.0
            .http_json(
                reqwest::Method::POST,
                paths::DEVICE_CONFIG_ACK,
                Some(&event),
            )
            .await
    }
}

fn topic(device: &DeviceKey, suffix: &str) -> String {
    format!(
        "v1/t/{}/p/{}/d/{}/{suffix}",
        device.tenant_id, device.product_id, device.device_id
    )
}

fn next_source_id() -> Result<SourceMessageId, DeviceSdkError> {
    SourceMessageId::new(EventId::generate().to_string())
        .map_err(|_| DeviceSdkError::Protocol("failed to generate source message ID".into()))
}

fn terminal_connect_error(code: ConnectReturnCode) -> Option<DeviceSdkError> {
    match code {
        ConnectReturnCode::Success | ConnectReturnCode::ServiceUnavailable => None,
        ConnectReturnCode::BadUserNamePassword | ConnectReturnCode::NotAuthorized => {
            Some(DeviceSdkError::Unauthenticated)
        }
        ConnectReturnCode::BadClientId => Some(DeviceSdkError::InvalidConfiguration(
            "MQTT broker rejected the client ID".into(),
        )),
        ConnectReturnCode::RefusedProtocolVersion => Some(DeviceSdkError::Protocol(
            "MQTT broker rejected protocol version 3.1.1".into(),
        )),
    }
}

fn device_reconnect_delay(policy: DeviceReconnectPolicy, attempt: u32, seed: u64) -> Duration {
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

async fn read_response(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, DeviceSdkError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(DeviceSdkError::Protocol(
            "response exceeds configured limit".into(),
        ));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(map_http_error)? {
        let length = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| DeviceSdkError::Protocol("response length overflow".into()))?;
        if length > maximum {
            return Err(DeviceSdkError::Protocol(
                "response exceeds configured limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn map_http_error(error: reqwest::Error) -> DeviceSdkError {
    if error.is_timeout() {
        DeviceSdkError::Timeout
    } else if error.is_connect() {
        DeviceSdkError::ServerUnavailable
    } else {
        DeviceSdkError::Transport(error.to_string())
    }
}

fn status_error(status: StatusCode) -> DeviceSdkError {
    match status {
        StatusCode::UNAUTHORIZED => DeviceSdkError::Unauthenticated,
        StatusCode::FORBIDDEN => DeviceSdkError::Forbidden,
        StatusCode::TOO_MANY_REQUESTS => DeviceSdkError::Overloaded,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => DeviceSdkError::Timeout,
        _ if status.is_server_error() => DeviceSdkError::ServerUnavailable,
        _ => DeviceSdkError::Protocol(format!("server returned HTTP {status}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn device() -> DeviceKey {
        DeviceKey {
            tenant_id: TenantId::new("tenant").unwrap(),
            product_id: ProductId::new("product").unwrap(),
            device_id: DeviceId::new("device").unwrap(),
        }
    }

    #[tokio::test]
    async fn credentials_are_redacted_and_transport_is_required() {
        let credentials = DeviceCredentials::new("credential", "secret-value").unwrap();
        assert!(!format!("{credentials:?}").contains("secret-value"));
        let result = DeviceClient::builder()
            .device(device())
            .credentials(credentials)
            .connect()
            .await;
        assert!(matches!(
            result,
            Err(DeviceSdkError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn canonical_topics_are_identity_scoped() {
        assert_eq!(topic(&device(), "up"), "v1/t/tenant/p/product/d/device/up");
    }

    #[test]
    fn reconnect_backoff_is_jittered_and_bounded() {
        let policy = DeviceReconnectPolicy::default();
        let mut previous_ceiling = policy.initial_backoff;
        for attempt in 1..100 {
            let delay = device_reconnect_delay(policy, attempt, 7);
            assert!(!delay.is_zero());
            assert!(delay <= policy.maximum_backoff);
            if attempt <= 6 {
                assert!(delay <= previous_ceiling);
                previous_ceiling = previous_ceiling
                    .saturating_mul(2)
                    .min(policy.maximum_backoff);
            }
        }
        assert_ne!(
            device_reconnect_delay(policy, 4, 7),
            device_reconnect_delay(policy, 4, 8)
        );
    }

    #[test]
    fn terminal_connack_codes_do_not_retry() {
        assert!(terminal_connect_error(ConnectReturnCode::ServiceUnavailable).is_none());
        assert!(matches!(
            terminal_connect_error(ConnectReturnCode::BadUserNamePassword),
            Some(DeviceSdkError::Unauthenticated)
        ));
        assert!(matches!(
            terminal_connect_error(ConnectReturnCode::BadClientId),
            Some(DeviceSdkError::InvalidConfiguration(_))
        ));
    }

    async fn read_mqtt_packet(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await.unwrap();
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let byte = stream.read_u8().await.unwrap();
            remaining += usize::from(byte & 0x7f) * multiplier;
            if byte & 0x80 == 0 {
                break;
            }
            multiplier *= 128;
        }
        let mut body = vec![0; remaining];
        stream.read_exact(&mut body).await.unwrap();
        let mut packet = vec![first[0]];
        packet.extend_from_slice(&body);
        packet
    }

    #[tokio::test]
    async fn command_connection_waits_for_successful_suback() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let broker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let connect = read_mqtt_packet(&mut socket).await;
            assert_eq!(connect[0] & 0xf0, 0x10);
            socket.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();
            let subscribe = read_mqtt_packet(&mut socket).await;
            assert_eq!(subscribe[0], 0x82);
            let packet_id = &subscribe[1..3];
            socket
                .write_all(&[0x90, 0x03, packet_id[0], packet_id[1], 0x80])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let result = DeviceClient::builder()
            .device(device())
            .credentials(DeviceCredentials::new("credential", "secret").unwrap())
            .mqtt_endpoint(format!("mqtt://{address}"))
            .mqtt_connect_timeout(Duration::from_secs(1))
            .connect()
            .await;
        assert!(matches!(result, Err(DeviceSdkError::Forbidden)));
        broker.await.unwrap();
    }
}
