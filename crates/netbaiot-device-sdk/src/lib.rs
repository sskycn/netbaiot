//! Optional Rust convenience SDK for standard NetbaIoT MQTT 3.1.1 and MQTT 5.0.
//!
//! The SDK does not create a runtime or local database. MQTT publishes are rejected
//! while disconnected by default; it never creates an unbounded offline queue.

use futures_core::Stream;
use netbaiot_protocol::*;
use rumqttc::v5;
use rumqttc::v5::mqttbytes::QoS as V5Qos;
use rumqttc::v5::mqttbytes::v5::ConnectReturnCode as V5ConnectReturnCode;
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
use url::Url;

const DEFAULT_MAX_PAYLOAD_BYTES: usize = 65_536;
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MqttProtocolVersion {
    #[default]
    V311,
    V5,
}

impl From<PublishQos> for QoS {
    fn from(value: PublishQos) -> Self {
        match value {
            PublishQos::AtMostOnce => QoS::AtMostOnce,
            PublishQos::AtLeastOnce => QoS::AtLeastOnce,
        }
    }
}

impl From<PublishQos> for V5Qos {
    fn from(value: PublishQos) -> Self {
        match value {
            PublishQos::AtMostOnce => Self::AtMostOnce,
            PublishQos::AtLeastOnce => Self::AtLeastOnce,
        }
    }
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
struct Metrics {
    mqtt_reconnects: AtomicU64,
    publishes_accepted: AtomicU64,
    commands_received: AtomicU64,
}

struct MqttState {
    client: MqttClient,
    connected: Arc<AtomicBool>,
    connection_state: watch::Receiver<MqttConnectionState>,
    command_receiver: Mutex<Option<mpsc::Receiver<DeviceCommand>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

enum MqttClient {
    V311(AsyncClient),
    V5(v5::AsyncClient),
}

impl MqttClient {
    fn try_publish(
        &self,
        topic: String,
        qos: PublishQos,
        payload: Vec<u8>,
        expiry: Option<u32>,
    ) -> Result<(), DeviceSdkError> {
        match self {
            Self::V311(client) => client
                .try_publish(topic, qos.into(), false, payload)
                .map_err(|_| DeviceSdkError::Overloaded),
            Self::V5(client) => {
                if let Some(seconds) = expiry {
                    client
                        .try_publish_with_properties(
                            topic,
                            qos.into(),
                            false,
                            payload,
                            v5::mqttbytes::v5::PublishProperties {
                                message_expiry_interval: Some(seconds),
                                ..Default::default()
                            },
                        )
                        .map_err(|_| DeviceSdkError::Overloaded)
                } else {
                    client
                        .try_publish(topic, qos.into(), false, payload)
                        .map_err(|_| DeviceSdkError::Overloaded)
                }
            }
        }
    }

    fn try_disconnect(&self) {
        match self {
            Self::V311(client) => {
                let _ = client.try_disconnect();
            }
            Self::V5(client) => {
                let _ = client.try_disconnect();
            }
        }
    }
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
    mqtt: MqttState,
    max_payload_bytes: usize,
    message_expiry_interval: Option<u32>,
    shutdown: CancellationToken,
    metrics: Arc<Metrics>,
}

impl Drop for DeviceInner {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Ok(mut task) = self.mqtt.task.lock()
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
    mqtt_queue_items: usize,
    command_items: usize,
    offline_policy: OfflinePublishPolicy,
    reconnect: DeviceReconnectPolicy,
    protocol_version: MqttProtocolVersion,
    session_expiry_interval: u32,
    message_expiry_interval: Option<u32>,
}

impl fmt::Debug for DeviceClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceClientBuilder")
            .field("device", &self.device)
            .field("credentials", &self.credentials)
            .field("mqtt_endpoint", &self.mqtt_endpoint)
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
            mqtt_queue_items: DEFAULT_MQTT_QUEUE_ITEMS,
            command_items: DEFAULT_COMMAND_ITEMS,
            offline_policy: OfflinePublishPolicy::Reject,
            reconnect: DeviceReconnectPolicy::default(),
            protocol_version: MqttProtocolVersion::V311,
            session_expiry_interval: 3_600,
            message_expiry_interval: None,
        }
    }
}

impl DeviceClientBuilder {
    pub fn protocol_version(mut self, version: MqttProtocolVersion) -> Self {
        self.protocol_version = version;
        self
    }

    pub fn session_expiry_interval(mut self, seconds: u32) -> Self {
        self.session_expiry_interval = seconds;
        self
    }

    pub fn message_expiry_interval(mut self, seconds: Option<u32>) -> Self {
        self.message_expiry_interval = seconds;
        self
    }
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

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
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
        let endpoint = self.mqtt_endpoint.ok_or_else(|| {
            DeviceSdkError::InvalidConfiguration("mqtt_endpoint is required".into())
        })?;
        if self.mqtt_connect_timeout.is_zero()
            || self.max_payload_bytes == 0
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
        let shutdown = CancellationToken::new();
        let metrics = Arc::new(Metrics::default());
        let mqtt = {
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
            if self.protocol_version == MqttProtocolVersion::V5 {
                let mut options = v5::MqttOptions::new(client_id, host, port);
                options
                    .set_credentials(&credentials.credential_id, &credentials.secret)
                    .set_keep_alive(Duration::from_secs(15))
                    .set_clean_start(false)
                    .set_manual_acks(true)
                    .set_session_expiry_interval(Some(self.session_expiry_interval))
                    .set_max_packet_size(Some(self.max_payload_bytes as u32));
                options.set_connection_timeout(self.mqtt_connect_timeout.as_secs().max(1));
                if url.scheme() == "mqtts" {
                    options.set_transport(MqttTransport::tls_with_default_config());
                }
                let (client, mut eventloop) = v5::AsyncClient::new(options, self.mqtt_queue_items);
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
                            Ok(v5::Event::Incoming(v5::Incoming::ConnAck(_))) => {
                                task_connected.store(false, Ordering::Release);
                                let _ = connection_sender.send(MqttConnectionState::Connecting);
                                if task_client
                                    .try_subscribe(down_topic.clone(), V5Qos::AtLeastOnce)
                                    .is_err()
                                {
                                    eventloop.clean();
                                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                                    let delay = device_reconnect_delay(
                                        reconnect,
                                        reconnect_attempt,
                                        reconnect_seed,
                                    );
                                    tokio::select! { _ = task_shutdown.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
                                    continue;
                                }
                                awaiting_suback = true;
                            }
                            Ok(v5::Event::Incoming(v5::Incoming::SubAck(suback))) => {
                                if !awaiting_suback
                                    || !matches!(
                                        suback.return_codes.as_slice(),
                                        [v5::mqttbytes::v5::SubscribeReasonCode::Success(
                                            V5Qos::AtMostOnce | V5Qos::AtLeastOnce
                                        )]
                                    )
                                {
                                    task_connected.store(false, Ordering::Release);
                                    let _ = connection_sender.send(MqttConnectionState::Terminal(
                                        DeviceSdkError::Forbidden,
                                    ));
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
                            Ok(v5::Event::Incoming(v5::Incoming::Publish(publish)))
                                if publish.topic.as_ref() == down_topic.as_bytes() =>
                            {
                                let command =
                                    serde_json::from_slice::<DeviceCommand>(&publish.payload);
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
                                            let _ = connection_sender
                                                .send(MqttConnectionState::Connecting);
                                            let _ = task_client.try_disconnect();
                                        }
                                    }
                                    _ => {
                                        task_connected.store(false, Ordering::Release);
                                        let _ =
                                            connection_sender.send(MqttConnectionState::Connecting);
                                        let _ = task_client.try_disconnect();
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                awaiting_suback = false;
                                task_connected.store(false, Ordering::Release);
                                if let v5::ConnectionError::ConnectionRefused(code) = error
                                    && let Some(error) = terminal_v5_connect_error(code)
                                {
                                    let _ = connection_sender
                                        .send(MqttConnectionState::Terminal(error));
                                    return;
                                }
                                let _ = connection_sender.send(MqttConnectionState::Connecting);
                                reconnect_attempt = reconnect_attempt.saturating_add(1);
                                let delay = device_reconnect_delay(
                                    reconnect,
                                    reconnect_attempt,
                                    reconnect_seed,
                                );
                                tokio::select! { _ = task_shutdown.cancelled() => break, _ = tokio::time::sleep(delay) => {} }
                            }
                        }
                    }
                    task_connected.store(false, Ordering::Release);
                    let _ = connection_sender.send(MqttConnectionState::Connecting);
                });
                MqttState {
                    client: MqttClient::V5(client),
                    connected,
                    connection_state,
                    command_receiver: Mutex::new(Some(command_receiver)),
                    task: Mutex::new(Some(task)),
                }
            } else {
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
                                    let _ = connection_sender.send(MqttConnectionState::Terminal(
                                        DeviceSdkError::Forbidden,
                                    ));
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
                                let command =
                                    serde_json::from_slice::<DeviceCommand>(&publish.payload);
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
                                            let _ = connection_sender
                                                .send(MqttConnectionState::Connecting);
                                            let _ = task_client.try_disconnect();
                                        }
                                    }
                                    _ => {
                                        task_connected.store(false, Ordering::Release);
                                        let _ =
                                            connection_sender.send(MqttConnectionState::Connecting);
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
                                    let _ = connection_sender
                                        .send(MqttConnectionState::Terminal(error));
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
                MqttState {
                    client: MqttClient::V311(client),
                    connected,
                    connection_state,
                    command_receiver: Mutex::new(Some(command_receiver)),
                    task: Mutex::new(Some(task)),
                }
            }
        };
        let _ = self.offline_policy;
        let client = DeviceClient {
            inner: Arc::new(DeviceInner {
                device,
                credentials,
                mqtt,
                max_payload_bytes: self.max_payload_bytes,
                message_expiry_interval: self.message_expiry_interval,
                shutdown,
                metrics,
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

    /// Returns whether the MQTT event loop currently has an authenticated connection.
    pub fn mqtt_connected(&self) -> bool {
        self.inner.mqtt.connected.load(Ordering::Acquire)
    }

    /// Waits for initial or recovered MQTT connectivity without creating a task.
    pub async fn wait_until_connected(&self, timeout: Duration) -> Result<(), DeviceSdkError> {
        if timeout.is_zero() {
            return Err(DeviceSdkError::InvalidConfiguration(
                "MQTT connection wait timeout must be nonzero".into(),
            ));
        }
        let mqtt = &self.inner.mqtt;
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
        let mqtt = &self.inner.mqtt;
        if !mqtt.connected.load(Ordering::Acquire) {
            return Err(DeviceSdkError::Offline);
        }
        let payload = serde_json::to_vec(&event)
            .map_err(|error| DeviceSdkError::Protocol(error.to_string()))?;
        if payload.len() > self.inner.max_payload_bytes {
            return Err(DeviceSdkError::Overloaded);
        }
        mqtt.client.try_publish(
            topic(&self.inner.device, "up"),
            qos,
            payload,
            self.inner.message_expiry_interval,
        )?;
        self.inner
            .metrics
            .publishes_accepted
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn commands(&self) -> Result<DeviceCommandStream, DeviceSdkError> {
        let mqtt = &self.inner.mqtt;
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
        let mqtt = &self.inner.mqtt;
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
        mqtt.client.try_publish(
            topic(&self.inner.device, "down_ack"),
            PublishQos::AtLeastOnce,
            payload,
            self.inner.message_expiry_interval,
        )
    }

    pub fn shutdown(&self) {
        self.inner.shutdown.cancel();
        self.inner.mqtt.client.try_disconnect();
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

fn terminal_v5_connect_error(code: V5ConnectReturnCode) -> Option<DeviceSdkError> {
    use V5ConnectReturnCode as Code;
    match code {
        Code::Success
        | Code::ServerBusy
        | Code::ServerUnavailable
        | Code::ServiceUnavailable
        | Code::QuotaExceeded
        | Code::ConnectionRateExceeded => None,
        Code::BadUserNamePassword
        | Code::NotAuthorized
        | Code::Banned
        | Code::BadAuthenticationMethod => Some(DeviceSdkError::Unauthenticated),
        Code::BadClientId | Code::ClientIdentifierNotValid => Some(
            DeviceSdkError::InvalidConfiguration("MQTT broker rejected the client ID".into()),
        ),
        Code::RefusedProtocolVersion | Code::UnsupportedProtocolVersion => Some(
            DeviceSdkError::Protocol("MQTT broker rejected protocol version 5.0".into()),
        ),
        _ => Some(DeviceSdkError::Protocol(format!(
            "MQTT 5 CONNACK: {code:?}"
        ))),
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
    async fn full_command_buffer_disconnects_without_acknowledging_overflow() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (start, ready) = tokio::sync::oneshot::channel();
            let command = DeviceCommand {
                command_id: CommandId::generate(),
                device: device(),
                expires_at: None,
                payload: DeviceCommandPayload {
                    name: "test".into(),
                    arguments: BTreeMap::new(),
                },
            };
            let expected = command.command_id;
            let broker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(read_mqtt_packet(&mut socket).await[0], 0x10);
                socket.write_all(&[0x20, 2, 0, 0]).await.unwrap();
                let subscribe = read_mqtt_packet(&mut socket).await;
                socket
                    .write_all(&[0x90, 3, subscribe[1], subscribe[2], 1])
                    .await
                    .unwrap();
                ready.await.unwrap();
                let topic = topic(&device(), "down");
                let payload = serde_json::to_vec(&command).unwrap();
                for id in [1u16, 2] {
                    let mut body = Vec::new();
                    body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
                    body.extend_from_slice(topic.as_bytes());
                    body.extend_from_slice(&id.to_be_bytes());
                    body.extend_from_slice(&payload);
                    let mut packet = vec![0x32];
                    let mut remaining = body.len();
                    loop {
                        let mut byte = (remaining % 128) as u8;
                        remaining /= 128;
                        if remaining != 0 {
                            byte |= 0x80;
                        }
                        packet.push(byte);
                        if remaining == 0 {
                            break;
                        }
                    }
                    packet.extend_from_slice(&body);
                    socket.write_all(&packet).await.unwrap();
                    let reply = read_mqtt_packet(&mut socket).await;
                    if id == 1 {
                        assert_eq!(reply, [0x40, 0, 1]);
                    } else {
                        assert_eq!(reply, [0xe0]); // Overflow must not PUBACK packet 2.
                    }
                }
            });
            let client = DeviceClient::builder()
                .device(device())
                .credentials(DeviceCredentials::new("credential", "secret").unwrap())
                .mqtt_endpoint(format!("mqtt://{address}"))
                .command_buffer_items(1)
                .connect()
                .await
                .unwrap();
            let oversized = BTreeMap::from([(
                "padding".into(),
                Scalar::Text("x".repeat(DEFAULT_MAX_PAYLOAD_BYTES)),
            )]);
            assert!(matches!(
                client.publish_telemetry(oversized).await,
                Err(DeviceSdkError::Overloaded)
            ));
            start.send(()).unwrap();
            broker.await.unwrap();
            assert_eq!(client.metrics().commands_received, 1);
            let mut commands = client.commands().unwrap();
            assert_eq!(commands.recv().await.unwrap().command_id, expected);
            assert!(commands.receiver.try_recv().is_err());
            client.shutdown();
        })
        .await
        .unwrap();
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

    #[tokio::test]
    async fn v5_sdk_connects_subscribes_and_publishes_with_expiry() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let broker = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let connect = read_mqtt_packet(&mut socket).await;
                assert_eq!(connect[7], 5);
                assert!(connect.windows(5).any(|part| part == [0x11, 0, 0, 0, 60]));
                socket.write_all(&[0x20, 0x03, 0, 0, 0]).await.unwrap();
                let subscribe = read_mqtt_packet(&mut socket).await;
                assert_eq!(subscribe[0], 0x82);
                socket
                    .write_all(&[0x90, 0x04, subscribe[1], subscribe[2], 0, 1])
                    .await
                    .unwrap();
                let publish = read_mqtt_packet(&mut socket).await;
                assert_eq!(publish[0] & 0xf0, 0x30);
                let topic_end = 3 + usize::from(u16::from_be_bytes([publish[1], publish[2]]));
                assert_eq!(publish[topic_end + 2], 5);
                assert_eq!(&publish[topic_end + 3..topic_end + 8], &[0x02, 0, 0, 0, 9]);
                socket
                    .write_all(&[0x40, 0x02, publish[topic_end], publish[topic_end + 1]])
                    .await
                    .unwrap();
            });
            let client = DeviceClient::builder()
                .device(device())
                .credentials(DeviceCredentials::new("credential", "secret").unwrap())
                .mqtt_endpoint(format!("mqtt://{address}"))
                .protocol_version(MqttProtocolVersion::V5)
                .session_expiry_interval(60)
                .message_expiry_interval(Some(9))
                .connect()
                .await
                .unwrap();
            client.publish_telemetry(BTreeMap::new()).await.unwrap();
            broker.await.unwrap();
            client.shutdown();
        })
        .await
        .unwrap();
    }
}
