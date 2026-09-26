//! Bounded, real-network Business RPC V2 load gate.
//! Run against a disposable gateway configured for development Business RPC.
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessDelivery, BusinessRpcClient, BusinessRpcClientConfig,
    BusinessRpcClientError, BusinessRpcTls, BusinessRpcV3Client, BusinessRpcV3ClientConfig,
    BusinessRpcV3Delivery,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, CommandAck, CommandId, DeliveryState, DeviceCommand,
    DeviceCommandPayload, DeviceEventKind, DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind,
    ExecutionState, Heartbeat, ProductId, Scalar, SourceMessageId, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, ResolveVerifierRequest,
        ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
};
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, DeviceSdkError, PublishQos};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    task::JoinSet,
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    scenario: String,
    business_address: SocketAddr,
    #[serde(default, skip_serializing)]
    business_transport: BusinessTransport,
    #[serde(default)]
    topology: Topology,
    #[serde(default)]
    network_profile: Option<String>,
    device_address: SocketAddr,
    udp_address: Option<SocketAddr>,
    management_url: Option<String>,
    gateway_pid: Option<u32>,
    duration_secs: u64,
    #[serde(default)]
    command_rate: u64,
    #[serde(default)]
    command_device_count: usize,
    #[serde(default)]
    tcp_command_device: bool,
    #[serde(default, skip_serializing)]
    command_transport: Option<BusinessTransport>,
    auth_concurrency: usize,
    #[serde(default)]
    auth_unique_devices: bool,
    #[serde(default)]
    auth_handler_delay_ms: u64,
    event_rate: u64,
    #[serde(default)]
    event_payload_bytes: usize,
    #[serde(default)]
    frame_payload_bytes: Option<u32>,
    #[serde(default)]
    stream_window_bytes: Option<u32>,
    event_ack_delay_ms: u64,
    #[serde(default)]
    event_reconnect_every_secs: u64,
    #[serde(default)]
    auth_reconnect_every_secs: u64,
    #[serde(default)]
    invalidate_every_secs: u64,
    #[serde(default)]
    verifier_rate: u64,
    reconnect_cycles: usize,
    #[serde(default)]
    reconnect_pause_ms: u64,
    sample_period_ms: u64,
    #[serde(default = "default_snapshot_every_secs")]
    snapshot_every_secs: u64,
    #[serde(default)]
    warmup_secs: u64,
    #[serde(default)]
    recovery_secs: u64,
}
fn default_snapshot_every_secs() -> u64 {
    900
}
#[derive(Clone, Default, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum BusinessTransport {
    #[default]
    DevelopmentToken,
    Mtls {
        server_name: String,
        ca_pem: PathBuf,
        certificate_pem: PathBuf,
        private_key_pem: PathBuf,
    },
}
impl BusinessTransport {
    fn name(&self) -> &'static str {
        match self {
            Self::DevelopmentToken => "development_token",
            Self::Mtls { .. } => "mtls",
        }
    }
}
#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Topology {
    #[default]
    Multiplexed,
    Dual,
    V3,
    V3Dual,
}
impl Topology {
    fn separate(self) -> bool {
        matches!(self, Self::Dual | Self::V3Dual)
    }
    fn v3(self) -> bool {
        matches!(self, Self::V3 | Self::V3Dual)
    }
}
impl Config {
    fn validate(&self) -> Result<()> {
        if ![
            "auth",
            "multiplexed",
            "reconnect",
            "verifier",
            "consumer_outage",
            "handshake",
            "steady_auth",
            "topology_compare",
            "soak",
        ]
        .contains(&self.scenario.as_str())
            || self.duration_secs == 0
            || self.duration_secs > 21_600
            || self.warmup_secs > self.duration_secs
            || self.recovery_secs > 600
            || self.auth_concurrency == 0
            || self.auth_concurrency > 256
            || self.auth_handler_delay_ms > 5_000
            || self.event_rate > 10_000
            || self.command_rate > 1_000
            || self.command_rate.saturating_mul(self.duration_secs) > 100_000
            || self.command_device_count > 200
            || (self.command_rate > 0
                && (!self.topology.v3()
                    || self.command_device_count == 0
                    || self.command_transport.is_none()))
            || self.event_payload_bytes > 16_384
            || self
                .frame_payload_bytes
                .is_some_and(|size| ![4096, 8192, 16384].contains(&size))
            || self.stream_window_bytes.is_some_and(|size| {
                size < self.frame_payload_bytes.unwrap_or(8192) || size > 4 * 1024 * 1024
            })
            || self
                .network_profile
                .as_ref()
                .is_some_and(|name| name.len() > 64)
            || self.event_ack_delay_ms > 60_000
            || self.event_reconnect_every_secs > 3600
            || self.auth_reconnect_every_secs > 3600
            || self.invalidate_every_secs > 3600
            || self.verifier_rate > 1000
            || self.reconnect_cycles > 10_000
            || self.reconnect_pause_ms > 60_000
            || !(100..=10_000).contains(&self.sample_period_ms)
            || !(1..=3600).contains(&self.snapshot_every_secs)
            || (matches!(self.business_transport, BusinessTransport::DevelopmentToken)
                && !self.business_address.ip().is_loopback())
            || !self.device_address.ip().is_loopback()
            || self
                .udp_address
                .is_some_and(|address| !address.ip().is_loopback())
        {
            return Err("invalid bounded Business RPC load configuration".into());
        }
        if let Some(url) = &self.management_url {
            let parsed = reqwest::Url::parse(url)?;
            if parsed.scheme() != "http"
                || parsed
                    .host_str()
                    .and_then(|host| host.parse::<std::net::IpAddr>().ok())
                    .is_none_or(|host| !host.is_loopback())
            {
                return Err("management metrics URL must use loopback HTTP".into());
            }
        }
        if self.scenario == "verifier" && self.udp_address.is_none() {
            return Err("verifier scenario requires udp_address".into());
        }
        if self.verifier_rate > 0 && self.udp_address.is_none() {
            return Err("verifier probe requires udp_address".into());
        }
        if self.scenario == "verifier" && self.duration_secs < 2 {
            return Err("verifier scenario requires at least 2 seconds for invalidation".into());
        }
        if let BusinessTransport::Mtls {
            server_name,
            ca_pem,
            certificate_pem,
            private_key_pem,
        } = &self.business_transport
            && (server_name.is_empty()
                || ca_pem.as_os_str().is_empty()
                || certificate_pem.as_os_str().is_empty()
                || private_key_pem.as_os_str().is_empty())
        {
            return Err("mTLS requires server_name, CA, certificate and private key paths".into());
        }
        Ok(())
    }
}

struct Handler {
    auth: AtomicU64,
    verifier: AtomicU64,
    auth_delay: Duration,
    latency: Mutex<Histogram>,
}
impl Handler {
    fn new(config: &Config) -> Self {
        Self {
            auth: AtomicU64::new(0),
            verifier: AtomicU64::new(0),
            auth_delay: Duration::from_millis(config.auth_handler_delay_ms),
            latency: Mutex::new(Histogram::default()),
        }
    }
}
fn identity(id: &str, revision: u64) -> std::result::Result<AuthenticatedDeviceWire, RpcError> {
    let device_id = DeviceId::new(id)
        .map_err(|_| RpcError::new(RpcErrorCode::DeviceRejected, "invalid device"))?;
    Ok(AuthenticatedDeviceWire {
        device_key: DeviceKey {
            tenant_id: TenantId::new("demo")
                .map_err(|_| RpcError::new(RpcErrorCode::Internal, "tenant"))?,
            product_id: ProductId::new("sensor")
                .map_err(|_| RpcError::new(RpcErrorCode::Internal, "product"))?,
            device_id,
        },
        credential_version: 1,
        auth_generation: 1,
        codec_id: CodecId::new("netbaiot-json")
            .map_err(|_| RpcError::new(RpcErrorCode::Internal, "codec"))?,
        codec_version: 1,
        publish: true,
        commands: true,
        auth_revision: revision,
    })
}
#[async_trait]
impl BusinessAuthHandler for Handler {
    async fn authenticate(
        &self,
        request: DeviceAuthenticateRequest,
    ) -> std::result::Result<AuthenticatedDeviceWire, RpcError> {
        let started = Instant::now();
        self.auth.fetch_add(1, Ordering::Relaxed);
        if !self.auth_delay.is_zero() {
            tokio::time::sleep(self.auth_delay).await;
        }
        if let Ok(mut latency) = self.latency.lock() {
            latency.add(started.elapsed());
        }
        let encoded_secret = SECRET
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        if request.secret_hex != encoded_secret || !request.credential_id.starts_with("cred-") {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "invalid credentials",
            ));
        }
        identity(&request.credential_id[5..], request.min_auth_revision)
    }
    async fn resolve_verifier(
        &self,
        request: ResolveVerifierRequest,
    ) -> std::result::Result<ResolveVerifierResponse, RpcError> {
        self.verifier.fetch_add(1, Ordering::Relaxed);
        if !request.credential_id.starts_with("cred-") {
            return Err(RpcError::new(
                RpcErrorCode::DeviceRejected,
                "invalid credential",
            ));
        }
        Ok(ResolveVerifierResponse {
            identity: identity(&request.credential_id[5..], request.min_auth_revision)?,
            verifier_key_hex: SECRET.into(),
        })
    }
}

#[derive(Default, Serialize)]
struct Counts {
    requests: u64,
    success: u64,
    rejected: u64,
    overloaded: u64,
    timeout: u64,
    transport_failures: u64,
    events: u64,
    event_acks: u64,
    event_retries: u64,
    command_requests: u64,
    command_accepted: u64,
    command_outcome_unknown: u64,
    command_errors: u64,
    command_unavailable: u64,
    command_delivery_after_unavailable: u64,
    command_accepted_without_delivery: u64,
    tcp_command_reconnects: u64,
    command_device_deliveries: u64,
    mqtt_command_device_deliveries: u64,
    tcp_command_device_deliveries: u64,
    command_device_duplicates: u64,
    command_device_acks: u64,
    command_ack_event_attempts: u64,
    command_ack_event_unique: u64,
    command_ack_event_acks: u64,
    publish_attempts: u64,
    publish_enqueued: u64,
    publish_errors: u64,
    reconnects: u64,
    sync_failures: u64,
    invalidations: u64,
    verifier_requests: u64,
    verifier_acks: u64,
    latency_ms: Percentiles,
    event_ack_ms: Percentiles,
}
#[derive(Clone, Copy, Default, Serialize)]
struct Percentiles {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}
#[derive(Default)]
struct Histogram {
    bins: Vec<u64>,
    count: u64,
}
impl Histogram {
    fn add(&mut self, elapsed: Duration) {
        if self.bins.is_empty() {
            self.bins.resize(70_001, 0);
        }
        let micros = elapsed.as_micros().min(60_000_000) as usize;
        let bucket = if micros < 100_000 {
            micros / 10
        } else {
            10_000 + micros / 1_000
        };
        self.bins[bucket.min(70_000)] += 1;
        self.count += 1;
    }
    fn percentile(&self, percent: u64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = self.count.saturating_mul(percent).div_ceil(100);
        let mut seen = 0;
        for (ms, count) in self.bins.iter().enumerate() {
            seen += count;
            if seen >= target {
                return if ms < 10_000 {
                    (ms as f64 + 1.0) / 100.0
                } else {
                    (ms - 10_000 + 1) as f64
                };
            }
        }
        60_000.0
    }
    fn summary(&self) -> Percentiles {
        Percentiles {
            p50: self.percentile(50),
            p95: self.percentile(95),
            p99: self.percentile(99),
            max: self.percentile(100),
        }
    }
}
#[derive(Default)]
struct Stats {
    counts: Counts,
    command_traces: BTreeMap<CommandId, CommandTrace>,
    command_device_seen: HashSet<CommandId>,
    command_unavailable_ids: HashSet<CommandId>,
    command_ack_event_seen: HashSet<netbaiot_core::EventId>,
    auth_latency: Histogram,
    ack_latency: Histogram,
    tls_handshake: Histogram,
    sync_ready: Histogram,
    full_ready: Histogram,
}
#[derive(Serialize)]
struct CommandTrace {
    command_id: CommandId,
    attempts: u32,
    rpc_outcomes: Vec<&'static str>,
    device_deliveries: u32,
    device_ack_submissions: u32,
    command_ack_event_attempts: u32,
    command_ack_event_unique: u32,
}
impl CommandTrace {
    fn new(command_id: CommandId) -> Self {
        Self {
            command_id,
            attempts: 1,
            rpc_outcomes: Vec::new(),
            device_deliveries: 0,
            device_ack_submissions: 0,
            command_ack_event_attempts: 0,
            command_ack_event_unique: 0,
        }
    }
}
type SharedStats = Arc<Mutex<Stats>>;

async fn connect_business(
    config: &Config,
    handler: Arc<Handler>,
    role: BusinessRole,
    stats: Option<&SharedStats>,
) -> Result<(
    BusinessRpcClient,
    tokio::sync::mpsc::Receiver<netbaiot_client::business_rpc::BusinessDelivery>,
)> {
    let mut settings = match &config.business_transport {
        BusinessTransport::DevelopmentToken => BusinessRpcClientConfig::development(
            config.business_address,
            std::env::var("NETBAIOT_BUSINESS_RPC_TOKEN")?,
            role,
        ),
        BusinessTransport::Mtls {
            server_name,
            ca_pem,
            certificate_pem,
            private_key_pem,
        } => {
            let mut settings =
                BusinessRpcClientConfig::development(config.business_address, String::new(), role);
            settings.token = None;
            settings.tls = Some(BusinessRpcTls {
                server_name: server_name.clone(),
                ca_pem: ca_pem.clone(),
                certificate_pem: certificate_pem.clone(),
                private_key_pem: private_key_pem.clone(),
            });
            settings
        }
    };
    settings.reconnect_initial = Duration::from_millis(100);
    let (client, deliveries) = BusinessRpcClient::connect(settings, Some(handler))?;
    tokio::time::timeout(Duration::from_secs(10), client.wait_ready()).await??;
    if let Some(stats) = stats
        && let Some(timing) = client.connection_timing()
    {
        let mut state = stats.lock().unwrap();
        state.full_ready.add(timing.full_ready);
        state.sync_ready.add(timing.sync_to_ready);
        if let Some(tls) = timing.tls_handshake {
            state.tls_handshake.add(tls);
        }
    }
    Ok((client, deliveries))
}

enum AnyBusiness {
    V2(BusinessRpcClient),
    V3(BusinessRpcV3Client),
}
impl AnyBusiness {
    async fn shutdown(&self) {
        match self {
            Self::V2(client) => client.shutdown().await,
            Self::V3(client) => client.shutdown().await,
        }
    }
    async fn invalidate(
        &self,
        revision: u64,
        scope: AuthInvalidation,
    ) -> std::result::Result<(), BusinessRpcClientError> {
        match self {
            Self::V2(client) => client.invalidate(revision, scope).await.map(|_| ()),
            Self::V3(client) => client.invalidate(revision, scope).await.map(|_| ()),
        }
    }
}
enum AnyDelivery {
    V2(BusinessDelivery),
    V3(BusinessRpcV3Delivery),
}
impl AnyDelivery {
    fn event_id(&self) -> netbaiot_core::EventId {
        match self {
            Self::V2(delivery) => delivery.delivery.event.event_id,
            Self::V3(delivery) => delivery.delivery.event.event_id,
        }
    }
    fn source_message_id(&self) -> &SourceMessageId {
        match self {
            Self::V2(delivery) => &delivery.delivery.event.source_message_id,
            Self::V3(delivery) => &delivery.delivery.event.source_message_id,
        }
    }
    fn command_ack_id(&self) -> Option<CommandId> {
        let kind = match self {
            Self::V2(delivery) => &delivery.delivery.event.kind,
            Self::V3(delivery) => &delivery.delivery.event.kind,
        };
        match kind {
            DeviceEventKind::CommandAck(ack) => Some(ack.command_id),
            _ => None,
        }
    }
    async fn ack(self) -> std::result::Result<(), BusinessRpcClientError> {
        match self {
            Self::V2(delivery) => delivery.ack().await,
            Self::V3(delivery) => delivery.ack().await,
        }
    }
}
enum AnyDeliveries {
    V2(tokio::sync::mpsc::Receiver<BusinessDelivery>),
    V3(tokio::sync::mpsc::Receiver<BusinessRpcV3Delivery>),
}
impl AnyDeliveries {
    async fn recv(&mut self) -> Option<AnyDelivery> {
        match self {
            Self::V2(deliveries) => deliveries.recv().await.map(AnyDelivery::V2),
            Self::V3(deliveries) => deliveries.recv().await.map(AnyDelivery::V3),
        }
    }
}
async fn connect_event_business(
    config: &Config,
    handler: Arc<Handler>,
    role: BusinessRole,
    stats: &SharedStats,
) -> Result<(AnyBusiness, AnyDeliveries)> {
    if !config.topology.v3() {
        let (client, deliveries) = connect_business(config, handler, role, Some(stats)).await?;
        return Ok((AnyBusiness::V2(client), AnyDeliveries::V2(deliveries)));
    }
    let mut settings = BusinessRpcV3ClientConfig::development(
        config.business_address,
        std::env::var("NETBAIOT_BUSINESS_RPC_TOKEN").unwrap_or_default(),
    );
    settings.provider = role.auth_control();
    settings.events = role.events();
    if let BusinessTransport::Mtls {
        server_name,
        ca_pem,
        certificate_pem,
        private_key_pem,
    } = &config.business_transport
    {
        settings.token = None;
        settings.tls = Some(BusinessRpcTls {
            server_name: server_name.clone(),
            ca_pem: ca_pem.clone(),
            certificate_pem: certificate_pem.clone(),
            private_key_pem: private_key_pem.clone(),
        });
        settings.filter.tenant = Some(TenantId::new("demo")?);
    }
    if let Some(frame_bytes) = config.frame_payload_bytes {
        settings.limits.max_frame_payload_bytes = frame_bytes;
    }
    if let Some(window_bytes) = config.stream_window_bytes {
        settings.limits.initial_stream_window_bytes = window_bytes;
    }
    settings.reconnect_initial = Duration::from_millis(100);
    let (client, deliveries) = BusinessRpcV3Client::connect(settings, Some(handler))?;
    tokio::time::timeout(Duration::from_secs(10), client.wait_ready()).await??;
    Ok((AnyBusiness::V3(client), AnyDeliveries::V3(deliveries)))
}

async fn connect_command_business(config: &Config) -> Result<BusinessRpcV3Client> {
    let mut settings = BusinessRpcV3ClientConfig::development(
        config.business_address,
        std::env::var("NETBAIOT_BUSINESS_RPC_TOKEN").unwrap_or_default(),
    );
    settings.provider = false;
    settings.events = false;
    if let Some(BusinessTransport::Mtls {
        server_name,
        ca_pem,
        certificate_pem,
        private_key_pem,
    }) = &config.command_transport
    {
        settings.token = None;
        settings.tls = Some(BusinessRpcTls {
            server_name: server_name.clone(),
            ca_pem: ca_pem.clone(),
            certificate_pem: certificate_pem.clone(),
            private_key_pem: private_key_pem.clone(),
        });
    }
    let (client, _) = BusinessRpcV3Client::connect(settings, None)?;
    tokio::time::timeout(Duration::from_secs(10), client.wait_ready()).await??;
    Ok(client)
}
async fn device(config: &Config, id: &str) -> Result<DeviceClient> {
    let client = DeviceClient::builder()
        .device(
            identity(id, 1)
                .map_err(|_| "invalid load device id")?
                .device_key,
        )
        .credentials(DeviceCredentials::new(format!("cred-{id}"), SECRET)?)
        .mqtt_endpoint(format!("mqtt://{}", config.device_address))
        .client_id(format!("load-{id}"))
        .connect()
        .await?;
    Ok(client)
}

async fn tcp_command_device(config: &Config) -> Result<TcpStream> {
    let mut socket = TcpStream::connect(config.device_address).await?;
    let hello = serde_json::to_vec(&serde_json::json!({
        "credential_id": "cred-tcp-command",
        "secret": SECRET,
    }))?;
    socket
        .write_all(&u32::try_from(hello.len())?.to_be_bytes())
        .await?;
    socket.write_all(&hello).await?;
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await?;
    let length = u32::from_be_bytes(header) as usize;
    if length > 1024 {
        return Err("oversized TCP authentication response".into());
    }
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await?;
    if body != br#"{"authenticated":true}"# {
        return Err("TCP command device was not authenticated".into());
    }
    Ok(socket)
}

async fn reconnect_tcp_command_device(config: &Config, until: Instant) -> Option<TcpStream> {
    while Instant::now() < until {
        if let Ok(Ok(socket)) =
            tokio::time::timeout(Duration::from_secs(2), tcp_command_device(config)).await
        {
            return Some(socket);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}
async fn auth_load(config: Config, stats: SharedStats, until: Instant) {
    let mut tasks = JoinSet::new();
    let mut next = 0u64;
    while Instant::now() < until || !tasks.is_empty() {
        while Instant::now() < until && tasks.len() < config.auth_concurrency {
            let config = config.clone();
            // The default repeats a bounded device set to exercise auth-cache hits.
            // The opt-in unique mode measures a fresh provider RPC for each attempt.
            let id = format!(
                "load{}",
                if config.auth_unique_devices {
                    next
                } else {
                    next % 32
                }
            );
            next += 1;
            tasks.spawn(async move {
                let start = Instant::now();
                let result = device(&config, &id).await;
                let latency = start.elapsed();
                if let Ok(ref client) = result {
                    let _ = client.shutdown_with_timeout(Duration::from_secs(1)).await;
                }
                (latency, result.map(|_| ()))
            });
        }
        let Some(result) = tasks.join_next().await else {
            break;
        };
        if let Ok((elapsed, answer)) = result {
            let mut state = stats.lock().unwrap();
            state.counts.requests += 1;
            state.auth_latency.add(elapsed);
            match answer {
                Ok(_) => state.counts.success += 1,
                Err(error) => match error.downcast_ref::<DeviceSdkError>() {
                    Some(DeviceSdkError::Overloaded) => state.counts.overloaded += 1,
                    Some(DeviceSdkError::Timeout) => state.counts.timeout += 1,
                    Some(DeviceSdkError::Unauthenticated | DeviceSdkError::Forbidden) => {
                        state.counts.rejected += 1
                    }
                    _ => state.counts.transport_failures += 1,
                },
            }
        }
    }
}

async fn event_load(
    config: &Config,
    stats: SharedStats,
    outage: bool,
) -> Result<(u64, Option<Percentiles>)> {
    let handler = Arc::new(Handler::new(config));
    let auth_role = if config.topology.separate() {
        BusinessRole::AuthControl
    } else {
        BusinessRole::Multiplexed
    };
    let (mut business, auth_deliveries) =
        connect_event_business(config, handler.clone(), auth_role, &stats).await?;
    let (mut event_business, mut deliveries) = if config.topology.separate() {
        let (events, deliveries) =
            connect_event_business(config, handler.clone(), BusinessRole::Events, &stats).await?;
        (Some(events), deliveries)
    } else {
        (None, auth_deliveries)
    };
    let publisher = device(config, "publisher").await?;
    let command_business = if config.command_rate > 0 {
        Some(connect_command_business(config).await?)
    } else {
        None
    };
    let mut command_receivers = Vec::new();
    for index in 0..config.command_device_count {
        let client = device(config, &format!("command-{index}")).await?;
        let commands = client.commands()?;
        client.wait_until_connected(Duration::from_secs(5)).await?;
        command_receivers.push((client, commands));
    }
    let tcp_command_socket = if config.command_rate > 0 && config.tcp_command_device {
        Some(tcp_command_device(config).await?)
    } else {
        None
    };
    let until = Instant::now() + Duration::from_secs(config.duration_secs);
    let command_worker_until = until + Duration::from_secs(2);
    let mut command_devices = Vec::new();
    let mut command_workers = JoinSet::new();
    for (client, mut commands) in command_receivers {
        let worker_client = client.clone();
        let worker_stats = stats.clone();
        command_workers.spawn(async move {
            loop {
                let received = tokio::select! {
                    _ = tokio::time::sleep_until(command_worker_until.into()) => break,
                    received = commands.recv() => received,
                };
                let Some(command) = received else { break };
                {
                    let mut state = worker_stats.lock().unwrap();
                    state.counts.command_device_deliveries += 1;
                    state.counts.mqtt_command_device_deliveries += 1;
                    if !state.command_device_seen.insert(command.command_id) {
                        state.counts.command_device_duplicates += 1;
                    }
                    if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                        trace.device_deliveries += 1;
                    }
                }
                if worker_client
                    .ack_command(command.command_id, ExecutionState::Succeeded)
                    .await
                    .is_ok()
                {
                    let mut state = worker_stats.lock().unwrap();
                    state.counts.command_device_acks += 1;
                    if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                        trace.device_ack_submissions += 1;
                    }
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
        command_devices.push(client);
    }
    if let Some(mut socket) = tcp_command_socket {
        let worker_stats = stats.clone();
        let tcp_config = config.clone();
        command_workers.spawn(async move {
            loop {
                let mut header = [0u8; 4];
                let read = tokio::select! {
                    _ = tokio::time::sleep_until(command_worker_until.into()) => break,
                    read = socket.read_exact(&mut header) => read,
                };
                if read.is_err() {
                    let Some(reconnected) =
                        reconnect_tcp_command_device(&tcp_config, command_worker_until).await
                    else {
                        break;
                    };
                    worker_stats.lock().unwrap().counts.tcp_command_reconnects += 1;
                    socket = reconnected;
                    continue;
                }
                let length = u32::from_be_bytes(header) as usize;
                if length > 65_536 {
                    return Err("oversized TCP command frame".into());
                }
                let mut body = vec![0; length];
                if socket.read_exact(&mut body).await.is_err() {
                    let Some(reconnected) =
                        reconnect_tcp_command_device(&tcp_config, command_worker_until).await
                    else {
                        break;
                    };
                    worker_stats.lock().unwrap().counts.tcp_command_reconnects += 1;
                    socket = reconnected;
                    continue;
                }
                let Ok(command) = serde_json::from_slice::<DeviceCommand>(&body) else {
                    // A TCP EventAccepted receipt may follow an application ACK.
                    continue;
                };
                {
                    let mut state = worker_stats.lock().unwrap();
                    state.counts.command_device_deliveries += 1;
                    state.counts.tcp_command_device_deliveries += 1;
                    if !state.command_device_seen.insert(command.command_id) {
                        state.counts.command_device_duplicates += 1;
                    }
                    if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                        trace.device_deliveries += 1;
                    }
                }
                let ack = DeviceUplink::new(
                    SourceMessageId::new(format!("tcp-command-ack-{}", uuid::Uuid::new_v4()))?,
                    DeviceUplinkKind::CommandAck(CommandAck {
                        command_id: command.command_id,
                        execution: ExecutionState::Succeeded,
                    }),
                );
                let encoded = serde_json::to_vec(&ack)?;
                let header = u32::try_from(encoded.len())?.to_be_bytes();
                if socket.write_all(&header).await.is_err()
                    || socket.write_all(&encoded).await.is_err()
                {
                    let Some(reconnected) =
                        reconnect_tcp_command_device(&tcp_config, command_worker_until).await
                    else {
                        break;
                    };
                    worker_stats.lock().unwrap().counts.tcp_command_reconnects += 1;
                    socket = reconnected;
                    continue;
                }
                let mut state = worker_stats.lock().unwrap();
                state.counts.command_device_acks += 1;
                if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                    trace.device_ack_submissions += 1;
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
    }
    let outage_until = Instant::now() + Duration::from_secs(config.duration_secs / 2);
    let mut workers = JoinSet::new();
    let auth_config = config.clone();
    let auth_stats = stats.clone();
    workers.spawn(async move {
        auth_load(auth_config, auth_stats, until).await;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    if config.verifier_rate > 0 {
        let verifier_config = config.clone();
        let verifier_stats = stats.clone();
        workers.spawn(async move { verifier_probe(verifier_config, verifier_stats, until).await });
    }
    let mut tick = tokio::time::interval(Duration::from_secs_f64(
        1.0 / config.event_rate.max(1) as f64,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut command_tick = tokio::time::interval(Duration::from_secs_f64(
        1.0 / config.command_rate.max(1) as f64,
    ));
    command_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let reconnect_period = Duration::from_secs(config.event_reconnect_every_secs.max(1));
    let mut reconnect_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + reconnect_period,
        reconnect_period,
    );
    let auth_reconnect_period = Duration::from_secs(config.auth_reconnect_every_secs.max(1));
    let mut auth_reconnect_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + auth_reconnect_period,
        auth_reconnect_period,
    );
    let invalidate_period = Duration::from_secs(config.invalidate_every_secs.max(1));
    let mut invalidate_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + invalidate_period,
        invalidate_period,
    );
    let mut revision = 1u64;
    let mut sequence = 0u64;
    let mut command_sequence = 0u64;
    let mut seen = HashSet::new();
    let source_prefix = format!("load-{}-", uuid::Uuid::new_v4().simple());
    while Instant::now() < until {
        tokio::select! {
            _ = command_tick.tick(), if config.command_rate > 0 => {
                let index = command_sequence as usize
                    % (config.command_device_count + usize::from(config.tcp_command_device));
                command_sequence += 1;
                let target = if index == config.command_device_count {
                    "tcp-command".to_owned()
                } else {
                    format!("command-{index}")
                };
                let command = DeviceCommand {
                    command_id: CommandId::generate(),
                    device: identity(&target, 1)
                        .map_err(|_| "invalid command device")?.device_key,
                    expires_at: None,
                    payload: DeviceCommandPayload {
                        name: "readiness-ping".into(),
                        arguments: Default::default(),
                    },
                };
                let Some(client) = &command_business else { return Err("missing command client".into()); };
                {
                    let mut state = stats.lock().unwrap();
                    state.counts.command_requests += 1;
                    state.command_traces.insert(command.command_id, CommandTrace::new(command.command_id));
                }
                let result = client.send_command(&command).await;
                match result {
                    Ok(receipt) if receipt.state == DeliveryState::Queued => {
                        {
                            let mut state = stats.lock().unwrap();
                            state.counts.command_accepted += 1;
                            if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                                trace.rpc_outcomes.push("queued");
                            }
                        }
                        if command_sequence.is_multiple_of(10) {
                            {
                                let mut state = stats.lock().unwrap();
                                state.counts.command_requests += 1;
                                if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                                    trace.attempts += 1;
                                }
                            }
                            let retry_ok = matches!(client.send_command(&command).await, Ok(retry) if retry == receipt);
                            let mut state = stats.lock().unwrap();
                            if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                                trace.rpc_outcomes.push(if retry_ok { "dedup_queued" } else { "retry_error" });
                            }
                            if !retry_ok {
                                state.counts.command_errors += 1;
                            }
                        }
                    }
                    Err(BusinessRpcClientError::OutcomeUnknown) => {
                        {
                            let mut state = stats.lock().unwrap();
                            state.counts.command_outcome_unknown += 1;
                            state.counts.command_requests += 1;
                            if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                                trace.rpc_outcomes.push("outcome_unknown");
                                trace.attempts += 1;
                            }
                        }
                        let retry_ok = client.send_command(&command).await.is_ok();
                        let mut state = stats.lock().unwrap();
                        if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                            trace.rpc_outcomes.push(if retry_ok { "queued_after_unknown" } else { "retry_error" });
                        }
                        if retry_ok {
                            state.counts.command_accepted += 1;
                        } else {
                            state.counts.command_errors += 1;
                        }
                    }
                    Err(BusinessRpcClientError::Remote(RpcErrorCode::Unavailable))
                    | Err(BusinessRpcClientError::Unavailable) => {
                        let mut state = stats.lock().unwrap();
                        state.counts.command_unavailable += 1;
                        state.command_unavailable_ids.insert(command.command_id);
                        if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                            trace.rpc_outcomes.push("unavailable");
                        }
                    }
                    _ => {
                        let mut state = stats.lock().unwrap();
                        state.counts.command_errors += 1;
                        if let Some(trace) = state.command_traces.get_mut(&command.command_id) {
                            trace.rpc_outcomes.push("unexpected_error");
                        }
                    }
                }
            }
            _ = invalidate_tick.tick(), if config.invalidate_every_secs > 0 => {
                revision = revision.saturating_add(1);
                let device = identity("udp", revision).map_err(|_| "invalid verifier identity")?.device_key;
                if business.invalidate(revision, AuthInvalidation::Device { device }).await.is_ok() {
                    stats.lock().unwrap().counts.invalidations += 1;
                } else {
                    stats.lock().unwrap().counts.sync_failures += 1;
                }
            }
            _ = auth_reconnect_tick.tick(), if config.auth_reconnect_every_secs > 0 && config.topology.separate() => {
                business.shutdown().await;
                match connect_event_business(config, handler.clone(), BusinessRole::AuthControl, &stats).await {
                    Ok((next, _)) => {
                        business = next;
                        revision = 1;
                        stats.lock().unwrap().counts.reconnects += 1;
                    }
                    Err(error) => {
                        stats.lock().unwrap().counts.sync_failures += 1;
                        return Err(error);
                    }
                }
            }
            _ = reconnect_tick.tick(), if config.event_reconnect_every_secs > 0 => {
                if let Some(events) = event_business.take() { events.shutdown().await; }
                else { business.shutdown().await; }
                let role = if config.topology.separate() { BusinessRole::Events } else { BusinessRole::Multiplexed };
                match connect_event_business(config, handler.clone(), role, &stats).await {
                    Ok((next, next_deliveries)) => {
                        if config.topology.separate() { event_business = Some(next); }
                        else { business = next; revision = 1; }
                        deliveries = next_deliveries;
                        stats.lock().unwrap().counts.reconnects += 1;
                    }
                    Err(error) => {
                        stats.lock().unwrap().counts.sync_failures += 1;
                        return Err(error);
                    }
                }
            }
            _ = tick.tick(), if config.event_rate > 0 => {
                sequence += 1;
                let kind = if config.event_payload_bytes == 0 {
                    DeviceUplinkKind::Heartbeat(Heartbeat { sequence })
                } else {
                    let mut fields = BTreeMap::new();
                    let mut remaining = config.event_payload_bytes;
                    for index in 0..64 {
                        if remaining == 0 { break; }
                        let length = remaining.min(256);
                        fields.insert(format!("f{index:02}"), Scalar::Text("x".repeat(length)));
                        remaining -= length;
                    }
                    DeviceUplinkKind::Telemetry(fields)
                };
                let payload = DeviceUplink::new(SourceMessageId::new(format!("{source_prefix}{sequence}"))?, kind);
                let published = publisher.publish(payload, PublishQos::AtLeastOnce).await;
                let mut state = stats.lock().unwrap();
                state.counts.publish_attempts += 1;
                if published.is_ok() { state.counts.publish_enqueued += 1; }
                else { state.counts.publish_errors += 1; }
            }
            received = deliveries.recv() => {
                let Some(delivery) = received else { break };
                if let Some(command_id) = delivery.command_ack_id() {
                    let event_id = delivery.event_id();
                    {
                        let mut state = stats.lock().unwrap();
                        state.counts.command_ack_event_attempts += 1;
                        let unique = state.command_ack_event_seen.insert(event_id);
                        if unique {
                            state.counts.command_ack_event_unique += 1;
                        }
                        if let Some(trace) = state.command_traces.get_mut(&command_id) {
                            trace.command_ack_event_attempts += 1;
                            if unique { trace.command_ack_event_unique += 1; }
                        }
                    }
                    if delivery.ack().await.is_ok() {
                        stats.lock().unwrap().counts.command_ack_event_acks += 1;
                    }
                    continue;
                }
                if !delivery.source_message_id().as_str().starts_with(&source_prefix) {
                    let _ = delivery.ack().await;
                    continue;
                }
                let start = Instant::now();
                {
                    let mut state = stats.lock().unwrap();
                    state.counts.events += 1;
                    if !seen.insert(delivery.event_id()) { state.counts.event_retries += 1; }
                }
                if seen.len() > 100_000 { seen.clear(); }
                if outage && Instant::now() < outage_until { continue; }
                tokio::time::sleep(Duration::from_millis(config.event_ack_delay_ms)).await;
                if delivery.ack().await.is_ok() {
                    let mut state = stats.lock().unwrap();
                    state.counts.event_acks += 1;
                    state.ack_latency.add(start.elapsed());
                }
            }
        }
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    if config.command_rate > 0 {
        let drain_until = Instant::now() + Duration::from_secs(5);
        while Instant::now() < drain_until {
            let Ok(Some(delivery)) =
                tokio::time::timeout(Duration::from_millis(200), deliveries.recv()).await
            else {
                continue;
            };
            if let Some(command_id) = delivery.command_ack_id() {
                let event_id = delivery.event_id();
                {
                    let mut state = stats.lock().unwrap();
                    state.counts.command_ack_event_attempts += 1;
                    let unique = state.command_ack_event_seen.insert(event_id);
                    if unique {
                        state.counts.command_ack_event_unique += 1;
                    }
                    if let Some(trace) = state.command_traces.get_mut(&command_id) {
                        trace.command_ack_event_attempts += 1;
                        if unique {
                            trace.command_ack_event_unique += 1;
                        }
                    }
                }
                if delivery.ack().await.is_ok() {
                    stats.lock().unwrap().counts.command_ack_event_acks += 1;
                }
            } else if delivery
                .source_message_id()
                .as_str()
                .starts_with(&source_prefix)
            {
                {
                    let mut state = stats.lock().unwrap();
                    state.counts.events += 1;
                    if !seen.insert(delivery.event_id()) {
                        state.counts.event_retries += 1;
                    }
                }
                if delivery.ack().await.is_ok() {
                    stats.lock().unwrap().counts.event_acks += 1;
                }
            } else {
                let _ = delivery.ack().await;
            }
        }
    }
    let _ = publisher
        .shutdown_with_timeout(Duration::from_secs(1))
        .await;
    for client in command_devices {
        let _ = client.shutdown_with_timeout(Duration::from_secs(1)).await;
    }
    while let Some(result) = command_workers.join_next().await {
        result??;
    }
    if let Some(client) = command_business {
        client.shutdown().await;
    }
    business.shutdown().await;
    if let Some(events) = event_business {
        events.shutdown().await;
    }
    let handler_latency = handler
        .latency
        .lock()
        .ok()
        .and_then(|h| (h.count > 0).then(|| h.summary()));
    Ok((handler.auth.load(Ordering::Relaxed), handler_latency))
}

async fn verifier_probe(config: Config, stats: SharedStats, until: Instant) -> Result<()> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let address = config
        .udp_address
        .ok_or("verifier probe requires udp_address")?;
    let boot_id = uuid::Uuid::new_v4().into_bytes();
    let mut tick =
        tokio::time::interval(Duration::from_secs_f64(1.0 / config.verifier_rate as f64));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0u64;
    while Instant::now() < until {
        tick.tick().await;
        sequence += 1;
        let payload = serde_json::to_vec(&DeviceUplink::new(
            SourceMessageId::new(format!("soak-udp-{sequence}"))?,
            DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
        ))?;
        let credential = b"cred-udp";
        let mut datagram = b"NBI1".to_vec();
        datagram.push(credential.len() as u8);
        datagram.extend_from_slice(credential);
        datagram.extend_from_slice(&1u32.to_be_bytes());
        datagram.extend_from_slice(&boot_id);
        datagram.extend_from_slice(&sequence.to_be_bytes());
        datagram.extend_from_slice(&netbaiot_runtime::now_ms().to_be_bytes());
        datagram.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        datagram.extend_from_slice(&payload);
        let key = (0..32u8).collect::<Vec<_>>();
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(&datagram);
        datagram.extend_from_slice(&mac.finalize().into_bytes());
        socket.send_to(&datagram, address).await?;
        let mut ack = [0u8; 64];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut ack)).await;
        let mut state = stats.lock().unwrap();
        state.counts.verifier_requests += 1;
        if matches!(result, Ok(Ok((64, _)))) && &ack[..4] == b"NBA1" {
            state.counts.verifier_acks += 1;
        }
    }
    Ok(())
}

async fn verifier_load(config: &Config, stats: SharedStats) -> Result<(u64, u64)> {
    let handler = Arc::new(Handler::new(config));
    let (business, mut deliveries) = connect_business(
        config,
        handler.clone(),
        BusinessRole::Multiplexed,
        Some(&stats),
    )
    .await?;
    let acknowledger = tokio::spawn(async move {
        while let Some(delivery) = deliveries.recv().await {
            let _ = delivery.ack().await;
        }
    });
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let address = config.udp_address.ok_or("missing UDP address")?;
    let until = Instant::now() + Duration::from_secs(config.duration_secs);
    let boot_id = uuid::Uuid::new_v4().into_bytes();
    let mut pacing = tokio::time::interval(Duration::from_secs_f64(
        1.0 / config.event_rate.max(1) as f64,
    ));
    pacing.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0u64;
    let mut calls_before_invalidation = None;
    while Instant::now() < until {
        pacing.tick().await;
        if calls_before_invalidation.is_none()
            && Instant::now() >= until - Duration::from_secs(config.duration_secs / 2)
        {
            calls_before_invalidation = Some(handler.verifier.load(Ordering::Relaxed));
            business
                .invalidate(
                    2,
                    AuthInvalidation::Device {
                        device: identity("udp", 2)
                            .map_err(|_| "invalid verifier identity")?
                            .device_key,
                    },
                )
                .await?;
        }
        sequence += 1;
        let payload = serde_json::to_vec(&DeviceUplink::new(
            SourceMessageId::new(format!("udp-{sequence}"))?,
            DeviceUplinkKind::Heartbeat(Heartbeat { sequence }),
        ))?;
        let credential = b"cred-udp";
        let mut datagram = b"NBI1".to_vec();
        datagram.push(credential.len() as u8);
        datagram.extend_from_slice(credential);
        datagram.extend_from_slice(&1u32.to_be_bytes());
        datagram.extend_from_slice(&boot_id);
        datagram.extend_from_slice(&sequence.to_be_bytes());
        datagram.extend_from_slice(&netbaiot_runtime::now_ms().to_be_bytes());
        datagram.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        datagram.extend_from_slice(&payload);
        let key = (0..32u8).collect::<Vec<_>>();
        let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
        mac.update(&datagram);
        datagram.extend_from_slice(&mac.finalize().into_bytes());
        let start = Instant::now();
        socket.send_to(&datagram, address).await?;
        let mut ack = [0u8; 64];
        let result = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut ack)).await;
        let mut state = stats.lock().unwrap();
        state.counts.requests += 1;
        state.auth_latency.add(start.elapsed());
        if matches!(result, Ok(Ok((64, _)))) && &ack[..4] == b"NBA1" {
            state.counts.success += 1;
        } else {
            state.counts.timeout += 1;
        }
    }
    business.shutdown().await;
    acknowledger.await?;
    let before = calls_before_invalidation.ok_or("verifier invalidation did not execute")?;
    let total = handler.verifier.load(Ordering::Relaxed);
    if before != 1 || total != 2 {
        return Err("verifier cache lookup count did not match cold/hot/invalidate/reload".into());
    }
    Ok((before, total))
}

fn rss_kb(pid: u32) -> Option<u64> {
    let proc_path = format!("/proc/{pid}/status");
    if Path::new(&proc_path).exists() {
        let status = std::fs::read_to_string(proc_path).ok()?;
        return status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|line| line.split_whitespace().next())
            .and_then(|value| value.parse().ok());
    }
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}
fn cpu_percent(pid: u32) -> Option<f64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "%cpu=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

#[derive(Default, Serialize)]
struct CpuSamples {
    baseline_percent: Option<f64>,
    warm_percent: Option<f64>,
    average_workload_percent: Option<f64>,
    peak_sampled_percent: Option<f64>,
    recovery_percent: Option<f64>,
    last_percent: Option<f64>,
    #[serde(skip)]
    total: f64,
    #[serde(skip)]
    count: u64,
}
impl CpuSamples {
    fn add(&mut self, value: f64, elapsed: Duration, config: &Config) {
        self.last_percent = Some(value);
        self.baseline_percent.get_or_insert(value);
        if elapsed >= Duration::from_secs(config.warmup_secs) {
            self.warm_percent.get_or_insert(value);
            let workload_secs =
                config
                    .duration_secs
                    .saturating_mul(if config.scenario == "topology_compare" {
                        2
                    } else {
                        1
                    });
            if elapsed <= Duration::from_secs(workload_secs) {
                self.total += value;
                self.count += 1;
                self.average_workload_percent = Some(self.total / self.count as f64);
                self.peak_sampled_percent =
                    Some(self.peak_sampled_percent.unwrap_or(value).max(value));
            } else {
                self.recovery_percent = Some(value);
            }
        }
    }
}

#[derive(Default, Serialize)]
struct Peaks {
    metrics_observed: bool,
    rss_baseline_kb: Option<u64>,
    rss_warm_kb: Option<u64>,
    rss_peak_kb: Option<u64>,
    rss_last_kb: Option<u64>,
    rss_post_soak_kb: Option<u64>,
    rss_post_recovery_kb: Option<u64>,
    gateway_cpu: CpuSamples,
    loadgen_cpu: CpuSamples,
    pending_items: u64,
    pending_bytes: u64,
    pending_items_last: u64,
    pending_bytes_last: u64,
    active_business_connections_peak: u64,
    active_business_connections_last: u64,
    v3_active_streams_peak: u64,
    v3_active_streams_last: u64,
    v3_queued_bytes_peak: u64,
    v3_queued_bytes_last: u64,
    v3_reassembly_bytes_peak: u64,
    v3_reassembly_bytes_last: u64,
    command_dedup_entries_peak: u64,
    command_dedup_entries_last: u64,
    command_dedup_inflight_peak: u64,
    command_dedup_inflight_last: u64,
    auth_queue_items: u64,
    control_queue_items: u64,
    control_queue_bytes: u64,
    control_queue_bytes_last: u64,
    event_queue_items: u64,
    event_queue_bytes: u64,
    event_queue_bytes_last: u64,
    command_queue_bytes_peak: u64,
    command_queue_bytes_last: u64,
    late_responses_total: u64,
    overloads_total: u64,
    provider_sync_success_total: u64,
    provider_sync_failure_total: u64,
    business_reconnects_total: u64,
    event_acks_total: u64,
    revision_gaps_total: u64,
    offline_grace_expirations_total: u64,
    timeline: Vec<Milestone>,
}
#[derive(Serialize)]
struct Milestone {
    elapsed_seconds: u64,
    rss_kib: Option<u64>,
    cpu_percent: Option<f64>,
    pending_items: Option<u64>,
    pending_bytes: Option<u64>,
    connections: Option<u64>,
    v3_active_streams: Option<u64>,
    v3_queued_bytes: Option<u64>,
    v3_reassembly_bytes: Option<u64>,
    command_dedup_entries: Option<u64>,
    command_dedup_inflight: Option<u64>,
    control_queue_bytes: Option<u64>,
    event_queue_bytes: Option<u64>,
    command_queue_bytes: Option<u64>,
    reconnects: u64,
    sync_failures: u64,
    late_responses_total: Option<u64>,
    overloads_total: Option<u64>,
    event_retries: u64,
    auth_success: u64,
    auth_failures: u64,
}
fn metric(body: &str, name: &str) -> u64 {
    body.lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}
async fn read_gateway_metrics(config: &Config) -> Option<String> {
    let url = config.management_url.as_ref()?;
    let admin = std::env::var("NETBAIOT_ADMIN_SECRET").ok()?;
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(2))
        .build()
        .ok()?
        .get(format!("{url}/api/v1/metrics"))
        .bearer_auth(admin)
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()
}
#[derive(Serialize)]
struct GatewayPercentiles {
    p50: Option<f64>,
    p95: Option<f64>,
    p99: Option<f64>,
    max: Option<f64>,
}
fn gateway_histogram(before: &str, after: &str, name: &str) -> Option<GatewayPercentiles> {
    let prefix = format!("netbaiot_{name}_bucket{{le=\"");
    let parse = |body: &str| -> Vec<(String, u64)> {
        body.lines()
            .filter_map(|line| {
                let rest = line.strip_prefix(&prefix)?;
                let (bound, value) = rest.split_once("\"} ")?;
                Some((bound.to_owned(), value.parse().ok()?))
            })
            .collect()
    };
    let old = parse(before);
    let new = parse(after);
    if old.len() != new.len() || old.is_empty() {
        return None;
    }
    let total = new.last()?.1.checked_sub(old.last()?.1)?;
    if total == 0 {
        return None;
    }
    let percentile = |percent: u64| -> Option<f64> {
        let target = total.saturating_mul(percent).div_ceil(100);
        new.iter()
            .zip(&old)
            .find_map(|((bound, count), (old_bound, prior))| {
                if bound != old_bound || count.checked_sub(*prior)? < target {
                    return None;
                }
                bound.parse::<f64>().ok().map(|us| us / 1000.0)
            })
    };
    Some(GatewayPercentiles {
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
        max: None,
    })
}
fn gateway_delta(before: &str, after: &str, name: &str) -> Option<u64> {
    let key = format!("netbaiot_{name} ");
    let parse = |body: &str| {
        body.lines()
            .find_map(|line| line.strip_prefix(&key)?.parse::<u64>().ok())
    };
    parse(after)?.checked_sub(parse(before)?)
}
async fn sample_gateway(
    config: Config,
    peaks: Arc<Mutex<Peaks>>,
    stats: SharedStats,
    stop: Arc<AtomicBool>,
) {
    let client = match reqwest::Client::builder().no_proxy().build() {
        Ok(client) => client,
        Err(_) => return,
    };
    let admin = std::env::var("NETBAIOT_ADMIN_SECRET").ok();
    let started = Instant::now();
    let mut next_snapshot = 0u64;
    let mut interval = tokio::time::interval(Duration::from_millis(config.sample_period_ms));
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
        let elapsed = started.elapsed();
        let own_pid = std::process::id();
        if let Ok(Some(cpu)) = tokio::task::spawn_blocking(move || cpu_percent(own_pid)).await {
            peaks.lock().unwrap().loadgen_cpu.add(cpu, elapsed, &config);
        }
        if let Some(pid) = config.gateway_pid {
            let rss = tokio::task::spawn_blocking(move || rss_kb(pid))
                .await
                .ok()
                .flatten();
            if let Some(rss) = rss {
                let mut guard = peaks.lock().unwrap();
                guard.rss_baseline_kb.get_or_insert(rss);
                if started.elapsed() >= Duration::from_secs(config.warmup_secs) {
                    guard.rss_warm_kb.get_or_insert(rss);
                }
                guard.rss_peak_kb = Some(guard.rss_peak_kb.unwrap_or(0).max(rss));
                guard.rss_last_kb = Some(rss);
            }
            if let Ok(Some(cpu)) = tokio::task::spawn_blocking(move || cpu_percent(pid)).await {
                peaks.lock().unwrap().gateway_cpu.add(cpu, elapsed, &config);
            }
        }
        if let (Some(url), Some(secret)) = (&config.management_url, &admin)
            && let Ok(reply) = client
                .get(format!("{url}/api/v1/metrics"))
                .bearer_auth(secret)
                .send()
                .await
            && let Ok(body) = reply.text().await
        {
            let mut guard = peaks.lock().unwrap();
            guard.metrics_observed = true;
            guard.pending_items = guard
                .pending_items
                .max(metric(&body, "netbaiot_business_rpc_pending "));
            guard.pending_bytes = guard
                .pending_bytes
                .max(metric(&body, "netbaiot_business_rpc_pending_bytes "));
            guard.pending_items_last = metric(&body, "netbaiot_business_rpc_pending ");
            guard.pending_bytes_last = metric(&body, "netbaiot_business_rpc_pending_bytes ");
            guard.active_business_connections_last =
                metric(&body, "netbaiot_business_rpc_active_connections ");
            guard.active_business_connections_peak = guard
                .active_business_connections_peak
                .max(guard.active_business_connections_last);
            guard.v3_active_streams_last =
                metric(&body, "netbaiot_business_rpc_v3_active_streams ");
            guard.v3_active_streams_peak = guard
                .v3_active_streams_peak
                .max(guard.v3_active_streams_last);
            guard.v3_queued_bytes_last = metric(&body, "netbaiot_business_rpc_v3_queued_bytes ");
            guard.v3_queued_bytes_peak = guard.v3_queued_bytes_peak.max(guard.v3_queued_bytes_last);
            guard.v3_reassembly_bytes_last =
                metric(&body, "netbaiot_business_rpc_v3_reassembly_reserved_bytes ");
            guard.v3_reassembly_bytes_peak = guard
                .v3_reassembly_bytes_peak
                .max(guard.v3_reassembly_bytes_last);
            guard.command_dedup_entries_last = metric(&body, "netbaiot_command_dedup_entries ");
            guard.command_dedup_entries_peak = guard
                .command_dedup_entries_peak
                .max(guard.command_dedup_entries_last);
            guard.command_dedup_inflight_last = metric(&body, "netbaiot_command_dedup_inflight ");
            guard.command_dedup_inflight_peak = guard
                .command_dedup_inflight_peak
                .max(guard.command_dedup_inflight_last);
            guard.auth_queue_items = guard
                .auth_queue_items
                .max(metric(&body, "netbaiot_business_rpc_auth_queue "));
            guard.control_queue_items = guard.control_queue_items.max(metric(
                &body,
                "netbaiot_business_rpc_queue_count{class=\"control\"} ",
            ));
            guard.control_queue_bytes = guard.control_queue_bytes.max(metric(
                &body,
                "netbaiot_business_rpc_queue_bytes{class=\"control\"} ",
            ));
            guard.control_queue_bytes_last = metric(
                &body,
                "netbaiot_business_rpc_queue_bytes{class=\"control\"} ",
            );
            guard.event_queue_items = guard.event_queue_items.max(metric(
                &body,
                "netbaiot_business_rpc_queue_count{class=\"event\"} ",
            ));
            guard.event_queue_bytes = guard.event_queue_bytes.max(metric(
                &body,
                "netbaiot_business_rpc_queue_bytes{class=\"event\"} ",
            ));
            guard.event_queue_bytes_last =
                metric(&body, "netbaiot_business_rpc_queue_bytes{class=\"event\"} ");
            guard.command_queue_bytes_last = metric(
                &body,
                "netbaiot_business_rpc_queue_bytes{class=\"command\"} ",
            );
            guard.command_queue_bytes_peak = guard
                .command_queue_bytes_peak
                .max(guard.command_queue_bytes_last);
            guard.late_responses_total =
                metric(&body, "netbaiot_business_rpc_late_responses_total ");
            guard.overloads_total = metric(&body, "netbaiot_business_rpc_overloads_total ");
            guard.provider_sync_success_total =
                metric(&body, "netbaiot_business_rpc_provider_sync_success_total ");
            guard.provider_sync_failure_total =
                metric(&body, "netbaiot_business_rpc_provider_sync_failure_total ");
            guard.business_reconnects_total =
                metric(&body, "netbaiot_business_rpc_reconnects_total ");
            guard.event_acks_total = metric(&body, "netbaiot_business_rpc_event_acks_total ");
            guard.revision_gaps_total = metric(&body, "netbaiot_business_rpc_revision_gaps_total ");
            guard.offline_grace_expirations_total = metric(
                &body,
                "netbaiot_business_rpc_offline_grace_expirations_total ",
            );
        }
        if elapsed.as_secs() >= next_snapshot {
            let counts = &stats.lock().unwrap().counts;
            let mut guard = peaks.lock().unwrap();
            let snapshot = Milestone {
                elapsed_seconds: elapsed.as_secs(),
                rss_kib: guard.rss_last_kb,
                cpu_percent: guard.gateway_cpu.last_percent,
                pending_items: guard.metrics_observed.then_some(guard.pending_items_last),
                pending_bytes: guard.metrics_observed.then_some(guard.pending_bytes_last),
                connections: guard
                    .metrics_observed
                    .then_some(guard.active_business_connections_last),
                v3_active_streams: guard
                    .metrics_observed
                    .then_some(guard.v3_active_streams_last),
                v3_queued_bytes: guard.metrics_observed.then_some(guard.v3_queued_bytes_last),
                v3_reassembly_bytes: guard
                    .metrics_observed
                    .then_some(guard.v3_reassembly_bytes_last),
                command_dedup_entries: guard
                    .metrics_observed
                    .then_some(guard.command_dedup_entries_last),
                command_dedup_inflight: guard
                    .metrics_observed
                    .then_some(guard.command_dedup_inflight_last),
                control_queue_bytes: guard
                    .metrics_observed
                    .then_some(guard.control_queue_bytes_last),
                event_queue_bytes: guard
                    .metrics_observed
                    .then_some(guard.event_queue_bytes_last),
                command_queue_bytes: guard
                    .metrics_observed
                    .then_some(guard.command_queue_bytes_last),
                reconnects: counts.reconnects,
                sync_failures: counts.sync_failures,
                late_responses_total: guard.metrics_observed.then_some(guard.late_responses_total),
                overloads_total: guard.metrics_observed.then_some(guard.overloads_total),
                event_retries: counts.event_retries,
                auth_success: counts.success,
                auth_failures: counts.requests.saturating_sub(counts.success),
            };
            guard.timeline.push(snapshot);
            next_snapshot = next_snapshot.saturating_add(config.snapshot_every_secs);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: business_rpc <config.json>")?;
    let config: Config = serde_json::from_slice(&std::fs::read(path)?)?;
    config.validate()?;
    let stats = Arc::new(Mutex::new(Stats::default()));
    let peaks = Arc::new(Mutex::new(Peaks::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = tokio::spawn(sample_gateway(
        config.clone(),
        peaks.clone(),
        stats.clone(),
        stop.clone(),
    ));
    let gateway_before = read_gateway_metrics(&config).await;
    let started = Instant::now();
    let mut verifier_calls = None;
    let mut verifier_calls_before_invalidation = None;
    let mut provider_auth_calls = None;
    let mut handler_latency = None;
    let mut topology_results = None;
    match config.scenario.as_str() {
        "auth" | "steady_auth" => {
            let handler = Arc::new(Handler::new(&config));
            let (business, _) = connect_business(
                &config,
                handler.clone(),
                BusinessRole::AuthControl,
                Some(&stats),
            )
            .await?;
            auth_load(
                config.clone(),
                stats.clone(),
                started + Duration::from_secs(config.duration_secs),
            )
            .await;
            business.shutdown().await;
            provider_auth_calls = Some(handler.auth.load(Ordering::Relaxed));
            handler_latency = handler
                .latency
                .lock()
                .ok()
                .and_then(|h| (h.count > 0).then(|| h.summary()));
        }
        "multiplexed" | "soak" => {
            let (calls, latency) = event_load(&config, stats.clone(), false).await?;
            provider_auth_calls = Some(calls);
            handler_latency = latency;
        }
        "consumer_outage" => {
            let (calls, latency) = event_load(&config, stats.clone(), true).await?;
            provider_auth_calls = Some(calls);
            handler_latency = latency;
        }
        "topology_compare" => {
            let mut results = Vec::new();
            for topology in [Topology::Multiplexed, Topology::Dual] {
                let mut run_config = config.clone();
                run_config.topology = topology;
                let run_stats = Arc::new(Mutex::new(Stats::default()));
                let before = read_gateway_metrics(&run_config).await;
                let started = Instant::now();
                let (calls, handler) = event_load(&run_config, run_stats.clone(), false).await?;
                let elapsed = started.elapsed();
                let after = read_gateway_metrics(&run_config).await;
                let mut run = run_stats.lock().unwrap();
                run.counts.latency_ms = run.auth_latency.summary();
                run.counts.event_ack_ms = run.ack_latency.summary();
                results.push(serde_json::json!({
                    "topology": topology,
                    "duration_seconds": elapsed.as_secs_f64(),
                    "counts": run.counts,
                    "auth_provider_calls": calls,
                    "latency": {
                        "end_to_end_ms": (run.auth_latency.count > 0).then(|| run.auth_latency.summary()),
                        "event_ack_ms": (run.ack_latency.count > 0).then(|| run.ack_latency.summary()),
                        "handler_ms": handler,
                        "rpc_ms_gateway": before.as_ref().zip(after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_auth_latency_us")),
                        "rpc_remote_wait_ms_gateway": before.as_ref().zip(after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_remote_wait_us")),
                        "rpc_queue_wait_ms_gateway": before.as_ref().zip(after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_queue_wait_us")),
                        "event_ack_ms_gateway": before.as_ref().zip(after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_event_ack_latency_us")),
                        "sync_ms": (run.sync_ready.count > 0).then(|| run.sync_ready.summary()),
                        "tls_handshake_ms": (run.tls_handshake.count > 0).then(|| run.tls_handshake.summary()),
                    },
                    "gateway_overloads": before.as_ref().zip(after.as_ref()).and_then(|(a,b)| gateway_delta(a,b,"business_rpc_overloads_total")),
                }));
            }
            topology_results = Some(results);
        }
        "handshake" => {
            let handler = Arc::new(Handler::new(&config));
            let until = started + Duration::from_secs(config.duration_secs);
            while Instant::now() < until {
                let attempt = connect_business(
                    &config,
                    handler.clone(),
                    BusinessRole::AuthControl,
                    Some(&stats),
                )
                .await;
                stats.lock().unwrap().counts.requests += 1;
                match attempt {
                    Ok((business, _)) => {
                        stats.lock().unwrap().counts.success += 1;
                        business.shutdown().await;
                    }
                    Err(_) => stats.lock().unwrap().counts.transport_failures += 1,
                }
            }
        }
        "verifier" => {
            let (before, total) = verifier_load(&config, stats.clone()).await?;
            verifier_calls_before_invalidation = Some(before);
            verifier_calls = Some(total);
        }
        "reconnect" => {
            let handler = Arc::new(Handler::new(&config));
            let mut cycles = 0usize;
            while cycles < config.reconnect_cycles.max(100)
                || Instant::now() < started + Duration::from_secs(config.duration_secs)
            {
                cycles += 1;
                match connect_business(
                    &config,
                    handler.clone(),
                    BusinessRole::AuthControl,
                    Some(&stats),
                )
                .await
                {
                    Ok((business, _)) => {
                        stats.lock().unwrap().counts.reconnects += 1;
                        let started = Instant::now();
                        let result = device(&config, "churn").await;
                        let latency = started.elapsed();
                        if let Ok(ref client) = result {
                            let _ = client.shutdown_with_timeout(Duration::from_secs(1)).await;
                        }
                        {
                            let mut state = stats.lock().unwrap();
                            state.counts.requests += 1;
                            state.auth_latency.add(latency);
                            if result.is_ok() {
                                state.counts.success += 1;
                            } else {
                                state.counts.transport_failures += 1;
                            }
                        }
                        business.shutdown().await;
                    }
                    Err(_) => stats.lock().unwrap().counts.sync_failures += 1,
                }
                tokio::time::sleep(Duration::from_millis(config.reconnect_pause_ms)).await;
            }
            provider_auth_calls = Some(handler.auth.load(Ordering::Relaxed));
        }
        _ => unreachable!(),
    }
    if let Some(pid) = config.gateway_pid {
        let rss = tokio::task::spawn_blocking(move || rss_kb(pid))
            .await
            .ok()
            .flatten();
        peaks.lock().unwrap().rss_post_soak_kb = rss;
    }
    tokio::time::sleep(Duration::from_secs(config.recovery_secs)).await;
    if let Some(pid) = config.gateway_pid {
        let rss = tokio::task::spawn_blocking(move || rss_kb(pid))
            .await
            .ok()
            .flatten();
        peaks.lock().unwrap().rss_post_recovery_kb = rss;
    }
    stop.store(true, Ordering::Relaxed);
    sampler.await?;
    let gateway_after = read_gateway_metrics(&config).await;
    let elapsed = started.elapsed();
    let mut state = stats.lock().unwrap();
    state.counts.command_delivery_after_unavailable = state
        .command_unavailable_ids
        .intersection(&state.command_device_seen)
        .count() as u64;
    state.counts.command_accepted_without_delivery = state
        .command_traces
        .values()
        .filter(|trace| {
            trace.device_deliveries == 0
                && trace
                    .rpc_outcomes
                    .iter()
                    .any(|outcome| matches!(*outcome, "queued" | "queued_after_unknown"))
        })
        .count() as u64;
    state.counts.latency_ms = state.auth_latency.summary();
    state.counts.event_ack_ms = state.ack_latency.summary();
    let git_commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned());
    let git_dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|output| !output.stdout.is_empty());
    let rust_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned());
    let observed = peaks.lock().unwrap().metrics_observed;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "scenario": config.scenario,
            "config": config,
            "environment": {
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
                "rust": rust_version,
                "build_profile": if cfg!(debug_assertions) { "dev" } else { "release" },
                "git_commit": git_commit,
                "git_dirty": git_dirty,
                "transport": config.business_transport.name(),
                "topology": config.topology,
                "network_profile": config.network_profile,
                "network_injection": "external_unverified",
                "sample_period_ms": config.sample_period_ms,
            },
            "latency": {
                "end_to_end_ms": (state.auth_latency.count > 0).then(|| state.auth_latency.summary()),
                "rpc_ms_gateway": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_auth_latency_us")),
                "rpc_remote_wait_ms_gateway": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_remote_wait_us")),
                "rpc_queue_wait_ms_gateway": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_queue_wait_us")),
                "event_ack_ms_gateway": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_histogram(a,b,"business_rpc_event_ack_latency_us")),
                "handler_ms": handler_latency,
                "event_ack_ms": (state.ack_latency.count > 0).then(|| state.ack_latency.summary()),
                "tls_handshake_ms": (state.tls_handshake.count > 0).then(|| state.tls_handshake.summary()),
                "sync_ms": (state.sync_ready.count > 0).then(|| state.sync_ready.summary()),
                "full_ready_ms": (state.full_ready.count > 0).then(|| state.full_ready.summary()),
            },
            "topology_results": topology_results,
            "gateway_deltas": {
                "overloads": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_delta(a,b,"business_rpc_overloads_total")),
                "late_responses": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_delta(a,b,"business_rpc_late_responses_total")),
                "sync_success": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_delta(a,b,"business_rpc_provider_sync_success_total")),
                "sync_failure": gateway_before.as_ref().zip(gateway_after.as_ref()).and_then(|(a,b)| gateway_delta(a,b,"business_rpc_provider_sync_failure_total")),
            },
            "duration_seconds": elapsed.as_secs_f64(),
            "requests_per_second": state.counts.requests as f64 / elapsed.as_secs_f64().max(0.001),
            "events_per_second": state.counts.events as f64 / elapsed.as_secs_f64().max(0.001),
            "event_acks_per_second": state.counts.event_acks as f64 / elapsed.as_secs_f64().max(0.001),
            "counts": state.counts,
            "command_traces": state.command_traces.values().collect::<Vec<_>>(),
        "verifier_provider_calls": verifier_calls,
        "verifier_calls_before_invalidation": verifier_calls_before_invalidation,
            "auth_provider_calls": provider_auth_calls,
            "peaks": if observed { Some(serde_json::to_value(&*peaks.lock().unwrap())?) } else { None },
        }))?
    );
    Ok(())
}
