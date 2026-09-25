//! Bounded, real-network Business RPC V2 load gate.
//! Run against a disposable gateway configured for development Business RPC.
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use netbaiot_client::business_rpc::{
    BusinessAuthHandler, BusinessRpcClient, BusinessRpcClientConfig,
};
use netbaiot_core::{
    AuthInvalidation, CodecId, DeviceId, DeviceKey, DeviceUplink, DeviceUplinkKind, Heartbeat,
    ProductId, SourceMessageId, TenantId,
    business_rpc::{
        AuthenticatedDeviceWire, BusinessRole, DeviceAuthenticateRequest, ResolveVerifierRequest,
        ResolveVerifierResponse, RpcError, RpcErrorCode,
    },
};
use netbaiot_device_sdk::{DeviceClient, DeviceCredentials, PublishQos};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::HashSet,
    error::Error,
    net::SocketAddr,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, task::JoinSet};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const SECRET: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    scenario: String,
    business_address: SocketAddr,
    device_address: SocketAddr,
    udp_address: Option<SocketAddr>,
    management_url: Option<String>,
    gateway_pid: Option<u32>,
    duration_secs: u64,
    auth_concurrency: usize,
    #[serde(default)]
    auth_handler_delay_ms: u64,
    event_rate: u64,
    event_ack_delay_ms: u64,
    #[serde(default)]
    event_reconnect_every_secs: u64,
    reconnect_cycles: usize,
    #[serde(default)]
    reconnect_pause_ms: u64,
    sample_period_ms: u64,
    #[serde(default)]
    warmup_secs: u64,
    #[serde(default)]
    recovery_secs: u64,
}
impl Config {
    fn validate(&self) -> Result<()> {
        if ![
            "auth",
            "multiplexed",
            "reconnect",
            "verifier",
            "consumer_outage",
        ]
        .contains(&self.scenario.as_str())
            || self.duration_secs == 0
            || self.duration_secs > 3600
            || self.warmup_secs > self.duration_secs
            || self.recovery_secs > 600
            || self.auth_concurrency == 0
            || self.auth_concurrency > 256
            || self.auth_handler_delay_ms > 5_000
            || self.event_rate > 10_000
            || self.event_ack_delay_ms > 60_000
            || self.event_reconnect_every_secs > 3600
            || self.reconnect_cycles > 10_000
            || self.reconnect_pause_ms > 60_000
            || !(100..=10_000).contains(&self.sample_period_ms)
            || !self.business_address.ip().is_loopback()
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
        if self.scenario == "verifier" && self.duration_secs < 2 {
            return Err("verifier scenario requires at least 2 seconds for invalidation".into());
        }
        Ok(())
    }
}

struct Handler {
    auth: AtomicU64,
    verifier: AtomicU64,
    auth_delay: Duration,
}
impl Handler {
    fn new(config: &Config) -> Self {
        Self {
            auth: AtomicU64::new(0),
            verifier: AtomicU64::new(0),
            auth_delay: Duration::from_millis(config.auth_handler_delay_ms),
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
        self.auth.fetch_add(1, Ordering::Relaxed);
        if !self.auth_delay.is_zero() {
            tokio::time::sleep(self.auth_delay).await;
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
    publish_attempts: u64,
    publish_enqueued: u64,
    publish_errors: u64,
    reconnects: u64,
    sync_failures: u64,
    latency_ms: Percentiles,
    event_ack_ms: Percentiles,
}
#[derive(Default, Serialize)]
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
    auth_latency: Histogram,
    ack_latency: Histogram,
}
type SharedStats = Arc<Mutex<Stats>>;

async fn connect_business(
    config: &Config,
    handler: Arc<Handler>,
    role: BusinessRole,
) -> Result<(
    BusinessRpcClient,
    tokio::sync::mpsc::Receiver<netbaiot_client::business_rpc::BusinessDelivery>,
)> {
    let token = std::env::var("NETBAIOT_BUSINESS_RPC_TOKEN")?;
    let settings = BusinessRpcClientConfig::development(config.business_address, token, role);
    let (client, deliveries) = BusinessRpcClient::connect(settings, Some(handler))?;
    tokio::time::timeout(Duration::from_secs(10), client.wait_ready()).await??;
    Ok((client, deliveries))
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
async fn auth_load(config: Config, stats: SharedStats, until: Instant) {
    let mut tasks = JoinSet::new();
    let mut next = 0u64;
    while Instant::now() < until || !tasks.is_empty() {
        while Instant::now() < until && tasks.len() < config.auth_concurrency {
            let config = config.clone();
            // Reuse a bounded device set; presence and auth-cache capacity are
            // independently bounded on the gateway under test.
            let id = format!("load{}", next % 32);
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
                Err(error) => {
                    let text = error.to_string();
                    if text.contains("overload") {
                        state.counts.overloaded += 1;
                    } else if text.contains("timeout") {
                        state.counts.timeout += 1;
                    } else if text.contains("auth") || text.contains("reject") {
                        state.counts.rejected += 1;
                    } else {
                        state.counts.transport_failures += 1;
                    }
                }
            }
        }
    }
}

async fn event_load(config: &Config, stats: SharedStats, outage: bool) -> Result<u64> {
    let handler = Arc::new(Handler::new(config));
    let (mut business, mut deliveries) =
        connect_business(config, handler.clone(), BusinessRole::Multiplexed).await?;
    let publisher = device(config, "publisher").await?;
    let until = Instant::now() + Duration::from_secs(config.duration_secs);
    let outage_until = Instant::now() + Duration::from_secs(config.duration_secs / 2);
    let auth_task = tokio::spawn(auth_load(config.clone(), stats.clone(), until));
    let mut tick = tokio::time::interval(Duration::from_secs_f64(
        1.0 / config.event_rate.max(1) as f64,
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let reconnect_period = Duration::from_secs(config.event_reconnect_every_secs.max(1));
    let mut reconnect_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + reconnect_period,
        reconnect_period,
    );
    let mut sequence = 0u64;
    let mut seen = HashSet::new();
    let source_prefix = format!("load-{}-", uuid::Uuid::new_v4().simple());
    while Instant::now() < until {
        tokio::select! {
            _ = reconnect_tick.tick(), if config.event_reconnect_every_secs > 0 => {
                business.shutdown().await;
                match connect_business(config, handler.clone(), BusinessRole::Multiplexed).await {
                    Ok((next, next_deliveries)) => {
                        business = next;
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
                let payload = DeviceUplink::new(SourceMessageId::new(format!("{source_prefix}{sequence}"))?, DeviceUplinkKind::Heartbeat(Heartbeat { sequence }));
                let published = publisher.publish(payload, PublishQos::AtLeastOnce).await;
                let mut state = stats.lock().unwrap();
                state.counts.publish_attempts += 1;
                if published.is_ok() { state.counts.publish_enqueued += 1; }
                else { state.counts.publish_errors += 1; }
            }
            received = deliveries.recv() => {
                let Some(delivery) = received else { break };
                if !delivery.delivery.event.source_message_id.as_str().starts_with(&source_prefix) {
                    let _ = delivery.ack().await;
                    continue;
                }
                let start = Instant::now();
                {
                    let mut state = stats.lock().unwrap();
                    state.counts.events += 1;
                    if !seen.insert(delivery.delivery.event.event_id) { state.counts.event_retries += 1; }
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
    auth_task.await?;
    let _ = publisher
        .shutdown_with_timeout(Duration::from_secs(1))
        .await;
    business.shutdown().await;
    Ok(handler.auth.load(Ordering::Relaxed))
}

async fn verifier_load(config: &Config, stats: SharedStats) -> Result<(u64, u64)> {
    let handler = Arc::new(Handler::new(config));
    let (business, mut deliveries) =
        connect_business(config, handler.clone(), BusinessRole::Multiplexed).await?;
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

#[derive(Default, Serialize)]
struct Peaks {
    rss_baseline_kb: Option<u64>,
    rss_warm_kb: Option<u64>,
    rss_peak_kb: Option<u64>,
    rss_post_soak_kb: Option<u64>,
    rss_post_recovery_kb: Option<u64>,
    pending_items: u64,
    pending_bytes: u64,
    pending_items_last: u64,
    pending_bytes_last: u64,
    active_business_connections_peak: u64,
    active_business_connections_last: u64,
    auth_queue_items: u64,
    control_queue_items: u64,
    control_queue_bytes: u64,
    event_queue_items: u64,
    event_queue_bytes: u64,
    late_responses_total: u64,
    overloads_total: u64,
    provider_sync_success_total: u64,
    provider_sync_failure_total: u64,
    business_reconnects_total: u64,
    event_acks_total: u64,
    revision_gaps_total: u64,
    offline_grace_expirations_total: u64,
}
fn metric(body: &str, name: &str) -> u64 {
    body.lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}
async fn sample_gateway(config: Config, peaks: Arc<Mutex<Peaks>>, stop: Arc<AtomicBool>) {
    let client = match reqwest::Client::builder().no_proxy().build() {
        Ok(client) => client,
        Err(_) => return,
    };
    let admin = std::env::var("NETBAIOT_ADMIN_SECRET").ok();
    let started = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(config.sample_period_ms));
    while !stop.load(Ordering::Relaxed) {
        interval.tick().await;
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
            guard.event_queue_items = guard.event_queue_items.max(metric(
                &body,
                "netbaiot_business_rpc_queue_count{class=\"event\"} ",
            ));
            guard.event_queue_bytes = guard.event_queue_bytes.max(metric(
                &body,
                "netbaiot_business_rpc_queue_bytes{class=\"event\"} ",
            ));
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
    let sampler = tokio::spawn(sample_gateway(config.clone(), peaks.clone(), stop.clone()));
    let started = Instant::now();
    let mut verifier_calls = None;
    let mut verifier_calls_before_invalidation = None;
    let mut provider_auth_calls = None;
    match config.scenario.as_str() {
        "auth" => {
            let handler = Arc::new(Handler::new(&config));
            let (business, _) =
                connect_business(&config, handler.clone(), BusinessRole::AuthControl).await?;
            auth_load(
                config.clone(),
                stats.clone(),
                started + Duration::from_secs(config.duration_secs),
            )
            .await;
            business.shutdown().await;
            provider_auth_calls = Some(handler.auth.load(Ordering::Relaxed));
        }
        "multiplexed" => {
            provider_auth_calls = Some(event_load(&config, stats.clone(), false).await?)
        }
        "consumer_outage" => {
            provider_auth_calls = Some(event_load(&config, stats.clone(), true).await?)
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
                match connect_business(&config, handler.clone(), BusinessRole::AuthControl).await {
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
    let elapsed = started.elapsed();
    let mut state = stats.lock().unwrap();
    state.counts.latency_ms = state.auth_latency.summary();
    state.counts.event_ack_ms = state.ack_latency.summary();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "scenario": config.scenario,
            "config": config,
            "duration_seconds": elapsed.as_secs_f64(),
            "requests_per_second": state.counts.requests as f64 / elapsed.as_secs_f64().max(0.001),
            "events_per_second": state.counts.events as f64 / elapsed.as_secs_f64().max(0.001),
            "event_acks_per_second": state.counts.event_acks as f64 / elapsed.as_secs_f64().max(0.001),
            "counts": state.counts,
        "verifier_provider_calls": verifier_calls,
        "verifier_calls_before_invalidation": verifier_calls_before_invalidation,
            "auth_provider_calls": provider_auth_calls,
            "peaks": &*peaks.lock().unwrap(),
        }))?
    );
    Ok(())
}
