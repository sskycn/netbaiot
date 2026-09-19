use std::sync::atomic::{AtomicU64, Ordering};
/// Closed label vocabulary. Never attach client or device identifiers.
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Metric {
    ConnectionsAccepted,
    ConnectionsRejected,
    MqttConnectSuccess,
    MqttConnectFailure,
    MqttPacketsReceived,
    MqttPacketsSent,
    MqttProtocolViolations,
    MqttPublishes,
    MqttSubscriptions,
    MqttPubacks,
    MqttKeepaliveDisconnects,
    HttpRequests,
    TcpFrames,
    UdpDatagrams,
    AuthFailures,
    CodecFailures,
    IngressAccepted,
    IngressRejected,
    DedupHits,
    QueueRejects,
    CommandQueued,
    CommandSent,
    CommandAcked,
    CommandReceived,
    IngressBytes,
    CommandFailed,
    DeliverySuccess,
    DeliveryFailed,
    DeliveryLatencyMs,
    DatabaseLatencyMs,
    Timeouts,
    ProtocolAdmissionRejects,
    IngressAdmissionRejects,
    DependencyDegraded,
    DependencyRecovered,
    CleanupRuns,
    CleanupIngressRows,
    CleanupCommandRows,
    CleanupJobs,
}
const NAMES: [&str; 39] = [
    "connections_accepted",
    "connections_rejected",
    "mqtt_connect_success",
    "mqtt_connect_failure",
    "mqtt_packets_received",
    "mqtt_packets_sent",
    "mqtt_protocol_violations",
    "mqtt_publishes",
    "mqtt_subscriptions",
    "mqtt_pubacks",
    "mqtt_keepalive_disconnects",
    "http_requests",
    "tcp_frames",
    "udp_datagrams",
    "auth_failures",
    "codec_failures",
    "ingress_accepted",
    "ingress_rejected",
    "dedup_hits",
    "queue_rejects",
    "command_queued",
    "command_sent",
    "command_acked",
    "command_received",
    "ingress_bytes",
    "command_failed",
    "delivery_success",
    "delivery_failed",
    "delivery_latency_ms",
    "database_latency_ms",
    "timeouts",
    "protocol_admission_rejects",
    "ingress_admission_rejects",
    "dependency_degraded",
    "dependency_recovered",
    "cleanup_runs",
    "cleanup_ingress_rows",
    "cleanup_command_rows",
    "cleanup_jobs",
];

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Histogram {
    MqttProtocolValidation,
    ValidationToAdmission,
    AdmissionWait,
    AdmissionLockWait,
    AdmissionLockHold,
    AdmissionToAuthentication,
    AuthenticationToCodec,
    CodecToPool,
    DatabasePoolWait,
    TransactionStart,
    QuotaWait,
    QuotaAccounting,
    QuotaLockHold,
    Dedup,
    PersistenceWrites,
    Commit,
    Transaction,
    CommitToPubackQueue,
    PubackWrite,
    Cleanup,
}

const HISTOGRAM_NAMES: [&str; 20] = [
    "mqtt_protocol_validation_us",
    "validation_to_admission_us",
    "admission_wait_us",
    "admission_lock_wait_us",
    "admission_lock_hold_us",
    "admission_to_authentication_us",
    "authentication_to_codec_us",
    "codec_to_pool_us",
    "database_pool_wait_us",
    "transaction_start_us",
    "quota_wait_us",
    "quota_accounting_us",
    "quota_lock_hold_us",
    "dedup_us",
    "persistence_writes_us",
    "commit_us",
    "transaction_us",
    "commit_to_puback_queue_us",
    "puback_write_us",
    "cleanup_us",
];

// Microsecond bounds cover parser work through the external-operation deadline.
const HISTOGRAM_BOUNDS: [u64; 20] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000, 30_000_000,
];

struct HistogramState {
    buckets: [AtomicU64; HISTOGRAM_BOUNDS.len() + 1],
    count: AtomicU64,
    sum: AtomicU64,
}

impl Default for HistogramState {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }
}
pub struct Metrics {
    values: [AtomicU64; 39],
    histograms: [HistogramState; HISTOGRAM_NAMES.len()],
}
impl Default for Metrics {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            histograms: std::array::from_fn(|_| HistogramState::default()),
        }
    }
}
impl Metrics {
    pub fn inc(&self, m: Metric) {
        self.add(m, 1);
    }
    pub fn add(&self, m: Metric, n: u64) {
        self.values[m as usize].fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self, m: Metric) -> u64 {
        self.values[m as usize].load(Ordering::Relaxed)
    }
    pub fn observe(&self, histogram: Histogram, micros: u64) {
        let state = &self.histograms[histogram as usize];
        let bucket = HISTOGRAM_BOUNDS.partition_point(|bound| *bound < micros);
        state.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        state.count.fetch_add(1, Ordering::Relaxed);
        state.sum.fetch_add(micros, Ordering::Relaxed);
    }
    pub fn render(&self) -> String {
        let mut output: String = NAMES
            .iter()
            .zip(&self.values)
            .map(|(n, v)| format!("netbaiot_{n}_total {}\n", v.load(Ordering::Relaxed)))
            .collect();
        for (name, state) in HISTOGRAM_NAMES.iter().zip(&self.histograms) {
            let mut cumulative = 0;
            for (bound, value) in HISTOGRAM_BOUNDS.iter().zip(&state.buckets) {
                cumulative += value.load(Ordering::Relaxed);
                output.push_str(&format!(
                    "netbaiot_{name}_bucket{{le=\"{bound}\"}} {cumulative}\n"
                ));
            }
            cumulative += state.buckets[HISTOGRAM_BOUNDS.len()].load(Ordering::Relaxed);
            output.push_str(&format!(
                "netbaiot_{name}_bucket{{le=\"+Inf\"}} {cumulative}\nnetbaiot_{name}_count {}\nnetbaiot_{name}_sum {}\n",
                state.count.load(Ordering::Relaxed),
                state.sum.load(Ordering::Relaxed)
            ));
        }
        output
    }
}
