use std::sync::atomic::{AtomicU64, Ordering};

/// Closed metric vocabulary: identifiers and URLs are never labels.
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
    AuthCacheHits,
    AuthCacheMisses,
    AuthNegativeHits,
    AuthEvictions,
    AuthInvalidations,
    CodecFailures,
    IngressAccepted,
    IngressRejected,
    IngressBytes,
    ProtocolAdmissionRejects,
    IngressAdmissionRejects,
    QueueRejects,
    CommandQueued,
    CommandSent,
    CommandReceived,
    CommandAcked,
    CommandFailed,
    EventsAccepted,
    EventsRejected,
    EventBytes,
    SinkAcks,
    SinkRetries,
    SinkFailures,
    SinkDrops,
    SpoolRecords,
    SpoolBytes,
    RecoveryRecords,
    Timeouts,
}

const NAMES: [&str; 43] = [
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
    "auth_cache_hits",
    "auth_cache_misses",
    "auth_negative_hits",
    "auth_evictions",
    "auth_invalidations",
    "codec_failures",
    "ingress_accepted",
    "ingress_rejected",
    "ingress_bytes",
    "protocol_admission_rejects",
    "ingress_admission_rejects",
    "queue_rejects",
    "command_queued",
    "command_sent",
    "command_received",
    "command_acked",
    "command_failed",
    "events_accepted",
    "events_rejected",
    "event_bytes",
    "sink_acks",
    "sink_retries",
    "sink_failures",
    "sink_drops",
    "spool_records",
    "spool_bytes",
    "recovery_records",
    "timeouts",
];

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Histogram {
    MqttProtocolValidation,
    ValidationToAdmission,
    AdmissionWait,
    AdmissionLockWait,
    AdmissionLockHold,
    AuthenticationToCodec,
    CodecToEventAccepted,
    EventAcceptedToSinkAck,
    PubackWrite,
}

const HISTOGRAM_NAMES: [&str; 9] = [
    "mqtt_protocol_validation_us",
    "validation_to_admission_us",
    "admission_wait_us",
    "admission_lock_wait_us",
    "admission_lock_hold_us",
    "authentication_to_codec_us",
    "codec_to_event_accepted_us",
    "event_accepted_to_sink_ack_us",
    "puback_write_us",
];
const BOUNDS: [u64; 16] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000,
    1_000_000, 5_000_000,
];

struct HistogramState {
    buckets: [AtomicU64; BOUNDS.len() + 1],
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
    values: [AtomicU64; NAMES.len()],
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
    pub fn inc(&self, metric: Metric) {
        self.add(metric, 1);
    }
    pub fn add(&self, metric: Metric, value: u64) {
        self.values[metric as usize].fetch_add(value, Ordering::Relaxed);
    }
    pub fn get(&self, metric: Metric) -> u64 {
        self.values[metric as usize].load(Ordering::Relaxed)
    }
    pub fn observe(&self, histogram: Histogram, micros: u64) {
        let state = &self.histograms[histogram as usize];
        let bucket = BOUNDS.partition_point(|bound| *bound < micros);
        state.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        state.count.fetch_add(1, Ordering::Relaxed);
        state.sum.fetch_add(micros, Ordering::Relaxed);
    }
    pub fn render(&self) -> String {
        let mut output: String = NAMES
            .iter()
            .zip(&self.values)
            .map(|(name, value)| {
                format!("netbaiot_{name}_total {}\n", value.load(Ordering::Relaxed))
            })
            .collect();
        for (name, state) in HISTOGRAM_NAMES.iter().zip(&self.histograms) {
            let mut cumulative = 0;
            for (bound, value) in BOUNDS.iter().zip(&state.buckets) {
                cumulative += value.load(Ordering::Relaxed);
                output.push_str(&format!(
                    "netbaiot_{name}_bucket{{le=\"{bound}\"}} {cumulative}\n"
                ));
            }
            cumulative += state.buckets[BOUNDS.len()].load(Ordering::Relaxed);
            output.push_str(&format!(
                "netbaiot_{name}_bucket{{le=\"+Inf\"}} {cumulative}\nnetbaiot_{name}_count {}\nnetbaiot_{name}_sum {}\n",
                state.count.load(Ordering::Relaxed),
                state.sum.load(Ordering::Relaxed)
            ));
        }
        output
    }
}
