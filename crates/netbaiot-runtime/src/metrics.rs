use netbaiot_core::business_rpc::RpcErrorCode;
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
    ManagementHttpRequests,
    TcpFrames,
    UdpDatagrams,
    AuthFailures,
    AuthCacheHits,
    AuthCacheMisses,
    AuthNegativeHits,
    AuthEvictions,
    AuthInvalidations,
    AuthStaleResponses,
    CodecDecode,
    CodecFailures,
    CodecProfileUnsupported,
    CodecProfileMismatch,
    CommandCodecFailures,
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
    CommandDedupHits,
    CommandDedupConflicts,
    CommandDedupReservations,
    CommandDedupEvictions,
    CommandDedupOverloads,
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
    ProtocolDetectionFailures,
    ProtocolDetectionTimeouts,
    UdpAcksSent,
    UdpAckSendFailures,
    UdpAcceptedDuplicates,
    UdpAccepted,
    UdpOverloadDrops,
    BusinessRpcConnections,
    BusinessRpcAbnormalClosures,
    BusinessRpcProviderSyncSuccess,
    BusinessRpcProviderSyncFailure,
    BusinessRpcInvalidationSuccess,
    BusinessRpcInvalidationFailure,
    BusinessRpcLateResponses,
    BusinessRpcTimeouts,
    BusinessRpcOverloads,
    BusinessRpcEventAcks,
    BusinessRpcReconnects,
    BusinessRpcRevisionGaps,
    BusinessRpcOfflineGraceExpirations,
    BusinessRpcMuxConnections,
    BusinessRpcMuxAbnormalClosures,
    BusinessRpcMuxStreamsOpened,
    BusinessRpcMuxStreamsReset,
    BusinessRpcMuxFramesSent,
    BusinessRpcMuxFramesReceived,
    BusinessRpcMuxDataBytesSent,
    BusinessRpcMuxDataBytesSelected,
    BusinessRpcMuxDataBytesReceived,
    BusinessRpcMuxConnectionWindowStalls,
    BusinessRpcMuxStreamWindowStalls,
    BusinessRpcMuxSendAheadStalls,
    BusinessRpcMuxOverloads,
    BusinessRpcMuxProtocolErrors,
    BusinessRpcMuxCommandRequests,
    BusinessRpcMuxCommandAccepted,
    BusinessRpcMuxCommandErrors,
}
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum BusinessRpcCallResult {
    Success,
    DeviceRejected,
    Timeout,
    Overloaded,
    Unavailable,
    Invalid,
}

const NAMES: [&str; 90] = [
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
    "management_http_requests",
    "tcp_frames",
    "udp_datagrams",
    "auth_failures",
    "auth_cache_hits",
    "auth_cache_misses",
    "auth_negative_hits",
    "auth_evictions",
    "auth_invalidations",
    "auth_stale_responses",
    "codec_decode",
    "codec_failures",
    "codec_profile_unsupported",
    "codec_profile_mismatch",
    "command_codec_failures",
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
    "command_dedup_hits",
    "command_dedup_conflicts",
    "command_dedup_reservations",
    "command_dedup_evictions",
    "command_dedup_overloads",
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
    "protocol_detection_failures",
    "protocol_detection_timeouts",
    "udp_acks_sent",
    "udp_ack_send_failures",
    "udp_accepted_duplicates",
    "udp_accepted",
    "udp_overload_drops",
    "business_rpc_connections",
    "business_rpc_abnormal_closures",
    "business_rpc_provider_sync_success",
    "business_rpc_provider_sync_failure",
    "business_rpc_invalidation_success",
    "business_rpc_invalidation_failure",
    "business_rpc_late_responses",
    "business_rpc_timeouts",
    "business_rpc_overloads",
    "business_rpc_event_acks",
    "business_rpc_reconnects",
    "business_rpc_revision_gaps",
    "business_rpc_offline_grace_expirations",
    "business_rpc_mux_connections",
    "business_rpc_mux_abnormal_closures",
    "business_rpc_mux_streams_opened",
    "business_rpc_mux_streams_reset",
    "business_rpc_mux_frames_sent",
    "business_rpc_mux_frames_received",
    "business_rpc_mux_data_bytes_sent",
    "business_rpc_mux_data_bytes_selected",
    "business_rpc_mux_data_bytes_received",
    "business_rpc_mux_connection_window_stalls",
    "business_rpc_mux_stream_window_stalls",
    "business_rpc_mux_send_ahead_stalls",
    "business_rpc_mux_overloads",
    "business_rpc_mux_protocol_errors",
    "business_rpc_mux_command_requests",
    "business_rpc_mux_command_accepted",
    "business_rpc_mux_command_errors",
];

/// Opt-in experiment counters, inactive unless lock timing is enabled.
/// Wake counters count worker select completions, not executor task polls.
#[derive(Clone, Copy)]
#[repr(usize)]
pub enum EventBusProbe {
    Publish,
    TakeReady,
    Complete,
    NextDelay,
    Other,
    NotifyWorker,
    NotifyDrain,
    WakeNotify,
    WakeTimer,
    WakeJoin,
    EmptyWake,
}
const EVENT_BUS_PROBES: [&str; 11] = [
    "publish",
    "take_ready",
    "complete",
    "next_delay",
    "other",
    "notify_worker",
    "notify_drain",
    "wake_notify",
    "wake_timer",
    "wake_join",
    "empty_wake",
];

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Histogram {
    MqttProtocolValidation,
    ValidationToAdmission,
    AdmissionWait,
    AdmissionLockWait,
    AdmissionLockHold,
    EventBusLockWait,
    EventBusLockHold,
    EventBusStateWait,
    EventBusStateHold,
    BrokerLockWait,
    BrokerLockHold,
    AuthenticationToCodec,
    CodecToEventAccepted,
    EventAcceptedToSinkAck,
    PubackWrite,
    BusinessRpcAuthLatency,
    BusinessRpcVerifierLatency,
    BusinessRpcEventAckLatency,
    BusinessRpcAdmission,
    BusinessRpcRemoteWait,
    BusinessRpcQueueWait,
    BusinessRpcMuxSchedulerWait,
    BusinessRpcMuxWriterWait,
    Authentication,
    CodecLatency,
    IngressLatency,
    SinkLatency,
    EventBusQueueLatency,
}

const HISTOGRAM_NAMES: [&str; 28] = [
    "mqtt_protocol_validation_us",
    "validation_to_admission_us",
    "admission_wait_us",
    "admission_lock_wait_us",
    "admission_lock_hold_us",
    "event_bus_lock_wait_us",
    "event_bus_lock_hold_us",
    "event_bus_state_wait_us",
    "event_bus_state_hold_us",
    "broker_lock_wait_us",
    "broker_lock_hold_us",
    "authentication_to_codec_us",
    "codec_to_event_accepted_us",
    "event_accepted_to_sink_ack_us",
    "puback_write_us",
    "business_rpc_auth_latency_us",
    "business_rpc_verifier_latency_us",
    "business_rpc_event_ack_latency_us",
    "business_rpc_admission_us",
    "business_rpc_remote_wait_us",
    "business_rpc_queue_wait_us",
    "business_rpc_mux_scheduler_wait_us",
    "business_rpc_mux_writer_wait_us",
    "authentication_us",
    "codec_latency_us",
    "ingress_latency_us",
    "sink_latency_us",
    "eventbus_queue_latency_us",
];
const BOUNDS: [u64; 16] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000,
    1_000_000, 5_000_000,
];

// Fixed, opt-in experiment series: no identity or sink labels. Nanosecond
// buckets retain sub-microsecond work that the legacy histograms truncate.
/// Closed broker operation vocabulary, enabled only with lock timing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum BrokerProbe {
    Route,
    Subscribe,
    Unsubscribe,
    Puback,
    Pubrec,
    Pubrel,
    Pubcomp,
    Attach,
    Detach,
    RetainedReplay,
    Maintenance,
    Recovery,
    Outbound,
    Will,
    Invalidation,
    Read,
    Inbound,
}
impl BrokerProbe {
    pub const ALL: [Self; 17] = [
        Self::Route,
        Self::Subscribe,
        Self::Unsubscribe,
        Self::Puback,
        Self::Pubrec,
        Self::Pubrel,
        Self::Pubcomp,
        Self::Attach,
        Self::Detach,
        Self::RetainedReplay,
        Self::Maintenance,
        Self::Recovery,
        Self::Outbound,
        Self::Will,
        Self::Invalidation,
        Self::Read,
        Self::Inbound,
    ];
}
const BROKER_SITES: [&str; 17] = [
    "route",
    "subscribe",
    "unsubscribe",
    "puback",
    "pubrec",
    "pubrel",
    "pubcomp",
    "attach",
    "detach",
    "retained_replay",
    "maintenance",
    "recovery",
    "outbound",
    "will",
    "invalidation",
    "read",
    "inbound",
];
#[derive(Default)]
struct BrokerTiming {
    wait: [HistogramState; 17],
    hold: [HistogramState; 17],
}
const EVENT_BUS_SITES: [&str; 5] = [
    "publish",
    "take_ready",
    "next_ready_delay",
    "complete",
    "control_restore_spool",
];
const TIMING_NS_BOUNDS: [u64; 16] = [
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    5_000_000,
    10_000_000,
    100_000_000,
];
const QUEUE_BOUNDS: [u64; 16] = [
    0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 4_096, 16_384, 65_536, 262_144,
];
const BATCH_BOUNDS: [u64; 16] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 16, 32, 64, 128, 256, 1_024, 16_384,
];

#[derive(Default)]
struct EventBusTiming {
    wait: [HistogramState; EVENT_BUS_SITES.len()],
    hold: [HistogramState; EVENT_BUS_SITES.len()],
    dequeue_size: HistogramState,
    dequeue_queue_len: HistogramState,
    dequeue_selection: HistogramState,
}

impl HistogramState {
    fn observe_value(&self, value: u64, bounds: &[u64; 16]) {
        let bucket = bounds.partition_point(|bound| *bound < value);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
    }

    fn render_experiment(&self, output: &mut String, name: &str, bounds: &[u64; 16]) {
        use std::fmt::Write;
        let mut cumulative = 0;
        for (bound, value) in bounds.iter().zip(&self.buckets) {
            cumulative += value.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "netbaiot_{name}_bucket{{le=\"{bound}\"}} {cumulative}"
            );
        }
        cumulative += self.buckets[bounds.len()].load(Ordering::Relaxed);
        let _ = writeln!(output, "netbaiot_{name}_bucket{{le=\"+Inf\"}} {cumulative}");
        let _ = writeln!(
            output,
            "netbaiot_{name}_count {}",
            self.count.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            output,
            "netbaiot_{name}_sum {}",
            self.sum.load(Ordering::Relaxed)
        );
    }
}

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

pub struct MetricTimer<'a> {
    metrics: &'a Metrics,
    histogram: Histogram,
    started: std::time::Instant,
}
impl Drop for MetricTimer<'_> {
    fn drop(&mut self) {
        self.metrics.observe(
            self.histogram,
            self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
        );
    }
}

pub struct Metrics {
    values: [AtomicU64; NAMES.len()],
    business_rpc_active: AtomicU64,
    business_rpc_mux_active: AtomicU64,
    business_rpc_mux_streams: AtomicU64,
    business_rpc_mux_streams_peak: AtomicU64,
    business_rpc_mux_queued_bytes: AtomicU64,
    business_rpc_mux_queued_bytes_peak: AtomicU64,
    business_rpc_mux_reassembly_bytes: AtomicU64,
    business_rpc_mux_reassembly_bytes_peak: AtomicU64,
    business_rpc_method_results: [[AtomicU64; 6]; 2],
    business_rpc_remote_errors: [[AtomicU64; 11]; 2],
    management_auth_attempts: [AtomicU64; 15],
    management_authz_denied: [AtomicU64; 12],
    management_jwks_cache: [AtomicU64; 2],
    histograms: [HistogramState; HISTOGRAM_NAMES.len()],
    business_rpc_queue_count: AtomicU64,
    business_rpc_queue_bytes: AtomicU64,
    lock_timing_enabled: bool,
    event_bus_probes: [AtomicU64; EVENT_BUS_PROBES.len()],
    event_bus_timing: Option<Box<EventBusTiming>>,
    broker_timing: Option<Box<BrokerTiming>>,
    sink_failures_by_reason: [AtomicU64; 10],
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            business_rpc_active: AtomicU64::new(0),
            business_rpc_mux_active: AtomicU64::new(0),
            business_rpc_mux_streams: AtomicU64::new(0),
            business_rpc_mux_streams_peak: AtomicU64::new(0),
            business_rpc_mux_queued_bytes: AtomicU64::new(0),
            business_rpc_mux_queued_bytes_peak: AtomicU64::new(0),
            business_rpc_mux_reassembly_bytes: AtomicU64::new(0),
            business_rpc_mux_reassembly_bytes_peak: AtomicU64::new(0),
            business_rpc_method_results: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }),
            business_rpc_remote_errors: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }),
            management_auth_attempts: std::array::from_fn(|_| AtomicU64::new(0)),
            management_authz_denied: std::array::from_fn(|_| AtomicU64::new(0)),
            management_jwks_cache: std::array::from_fn(|_| AtomicU64::new(0)),
            histograms: std::array::from_fn(|_| HistogramState::default()),
            business_rpc_queue_count: AtomicU64::new(0),
            business_rpc_queue_bytes: AtomicU64::new(0),
            lock_timing_enabled: false,
            event_bus_probes: std::array::from_fn(|_| AtomicU64::new(0)),
            event_bus_timing: None,
            broker_timing: None,
            sink_failures_by_reason: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Metrics {
    /// Measures success and error exits without allocating or changing admission semantics.
    pub fn timer(&self, histogram: Histogram) -> MetricTimer<'_> {
        MetricTimer {
            metrics: self,
            histogram,
            started: std::time::Instant::now(),
        }
    }
    pub(crate) fn sink_failure(&self, reason: crate::SinkFailureReason) {
        self.sink_failures_by_reason[reason as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn business_rpc_method_result(&self, method: &str, result: BusinessRpcCallResult) {
        let index = match method {
            "device.authenticate" => 0,
            "device.resolve_verifier" => 1,
            _ => return,
        };
        self.business_rpc_method_results[index][result as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn business_rpc_remote_error(&self, method: &str, code: RpcErrorCode) {
        let index = match method {
            "device.authenticate" => 0,
            "device.resolve_verifier" => 1,
            _ => return,
        };
        self.business_rpc_remote_errors[index][code as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn business_rpc_connection_started(&self) {
        self.business_rpc_active.fetch_add(1, Ordering::Relaxed);
    }
    pub fn business_rpc_connection_finished(&self) {
        self.business_rpc_active.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn business_rpc_mux_connection_started(&self) {
        self.business_rpc_mux_active.fetch_add(1, Ordering::Relaxed);
        self.inc(Metric::BusinessRpcMuxConnections);
    }
    pub fn business_rpc_mux_connection_finished(&self) {
        self.business_rpc_mux_active.fetch_sub(1, Ordering::Relaxed);
    }
    fn adjust_rpc_gauge(gauge: &AtomicU64, previous: usize, current: usize) {
        let delta = previous.abs_diff(current) as u64;
        if current >= previous {
            gauge.fetch_add(delta, Ordering::Relaxed);
        } else {
            gauge.fetch_sub(delta, Ordering::Relaxed);
        }
    }
    pub fn business_rpc_mux_connection_gauges(
        &self,
        previous: (usize, usize),
        current: (usize, usize),
    ) {
        Self::adjust_rpc_gauge(&self.business_rpc_mux_streams, previous.0, current.0);
        Self::adjust_rpc_gauge(
            &self.business_rpc_mux_reassembly_bytes,
            previous.1,
            current.1,
        );
        self.business_rpc_mux_streams_peak.fetch_max(
            self.business_rpc_mux_streams.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.business_rpc_mux_reassembly_bytes_peak.fetch_max(
            self.business_rpc_mux_reassembly_bytes
                .load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }
    pub fn business_rpc_mux_writer_bytes(&self, previous: usize, current: usize) {
        Self::adjust_rpc_gauge(&self.business_rpc_mux_queued_bytes, previous, current);
        self.business_rpc_mux_queued_bytes_peak.fetch_max(
            self.business_rpc_mux_queued_bytes.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
    }
    pub fn business_rpc_queue_add(&self, bytes: u64) {
        self.business_rpc_queue_count
            .fetch_add(1, Ordering::Relaxed);
        self.business_rpc_queue_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn business_rpc_queue_sub(&self, bytes: u64) {
        self.business_rpc_queue_count
            .fetch_sub(1, Ordering::Relaxed);
        self.business_rpc_queue_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }
    pub fn management_auth_attempt(&self, method: &str, result: &str) {
        let method = match method {
            "static_token" => 0,
            "api_key" => 1,
            "jwt" => 2,
            "mtls" => 3,
            _ => 4,
        };
        let result = match result {
            "success" => 0,
            "unavailable" => 1,
            _ => 2,
        };
        self.management_auth_attempts[method * 3 + result].fetch_add(1, Ordering::Relaxed);
    }
    pub fn management_authz_denied(&self, action: crate::AdminScope) {
        self.management_authz_denied[action as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn management_jwks_cache(&self, hit: bool) {
        self.management_jwks_cache[usize::from(!hit)].fetch_add(1, Ordering::Relaxed);
    }
    pub fn with_lock_timing() -> Self {
        Self {
            lock_timing_enabled: true,
            event_bus_timing: Some(Box::default()),
            broker_timing: Some(Box::default()),
            ..Self::default()
        }
    }
    pub fn lock_timing_enabled(&self) -> bool {
        self.lock_timing_enabled
    }
    pub fn event_bus_probe(&self, probe: EventBusProbe) {
        if self.lock_timing_enabled {
            self.event_bus_probes[probe as usize].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn broker_state_timing(&self, site: BrokerProbe, wait_ns: u64, hold_ns: u64) {
        if let Some(timing) = &self.broker_timing {
            timing.wait[site as usize].observe_value(wait_ns, &TIMING_NS_BOUNDS);
            timing.hold[site as usize].observe_value(hold_ns, &TIMING_NS_BOUNDS);
        }
    }
    pub(crate) fn event_bus_state_timing(&self, site: EventBusProbe, wait_ns: u64, hold_ns: u64) {
        let Some(timing) = &self.event_bus_timing else {
            return;
        };
        let index = match site {
            EventBusProbe::Publish => 0,
            EventBusProbe::TakeReady => 1,
            EventBusProbe::NextDelay => 2,
            EventBusProbe::Complete => 3,
            EventBusProbe::Other => 4,
            _ => return,
        };
        timing.wait[index].observe_value(wait_ns, &TIMING_NS_BOUNDS);
        timing.hold[index].observe_value(hold_ns, &TIMING_NS_BOUNDS);
    }

    pub(crate) fn event_bus_dequeue(&self, queue_len: usize, records: usize, selection_ns: u64) {
        let Some(timing) = &self.event_bus_timing else {
            return;
        };
        timing
            .dequeue_queue_len
            .observe_value(queue_len as u64, &QUEUE_BOUNDS);
        // Zero is an empty acquisition, not a successful dispatch batch.
        timing
            .dequeue_size
            .observe_value(records as u64, &BATCH_BOUNDS);
        timing
            .dequeue_selection
            .observe_value(selection_ns, &TIMING_NS_BOUNDS);
    }
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
        for (name, metric) in [
            ("connections", Metric::ConnectionsAccepted),
            ("ingress", Metric::IngressAccepted),
            ("ingress_error", Metric::IngressRejected),
            ("codec_error", Metric::CodecFailures),
            ("eventbus_rejected", Metric::EventsRejected),
            ("sink_failed", Metric::SinkFailures),
        ] {
            output.push_str(&format!("netbaiot_{name}_total {}\n", self.get(metric)));
        }
        for (reason, value) in [
            "other",
            "network",
            "timeout",
            "http_429",
            "http_5xx",
            "http_auth",
            "http_4xx",
            "invalid_response",
            "response_too_large",
            "panic",
        ]
        .iter()
        .zip(&self.sink_failures_by_reason)
        {
            output.push_str(&format!(
                "netbaiot_sink_error_total{{reason=\"{reason}\"}} {}\n",
                value.load(Ordering::Relaxed)
            ));
        }
        output.push_str(&format!(
            "netbaiot_business_rpc_active_connections {}\n",
            self.business_rpc_active.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "netbaiot_business_rpc_mux_active_connections {}\n",
            self.business_rpc_mux_active.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "netbaiot_business_rpc_mux_active_streams {}\nnetbaiot_business_rpc_mux_queued_bytes {}\nnetbaiot_business_rpc_mux_reassembly_reserved_bytes {}\n",
            self.business_rpc_mux_streams.load(Ordering::Relaxed),
            self.business_rpc_mux_queued_bytes.load(Ordering::Relaxed),
            self.business_rpc_mux_reassembly_bytes.load(Ordering::Relaxed),
        ));
        output.push_str(&format!(
            "netbaiot_business_rpc_mux_peak_active_streams {}\nnetbaiot_business_rpc_mux_peak_queued_bytes {}\nnetbaiot_business_rpc_mux_peak_reassembly_reserved_bytes {}\n",
            self.business_rpc_mux_streams_peak.load(Ordering::Relaxed),
            self.business_rpc_mux_queued_bytes_peak.load(Ordering::Relaxed),
            self.business_rpc_mux_reassembly_bytes_peak.load(Ordering::Relaxed),
        ));
        for (method_index, method) in ["device.authenticate", "device.resolve_verifier"]
            .iter()
            .enumerate()
        {
            for (result_index, result) in [
                "success",
                "device_rejected",
                "timeout",
                "overloaded",
                "unavailable",
                "invalid",
            ]
            .iter()
            .enumerate()
            {
                output.push_str(&format!("netbaiot_business_rpc_method_results_total{{method=\"{method}\",result=\"{result}\"}} {}\n", self.business_rpc_method_results[method_index][result_index].load(Ordering::Relaxed)));
            }
            for (code_index, code) in [
                "invalid_request",
                "unknown_method",
                "unauthenticated",
                "forbidden",
                "device_rejected",
                "unavailable",
                "overloaded",
                "timeout",
                "conflict",
                "stale_revision",
                "internal",
            ]
            .iter()
            .enumerate()
            {
                output.push_str(&format!("netbaiot_business_rpc_remote_errors_total{{method=\"{method}\",code=\"{code}\"}} {}\n", self.business_rpc_remote_errors[method_index][code_index].load(Ordering::Relaxed)));
            }
        }
        output.push_str(&format!("netbaiot_business_rpc_queue_count{{class=\"control\"}} {}\nnetbaiot_business_rpc_queue_bytes{{class=\"control\"}} {}\n", self.business_rpc_queue_count.load(Ordering::Relaxed), self.business_rpc_queue_bytes.load(Ordering::Relaxed)));
        for (index, method) in ["static_token", "api_key", "jwt", "mtls", "unknown"]
            .iter()
            .enumerate()
        {
            for (result, label) in ["success", "unavailable", "unauthenticated"]
                .iter()
                .enumerate()
            {
                output.push_str(&format!("netbaiot_management_auth_attempts_total{{method=\"{method}\",result=\"{label}\"}} {}\n", self.management_auth_attempts[index * 3 + result].load(Ordering::Relaxed)));
            }
        }
        for (index, action) in [
            "runtime.read",
            "metrics.read",
            "connection.read",
            "device.command",
            "auth.invalidate",
            "auth.invalidate.all",
            "control.read",
            "control.write",
            "routes.read",
            "routes.write",
            "runtime.drain",
            "admin.*",
        ]
        .iter()
        .enumerate()
        {
            output.push_str(&format!(
                "netbaiot_management_authz_denied_total{{action=\"{action}\"}} {}\n",
                self.management_authz_denied[index].load(Ordering::Relaxed)
            ));
        }
        output.push_str(&format!(
            "netbaiot_management_auth_cache_hits_total{{provider=\"jwks\"}} {}\n",
            self.management_jwks_cache[0].load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "netbaiot_management_auth_cache_misses_total{{provider=\"jwks\"}} {}\n",
            self.management_jwks_cache[1].load(Ordering::Relaxed)
        ));
        if self.lock_timing_enabled {
            for (name, value) in EVENT_BUS_PROBES.iter().zip(&self.event_bus_probes) {
                output.push_str(&format!(
                    "netbaiot_event_bus_probe_{name}_total {}\n",
                    value.load(Ordering::Relaxed)
                ));
            }
        }
        if let Some(timing) = &self.event_bus_timing {
            for (index, site) in EVENT_BUS_SITES.iter().enumerate() {
                timing.wait[index].render_experiment(
                    &mut output,
                    &format!("event_bus_site_{site}_wait_ns"),
                    &TIMING_NS_BOUNDS,
                );
                timing.hold[index].render_experiment(
                    &mut output,
                    &format!("event_bus_site_{site}_hold_ns"),
                    &TIMING_NS_BOUNDS,
                );
            }
            timing.dequeue_size.render_experiment(
                &mut output,
                "event_bus_dequeue_records",
                &BATCH_BOUNDS,
            );
            timing.dequeue_queue_len.render_experiment(
                &mut output,
                "event_bus_dequeue_queue_len",
                &QUEUE_BOUNDS,
            );
            timing.dequeue_selection.render_experiment(
                &mut output,
                "event_bus_dequeue_selection_ns",
                &TIMING_NS_BOUNDS,
            );
        }
        if let Some(timing) = &self.broker_timing {
            for (index, site) in BROKER_SITES.iter().enumerate() {
                timing.wait[index].render_experiment(
                    &mut output,
                    &format!("broker_site_{site}_wait_ns"),
                    &TIMING_NS_BOUNDS,
                );
                timing.hold[index].render_experiment(
                    &mut output,
                    &format!("broker_site_{site}_hold_ns"),
                    &TIMING_NS_BOUNDS,
                );
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rpc_mux_metrics_have_version_independent_names_and_gauge_cleanup() {
        let metrics = Metrics::default();
        let counters = [
            (Metric::BusinessRpcMuxConnections, "connections"),
            (Metric::BusinessRpcMuxAbnormalClosures, "abnormal_closures"),
            (Metric::BusinessRpcMuxStreamsOpened, "streams_opened"),
            (Metric::BusinessRpcMuxStreamsReset, "streams_reset"),
            (Metric::BusinessRpcMuxFramesSent, "frames_sent"),
            (Metric::BusinessRpcMuxFramesReceived, "frames_received"),
            (Metric::BusinessRpcMuxDataBytesSent, "data_bytes_sent"),
            (
                Metric::BusinessRpcMuxDataBytesSelected,
                "data_bytes_selected",
            ),
            (
                Metric::BusinessRpcMuxDataBytesReceived,
                "data_bytes_received",
            ),
            (
                Metric::BusinessRpcMuxConnectionWindowStalls,
                "connection_window_stalls",
            ),
            (
                Metric::BusinessRpcMuxStreamWindowStalls,
                "stream_window_stalls",
            ),
            (Metric::BusinessRpcMuxSendAheadStalls, "send_ahead_stalls"),
            (Metric::BusinessRpcMuxOverloads, "overloads"),
            (Metric::BusinessRpcMuxProtocolErrors, "protocol_errors"),
            (Metric::BusinessRpcMuxCommandRequests, "command_requests"),
            (Metric::BusinessRpcMuxCommandAccepted, "command_accepted"),
            (Metric::BusinessRpcMuxCommandErrors, "command_errors"),
        ];
        metrics.business_rpc_mux_connection_started();
        for (counter, _) in counters.into_iter().skip(1) {
            metrics.inc(counter);
        }
        metrics.business_rpc_mux_connection_gauges((0, 0), (2, 4096));
        metrics.business_rpc_mux_writer_bytes(0, 1024);
        metrics.observe(Histogram::BusinessRpcMuxSchedulerWait, 25);
        metrics.observe(Histogram::BusinessRpcMuxWriterWait, 50);
        let rendered = metrics.render();
        let mut series = std::collections::BTreeSet::new();
        for line in rendered.lines().filter(|line| !line.starts_with('#')) {
            let (name, _) = line.split_once(' ').unwrap();
            assert!(series.insert(name), "duplicate Prometheus series: {name}");
        }
        for (_, suffix) in counters {
            assert!(rendered.contains(&format!("netbaiot_business_rpc_mux_{suffix}_total 1\n")));
        }
        for (suffix, value) in [
            ("active_connections", 1),
            ("active_streams", 2),
            ("queued_bytes", 1024),
            ("reassembly_reserved_bytes", 4096),
            ("peak_active_streams", 2),
            ("peak_queued_bytes", 1024),
            ("peak_reassembly_reserved_bytes", 4096),
            ("scheduler_wait_us_count", 1),
            ("scheduler_wait_us_sum", 25),
            ("writer_wait_us_count", 1),
            ("writer_wait_us_sum", 50),
        ] {
            assert!(rendered.contains(&format!("netbaiot_business_rpc_mux_{suffix} {value}\n")));
        }
        metrics.business_rpc_mux_connection_gauges((2, 4096), (0, 0));
        metrics.business_rpc_mux_writer_bytes(1024, 0);
        metrics.business_rpc_mux_connection_finished();
        let rendered = metrics.render();
        for suffix in [
            "active_connections",
            "active_streams",
            "queued_bytes",
            "reassembly_reserved_bytes",
        ] {
            assert!(rendered.contains(&format!("netbaiot_business_rpc_mux_{suffix} 0\n")));
        }
        assert!(!rendered.contains("business_rpc_v3_"));
    }

    #[test]
    fn production_timings_count_error_exits_and_have_no_dynamic_labels() {
        let metrics = Metrics::default();
        fn failed(metrics: &Metrics) -> Result<(), ()> {
            let _timer = metrics.timer(Histogram::CodecLatency);
            Err(())
        }
        assert!(failed(&metrics).is_err());
        metrics.inc(Metric::CodecDecode);
        let rendered = metrics.render();
        assert!(rendered.contains("netbaiot_codec_latency_us_count 1\n"));
        assert!(rendered.contains("netbaiot_codec_decode_total 1\n"));
        for label in ["device_id", "event_id", "client_id", "codec_id", "sink_id"] {
            assert!(!rendered.contains(label));
        }
    }

    #[test]
    fn business_rpc_remote_error_labels_match_every_wire_code() {
        let metrics = Metrics::default();
        let cases = [
            (RpcErrorCode::InvalidRequest, "invalid_request"),
            (RpcErrorCode::UnknownMethod, "unknown_method"),
            (RpcErrorCode::Unauthenticated, "unauthenticated"),
            (RpcErrorCode::Forbidden, "forbidden"),
            (RpcErrorCode::DeviceRejected, "device_rejected"),
            (RpcErrorCode::Unavailable, "unavailable"),
            (RpcErrorCode::Overloaded, "overloaded"),
            (RpcErrorCode::Timeout, "timeout"),
            (RpcErrorCode::Conflict, "conflict"),
            (RpcErrorCode::StaleRevision, "stale_revision"),
            (RpcErrorCode::Internal, "internal"),
        ];
        for (code, _) in cases {
            metrics.business_rpc_remote_error("device.authenticate", code);
        }
        metrics.business_rpc_remote_error("unexpected.method", RpcErrorCode::Overloaded);
        let rendered = metrics.render();
        for (_, label) in cases {
            assert!(rendered.contains(&format!(
                "netbaiot_business_rpc_remote_errors_total{{method=\"device.authenticate\",code=\"{label}\"}} 1\n"
            )));
            assert!(rendered.contains(&format!(
                "netbaiot_business_rpc_remote_errors_total{{method=\"device.resolve_verifier\",code=\"{label}\"}} 0\n"
            )));
        }
        assert!(!rendered.contains("unexpected.method"));
    }

    #[test]
    fn eventbus_probes_require_explicit_opt_in() {
        let disabled = Metrics::default();
        disabled.event_bus_probe(EventBusProbe::WakeNotify);
        assert!(!disabled.render().contains("event_bus_probe_"));
        disabled.event_bus_state_timing(EventBusProbe::TakeReady, 200, 300);
        disabled.event_bus_dequeue(8, 1, 100);
        assert!(disabled.event_bus_timing.is_none());
        assert!(!disabled.render().contains("event_bus_site_"));
        assert!(!disabled.render().contains("event_bus_dequeue_"));
        let enabled = Metrics::with_lock_timing();
        enabled.event_bus_probe(EventBusProbe::WakeNotify);
        assert!(
            enabled
                .render()
                .contains("netbaiot_event_bus_probe_wake_notify_total 1\n")
        );
    }

    #[test]
    fn eventbus_site_and_batch_histograms_preserve_counts_and_units() {
        let metrics = Metrics::with_lock_timing();
        metrics.event_bus_state_timing(EventBusProbe::TakeReady, 200, 300);
        metrics.event_bus_state_timing(EventBusProbe::Publish, 400, 500);
        metrics.event_bus_dequeue(8, 0, 100);
        metrics.event_bus_dequeue(8, 4, 200);
        let text = metrics.render();
        assert!(text.contains("netbaiot_event_bus_site_take_ready_wait_ns_count 1\n"));
        assert!(text.contains("netbaiot_event_bus_site_take_ready_wait_ns_sum 200\n"));
        assert!(text.contains("netbaiot_event_bus_site_publish_hold_ns_sum 500\n"));
        assert!(text.contains("netbaiot_event_bus_dequeue_records_count 2\n"));
        assert!(text.contains("netbaiot_event_bus_dequeue_records_sum 4\n"));
        assert!(text.contains("netbaiot_event_bus_dequeue_records_bucket{le=\"0\"} 1\n"));
        assert!(text.contains("netbaiot_event_bus_dequeue_records_bucket{le=\"4\"} 2\n"));
        assert!(text.contains("netbaiot_event_bus_dequeue_selection_ns_sum 300\n"));
    }

    #[test]
    fn renders_lock_histograms_with_closed_names() {
        assert!(!Metrics::default().lock_timing_enabled());
        let metrics = Metrics::with_lock_timing();
        assert!(metrics.lock_timing_enabled());
        metrics.observe(Histogram::EventBusLockWait, 25);
        metrics.observe(Histogram::BrokerLockHold, 26);
        let rendered = metrics.render();
        assert!(rendered.contains("netbaiot_event_bus_lock_wait_us_count 1\n"));
        assert!(rendered.contains("netbaiot_event_bus_lock_wait_us_bucket{le=\"25\"} 1\n"));
        assert!(rendered.contains("netbaiot_broker_lock_hold_us_count 1\n"));
        assert!(rendered.contains("netbaiot_broker_lock_hold_us_bucket{le=\"25\"} 0\n"));
        assert!(rendered.contains("netbaiot_broker_lock_hold_us_bucket{le=\"50\"} 1\n"));
    }
}
