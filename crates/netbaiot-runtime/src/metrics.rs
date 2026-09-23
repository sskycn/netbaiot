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
    ProtocolDetectionFailures,
    ProtocolDetectionTimeouts,
    UdpAcksSent,
    UdpAckSendFailures,
    UdpAcceptedDuplicates,
    UdpAccepted,
}

const NAMES: [&str; 49] = [
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
    "protocol_detection_failures",
    "protocol_detection_timeouts",
    "udp_acks_sent",
    "udp_ack_send_failures",
    "udp_accepted_duplicates",
    "udp_accepted",
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
}

const HISTOGRAM_NAMES: [&str; 15] = [
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
];
const BOUNDS: [u64; 16] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 500_000,
    1_000_000, 5_000_000,
];

// Fixed, opt-in experiment series: no identity or sink labels. Nanosecond
// buckets retain sub-microsecond work that the legacy histograms truncate.
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

pub struct Metrics {
    values: [AtomicU64; NAMES.len()],
    histograms: [HistogramState; HISTOGRAM_NAMES.len()],
    lock_timing_enabled: bool,
    event_bus_probes: [AtomicU64; EVENT_BUS_PROBES.len()],
    event_bus_timing: Option<Box<EventBusTiming>>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            histograms: std::array::from_fn(|_| HistogramState::default()),
            lock_timing_enabled: false,
            event_bus_probes: std::array::from_fn(|_| AtomicU64::new(0)),
            event_bus_timing: None,
        }
    }
}

impl Metrics {
    pub fn with_lock_timing() -> Self {
        Self {
            lock_timing_enabled: true,
            event_bus_timing: Some(Box::default()),
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
