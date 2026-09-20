use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// One validated location for every in-process resource ceiling.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub max_connections_per_device: usize,
    pub max_connections_per_tenant: usize,
    pub max_mqtt_packet_size: usize,
    pub max_http_body_size: usize,
    pub max_http_header_bytes: usize,
    pub max_http_headers: usize,
    pub max_tcp_frame_size: usize,
    pub max_udp_datagram_size: usize,
    pub max_client_id_bytes: usize,
    pub max_username_bytes: usize,
    pub max_password_bytes: usize,
    pub max_topic_bytes: usize,
    pub max_topic_depth: usize,
    pub max_subscriptions_per_connection: usize,
    pub max_subscriptions_per_session: usize,
    pub max_subscriptions_per_device: usize,
    pub max_subscriptions_per_tenant: usize,
    pub max_subscriptions: usize,
    pub max_subscription_filters_per_packet: usize,
    pub max_inflight_qos1_per_connection: usize,
    pub max_inflight_qos1_per_session: usize,
    pub max_inflight_qos2_per_session: usize,
    pub max_inflight_qos1_per_tenant: usize,
    pub max_inflight_qos2_per_tenant: usize,
    pub max_persistent_sessions: usize,
    pub max_persistent_sessions_per_tenant: usize,
    pub max_offline_messages_per_session: usize,
    pub max_offline_bytes_per_session: usize,
    pub max_offline_messages_per_tenant: usize,
    pub max_offline_bytes_per_tenant: usize,
    pub max_offline_messages: usize,
    pub max_offline_bytes: usize,
    pub max_retained_messages: usize,
    pub max_retained_bytes: usize,
    pub max_retained_messages_per_tenant: usize,
    pub max_retained_bytes_per_tenant: usize,
    pub max_retained_message_bytes: usize,
    pub max_will_payload_bytes: usize,
    pub max_mqtt_session_state_bytes: usize,
    pub max_mqtt_session_state_bytes_per_tenant: usize,
    pub global_mqtt_session_bytes: usize,
    pub mqtt_session_idle_ttl_ms: u64,
    pub max_read_buffer_per_connection: usize,
    pub max_write_buffer_per_connection: usize,
    pub max_outbound_messages_per_connection: usize,
    pub max_outbound_bytes_per_connection: usize,
    pub max_outbound_bytes_per_tenant: usize,
    pub max_outbound_bytes: usize,
    pub global_connection_logical_bytes: usize,
    pub connection_memory_reservation: usize,
    pub max_ingress: usize,
    pub max_ingress_per_tenant: usize,
    pub max_ingress_per_device: usize,
    pub max_ingress_bytes: usize,
    pub max_ingress_waiters: usize,
    pub max_ingress_wait_bytes: usize,
    pub ingress_wait_timeout_ms: u64,
    pub max_pending_commands_per_device: usize,
    pub max_pending_commands_per_tenant: usize,
    pub max_pending_commands: usize,
    pub max_command_bytes: usize,
    pub max_devices: usize,
    pub max_devices_per_tenant: usize,
    pub max_replay_entries: usize,
    pub max_replay_entries_per_device: usize,
    pub max_replay_entries_per_tenant: usize,
    pub udp_clock_skew_ms: u64,
    pub rate_entries: usize,
    pub requests_per_second: usize,
    pub requests_per_ip_second: usize,
    pub messages_per_device_second: usize,
    pub messages_per_tenant_second: usize,
    pub auth_cache_max_entries: usize,
    pub auth_cache_max_bytes: usize,
    pub auth_cache_max_waiters: usize,
    pub auth_positive_ttl_ms: u64,
    pub auth_negative_ttl_ms: u64,
    pub config_cache_max_entries: usize,
    pub config_cache_max_bytes: usize,
    pub max_sinks: usize,
    pub max_sinks_per_tenant: usize,
    pub max_routing_filters: usize,
    pub max_fanout_per_event: usize,
    pub global_event_max_count: usize,
    pub global_event_max_bytes: usize,
    pub sink_queue_max_count: usize,
    pub sink_queue_max_bytes: usize,
    pub sink_delivery_concurrency: usize,
    pub sink_timeout_ms: u64,
    pub sink_max_attempts: u32,
    pub sink_max_age_ms: u64,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
    pub connect_timeout_ms: u64,
    pub authentication_timeout_ms: u64,
    pub packet_read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub external_timeout_ms: u64,
    pub shutdown_drain_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub replay_ttl_ms: u64,
    pub command_ttl_ms: u64,
    pub spool_max_records: usize,
    pub spool_max_bytes: usize,
    pub spool_segment_max_bytes: usize,
    pub spool_record_max_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_connections_per_ip: 32,
            max_connections_per_device: 2,
            max_connections_per_tenant: 64,
            max_mqtt_packet_size: 65_536,
            max_http_body_size: 65_536,
            max_http_header_bytes: 8_192,
            max_http_headers: 32,
            max_tcp_frame_size: 65_536,
            max_udp_datagram_size: 1_200,
            max_client_id_bytes: 64,
            max_username_bytes: 64,
            max_password_bytes: 128,
            max_topic_bytes: 256,
            max_topic_depth: 8,
            max_subscriptions_per_connection: 2,
            max_subscriptions_per_session: 32,
            max_subscriptions_per_device: 64,
            max_subscriptions_per_tenant: 128,
            max_subscriptions: 512,
            max_subscription_filters_per_packet: 16,
            max_inflight_qos1_per_connection: 32,
            max_inflight_qos1_per_session: 32,
            max_inflight_qos2_per_session: 32,
            max_inflight_qos1_per_tenant: 4_096,
            max_inflight_qos2_per_tenant: 4_096,
            max_persistent_sessions: 4_096,
            max_persistent_sessions_per_tenant: 512,
            max_offline_messages_per_session: 128,
            max_offline_bytes_per_session: 1_048_576,
            max_offline_messages_per_tenant: 4_096,
            max_offline_bytes_per_tenant: 33_554_432,
            max_offline_messages: 16_384,
            max_offline_bytes: 134_217_728,
            max_retained_messages: 4_096,
            max_retained_bytes: 67_108_864,
            max_retained_messages_per_tenant: 512,
            max_retained_bytes_per_tenant: 8_388_608,
            max_retained_message_bytes: 65_536,
            max_will_payload_bytes: 65_536,
            max_mqtt_session_state_bytes: 2_097_152,
            max_mqtt_session_state_bytes_per_tenant: 33_554_432,
            global_mqtt_session_bytes: 134_217_728,
            mqtt_session_idle_ttl_ms: 86_400_000,
            max_read_buffer_per_connection: 65_536,
            max_write_buffer_per_connection: 65_536,
            // Reconnect may need to enqueue every independently bounded QoS1 and QoS2
            // retransmission before it can accept more protocol work.
            max_outbound_messages_per_connection: 64,
            max_outbound_bytes_per_connection: 262_144,
            max_outbound_bytes_per_tenant: 2_097_152,
            max_outbound_bytes: 8_388_608,
            global_connection_logical_bytes: 134_217_728,
            connection_memory_reservation: 524_288,
            max_ingress: 16,
            max_ingress_per_tenant: 4,
            max_ingress_per_device: 1,
            max_ingress_bytes: 2_097_152,
            max_ingress_waiters: 16,
            max_ingress_wait_bytes: 2_097_152,
            ingress_wait_timeout_ms: 25,
            max_pending_commands_per_device: 16,
            max_pending_commands_per_tenant: 128,
            max_pending_commands: 1_024,
            max_command_bytes: 16_384,
            max_devices: 1_024,
            max_devices_per_tenant: 128,
            max_replay_entries: 1_024,
            max_replay_entries_per_device: 2,
            max_replay_entries_per_tenant: 256,
            udp_clock_skew_ms: 30_000,
            rate_entries: 1_024,
            requests_per_second: 512,
            requests_per_ip_second: 32,
            messages_per_device_second: 16,
            messages_per_tenant_second: 128,
            auth_cache_max_entries: 4_096,
            auth_cache_max_bytes: 4_194_304,
            auth_cache_max_waiters: 256,
            auth_positive_ttl_ms: 300_000,
            auth_negative_ttl_ms: 5_000,
            config_cache_max_entries: 4_096,
            config_cache_max_bytes: 16_777_216,
            max_sinks: 32,
            max_sinks_per_tenant: 8,
            max_routing_filters: 256,
            max_fanout_per_event: 8,
            global_event_max_count: 16_384,
            global_event_max_bytes: 67_108_864,
            sink_queue_max_count: 4_096,
            sink_queue_max_bytes: 16_777_216,
            sink_delivery_concurrency: 8,
            sink_timeout_ms: 5_000,
            sink_max_attempts: 5,
            sink_max_age_ms: 3_600_000,
            retry_base_ms: 100,
            retry_max_ms: 30_000,
            connect_timeout_ms: 10_000,
            authentication_timeout_ms: 5_000,
            packet_read_timeout_ms: 30_000,
            write_timeout_ms: 10_000,
            idle_timeout_ms: 120_000,
            request_timeout_ms: 15_000,
            external_timeout_ms: 5_000,
            shutdown_drain_timeout_ms: 20_000,
            shutdown_timeout_ms: 30_000,
            replay_ttl_ms: 120_000,
            command_ttl_ms: 300_000,
            spool_max_records: 100_000,
            spool_max_bytes: 268_435_456,
            spool_segment_max_bytes: 67_108_864,
            spool_record_max_bytes: 1_048_576,
        }
    }
}

impl Limits {
    pub fn validate(&self) -> Result<()> {
        let value = serde_json::to_value(self).map_err(|_| Error::Configuration)?;
        if value
            .as_object()
            .ok_or(Error::Configuration)?
            .values()
            .any(|v| {
                v.as_u64()
                    .is_none_or(|number| number == 0 || number > u64::from(u32::MAX))
            })
        {
            return Err(Error::Configuration);
        }
        let max_frame = self
            .max_mqtt_packet_size
            .max(self.max_http_body_size)
            .max(self.max_tcp_frame_size);
        if max_frame > 1_048_576
            || self.max_udp_datagram_size > 1_200
            || self.max_http_header_bytes < 8_192
            || self.max_http_header_bytes > max_frame
            || self.max_http_headers > 128
            || self.max_connections_per_ip > self.max_connections
            || self.max_connections_per_device > self.max_connections_per_tenant
            || self.max_connections_per_tenant > self.max_connections
            || self.max_ingress_per_device > self.max_ingress_per_tenant
            || self.max_ingress_per_tenant > self.max_ingress
            || self.max_ingress_waiters > self.max_connections
            || self.max_outbound_bytes_per_connection > self.max_outbound_bytes_per_tenant
            || self.max_outbound_bytes_per_tenant > self.max_outbound_bytes
            || self.max_subscriptions_per_connection > self.max_subscriptions_per_device
            || self.max_subscriptions_per_connection > self.max_subscriptions_per_session
            || self.max_subscriptions_per_session > self.max_subscriptions_per_device
            || self.max_subscriptions_per_session > self.max_subscriptions_per_tenant
            || self.max_subscriptions_per_device > self.max_subscriptions_per_tenant
            || self.max_subscriptions_per_tenant > self.max_subscriptions
            || self.max_inflight_qos1_per_connection > self.max_inflight_qos1_per_session
            || self.max_inflight_qos1_per_session > self.max_inflight_qos1_per_tenant
            || self.max_inflight_qos2_per_session > self.max_inflight_qos2_per_tenant
            || self.max_outbound_messages_per_connection
                < self
                    .max_inflight_qos1_per_session
                    .saturating_add(self.max_inflight_qos2_per_session)
            || self.max_persistent_sessions_per_tenant > self.max_persistent_sessions
            || self.max_offline_messages_per_session > self.max_offline_messages_per_tenant
            || self.max_offline_messages_per_tenant > self.max_offline_messages
            || self.max_offline_bytes_per_session > self.max_offline_bytes_per_tenant
            || self.max_offline_bytes_per_tenant > self.max_offline_bytes
            || self.max_retained_messages_per_tenant > self.max_retained_messages
            || self.max_retained_bytes_per_tenant > self.max_retained_bytes
            || self.max_retained_message_bytes > self.max_mqtt_packet_size
            || self.max_will_payload_bytes > self.max_mqtt_packet_size
            || self.max_mqtt_session_state_bytes > self.global_mqtt_session_bytes
            || self.max_mqtt_session_state_bytes > self.max_mqtt_session_state_bytes_per_tenant
            || self.max_mqtt_session_state_bytes_per_tenant > self.global_mqtt_session_bytes
            || self.max_replay_entries_per_device > self.max_replay_entries_per_tenant
            || self.max_replay_entries_per_tenant > self.max_replay_entries
            || self.max_devices_per_tenant > self.max_devices
            || self.max_fanout_per_event > self.max_sinks
            || self.max_sinks_per_tenant > self.max_sinks
            || self.sink_delivery_concurrency > self.sink_queue_max_count
            || self.retry_base_ms > self.retry_max_ms
            || self.auth_negative_ttl_ms >= self.auth_positive_ttl_ms
            || self.sink_timeout_ms > self.sink_max_age_ms
            || self.spool_record_max_bytes > self.spool_segment_max_bytes
            || self.spool_segment_max_bytes > self.spool_max_bytes
            || self.max_read_buffer_per_connection < max_frame
            || self.connection_memory_reservation < max_frame
            || self.global_connection_logical_bytes < self.connection_memory_reservation
            || self.max_ingress_bytes < max_frame
            || self.max_ingress_wait_bytes < max_frame
            || self.max_command_bytes > self.max_write_buffer_per_connection
            || self.replay_ttl_ms <= self.udp_clock_skew_ms.saturating_mul(2)
        {
            return Err(Error::Configuration);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate_and_inconsistent_hierarchies_fail() {
        assert!(Limits::default().validate().is_ok());
        let mut limits = Limits::default();
        limits.max_fanout_per_event = limits.max_sinks + 1;
        assert!(limits.validate().is_err());
    }
}
