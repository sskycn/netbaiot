use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub const LIMIT_SCALAR_MIN: u64 = 1;
pub const LIMIT_SCALAR_MAX: u64 = u32::MAX as u64;
/// Measured fixed task/session/socket ownership requires a real allowance even
/// when packet buffers are charged separately. A one-byte configured allowance
/// must not admit arbitrarily many tasks against a tiny connection byte budget.
pub const MIN_CONNECTION_MEMORY_RESERVATION: u64 = 16_384;
pub fn limit_scalar_minimum(name: &str) -> u64 {
    if name == "connection_memory_reservation" {
        MIN_CONNECTION_MEMORY_RESERVATION
    } else {
        LIMIT_SCALAR_MIN
    }
}
/// Aggregate byte ceilings may exceed 4 GiB. Stay within both Tokio's semaphore
/// representation and JSON's exact integer range; individual allocations retain
/// the u32 ceiling required by acquire_many.
pub const LIMIT_AGGREGATE_BYTES_MAX: u64 = {
    let exact_json_max = (1u64 << 53) - 1;
    let semaphore_max = tokio::sync::Semaphore::MAX_PERMITS as u64;
    if semaphore_max < exact_json_max {
        semaphore_max
    } else {
        exact_json_max
    }
};

pub fn limit_scalar_maximum(name: &str) -> u64 {
    match name {
        "global_connection_logical_bytes"
        | "max_connection_bytes_per_tenant"
        | "max_outbound_bytes_per_tenant"
        | "max_outbound_bytes"
        | "max_offline_bytes_per_tenant"
        | "max_offline_bytes"
        | "max_retained_bytes_per_tenant"
        | "max_retained_bytes"
        | "max_mqtt_session_state_bytes_per_tenant"
        | "global_mqtt_session_bytes"
        | "global_event_max_bytes"
        | "event_queue_max_bytes_per_tenant"
        | "sink_queue_max_bytes"
        | "auth_cache_max_bytes"
        | "spool_max_bytes"
        | "mqtt_recovery_max_bytes" => LIMIT_AGGREGATE_BYTES_MAX,
        _ => LIMIT_SCALAR_MAX,
    }
}

/// One validated location for every in-process resource ceiling.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
pub struct Limits {
    pub max_connections: usize,
    /// Device ingress only. Effective ceiling is also bounded by the global maximum.
    /// Management/standalone transport listeners retain their existing global accounting.
    pub max_device_connections_per_protocol: usize,
    pub max_connections_per_ip: usize,
    pub max_connections_per_device: usize,
    pub max_connections_per_tenant: usize,
    /// Concurrent device/management TLS handshakes. No task waits for a slot.
    pub max_tls_handshakes: usize,
    pub max_mqtt_packet_size: usize,
    pub max_mqtt_property_bytes: usize,
    pub max_mqtt_user_properties: usize,
    pub max_mqtt_user_property_bytes: usize,
    pub max_mqtt_content_type_bytes: usize,
    pub max_mqtt_response_topic_bytes: usize,
    pub max_mqtt_correlation_data_bytes: usize,
    pub max_http_body_size: usize,
    pub max_http_header_bytes: usize,
    pub max_http_headers: usize,
    pub max_tcp_frame_size: usize,
    pub max_udp_datagram_size: usize,
    /// UDP datagrams being authenticated or admitted; excess datagrams are dropped.
    pub max_udp_inflight_datagrams: usize,
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
    /// Stable capacity of live reader allocations, including shared frames and
    /// the four-byte TCP envelope. Exclusive replacement may temporarily hold
    /// twice this capacity; both allocations still obey global/tenant budgets.
    pub max_read_buffer_per_connection: usize,
    pub max_write_buffer_per_connection: usize,
    pub max_outbound_messages_per_connection: usize,
    pub max_outbound_bytes_per_connection: usize,
    pub max_outbound_bytes_per_tenant: usize,
    pub max_outbound_bytes: usize,
    /// Fixed connection allowances plus actual dynamically allocated read capacity.
    pub global_connection_logical_bytes: usize,
    /// Same accounting for authenticated connections of one tenant. Before
    /// authentication, global count/IP/fixed/dynamic ceilings still apply.
    pub max_connection_bytes_per_tenant: usize,
    /// Fixed connection allowance only. Packet capacity is charged on demand.
    /// This is a resource reservation, not measured RSS or kernel socket memory.
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
    /// Process-local accepted and in-flight CommandId reservations.
    pub command_dedup_max_entries: usize,
    pub command_dedup_ttl_ms: u64,
    pub max_devices: usize,
    pub max_devices_per_tenant: usize,
    /// Retention window for disconnected device presence. Active sessions are never evicted.
    pub presence_ttl_ms: u64,
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
    /// Independent concurrency ceiling for outbound HTTP authentication requests.
    pub max_auth_provider_requests: usize,
    pub auth_positive_ttl_ms: u64,
    pub auth_negative_ttl_ms: u64,
    pub management_auth_max_subject_bytes: usize,
    pub management_auth_max_scopes: usize,
    pub management_auth_max_scope_bytes: usize,
    pub management_auth_max_resource_entries: usize,
    pub management_auth_max_resource_bytes: usize,
    pub management_api_key_max_entries: usize,
    pub management_api_key_max_bytes: usize,
    pub management_jwks_max_keys: usize,
    pub management_jwks_max_bytes: usize,
    pub management_jwks_ttl_ms: u64,
    pub management_jwks_refresh_min_interval_ms: u64,
    pub management_jwt_max_bytes: usize,
    /// Gateway product/codec profiles only; no per-device business state.
    pub control_max_products: usize,
    /// Serialized gateway product profiles and routes (not preallocated).
    pub control_max_bytes: usize,
    pub max_sinks: usize,
    pub max_sinks_per_tenant: usize,
    pub max_routing_filters: usize,
    pub max_fanout_per_event: usize,
    pub global_event_max_count: usize,
    pub global_event_max_bytes: usize,
    /// Outstanding EventBus events per tenant, including retries and inflight work.
    /// The global count ceiling also applies; the existing default ceiling is unchanged.
    pub event_queue_max_count_per_tenant: usize,
    /// Serialized outstanding event bytes per tenant, charged once per event.
    /// The global byte ceiling also applies. No payload buffer is preallocated.
    pub event_queue_max_bytes_per_tenant: usize,
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
    pub mqtt_recovery_max_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_device_connections_per_protocol: 192,
            max_connections_per_ip: 32,
            max_connections_per_device: 2,
            max_connections_per_tenant: 64,
            max_tls_handshakes: 64,
            max_mqtt_packet_size: 65_536,
            max_mqtt_property_bytes: 4_096,
            max_mqtt_user_properties: 16,
            max_mqtt_user_property_bytes: 2_048,
            max_mqtt_content_type_bytes: 256,
            max_mqtt_response_topic_bytes: 256,
            max_mqtt_correlation_data_bytes: 1_024,
            max_http_body_size: 65_536,
            max_http_header_bytes: 8_192,
            max_http_headers: 32,
            max_tcp_frame_size: 65_536,
            max_udp_datagram_size: 1_200,
            max_udp_inflight_datagrams: 64,
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
            max_read_buffer_per_connection: 65_540,
            max_write_buffer_per_connection: 65_536,
            // Reconnect may need to enqueue every independently bounded QoS1 and QoS2
            // retransmission before it can accept more protocol work.
            max_outbound_messages_per_connection: 64,
            max_outbound_bytes_per_connection: 262_144,
            max_outbound_bytes_per_tenant: 2_097_152,
            max_outbound_bytes: 8_388_608,
            global_connection_logical_bytes: 134_217_728,
            max_connection_bytes_per_tenant: 67_108_864,
            connection_memory_reservation: 16_384,
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
            command_dedup_max_entries: 4_096,
            command_dedup_ttl_ms: 300_000,
            max_devices: 1_024,
            max_devices_per_tenant: 128,
            presence_ttl_ms: 3_600_000,
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
            max_auth_provider_requests: 16,
            auth_positive_ttl_ms: 300_000,
            auth_negative_ttl_ms: 5_000,
            management_auth_max_subject_bytes: 128,
            management_auth_max_scopes: 32,
            management_auth_max_scope_bytes: 512,
            management_auth_max_resource_entries: 128,
            management_auth_max_resource_bytes: 8_192,
            management_api_key_max_entries: 128,
            management_api_key_max_bytes: 32_768,
            management_jwks_max_keys: 32,
            management_jwks_max_bytes: 65_536,
            management_jwks_ttl_ms: 300_000,
            management_jwks_refresh_min_interval_ms: 30_000,
            management_jwt_max_bytes: 16_384,
            control_max_products: 4_096,
            control_max_bytes: 16_777_216,
            max_sinks: 32,
            max_sinks_per_tenant: 8,
            max_routing_filters: 256,
            max_fanout_per_event: 8,
            global_event_max_count: 16_384,
            global_event_max_bytes: 67_108_864,
            event_queue_max_count_per_tenant: 16_384,
            event_queue_max_bytes_per_tenant: 67_108_864,
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
            // NBMQ v6 stores raw payload bytes, bounded record envelopes, profile metadata, and a
            // fixed integrity trailer. The checked upper-bound formula below covers admitted state.
            mqtt_recovery_max_bytes: 202_195_044,
        }
    }
}

impl Limits {
    /// Conservative upper bound for compact NBMQ v6. Logical state accounting covers payloads and
    /// the common record envelope; these extra terms cover retained owners, session profile data,
    /// pending-Will owners, and the fixed header/trailer outside that accounting.
    pub fn mqtt_recovery_upper_bound(&self) -> Result<usize> {
        self.global_mqtt_session_bytes
            .checked_add(self.max_retained_bytes)
            .and_then(|bytes| bytes.checked_add(self.max_retained_messages.checked_mul(64)?))
            .and_then(|bytes| bytes.checked_add(self.max_persistent_sessions.checked_mul(128)?))
            // A delayed Will record writes its cancellation ClientId as well as its origin.
            // Logical Will bytes charge the origin once; allow the second bounded copy here.
            .and_then(|bytes| {
                bytes.checked_add(
                    self.max_connections
                        .checked_mul(256usize.checked_add(self.max_client_id_bytes)?)?,
                )
            })
            .and_then(|bytes| bytes.checked_add(100))
            .ok_or(Error::Configuration)
    }

    pub fn validate(&self) -> Result<()> {
        if self.management_auth_max_subject_bytes > 256
            || self.management_auth_max_scopes > 64
            || self.management_auth_max_scope_bytes > 1_024
            || self.management_auth_max_resource_entries > 1_024
            || self.management_auth_max_resource_bytes > 65_536
            || self.management_api_key_max_entries > 1_024
            || self.management_api_key_max_bytes > 262_144
            || self.management_jwks_max_keys > 128
            || self.management_jwks_max_bytes > 262_144
            || self.management_jwks_ttl_ms > 3_600_000
            || self.management_jwks_refresh_min_interval_ms > self.management_jwks_ttl_ms
            || self.management_jwt_max_bytes > 65_536
        {
            return Err(Error::Configuration);
        }
        let value = serde_json::to_value(self).map_err(|_| Error::Configuration)?;
        if value
            .as_object()
            .ok_or(Error::Configuration)?
            .iter()
            .any(|(name, v)| {
                v.as_u64().is_none_or(|number| {
                    !(limit_scalar_minimum(name)..=limit_scalar_maximum(name)).contains(&number)
                })
            })
        {
            return Err(Error::Configuration);
        }
        let max_frame = self
            .max_mqtt_packet_size
            .max(self.max_http_body_size)
            .max(self.max_tcp_frame_size);
        let max_read_frame = self
            .max_tcp_frame_size
            .checked_add(4)
            .ok_or(Error::Configuration)?
            .max(self.max_mqtt_packet_size)
            .max(self.max_http_body_size);
        if max_frame > 1_048_576
            || self.max_tls_handshakes > 65_536
            || self.max_mqtt_property_bytes > self.max_mqtt_packet_size
            || self.max_mqtt_user_property_bytes > self.max_mqtt_property_bytes
            || self.max_mqtt_content_type_bytes > self.max_mqtt_property_bytes
            || self.max_mqtt_response_topic_bytes > self.max_mqtt_property_bytes
            || self.max_mqtt_correlation_data_bytes > self.max_mqtt_property_bytes
            || self.max_mqtt_user_properties > self.max_mqtt_property_bytes
            || self.max_udp_datagram_size > 1_200
            || self.max_udp_inflight_datagrams > 1_024
            || self.max_auth_provider_requests > 1_024
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
            || self.mqtt_recovery_max_bytes < self.mqtt_recovery_upper_bound()?
            || self.max_read_buffer_per_connection < max_read_frame
            || self.global_connection_logical_bytes < self.connection_memory_reservation
            || self.max_connection_bytes_per_tenant < self.connection_memory_reservation
            || self.max_ingress_bytes < max_frame
            || self.max_ingress_wait_bytes < max_frame
            || self.max_command_bytes > self.max_write_buffer_per_connection
            || self.command_dedup_max_entries > 65_536
            || self.command_dedup_ttl_ms > 86_400_000
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
    fn tenant_backlog_limits_keep_old_configs_and_validate_new_scalars() {
        let old: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(
            old.event_queue_max_count_per_tenant,
            old.global_event_max_count
        );
        assert_eq!(
            old.event_queue_max_bytes_per_tenant,
            old.global_event_max_bytes
        );
        let configured: Limits = serde_json::from_str(
            r#"{"event_queue_max_count_per_tenant":2,"event_queue_max_bytes_per_tenant":512}"#,
        )
        .unwrap();
        configured.validate().unwrap();
        let json = serde_json::to_value(&configured).unwrap();
        assert_eq!(json["event_queue_max_count_per_tenant"], 2);
        assert_eq!(json["event_queue_max_bytes_per_tenant"], 512);
        let mut invalid = configured;
        invalid.event_queue_max_count_per_tenant = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn defaults_validate_and_inconsistent_hierarchies_fail() {
        assert!(Limits::default().validate().is_ok());
        let mut limits = Limits::default();
        limits.max_fanout_per_event = limits.max_sinks + 1;
        assert!(limits.validate().is_err());
        let mut limits = Limits::default();
        limits.mqtt_recovery_max_bytes = limits.global_mqtt_session_bytes;
        assert!(limits.validate().is_err());
        let mut limits = Limits::default();
        limits.max_mqtt_user_property_bytes = limits.max_mqtt_property_bytes + 1;
        assert!(limits.validate().is_err());
    }

    #[test]
    fn udp_worker_ceiling_defaults_for_old_configs_and_is_bounded() {
        let old: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(old.max_udp_inflight_datagrams, 64);
        let configured: Limits =
            serde_json::from_str(r#"{"max_udp_inflight_datagrams":8}"#).unwrap();
        configured.validate().unwrap();
        for invalid in [0, 1_025] {
            let mut limits = configured.clone();
            limits.max_udp_inflight_datagrams = invalid;
            assert!(matches!(limits.validate(), Err(Error::Configuration)));
        }
    }

    #[test]
    fn auth_provider_capacity_is_independent_and_old_configs_still_load() {
        let old: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(old.max_auth_provider_requests, 16);
        let mut limits = Limits {
            max_ingress: 1,
            max_ingress_per_tenant: 1,
            max_ingress_per_device: 1,
            max_auth_provider_requests: 32,
            ..Limits::default()
        };
        limits.validate().unwrap();
        for invalid in [0, 1_025] {
            limits.max_auth_provider_requests = invalid;
            assert!(matches!(limits.validate(), Err(Error::Configuration)));
        }
    }

    #[test]
    fn fixed_connection_allowance_is_independent_of_maximum_frame() {
        let limits = Limits {
            connection_memory_reservation: 16_384,
            max_tcp_frame_size: 1_048_576,
            max_read_buffer_per_connection: 1_048_580,
            ..Limits::default()
        };
        limits.validate().unwrap();
        let mut missing_envelope = limits;
        missing_envelope.max_read_buffer_per_connection = 1_048_576;
        assert!(matches!(
            missing_envelope.validate(),
            Err(Error::Configuration)
        ));
    }

    #[test]
    fn aggregate_bytes_above_four_gib_validate_without_enlarging_allocations() {
        if usize::BITS < 64 {
            return;
        }
        let large = usize::try_from(64u64 * 1_024 * 1_024 * 1_024).unwrap();
        let mut limits = Limits {
            max_connections: 1_000_000,
            max_device_connections_per_protocol: 1_000_000,
            max_connections_per_ip: 1_000_000,
            max_connections_per_tenant: 1_000_000,
            max_connection_bytes_per_tenant: large,
            global_connection_logical_bytes: large,
            mqtt_recovery_max_bytes: 1_073_741_824,
            connection_memory_reservation: 16_384,
            ..Limits::default()
        };
        limits.validate().unwrap();
        limits.max_read_buffer_per_connection = large;
        assert!(matches!(limits.validate(), Err(Error::Configuration)));
        limits.max_read_buffer_per_connection = 65_540;
        limits.global_connection_logical_bytes =
            usize::try_from(LIMIT_AGGREGATE_BYTES_MAX + 1).unwrap();
        assert!(matches!(limits.validate(), Err(Error::Configuration)));
    }

    #[test]
    fn tls_handshakes_are_independently_bounded_and_old_configs_default() {
        let mut limits: Limits = serde_json::from_str("{}").unwrap();
        assert_eq!(limits.max_tls_handshakes, 64);
        for invalid in [0, 65_537] {
            limits.max_tls_handshakes = invalid;
            assert!(matches!(limits.validate(), Err(Error::Configuration)));
        }
        limits.max_tls_handshakes = 65_536;
        limits.validate().unwrap();
    }

    #[test]
    fn fixed_allowance_cannot_undercharge_measured_connection_ownership() {
        let mut limits = Limits::default();
        for bytes in [1, 2_048, 16_383] {
            limits.connection_memory_reservation = bytes;
            assert!(matches!(limits.validate(), Err(Error::Configuration)));
        }
        limits.connection_memory_reservation = 16_384;
        limits.validate().unwrap();
    }
}
