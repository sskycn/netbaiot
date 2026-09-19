use crate::{Error, Result};
use serde::{Deserialize, Serialize};

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
    pub max_subscriptions_per_device: usize,
    pub max_subscriptions_per_tenant: usize,
    pub max_subscriptions: usize,
    pub max_subscription_filters_per_packet: usize,
    pub max_inflight_qos1_per_connection: usize,
    pub max_outbound_messages_per_connection: usize,
    pub max_outbound_bytes_per_connection: usize,
    pub max_outbound_bytes_per_tenant: usize,
    pub max_outbound_bytes: usize,
    pub max_network_bytes: usize,
    pub connection_memory_reservation: usize,
    pub max_ingress: usize,
    pub max_ingress_per_tenant: usize,
    pub max_ingress_per_device: usize,
    pub max_ingress_bytes: usize,
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
    pub max_stored_messages: usize,
    pub max_stored_messages_per_tenant: usize,
    pub max_stored_messages_per_device: usize,
    pub max_stored_bytes: usize,
    pub max_stored_bytes_per_device: usize,
    pub max_stored_bytes_per_tenant: usize,
    pub delivery_batch: usize,
    pub max_database_connections: u32,
    pub worker_poll_interval_ms: u64,
    pub max_attempts: u32,
    pub connect_timeout_ms: u64,
    pub authentication_timeout_ms: u64,
    pub packet_read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub external_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub dedup_ttl_ms: u64,
    pub delivery_ttl_ms: u64,
    pub command_ttl_ms: u64,
    pub replay_ttl_ms: u64,
    pub lease_ms: u64,
    pub retry_base_ms: u64,
    pub retry_max_ms: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_connections_per_ip: 32,
            max_connections_per_device: 2,
            max_connections_per_tenant: 64,
            max_mqtt_packet_size: 65536,
            max_http_body_size: 65536,
            max_http_header_bytes: 8192,
            max_http_headers: 32,
            max_tcp_frame_size: 65536,
            max_udp_datagram_size: 1200,
            max_client_id_bytes: 64,
            max_username_bytes: 64,
            max_password_bytes: 128,
            max_topic_bytes: 256,
            max_topic_depth: 8,
            max_subscriptions_per_connection: 2,
            max_subscriptions_per_device: 2,
            max_subscriptions_per_tenant: 128,
            max_subscriptions: 512,
            max_subscription_filters_per_packet: 16,
            max_inflight_qos1_per_connection: 32,
            max_outbound_messages_per_connection: 32,
            max_outbound_bytes_per_connection: 262144,
            max_outbound_bytes_per_tenant: 2097152,
            max_outbound_bytes: 8388608,
            max_network_bytes: 134217728,
            connection_memory_reservation: 524288,
            max_ingress: 16,
            max_ingress_per_tenant: 4,
            max_ingress_per_device: 1,
            max_ingress_bytes: 2097152,
            max_pending_commands_per_device: 16,
            max_pending_commands_per_tenant: 128,
            max_pending_commands: 1024,
            max_command_bytes: 16384,
            max_devices: 1024,
            max_devices_per_tenant: 128,
            max_replay_entries: 1024,
            max_replay_entries_per_device: 2,
            max_replay_entries_per_tenant: 256,
            udp_clock_skew_ms: 30000,
            rate_entries: 1024,
            requests_per_second: 512,
            requests_per_ip_second: 32,
            messages_per_device_second: 16,
            messages_per_tenant_second: 128,
            max_stored_messages: 100000,
            max_stored_messages_per_tenant: 10000,
            max_stored_messages_per_device: 1000,
            max_stored_bytes: 134217728,
            max_stored_bytes_per_device: 2097152,
            max_stored_bytes_per_tenant: 16777216,
            delivery_batch: 16,
            max_database_connections: 8,
            worker_poll_interval_ms: 200,
            max_attempts: 5,
            connect_timeout_ms: 10000,
            authentication_timeout_ms: 5000,
            packet_read_timeout_ms: 30000,
            write_timeout_ms: 10000,
            idle_timeout_ms: 120000,
            request_timeout_ms: 15000,
            external_timeout_ms: 5000,
            shutdown_timeout_ms: 30000,
            dedup_ttl_ms: 86400000,
            delivery_ttl_ms: 3600000,
            command_ttl_ms: 300000,
            replay_ttl_ms: 120000,
            lease_ms: 30000,
            retry_base_ms: 1000,
            retry_max_ms: 30000,
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        let value = serde_json::to_value(self).map_err(|_| Error::Configuration)?;
        let fields = value.as_object().ok_or(Error::Configuration)?;
        if fields
            .values()
            .any(|v| v.as_u64().is_none_or(|n| n == 0 || n > 1_073_741_824))
        {
            return Err(Error::Configuration);
        }
        let max_packet = self
            .max_mqtt_packet_size
            .max(self.max_http_body_size)
            .max(self.max_tcp_frame_size);
        if max_packet > 1_048_576
            || self.max_udp_datagram_size > 1200
            || self.max_topic_bytes > u16::MAX as usize
            || self.max_inflight_qos1_per_connection > u16::MAX as usize
            || self.max_http_header_bytes < 8192
            || self.max_http_header_bytes > max_packet
            || self.max_http_headers > 128
            || self.max_subscription_filters_per_packet > 256
            || self.max_client_id_bytes > 64
            || self.max_username_bytes > 64
            || self.max_password_bytes > 256
            || self.max_database_connections > 64
            || self.delivery_batch > 256
            || self.max_attempts > 100
            || self.max_stored_bytes_per_device >= self.max_stored_bytes_per_tenant
            || self.max_stored_bytes_per_tenant >= self.max_stored_bytes
            || self
                .max_connections_per_tenant
                .saturating_mul(self.connection_memory_reservation)
                >= self.max_network_bytes
            || self.max_connections_per_ip > self.max_connections
            || self.max_connections_per_device > self.max_connections_per_tenant
            || self.max_connections_per_tenant > self.max_connections
            || self.max_ingress_per_device > self.max_ingress_per_tenant
            || self.max_ingress_per_tenant > self.max_ingress
            || self.max_pending_commands_per_device > self.max_pending_commands_per_tenant
            || self.max_pending_commands_per_tenant > self.max_pending_commands
            || self.replay_ttl_ms <= self.udp_clock_skew_ms.saturating_mul(2)
            || self.max_replay_entries_per_device > self.max_replay_entries_per_tenant
            || self.max_replay_entries_per_tenant > self.max_replay_entries
            || self.max_devices_per_tenant > self.max_devices
            || self.max_replay_entries < self.max_devices
            || self.max_outbound_bytes_per_connection > self.max_outbound_bytes_per_tenant
            || self.max_outbound_bytes_per_tenant > self.max_outbound_bytes
            || self.max_subscriptions_per_device > self.max_subscriptions_per_tenant
            || self.max_subscriptions_per_tenant > self.max_subscriptions
            || self.max_subscriptions_per_connection > self.max_subscriptions_per_device
            || self.max_stored_messages_per_device > self.max_stored_messages_per_tenant
            || self.max_stored_messages_per_tenant > self.max_stored_messages
            || self.delivery_ttl_ms > self.dedup_ttl_ms
            || self.external_timeout_ms >= self.lease_ms
            || self.retry_base_ms > self.retry_max_ms
            || self.max_command_bytes > max_packet / 2
            || self.max_ingress_bytes < max_packet
            || self.connection_memory_reservation < max_packet.saturating_mul(8)
            || self.max_network_bytes < self.connection_memory_reservation
            || self.max_password_bytes < 64
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
    fn defaults_and_invalid() {
        let mut l = Limits::default();
        assert!(l.validate().is_ok());
        l.max_ingress = 0;
        assert!(l.validate().is_err());
        l = Limits::default();
        l.connection_memory_reservation = 1;
        assert!(l.validate().is_err());
    }
}
