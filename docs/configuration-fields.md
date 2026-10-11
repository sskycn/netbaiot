# Configuration fields (generated)

Generated from Rust Config/serde types. Run `cargo xtask config-reference`.
Schema does not replace `netbaiot config check`; cross-field, TLS, secret-source and runtime checks remain authoritative.

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |

## Config

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `auth_provider_url` | ["string","null"] | `required/no default` | Optional HTTP device-auth provider URL; default checks do not contact it. |
| `business_rpc` | union/object | `null` |  |
| `business_tcp` | ["string","null"] | `required/no default` | Optional confirmed business listener; role/TLS constraints require config check. |
| `credentials` | "array" | `[]` | Static device credentials; secrets must not be logged or reused from development. |
| `delivery_url` | ["string","null"] | `required/no default` | Required HTTP sink URL; HTTPS or loopback HTTP, without URL credentials. |
| `development` | "boolean" | `false` | Development mode requires loopback listeners; never reuse demo credentials in production. |
| `device_auth` | union/object | `null` |  |
| `device_ingress` | "string" | `required/no default` | Device MQTT/framed TCP listener; UDP uses the same actual port. Public TCP requires TLS. |
| `event_delivery` | union/object | `null` |  |
| `limits` | "#/$defs/Limits" | `{"auth_cache_max_bytes":4194304,"auth_cache_max_entries":4096,"auth_cache_max_waiters":256,"auth_negative_ttl_ms":5000,"auth_positive_ttl_ms":300000,"authentication_timeout_ms":5000,"command_dedup_max_entries":4096,"command_dedup_ttl_ms":300000,"command_ttl_ms":300000,"connect_timeout_ms":10000,"connection_memory_reservation":524288,"control_max_bytes":16777216,"control_max_products":4096,"event_queue_max_bytes_per_tenant":67108864,"event_queue_max_count_per_tenant":16384,"external_timeout_ms":5000,"global_connection_logical_bytes":134217728,"global_event_max_bytes":67108864,"global_event_max_count":16384,"global_mqtt_session_bytes":134217728,"idle_timeout_ms":120000,"ingress_wait_timeout_ms":25,"management_api_key_max_bytes":32768,"management_api_key_max_entries":128,"management_auth_max_resource_bytes":8192,"management_auth_max_resource_entries":128,"management_auth_max_scope_bytes":512,"management_auth_max_scopes":32,"management_auth_max_subject_bytes":128,"management_jwks_max_bytes":65536,"management_jwks_max_keys":32,"management_jwks_refresh_min_interval_ms":30000,"management_jwks_ttl_ms":300000,"management_jwt_max_bytes":16384,"max_auth_provider_requests":16,"max_client_id_bytes":64,"max_command_bytes":16384,"max_connections":256,"max_connections_per_device":2,"max_connections_per_ip":32,"max_connections_per_tenant":64,"max_device_connections_per_protocol":192,"max_devices":1024,"max_devices_per_tenant":128,"max_fanout_per_event":8,"max_http_body_size":65536,"max_http_header_bytes":8192,"max_http_headers":32,"max_inflight_qos1_per_connection":32,"max_inflight_qos1_per_session":32,"max_inflight_qos1_per_tenant":4096,"max_inflight_qos2_per_session":32,"max_inflight_qos2_per_tenant":4096,"max_ingress":16,"max_ingress_bytes":2097152,"max_ingress_per_device":1,"max_ingress_per_tenant":4,"max_ingress_wait_bytes":2097152,"max_ingress_waiters":16,"max_mqtt_content_type_bytes":256,"max_mqtt_correlation_data_bytes":1024,"max_mqtt_packet_size":65536,"max_mqtt_property_bytes":4096,"max_mqtt_response_topic_bytes":256,"max_mqtt_session_state_bytes":2097152,"max_mqtt_session_state_bytes_per_tenant":33554432,"max_mqtt_user_properties":16,"max_mqtt_user_property_bytes":2048,"max_offline_bytes":134217728,"max_offline_bytes_per_session":1048576,"max_offline_bytes_per_tenant":33554432,"max_offline_messages":16384,"max_offline_messages_per_session":128,"max_offline_messages_per_tenant":4096,"max_outbound_bytes":8388608,"max_outbound_bytes_per_connection":262144,"max_outbound_bytes_per_tenant":2097152,"max_outbound_messages_per_connection":64,"max_password_bytes":128,"max_pending_commands":1024,"max_pending_commands_per_device":16,"max_pending_commands_per_tenant":128,"max_persistent_sessions":4096,"max_persistent_sessions_per_tenant":512,"max_read_buffer_per_connection":65536,"max_replay_entries":1024,"max_replay_entries_per_device":2,"max_replay_entries_per_tenant":256,"max_retained_bytes":67108864,"max_retained_bytes_per_tenant":8388608,"max_retained_message_bytes":65536,"max_retained_messages":4096,"max_retained_messages_per_tenant":512,"max_routing_filters":256,"max_sinks":32,"max_sinks_per_tenant":8,"max_subscription_filters_per_packet":16,"max_subscriptions":512,"max_subscriptions_per_connection":2,"max_subscriptions_per_device":64,"max_subscriptions_per_session":32,"max_subscriptions_per_tenant":128,"max_tcp_frame_size":65536,"max_topic_bytes":256,"max_topic_depth":8,"max_udp_datagram_size":1200,"max_udp_inflight_datagrams":64,"max_username_bytes":64,"max_will_payload_bytes":65536,"max_write_buffer_per_connection":65536,"messages_per_device_second":16,"messages_per_tenant_second":128,"mqtt_recovery_max_bytes":202195044,"mqtt_session_idle_ttl_ms":86400000,"packet_read_timeout_ms":30000,"presence_ttl_ms":3600000,"rate_entries":1024,"replay_ttl_ms":120000,"request_timeout_ms":15000,"requests_per_ip_second":32,"requests_per_second":512,"retry_base_ms":100,"retry_max_ms":30000,"shutdown_drain_timeout_ms":20000,"shutdown_timeout_ms":30000,"sink_delivery_concurrency":8,"sink_max_age_ms":3600000,"sink_max_attempts":5,"sink_queue_max_bytes":16777216,"sink_queue_max_count":4096,"sink_timeout_ms":5000,"spool_max_bytes":268435456,"spool_max_records":100000,"spool_record_max_bytes":1048576,"spool_segment_max_bytes":67108864,"udp_clock_skew_ms":30000,"write_timeout_ms":10000}` | Bounded runtime resource limits, defaulted from Limits::default(). |
| `management_auth` | "#/$defs/ManagementAuthConfig" | `{"api_keys":[],"jwt":null,"legacy_static_token_enabled":null,"mtls_identities":[]}` | Management providers, scoped identities and protected secret-source names. |
| `management_http` | "string" | `required/no default` | Independent authenticated management listener. Public management requires TLS. |
| `management_tls` | union/object | `null` | Independent management TLS and optional mapped client certificate authentication. |
| `spool_directory` | "string" | `required/no default` | Dedicated local planned-restart recovery directory; one gateway owner only. |
| `tls` | union/object | `required/no default` | Device server PEM certificate and matching private-key paths. |

## AdminProduct

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `product_id` | "string" | `required/no default` |  |
| `tenant_id` | "string" | `required/no default` |  |

## AdminResourceConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `devices` | "array" | `[]` |  |
| `products` | "array" | `[]` |  |
| `tenants` | "array" | `[]` |  |

## ApiKeyConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `auth_generation` | "integer" | `0` |  |
| `enabled` | "boolean" | `true` |  |
| `expires_at` | ["integer","null"] | `required/no default` |  |
| `global` | "boolean" | `false` |  |
| `key_id` | "string" | `required/no default` |  |
| `resources` | "#/$defs/AdminResourceConfig" | `{"devices":[],"products":[],"tenants":[]}` |  |
| `scopes` | "array" | `required/no default` |  |
| `secret_env` | "string" | `required/no default` |  |
| `subject` | "string" | `required/no default` |  |

## AuthenticatedDevice

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `auth_generation` | "integer" | `1` |  |
| `codec_id` | "string" | `required/no default` |  |
| `codec_version` | "integer" | `required/no default` |  |
| `credential_version` | "integer" | `required/no default` |  |
| `device_key` | "#/$defs/DeviceKey" | `required/no default` |  |
| `permissions` | "#/$defs/Permissions" | `required/no default` |  |

## BusinessRpcConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `auth_max_inflight` | "integer" | `128` |  |
| `development_role` | union/object | `null` |  |
| `development_token_env` | ["string","null"] | `required/no default` |  |
| `experiment_socket_send_buffer_bytes` | ["integer","null"] | `null` |  |
| `identities` | "array" | `[]` |  |
| `limits` | "#/$defs/V3Limits" | `{"heartbeat_ms":5000,"initial_connection_window_bytes":4194304,"initial_stream_window_bytes":262144,"max_concurrent_streams":256,"max_frame_payload_bytes":8192}` | Limits for the sole current Business RPC wire format. |
| `max_auth_control_offline_ms` | "integer" | `30000` |  |
| `max_connections` | "integer" | `8` |  |
| `send_ahead` | union/object | `null` | Local sender policy, separate from the negotiated V3 receive windows. |
| `tls` | union/object | `required/no default` |  |

## BusinessRpcIdentityConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `call_methods` | "array" | `required/no default` |  |
| `certificate_sha256` | "string" | `required/no default` |  |
| `expires_at_ms` | ["integer","null"] | `required/no default` |  |
| `global` | "boolean" | `false` |  |
| `principal_id` | "string" | `required/no default` |  |
| `provide_methods` | "array" | `required/no default` |  |
| `provider_id` | ["string","null"] | `required/no default` |  |
| `role` | "#/$defs/BusinessRole" | `required/no default` |  |
| `sink_id` | ["string","null"] | `required/no default` |  |
| `tenants` | "array" | `[]` |  |

## Credential

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `credential_id` | "string" | `required/no default` |  |
| `identity` | "#/$defs/AuthenticatedDevice" | `required/no default` |  |
| `secret_hex` | "string" | `required/no default` |  |

## DeviceKey

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `device_id` | "string" | `required/no default` |  |
| `product_id` | "string" | `required/no default` |  |
| `tenant_id` | "string" | `required/no default` |  |

## JwtConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `audience` | "string" | `required/no default` |  |
| `global_roles` | "array" | `[]` |  |
| `issuer` | "string" | `required/no default` |  |
| `jwks_url` | "string" | `required/no default` |  |
| `role_scopes` | "object" | `{}` |  |
| `roles_claim` | "string" | `"roles"` |  |
| `scope_claim` | "string" | `"scope"` |  |
| `subject_claim` | "string" | `"sub"` |  |
| `tenant_claim` | "string" | `"tenants"` |  |

## Limits

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `auth_cache_max_bytes` | "integer" | `4194304` |  |
| `auth_cache_max_entries` | "integer" | `4096` |  |
| `auth_cache_max_waiters` | "integer" | `256` |  |
| `auth_negative_ttl_ms` | "integer" | `5000` |  |
| `auth_positive_ttl_ms` | "integer" | `300000` |  |
| `authentication_timeout_ms` | "integer" | `5000` |  |
| `command_dedup_max_entries` | "integer" | `4096` | Process-local accepted and in-flight CommandId reservations. |
| `command_dedup_ttl_ms` | "integer" | `300000` |  |
| `command_ttl_ms` | "integer" | `300000` |  |
| `connect_timeout_ms` | "integer" | `10000` |  |
| `connection_memory_reservation` | "integer" | `524288` |  |
| `control_max_bytes` | "integer" | `16777216` | Serialized gateway product profiles and routes (not preallocated). |
| `control_max_products` | "integer" | `4096` | Gateway product/codec profiles only; no per-device business state. |
| `event_queue_max_bytes_per_tenant` | "integer" | `67108864` | Serialized outstanding event bytes per tenant, charged once per event. The global byte ceiling also applies. No payload buffer is preallocated. |
| `event_queue_max_count_per_tenant` | "integer" | `16384` | Outstanding EventBus events per tenant, including retries and inflight work. The global count ceiling also applies; the existing default ceiling is unchanged. |
| `external_timeout_ms` | "integer" | `5000` |  |
| `global_connection_logical_bytes` | "integer" | `134217728` |  |
| `global_event_max_bytes` | "integer" | `67108864` |  |
| `global_event_max_count` | "integer" | `16384` |  |
| `global_mqtt_session_bytes` | "integer" | `134217728` |  |
| `idle_timeout_ms` | "integer" | `120000` |  |
| `ingress_wait_timeout_ms` | "integer" | `25` |  |
| `management_api_key_max_bytes` | "integer" | `32768` |  |
| `management_api_key_max_entries` | "integer" | `128` |  |
| `management_auth_max_resource_bytes` | "integer" | `8192` |  |
| `management_auth_max_resource_entries` | "integer" | `128` |  |
| `management_auth_max_scope_bytes` | "integer" | `512` |  |
| `management_auth_max_scopes` | "integer" | `32` |  |
| `management_auth_max_subject_bytes` | "integer" | `128` |  |
| `management_jwks_max_bytes` | "integer" | `65536` |  |
| `management_jwks_max_keys` | "integer" | `32` |  |
| `management_jwks_refresh_min_interval_ms` | "integer" | `30000` |  |
| `management_jwks_ttl_ms` | "integer" | `300000` |  |
| `management_jwt_max_bytes` | "integer" | `16384` |  |
| `max_auth_provider_requests` | "integer" | `16` | Independent concurrency ceiling for outbound HTTP authentication requests. |
| `max_client_id_bytes` | "integer" | `64` |  |
| `max_command_bytes` | "integer" | `16384` |  |
| `max_connections` | "integer" | `256` |  |
| `max_connections_per_device` | "integer" | `2` |  |
| `max_connections_per_ip` | "integer" | `32` |  |
| `max_connections_per_tenant` | "integer" | `64` |  |
| `max_device_connections_per_protocol` | "integer" | `192` | Device ingress only. Effective ceiling is also bounded by the global maximum. Management/standalone transport listeners retain their existing global accounting. |
| `max_devices` | "integer" | `1024` |  |
| `max_devices_per_tenant` | "integer" | `128` |  |
| `max_fanout_per_event` | "integer" | `8` |  |
| `max_http_body_size` | "integer" | `65536` |  |
| `max_http_header_bytes` | "integer" | `8192` |  |
| `max_http_headers` | "integer" | `32` |  |
| `max_inflight_qos1_per_connection` | "integer" | `32` |  |
| `max_inflight_qos1_per_session` | "integer" | `32` |  |
| `max_inflight_qos1_per_tenant` | "integer" | `4096` |  |
| `max_inflight_qos2_per_session` | "integer" | `32` |  |
| `max_inflight_qos2_per_tenant` | "integer" | `4096` |  |
| `max_ingress` | "integer" | `16` |  |
| `max_ingress_bytes` | "integer" | `2097152` |  |
| `max_ingress_per_device` | "integer" | `1` |  |
| `max_ingress_per_tenant` | "integer" | `4` |  |
| `max_ingress_wait_bytes` | "integer" | `2097152` |  |
| `max_ingress_waiters` | "integer" | `16` |  |
| `max_mqtt_content_type_bytes` | "integer" | `256` |  |
| `max_mqtt_correlation_data_bytes` | "integer" | `1024` |  |
| `max_mqtt_packet_size` | "integer" | `65536` |  |
| `max_mqtt_property_bytes` | "integer" | `4096` |  |
| `max_mqtt_response_topic_bytes` | "integer" | `256` |  |
| `max_mqtt_session_state_bytes` | "integer" | `2097152` |  |
| `max_mqtt_session_state_bytes_per_tenant` | "integer" | `33554432` |  |
| `max_mqtt_user_properties` | "integer" | `16` |  |
| `max_mqtt_user_property_bytes` | "integer" | `2048` |  |
| `max_offline_bytes` | "integer" | `134217728` |  |
| `max_offline_bytes_per_session` | "integer" | `1048576` |  |
| `max_offline_bytes_per_tenant` | "integer" | `33554432` |  |
| `max_offline_messages` | "integer" | `16384` |  |
| `max_offline_messages_per_session` | "integer" | `128` |  |
| `max_offline_messages_per_tenant` | "integer" | `4096` |  |
| `max_outbound_bytes` | "integer" | `8388608` |  |
| `max_outbound_bytes_per_connection` | "integer" | `262144` |  |
| `max_outbound_bytes_per_tenant` | "integer" | `2097152` |  |
| `max_outbound_messages_per_connection` | "integer" | `64` |  |
| `max_password_bytes` | "integer" | `128` |  |
| `max_pending_commands` | "integer" | `1024` |  |
| `max_pending_commands_per_device` | "integer" | `16` |  |
| `max_pending_commands_per_tenant` | "integer" | `128` |  |
| `max_persistent_sessions` | "integer" | `4096` |  |
| `max_persistent_sessions_per_tenant` | "integer" | `512` |  |
| `max_read_buffer_per_connection` | "integer" | `65536` |  |
| `max_replay_entries` | "integer" | `1024` |  |
| `max_replay_entries_per_device` | "integer" | `2` |  |
| `max_replay_entries_per_tenant` | "integer" | `256` |  |
| `max_retained_bytes` | "integer" | `67108864` |  |
| `max_retained_bytes_per_tenant` | "integer" | `8388608` |  |
| `max_retained_message_bytes` | "integer" | `65536` |  |
| `max_retained_messages` | "integer" | `4096` |  |
| `max_retained_messages_per_tenant` | "integer" | `512` |  |
| `max_routing_filters` | "integer" | `256` |  |
| `max_sinks` | "integer" | `32` |  |
| `max_sinks_per_tenant` | "integer" | `8` |  |
| `max_subscription_filters_per_packet` | "integer" | `16` |  |
| `max_subscriptions` | "integer" | `512` |  |
| `max_subscriptions_per_connection` | "integer" | `2` |  |
| `max_subscriptions_per_device` | "integer" | `64` |  |
| `max_subscriptions_per_session` | "integer" | `32` |  |
| `max_subscriptions_per_tenant` | "integer" | `128` |  |
| `max_tcp_frame_size` | "integer" | `65536` |  |
| `max_topic_bytes` | "integer" | `256` |  |
| `max_topic_depth` | "integer" | `8` |  |
| `max_udp_datagram_size` | "integer" | `1200` |  |
| `max_udp_inflight_datagrams` | "integer" | `64` | UDP datagrams being authenticated or admitted; excess datagrams are dropped. |
| `max_username_bytes` | "integer" | `64` |  |
| `max_will_payload_bytes` | "integer" | `65536` |  |
| `max_write_buffer_per_connection` | "integer" | `65536` |  |
| `messages_per_device_second` | "integer" | `16` |  |
| `messages_per_tenant_second` | "integer" | `128` |  |
| `mqtt_recovery_max_bytes` | "integer" | `202195044` |  |
| `mqtt_session_idle_ttl_ms` | "integer" | `86400000` |  |
| `packet_read_timeout_ms` | "integer" | `30000` |  |
| `presence_ttl_ms` | "integer" | `3600000` | Retention window for disconnected device presence. Active sessions are never evicted. |
| `rate_entries` | "integer" | `1024` |  |
| `replay_ttl_ms` | "integer" | `120000` |  |
| `request_timeout_ms` | "integer" | `15000` |  |
| `requests_per_ip_second` | "integer" | `32` |  |
| `requests_per_second` | "integer" | `512` |  |
| `retry_base_ms` | "integer" | `100` |  |
| `retry_max_ms` | "integer" | `30000` |  |
| `shutdown_drain_timeout_ms` | "integer" | `20000` |  |
| `shutdown_timeout_ms` | "integer" | `30000` |  |
| `sink_delivery_concurrency` | "integer" | `8` |  |
| `sink_max_age_ms` | "integer" | `3600000` |  |
| `sink_max_attempts` | "integer" | `5` |  |
| `sink_queue_max_bytes` | "integer" | `16777216` |  |
| `sink_queue_max_count` | "integer" | `4096` |  |
| `sink_timeout_ms` | "integer" | `5000` |  |
| `spool_max_bytes` | "integer" | `268435456` |  |
| `spool_max_records` | "integer" | `100000` |  |
| `spool_record_max_bytes` | "integer" | `1048576` |  |
| `spool_segment_max_bytes` | "integer" | `67108864` |  |
| `udp_clock_skew_ms` | "integer" | `30000` |  |
| `write_timeout_ms` | "integer" | `10000` |  |

## ManagementAuthConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `api_keys` | "array" | `[]` |  |
| `jwt` | union/object | `null` |  |
| `legacy_static_token_enabled` | ["boolean","null"] | `null` |  |
| `mtls_identities` | "array" | `[]` |  |

## ManagementTlsFiles

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `certificate` | "string" | `required/no default` |  |
| `client_ca` | ["string","null"] | `required/no default` |  |
| `private_key` | "string" | `required/no default` |  |
| `require_client_certificate` | "boolean" | `false` |  |

## MtlsIdentityConfig

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `certificate_sha256` | "string" | `required/no default` |  |
| `global` | "boolean" | `false` |  |
| `resources` | "#/$defs/AdminResourceConfig" | `{"devices":[],"products":[],"tenants":[]}` |  |
| `scopes` | "array" | `required/no default` |  |
| `subject` | "string" | `required/no default` |  |

## Permissions

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `commands` | "boolean" | `required/no default` |  |
| `publish` | "boolean" | `required/no default` |  |

## TlsFiles

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `certificate` | "string" | `required/no default` |  |
| `private_key` | "string" | `required/no default` |  |

## V3Limits

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `heartbeat_ms` | "integer" | `required/no default` |  |
| `initial_connection_window_bytes` | "integer" | `required/no default` |  |
| `initial_stream_window_bytes` | "integer" | `required/no default` |  |
| `max_concurrent_streams` | "integer" | `required/no default` |  |
| `max_frame_payload_bytes` | "integer" | `required/no default` |  |

## V3SendAhead

| Field | Schema type | Default | Description |
| --- | --- | --- | --- |
| `connection_bytes` | "integer" | `required/no default` |  |
| `stream_bytes` | "integer" | `required/no default` |  |
