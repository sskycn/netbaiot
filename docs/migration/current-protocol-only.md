# Breaking change: current protocols only

From this cleanup, Business RPC V1 and V2 implementations are removed. Business RPC V3 is the only accepted business stream. MQTT recovery accepts NBMQ v6 only; the independent EventBus spool accepts NBSP v3 only. NBMQ v1–v5 and NBSP v1/v2 readers, JSON recovery, migration paths and historical fixtures are removed. Old/unknown headers fail with `UnsupportedRecoveryVersion(version)` or protocol rejection/connection close. Current framing, payload bytes, checksum/trailer, permissions and resource limits remain in force. MQTT 3.1.1/5.0 and management `/api/v1` remain supported.

## Clients and configuration

Upgrade business clients to `BusinessRpcV3Client` or the current `NetbaIoTClient` event façade before deploying the listener. Old public stream/V2 frame/client/config types have been removed, without deprecated wrappers. The Python V1 example and V1 capacity harness are removed; use the current Rust examples and RPC loadgen. Old configurations containing `version`, `allow_v1`, `v3`, `v3_send_ahead` or `v3_experiment_socket_send_buffer_bytes` fail under `deny_unknown_fields`. Configure `limits`, `send_ahead` and `experiment_socket_send_buffer_bytes` instead. Enabling `business_tcp` requires explicit `business_rpc`; omitting it no longer enables an old protocol. Replace `NETBAIOT_BUSINESS_STREAM_TOKEN` with the current configured development token environment variable on loopback, or use mTLS with an explicitly mapped principal in production. Management/device credentials never substitute for business credentials.

The current client retains bounded count/byte buffers, manual application ACK, stable EventId replay, epoch fencing, cancellation and command outcome semantics. `StaleRevision` requires Provider reset sync. A low-level readiness wait is a persistent wait; apply application timeout/cancellation. Loadgen no longer exposes V2 topologies or empty V2-only handshake/sync latency fields.

## Recovery upgrade

1. Stop new ingress externally and back up the complete recovery directory and original configuration. Preserve every committed responsibility; do not delete files to bypass validation.
2. Before upgrading, run a suitable previous release that understands the old records and can complete them or write the current formats. Baseline `21a6945` reads supported NBMQ v1–v5 and NBSP v1/v2 state and writes NBMQ v6/NBSP v3. Older removed business event kinds may require an earlier compatible consumer/release to finish the work first.
3. Let required consumers ACK their work and inspect `pending_required` plus committed files using that release. Request planned drain/shutdown and verify durable commit/successful exit. A successful shutdown can still leave pending spooled work; it is not proof of complete business processing. Verify remaining files with the previous release's current-format reader before deploying this version.
4. Deploy current clients/configuration and this gateway using validated NBMQ v6/NBSP v3 files. Reconnect MQTT clients under the correct identity. Publish readiness only after both recovery domains and required setup validate.

The new gateway contains no converter, migration service or automatic downgrade. Unsupported files remain preserved. Do not reinterpret their bytes as the current format. A current NBMQ file's explicit unknown-profile marker cannot resume subscriptions without reauthentication; it does not grant invented codec/authorization provenance or promise seamless replay of older MQTT state.

Rollback requires a release that can read both current file formats, or fully completing responsibilities before switching to an incompatible reader. Preserve the original backup. SIGKILL, power loss and OS crash can lose recent in-memory traffic; this cleanup adds no crash durability, cross-domain transaction or offline command store.
