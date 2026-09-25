# Business RPC V2 implementation checklist

This checklist maps the initial V2 scope to implementation and evidence. The validation section is updated only with commands actually run.

## Reliability and load gate (task starting at `583bb0d`)

The attachment's reference commit `ef8ee53` precedes the already merged MQTT Device Profile commit; no history was reset. Results below apply to the current task branch. Performance figures are observations, not production limits.

| Invariant | Implementation | Test | Command / evidence | Status |
| --- | --- | --- | --- | --- |
| Stale auth response cannot cross local Device/All invalidation or register a session, even with higher remote revision | `AuthCache` epoch, `Ingress` registration gate, registry revision fence | `local_epoch_rejects_high_revision_response_after_device_and_all_invalidation`; `delayed_business_rpc_auth_cannot_register_after_device_invalidation` | `cargo test --workspace --all-features` | PASS |
| Stale verifier cannot enter cache or accept signed UDP | `AuthCache::verify_signed_with_verifier` epoch | `stale_verifier_cannot_validate_datagram_or_poison_cache` | workspace test; verifier load JSON | PASS |
| A disconnected provider cannot touch a replacement generation | registry lease epoch and `invalidate_if_offline` lock boundary | `stale_lease_cleanup_and_response_cannot_touch_reconnected_provider`; `offline_invalidation_and_new_serving_share_one_generation_boundary` | workspace test | PASS |
| Gap requires full reset; duplicate revision is idempotent | control worker gap branch, full ingress/MQTT invalidation, SDK reset sync | `one_socket_authentication_progresses_while_event_ack_waits` | `cargo test -p netbaiot-server --test business_rpc_v2` | PASS |
| Zero grace revokes live session immediately; finite deadline, collapsed status updates and reconnect are fenced | timestamped watch status, monotonic deadline, registry state lock | paused-time server tests, including `collapsed_serving_disconnect_starts_grace_at_actual_disconnect`; `zero_offline_grace_revokes_live_session_and_requires_reset_sync` | workspace test | PASS |
| Pending count/bytes and queue permits recover under overload, timeout, cancellation, revision advance and late response | registry RAII guard and `BusinessRpcUsage` | `queue_and_byte_overload_release_all_admission_permits`; `syncing_and_revision_advance_reclaim_pending_and_fence_late_responses`; existing timeout/cancellation tests | workspace test | PASS |
| SDK shutdown joins owned tasks, old ACK/handler response does not cross generation, reconnect stays bounded | SDK connected cleanup and old writer ownership | `one_hundred_reconnect_generations_and_shutdown_release_driver`; `old_delivery_ack_cannot_use_replacement_writer`; `old_auth_handler_is_cancelled_before_replacement_writer_is_ready` | workspace test | PASS |
| Slow EventAck does not stop same-socket auth | independent event/control/auth workers | existing E2E test; multiplexed load | workspace test; `docs/performance/business-rpc-v2/multiplexed_soak.json` | PASS |
| Real-network load gate covers auth, multiplexed, reconnect, verifier and consumer outage | `tools/netbaiot-loadgen/src/bin/business_rpc.rs` | five bounded scenarios | `cargo run -p netbaiot-loadgen --bin business_rpc -- <config.json>`; JSON evidence directory | MEASURED |
| Five-minute reconnect and 60-second multiplexed soaks finish with zero pending and active business connections | registry usage metrics, SDK owned-task shutdown | 300-generation soak; slow ACK with periodic reconnect | `docs/performance/business-rpc-v2/reconnect_soak.json`; `docs/performance/business-rpc-v2/multiplexed_soak.json` | MEASURED |
| At configured auth inflight 2, overload stays explicit and pending count/bytes recover | registry count/byte admission, fixed overload counter | near-capacity and saturation loads followed by successful auth | `docs/performance/business-rpc-v2/near_capacity.json`; `docs/performance/business-rpc-v2/saturation.json`; `docs/performance/business-rpc-v2/auth.json` | PASS |

Detailed semantics, commands, environment, limits and measured results: `docs/business-rpc-v2-reliability.zh-CN.md`.

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| Keep V1 contract and stable sink ID | `apps/netbaiot-server/src/business_stream_v1.rs`, `crates/netbaiot-runtime/src/business_event.rs` | V1 golden JSON test, V1/V2 owner conflict in `apps/netbaiot-server/tests/business_rpc_v2.rs` |
| Versioned V2 wire and bounded method DTOs | `crates/netbaiot-protocol/src/business_rpc.rs` | V2 contract test and `fuzz/fuzz_targets/business_rpc_v2.rs` |
| mTLS principal mapping and loopback token | `crates/netbaiot-transports/src/business_rpc.rs`, server config validation | `mtls_verifies_server_and_maps_exact_client_certificate` |
| Multiplexed single connection and dual roles | transport reader/writer, control worker and event worker | `one_socket_authentication_progresses_while_event_ack_waits` holds ACK while two authentication calls finish, then ACKs; same test opens separate roles |
| Bounded pending, cancellation and lease fencing | `crates/netbaiot-runtime/src/business_rpc.rs` | Out-of-order, cancellation, scope tests; `pending_usage` status |
| Device auth and verifier | `BusinessRpcAuthProvider` in runtime | Single connection E2E sends two real signed UDP datagrams, receives NBA1 ACKs and observes one remote verifier lookup; runtime singleflight/outage/recovery test |
| Full invalidation and reset sync | transport `control_loop`, shared ingress invalidation domain | E2E invalidation deny/recovery, duplicate and gap recovery, and scoped mTLS rejection; existing ingress/MQTT invalidation races |
| Offline control grace | `apps/netbaiot-server/src/lib.rs` watcher | E2E 1.5 s grace, zero-grace live-session invalidation, paused-clock 29,999/30,000 ms and collapsed-notification tests |
| EventBus responsibility and manual ACK | `BusinessRpcEventSink`, transport event loop, client delivery | E2E held ACK, V1/V2 conflict and `v1_spooled_required_event_replays_to_v2_with_stable_event_id` subprocess test |
| Four configuration combinations | server `Config::validate` | `business_rpc_auth_and_event_delivery_are_independent` |
| SDK and runnable examples | `crates/netbaiot-client/src/business_rpc.rs`, `examples/business_rpc_v2.rs` | Example `cargo check`; subprocess E2E uses official SDK |
| Low-cardinality metrics | runtime `metrics.rs`, registry render and transport | Fixed series via `/api/v1/metrics`; control/event queue occupancy and pending bytes |
| Resource-safe parser | transport `read_frame` / `write_frame`, SDK read budget | Split, coalesced, truncated, oversized, zero and slow-header tests; V2 fuzz target |

## Validation log

The first V2 implementation started at `afade44563b8f0f075faec356838af543451f3be` on `codex/business-rpc-v2`. This reliability task started from clean `583bb0dcf486faae9d95192fa37ffc5dce5cb7fc` on `codex/business-rpc-v2-reliability`; final current-task validation is reported in `docs/business-rpc-v2-reliability.zh-CN.md` and the delivery report.

For this reliability task, final stable and Rust 1.88.0 format/clippy/workspace tests passed. MQTT conformance passed 31/31, the V2 fuzz target ran 10,000 inputs without a crash, and the ignored 60-second subprocess restart soak passed in 63.86 seconds. The final gateway also completed a 307.27-second / 300-generation reconnect soak, a 66.76-second multiplexed slow-ACK/reconnect run, a consumer outage/retry run, and near-limit/above-limit auth saturation runs. Exact commands, measurements and limits are in `docs/business-rpc-v2-reliability.zh-CN.md`.

The following checks passed for the earlier V2 implementation:

- Stable and Rust 1.88.0: `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`, and `cargo test --locked --workspace --all-features` (using `cargo +1.88.0` for the latter toolchain).
- `python3 tests/mqtt_conformance/run.py --netbaiot-only`: 31/31.
- `cargo +nightly fuzz run business_rpc_v2 -- -runs=10000`: 10,000 inputs without a crash.
- `cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture`: passed (64.51 s).
- `cargo check --locked -q -p netbaiot-client --example business_rpc_v2`: passed.
- `cargo-audit audit --file Cargo.lock --no-fetch` and the same check for `fuzz/Cargo.lock`: passed; the main lock retains the existing `rustls-pemfile` unmaintained advisory.

The first workspace-test attempt in the restricted sandbox failed when an existing CLI smoke test could not bind a local socket (`Operation not permitted`). Repeating the unchanged command with local loopback access passed on both toolchains. The final test-only UDP addition was followed by both complete workspace test runs. MQTT conformance, fuzzing, and the restart soak were run before that test-only addition; production code did not change afterward.
