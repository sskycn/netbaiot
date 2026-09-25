# Business RPC V2 implementation checklist

This checklist maps the initial V2 scope to implementation and evidence. The validation section is updated only with commands actually run.

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| Keep V1 contract and stable sink ID | `apps/netbaiot-server/src/business_stream_v1.rs`, `crates/netbaiot-runtime/src/business_event.rs` | V1 golden JSON test, V1/V2 owner conflict in `apps/netbaiot-server/tests/business_rpc_v2.rs` |
| Versioned V2 wire and bounded method DTOs | `crates/netbaiot-protocol/src/business_rpc.rs` | V2 contract test and `fuzz/fuzz_targets/business_rpc_v2.rs` |
| mTLS principal mapping and loopback token | `crates/netbaiot-transports/src/business_rpc.rs`, server config validation | `mtls_verifies_server_and_maps_exact_client_certificate` |
| Multiplexed single connection and dual roles | transport reader/writer, control worker and event worker | `one_socket_authentication_progresses_while_event_ack_waits` holds ACK while two authentication calls finish, then ACKs; same test opens separate roles |
| Bounded pending, cancellation and lease fencing | `crates/netbaiot-runtime/src/business_rpc.rs` | Out-of-order, cancellation, scope tests; `pending_usage` status |
| Device auth and verifier | `BusinessRpcAuthProvider` in runtime | Single connection E2E sends two real signed UDP datagrams, receives NBA1 ACKs and observes one remote verifier lookup; runtime singleflight/outage/recovery test |
| Full invalidation and reset sync | transport `control_loop`, shared ingress invalidation domain | E2E invalidation deny/recovery and scoped mTLS rejection; existing ingress/MQTT invalidation races; V2 revision-gap race coverage remains to be expanded |
| Offline control grace | `apps/netbaiot-server/src/lib.rs` watcher | E2E 1.5 s grace keeps a live device connected immediately after provider shutdown and disconnects it when grace expires; zero-grace and clock-controlled expiration remain untested |
| EventBus responsibility and manual ACK | `BusinessRpcEventSink`, transport event loop, client delivery | E2E held ACK, V1/V2 conflict and `v1_spooled_required_event_replays_to_v2_with_stable_event_id` subprocess test |
| Four configuration combinations | server `Config::validate` | `business_rpc_auth_and_event_delivery_are_independent` |
| SDK and runnable examples | `crates/netbaiot-client/src/business_rpc.rs`, `examples/business_rpc_v2.rs` | Example `cargo check`; subprocess E2E uses official SDK |
| Low-cardinality metrics | runtime `metrics.rs`, registry render and transport | Fixed series via `/api/v1/metrics`; control/event queue occupancy and pending bytes |
| Resource-safe parser | transport `read_frame` / `write_frame`, SDK read budget | Split, coalesced, truncated, oversized, zero and slow-header tests; V2 fuzz target |

## Validation log

Record final command results in the delivery report. The repository baseline before this task was `afade44563b8f0f075faec356838af543451f3be` on clean `main`. The task branch is `codex/business-rpc-v2`.

The following checks passed on the completed code and tests:

- Stable and Rust 1.88.0: `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`, and `cargo test --locked --workspace --all-features` (using `cargo +1.88.0` for the latter toolchain).
- `python3 tests/mqtt_conformance/run.py --netbaiot-only`: 31/31.
- `cargo +nightly fuzz run business_rpc_v2 -- -runs=10000`: 10,000 inputs without a crash.
- `cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture`: passed (64.51 s).
- `cargo check --locked -q -p netbaiot-client --example business_rpc_v2`: passed.
- `cargo-audit audit --file Cargo.lock --no-fetch` and the same check for `fuzz/Cargo.lock`: passed; the main lock retains the existing `rustls-pemfile` unmaintained advisory.

The first workspace-test attempt in the restricted sandbox failed when an existing CLI smoke test could not bind a local socket (`Operation not permitted`). Repeating the unchanged command with local loopback access passed on both toolchains. The final test-only UDP addition was followed by both complete workspace test runs. MQTT conformance, fuzzing, and the restart soak were run before that test-only addition; production code did not change afterward.
