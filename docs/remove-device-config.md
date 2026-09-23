# Remove device configuration ownership

Baseline: `8ec59f37530659d65fe4ea398aba831b156d1d6b` (clean local `main`).
Work branch: `codex/remove-device-config`. Local integration only; no push.
Implementation: `eb20e865b368b14f32bceac580c7215ed1ae2634`.

## Inventory before implementation

| Area | Baseline ownership to remove | Gateway responsibility to retain |
| --- | --- | --- |
| Domain/public protocol | `DeviceConfig`, `DeviceConfigSnapshot`, `ConfigRevision`, `ConfigAck`, `ConfigApplyStatus`, unused `ConfigStatus`; config ACK variants in event, filter and uplink enums | Generic commands/ACKs, Connected/Disconnected types, identities, auth and route types |
| Runtime state | `config.rs` mixes per-device desired payloads, revisions, device invalidation and cache statistics with products/routes | Extract bounded `GatewayControl` in `control.rs` for product codec profiles and revisioned route snapshots; no per-device business state |
| Ingress | `Ingress.config`, `IngressEnvelope.require_config_ack`, specialized ACK validation | Authentication, codec validation, EventAccepted, generic command ACKs; control state shared through `Arc` |
| Management | POST/PUT `/api/v1/devices/config`, POST `/api/v1/config/invalidate`; path constants; status cache count/bytes | Health, readiness, metrics, status, connections, commands, auth invalidation, routes, control snapshots and drain |
| Business client | `NetbaIoTClient::configs`, `Configs::{get_device_config,set_device_config}` | Events, commands, devices, runtime, auth cache and routes; no config invalidation client method existed |
| CLI | `config get/set`, `config_ack` event filter | Status, events, online commands, device connection queries, auth invalidation, drain; no config invalidate CLI existed |
| Device SDK | Protocol enum exposes config ACK; docs recommend publishing it; no remaining dedicated config helper | MQTT connect, publish, commands, ordinary command ACK, reconnect and bounded queues |
| Codec | JSON `kind=config_ack` decoder/validation | Existing schema envelope version 1; telemetry, event, heartbeat, command_ack |
| Startup/control | Server `device_configs`, `ControlSnapshot.devices`, example bootstrap payloads | Server `Config`, credentials, codec profiles, TLS, sinks, auth provider, spool, routing revision |
| Limits | `config_cache_max_entries/bytes` bound both devices and retained control data | Remove old names; explicit control product count/serialized byte ceilings keep gateway state bounded at existing ceilings |
| Metrics | Three internal device cache hit/miss/invalidation counters; status cache usage | No exported ConfigCache metric enum was found; all transport/auth/event metrics remain |
| Tests | Config cache mutation/revision tests, config management/client/CLI flows | Replace with strict rejection and bounded control replacement checks; preserve real MQTT/TCP command/ACK and lifecycle coverage |
| Docs | Both READMEs, AGENTS, client/SDK/protocol/control/architecture/HTTP/integration/resource docs | Explain external desired/reported state and online command delivery; annotate historical audits without rewriting measurements |
| Examples | `netbaiot-client/examples/update_device_config.rs`, development/tutorial JSON | Replace example with an application-defined ordinary command; tutorial remains MQTT/TCP/UDP events plus commands |
| Benchmarks | Foundation config hit/miss microbench; performance generators emit `device_configs: []` | Delete obsolete config bench, keep auth/event/transport measurements; use same frozen loadgen for fresh before/after MQTT/TCP/UDP runs |

`DeviceConnected` and `DeviceDisconnected` were public domain types at this report’s baseline, but baseline
MQTT/TCP session registration/drop do not emit these DeviceEvents. Session leases,
generation fencing and management connection queries remain the actual presence
mechanisms. This task will not add a presence subsystem.

Deleting the config ACK variant also makes old pending config-ACK spool records
incompatible. Upgrade must first drain/acknowledge them using the old binary;
never discard accepted pending work. Spool framing/version and failure behavior
will not change, and no legacy event alias will be retained.

## Responsibility and migration

Business systems now own desired configuration, reported state, revisions/history,
persistence, retries, rollout/rollback and offline reconciliation. NetbaIoT owns
authenticated transport, DeviceEvent ingress, bounded routing, online DeviceCommand
delivery and CommandAck/event transport. No database, outbox or offline command
store was added. UDP stays sessionless NBI1/NBA1 with no downlink.

1. Export business configuration from the old application's bootstrap/control
   source or old management API into the business persistence layer before upgrade.
   The gateway's in-memory state is not a durable source of truth.
2. Stop producing the old config-ACK variant. Drain required deliveries with the
   old binary and consumers, including committed restart records containing it.
   If recovery fails after an attempted upgrade, retain the spool and use the old
   binary to finish; do not delete accepted work. MQTT retained/offline/inflight/Will
   payloads containing the removed uplink kind also require application migration
   before replay. MQTT framing, state machine and recovery format are unchanged.
3. Remove `device_configs` from server JSON and `devices` from control snapshots.
   Even empty old fields fail strict deserialization. Remove old config-cache
   limits; use `control_max_products` and `control_max_bytes` for gateway profiles
   and routes only. Transport/EventBus/auth/connection/spool defaults stay unchanged.
4. Replace config-client persistence calls with your business persistence layer.
   Send changes only through ordinary commands to live MQTT/TCP devices, respecting
   existing command permissions, TTL and queue limits. Offline returns an explicit
   error; the application retains enough state to retry/reconcile later.
5. Consume CommandAck by stable command ID. A queued response is not transport SENT,
   device execution, or configuration convergence. Business code decides when to
   update reported state and whether to retry or roll back.

An application-defined command payload can be:

```json
{"name":"apply_config","arguments":{"revision":42,"sample_interval_seconds":5}}
```

This is an example convention, never a gateway special case. Arguments use the
existing scalar map. There is no special permission, topic, TCP frame, revision
comparison, reconciliation loop or UDP command path. See
[`send_application_command.rs`](../crates/netbaiot-client/examples/send_application_command.rs).

Business reconciliation may start from an application's presence/heartbeat logic
or management connection queries. At this report’s baseline, Connected/Disconnected public types remained, but
current MQTT/TCP register/drop paths **do not emit automatic presence DeviceEvents**.
Do not wait for a built-in DeviceConnected event that this implementation does not
produce. Session generation fencing, connection timestamps and offline command
rejection remain intact. Reconnecting never automatically sends configuration.

## Breaking API and wire changes

- Removed the types, aliases and enum variants in the inventory, including unused
  `ConfigStatus`; removed `Ingress.config` and `require_config_ack`. Gateway control
  is `Ingress.control: Arc<GatewayControl>` in `runtime::control`.
- Removed `POST` and `PUT /api/v1/devices/config` and
  `POST /api/v1/config/invalidate`, and their public path constants. Authenticated
  requests receive 404. GET on the old device-config path was already unsupported.
- Removed `NetbaIoTClient::configs()` and
  `Configs::{get_device_config,set_device_config}`; CLI `config get/set` and the
  config-ACK event filter. No deprecated or empty facade remains.
- Removed status `config_cache_entries/bytes`, startup `device_configs`, and
  `ControlSnapshot.devices`. The SDK no longer exposes a config-ACK uplink variant
  through the public protocol crate; it had no dedicated config helper remaining.
- JSON `kind=config_ack` is rejected. `schema_version=1` and codec version 1 remain:
  this pre-1.0 breaking cleanup leaves the envelope and surviving wire encodings
  unchanged. It is deliberately not wire-compatible with that deleted variant.
  Consumers and producers must upgrade together; no synthetic version 2 is added.

Gateway runtime configuration, products/codecs, routing revisions, authentication
invalidation, management HTTP, generic DeviceCommand/CommandAck and MQTT/TCP/UDP
are retained. `commands=false` still prevents all device commands, including any
application-defined configuration operation.

## Validation scope

The final workspace passes both `cargo +1.88.0 test --workspace --all-targets
--all-features` and `cargo +1.88.0 test --workspace --all-features`: **174 passed,
0 failed, 4 ignored** in each invocation. The all-targets invocation also executes
the existing foundation benchmark; its debug microbenchmark output is not used as
capacity evidence. `fmt`, workspace/all-targets `check` and strict all-feature
Clippy pass.

The real MQTT SDK and raw framed TCP contract test sends an application-defined
`apply_config` command, verifies caller command ID/arguments, receives a successful
CommandAck as a DeviceEvent through the confirmed business stream, and explicitly
ACKs it. TCP repeats after disconnect/reconnect with a newer session generation;
offline commands fail explicitly. The test uses a higher per-IP request allowance
for its many management queries; production defaults are unchanged.

Plaintext and TLS shared-listener tests verify deleted management endpoints return
404, status omits old cache fields, routing mutation works, device credentials do
not authorize management, and MQTT/TCP/UDP ingress still works. Startup rejects
the old empty field; public serialization/codec tests reject the removed kind.
Control tests preserve atomic revision replacement, shared old snapshots, count
and byte rollback, and product/route bounds. Spool regression verifies an old
removed event variant fails recovery without deleting the committed file.

Existing suites cover UDP authenticated reliable ACK/replay/invalidation, auth
cache/provider outages, one provider call for 10,000 publishes, required admission
rollback, slow sink isolation, count/byte release, connection anti-monopoly,
SIGKILL loss, spool failure, same-ID replay, and MQTT session/QoS/Will/recovery.
The external MQTT release gate passes **76/76** checks and **125/125** normative
requirements, including Mosquitto reference/client coverage. Tutorial smoke passes
with real MQTT/TCP/UDP, webhook and confirmed business stream. The explicitly run
60-second restart soak passes in 61.79 seconds; it is not a multi-hour soak.

During development, Clippy found one stale test import; new tests initially assumed
an absent command TTL and applied one command-name assertion too broadly. These
were corrected. The expanded contract test then hit the default loopback request
rate and was given a test-only allowance. A later full workspace run had one
existing QoS2 restart readiness timeout; its independent rerun and the subsequent
full workspace run both passed without production changes. The original timeout
cause was not established (child stderr is discarded by that existing test).
These failed logs remain alongside the successful final logs.

No separate-host capacity test, multi-hour soak, power-loss experiment or syscall
profile is claimed. TLS correctness is covered; TLS throughput is not repeated in
this control-state removal task.

| Actual command | Final result |
| --- | --- |
| `cargo +1.88.0 fmt --all -- --check` | PASS |
| `cargo +1.88.0 check --workspace --all-targets` | PASS |
| `cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo +1.88.0 test --workspace --all-targets --all-features` | 174 passed / 0 failed / 4 ignored |
| `cargo +1.88.0 test --workspace --all-features` | 174 passed / 0 failed / 4 ignored |
| `cargo +1.88.0 test -p netbaiot-server --test official_client` | 3 passed |
| `cargo +1.88.0 test -p netbaiot-server --test server subprocess_mqtt_qos2_resumes_outbound_and_inbound_restart_stages -- --nocapture` | 1 passed |
| `RUSTUP_TOOLCHAIN=1.88.0 python3 tests/mqtt_conformance/run.py --release-gate` | 76/76 passed; 125/125 requirements |
| `RUSTUP_TOOLCHAIN=1.88.0 bash scripts/tutorial_smoke.sh` | PASS |
| `cargo +1.88.0 test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | 1 passed, 61.79 s |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run json_codec -- -max_total_time=60 -max_len=65537` | 8,530,825 executions; no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run restart_spool -- -max_total_time=60 -max_len=1048576` | Initial 2,838,735; seeded rerun 2,266,290 executions; no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run device_classifier -- -max_total_time=60 -max_len=65537` | 1,103,173 executions; no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run udp_envelope -- -max_total_time=60 -max_len=1201` | 670,330 executions; no failure |
| `PYTHONPYCACHEPREFIX=target/remove-device-config/pycache python3 -m unittest discover -s scripts/perf -p 'test_*.py'` | 5 passed |
| `cargo +1.88.0 build --release -p netbaiot-server -p netbaiot-client -p netbaiot-loadgen` | PASS for baseline and candidate |

Each fuzz invocation completed in 61 seconds. The first spool run loaded valid
heartbeat and removed-kind records near its end; the additional seeded run gives
the full window to checksum-valid decode paths. Seed encodings and hashes are in
[`spool-fuzz-seeds.json`](performance/remove-device-config/spool-fuzz-seeds.json).
The four normally ignored tests are the queue-depth probe, restart soak, manual
MQTT recovery size benchmark and manual MQTT route-preflight benchmark. Only the
restart soak was explicitly selected separately in this task. Full command/log
hashes, intermediate failures and conformance output are in the
[validation index](performance/remove-device-config/validation/index.json).

## Residual and dependency audit

The exact symbol/route/wire matches in source, tests and tools are recorded in
[`residual-audit.json`](performance/remove-device-config/residual-audit.json).
All current production declarations and handlers for device business configuration
are gone. Negative tests intentionally use old wire names, status keys, paths and
startup fields. The result verifier asserts removed status keys are absent. Current
protocol and migration docs name rejected inputs to explain the breaking change.
Annotated historical Markdown and historical raw benchmark JSON/logs retain their
original evidence, including old cache fields in baseline status samples.

The protocol crate now needs `serde_json` only as a dev dependency. No database,
external broker, runtime dependency or replacement configuration client was added.
There was no exported ConfigCache metric enum to remove: the deleted metrics were
the cache's private hit/miss/invalidation counters and its two management gauges.

## Code reduction

The implementation commit changes **66 files, +777 / -880 lines** (net -103),
including current docs, migration notices in historical reports and test tooling.
Rust sources/tests/examples/benchmarks account for **23 files, +547 / -762 lines**
(net -215). `config.rs` was deleted; `control.rs` retains only bounded gateway
profiles/routes and focused replacement tests. Measurement artifacts and this
report are a separate evidence commit and are not counted as runtime code savings.

Including the report, raw measurements, validation logs and verifier, the complete
task changes 129 files, +19,690 / -880 lines. The added evidence is not production code.

## Binary and dependencies

Both versions were built with Rust 1.88.0 and the same command/package selection,
then copied to immutable task-local paths before measurement. Hashes and dependency
trees are stored with the [raw evidence](performance/remove-device-config/).

| Artifact / graph | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Release server bytes | 8,495,776 | 8,375,536 | -120,240 (-1.42%) |
| netbaiot-client rlib bytes | 565,184 | 554,440 | -10,744 (-1.90%) |
| Client unique normal dependencies, excluding root | 103 | 103 | 0 |

Dependency counts deduplicate `cargo tree --edges normal --prefix none` entries;
dev dependencies are excluded. Server/client artifacts are not standalone runtime
memory measurements. The protocol crate's direct JSON dependency moved to tests,
but the client still needs JSON through its existing HTTP/event implementations.

[`resource-defaults.json`](performance/remove-device-config/resource-defaults.json)
compares the complete `--print-default-limits` outputs: only the two control-limit
names differ. The resource fixture matches all its supplied values. Its pre-existing
omission of `presence_ttl_ms=3600000` is recorded, not silently described as a complete
field-for-field fixture. Product count and total control bytes remain bounded at
4,096 and 16 MiB; none of that byte ceiling is preallocated.

## Known limitations

- NetbaIoT no longer stores or serves desired device configuration.
- Device configuration persistence and offline reconciliation are entirely external.
- DeviceCommand remains online-only; a socket write does not prove execution.
- UDP remains sessionless and has no downlink.
- Business applications must retain enough state to retry/reconcile changes.
- Built-in Connected/Disconnected event production is absent, as it was at baseline.
- Upgrading with unprocessed legacy config-ACK recovery work is unsupported; finish
  it with the old binary first, preserving ownership and consumer acknowledgements.

Devices send events. Business systems send commands. NetbaIoT moves them safely
and quickly.

## Performance and memory

Same-host Apple M4 / 16 GiB / macOS loopback, three 30-second measurement windows
per protocol and version, five-second warmup and one-second ramp, 256-byte payloads.
MQTT/TCP offer 45,000/s with 32 clients; UDP offers 80,000/s with eight workers and
a bounded 128-packet window. Server runtime has ten workers; the exact same frozen
loadgen has four workers in both treatments. No compilation or tests ran alongside
measurement. The sink is the immediate required AuditSink; actual webhook and
confirmed stream semantics are covered by integration tests, not this throughput
comparison. All before trials precede all after trials, without randomization.

| Protocol | Before accepted/s | After accepted/s | Delta | p95 ms before → after | p99 ms before → after |
| --- | ---: | ---: | ---: | --- | --- |
| MQTT | 43,657.1 | 43,726.7 | +0.16% | 1.36 → 1.31 | 2.25 → 2.19 |
| TCP | 43,660.8 | 43,727.1 | +0.15% | 1.36 → 1.35 | 2.39 → 2.32 |
| UDP | 79,289.6 | 79,830.0 | +0.68% | 12.93 → 10.02 | 14.29 → 12.31 |

Cells are medians of runs, not pooled latency percentiles. Latency covers successful
receipts only. Stream overload causes disconnect/unconfirmed work; UDP's client
window can shed scheduled sends. These are not loss-free capacity measurements.

| Protocol | Accepted/offered % before → after | CPU cores before → after | Idle RSS KiB before → after | Peak sampled RSS KiB before → after |
| --- | --- | --- | --- | --- |
| MQTT | 97.02 → 97.17 | 1.867 → 1.936 | 4,816 → 4,896 | 8,512 → 8,096 |
| TCP | 97.02 → 97.17 | 1.779 → 1.882 | 4,816 → 4,896 | 8,064 → 8,240 |
| UDP | 99.11 → 99.79 | 1.233 → 1.243 | 4,720 → 4,768 | 6,112 → 5,936 |

There is no evident accepted-rate or tail-latency regression in this sample. CPU
medians increased slightly: MQTT by about 0.069 core and TCP by about 0.103 core.
Baseline CPU ranges were 1.668–2.001 and 1.742–1.899 cores; candidate ranges were
1.843–1.972 and 1.838–1.908. With N=3, sequential treatments and a shared host, no
statistical improvement or CPU-saving claim is made. RSS does not consistently
fall. Idle fixtures already had zero business device configs before removal; the
old status counted one gateway product profile. One-second process samples are
not exact allocation measurements, and the deleted cache budget was not reserved RSS.

All **18/18** measured runs exited gracefully and returned event count/bytes,
pending required work and device connection counts to zero, with task/FD counts
back at their idle values. Server/loadgen artifact hashes match the frozen inputs.
One additional baseline UDP attempt failed before readiness and never started load;
it is preserved and excluded. The harness now reserves the same device port for
both TCP and UDP before handoff (a TCP-only reservation cannot exclude unrelated
UDP use); the original bind failure's exact cause was not instrumented. All three
UDP measurements were rerun under the paired reservation. The OS handoff race is
reduced, not eliminated.

[Machine-readable results](remove-device-config-results.json) contain individual
runs, ranges, counters, hashes, failed-start evidence and cleanup checks. Reproduce
using the saved plans and artifact revisions with `device_protocol_benchmark.py`;
run `python3 scripts/perf/device_config_summary.py` to verify and regenerate the
aggregate. Full commands are in the validation index.

## Decision

**KEEP.** Device configuration ownership is removed across runtime, domain, API,
startup, clients, CLI, codec and current docs. Bounded gateway control survives
without per-device business state. The required checks, existing transport/restart
behavior and generic command/ACK paths pass. Throughput and successful-receipt
tail latency do not show an evident regression in the measured scope.

Implementation and evidence are integrated locally into `main`; the task branch
is deleted after integration. Nothing is pushed.

Follow-up: the dead connection event variants are now removed; see
[connection event cleanup](connection-events-spool-upgrade-cleanup.md). Legacy
ConfigAck spool failures now have a typed diagnostic and an explicit
[pre-upgrade drain procedure](restart-spool.md#legacy-configack-restart-spool-compatibility).
