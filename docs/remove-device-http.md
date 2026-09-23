# Remove Device HTTP/HTTPS Transport

> Historical audit: device-configuration ownership described here was removed later.
> Current behavior and migration: [Remove device configuration](remove-device-config.md).

Baseline: `945fe5e386d623c32e2c7d2d0568fe0c058107ec`.
This was the clean, latest completed local HEAD, newer than the suggested
`bf9c611bc16d1fb9c95b21c939f9ef00e4530ddc`, and already included signed UDP ACK.
Work used `codex/remove-device-http` in the existing checkout; no worktree, reset,
remote update or push was used.

Final implementation: dda8027df0499dfb173f47e43cda08aa02359158.
The following documentation/evidence commit does not change runtime behavior.
The final local merge revision is reported in the task completion message.
Decision: **KEEP** — intended protocol/source cleanup with passing correctness and
resource checks; no throughput improvement claim.

## Result and retained boundaries

Device protocols are now **MQTT 3.1.1/TLS, generic framed TCP/TLS, UDP NBI1/NBA1**.
The single device address still binds TCP and UDP at the same numeric port.
Non-loopback TCP still requires TLS. Port 443 is a deployment choice, not a claim
that the device ingress speaks HTTPS. Loopback development may use plaintext.

Removed:

- HTTP classifier methods and dispatch, device request/auth/body/config handlers,
  and all five `/v1/device/*` paths and public path constants.
- `HttpRole`, `with_http_role`, role-specific service copies, device HTTP request
  slot usage, connection counters and request/response work.
- Public `TransportKind::Http`/`Transport::Http`, `ConnectionCounts.http`.
- Device SDK HTTP client, builder endpoint/timeout/response-limit configuration,
  upload, heartbeat helper, config-pull/config-ACK wrapper, HTTP errors/metrics,
  and two HTTP examples. MQTT state is required, not an optional transport.
- Device HTTP loadgen modes, slow-header device workload, and old four-protocol
  comparison/fairness runners. Their original code remains available at baseline;
  this task does not rerun the retired fairness audit.

Retained:

- Independent `management_http` listener, admin authorization, body/header/response
  bounds, request concurrency and lifecycle. `http.rs` is now `management_http.rs`.
  All existing `/api/v1` health, ready, status, metrics, connections, device
  connection, commands, config, auth/config invalidation, control snapshot, routes
  and drain APIs remain. See the [complete API table](http-api.md).
- `reqwest`, `HttpSink`, `delivery_url`, HTTPS business webhooks, external auth
  provider and confirmed business TCP. Management/client HTTP dependencies remain.
- CONNECT authentication, `AuthenticationRequest::Secret`, codec JSON v1,
  `ConfigAck`, `CommandAck`, `CommandRouter` and live MQTT/TCP command endpoints.
- MQTT broker/session/QoS/retained/Will/recovery implementation, TCP framing/auth/
  receipt code, and UDP NBI1/NBA1 code are unchanged from baseline.

HTTP bytes sent to device ingress, including after a valid TLS handshake, are
classification failures. The connection closes without HTTP 404/410/JSON response
or hidden parser fallback. Management HTTP cannot be reached through device ingress.

The fixed 12-byte probe remains justified by MQTT: 1 fixed-header byte, up to 4
Remaining Length bytes, 2 protocol-name length bytes, 4 name bytes, 1 level byte.
Generic TCP lengths are at most 1 MiB, begin with zero, and require an object/space
payload prefix. MQTT starts with `0x10`; these cases cannot collide. The same
admission-to-TLS/classification/first-packet deadline is retained.

## Resources and shutdown

Management leases retain the same process-global count, per-IP count and byte
ownership without being converted into a device transport. Device status now emits
only `mqtt`, `tcp`, `udp`; management is excluded and sessionless UDP reports no
active connection. The previous shared `http_requests` metric is now explicitly
`management_http_requests` (`netbaiot_management_http_requests_total`).

`max_http_body_size`, header limits, `http_slots`, request and response timeouts
remain for management. Global pre-classification connection ownership, MQTT/TCP
protocol ceilings, device/tenant admission, UDP replay/receipt bounds, EventBus and
sink limits remain. No new queue, task per message, semaphore or hot-path lock was
introduced. Idle connection readers still start small.

Shutdown still closes device admission and shared TCP/UDP first, waits for owned
work, drains/spools accepted required deliveries and commits MQTT recovery, marks
DRAINED, then stops management HTTP. Spool failure retains a live, unready process.
Abrupt crashes may lose accepted in-memory work; no crash durability is claimed.

## Breaking changes and migration

This is an intentional 0.x source/control-API cleanup. Existing Device HTTP clients
stop working; there is no deprecated stub or compatibility listener.

| Removed surface | Migration |
|---|---|
| HTTP telemetry upload / `upload_data` | Standard MQTT QoS1 recommended; framed TCP or authenticated UDP also carry the same codec envelope |
| HTTP heartbeat / SDK `heartbeat` | MQTT `publish(DeviceUplink::new(..., DeviceUplinkKind::Heartbeat(...)), PublishQos::AtLeastOnce)` or UDP NBI1/NBA1 |
| HTTP command ACK | MQTT SDK `ack_command`, or `CommandAck` through MQTT/TCP codec ingress |
| HTTP config ACK / `config().ack` | Publish codec `ConfigAck` through MQTT/TCP after application |
| `GET /v1/device/config`, ETag/304, `config().check`, `DeviceConfigApi`, `ConfigUpdate` | **No equivalent automatic device config pull remains** |
| SDK `http_endpoint`, `request_timeout`, `max_response_bytes`, `TransportNotConfigured`, `DeviceMetrics.http_requests` | Remove those calls/fields; `mqtt_endpoint` is required |
| `Transport::Http`, `ConnectionCounts.http` | Rebuild public clients; transport JSON strings are `mqtt`, `tcp`, `udp`; update status consumers |
| `http_requests` counter | Use `management_http_requests`; business sink ACK metrics are unchanged |

Configuration models and management `POST`/`PUT /api/v1/devices/config` remain.
Existing commands may carry application-defined configuration data to a live
MQTT/TCP session; this requires the application's own command contract. It is not
an automatic configuration download or built-in replacement for the removed GET.
Configuration download/command delivery is distinct from applying a revision and
publishing `ConfigAck`. Offline commands are still rejected, never queued durably.

Device JSON `schema_version=1`, MQTT 3.1.1, generic TCP frames, NBI1/NBA1, and the
confirmed business stream v1 are unchanged. The control status field removal does
not silently bump these independent wire protocols. Focused serialization tests
reject the old `http` transport string and verify exactly three emitted counts.
The crate version remains 0.1.0; no release tag or package publication is created.

## Validation

Rust 1.88.0 commands on the implementation above:

| Command | Result |
|---|---|
| `cargo +1.88.0 fmt --all -- --check` | PASS |
| `cargo +1.88.0 check --workspace --all-targets` | PASS |
| `cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo +1.88.0 test --workspace --all-targets --all-features` | 171 passed, 4 intentionally ignored; includes existing foundation bench target execution |
| `cargo +1.88.0 test --workspace --all-features` | 171 passed, 4 intentionally ignored; doc tests passed |
| `RUSTUP_TOOLCHAIN=1.88.0 python3 tests/mqtt_conformance/run.py --release-gate` | 76/76 PASS, 125/125 normative requirements covered; raw state machines, Mosquitto differential/client and TLS evidence |
| `RUSTUP_TOOLCHAIN=1.88.0 bash scripts/tutorial_smoke.sh` | PASS; README MQTT payload `demo:1` observed at real HTTP business webhook, TCP/NBA1 receipts, live command, invalidation, SDK MQTT → confirmed business TCP, graceful drain |
| `cargo +1.88.0 test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | PASS, 61.83 s, forced-spool/recovery pair plus 12 healthy process generations |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run device_classifier -- -max_total_time=60 -max_len=65537` | PASS, 1,120,415 iterations, 61 s, ASan |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run udp_envelope -- -max_total_time=60 -max_len=1201` | PASS, 673,155 iterations, 61 s, ASan |
| `python3 scripts/perf/dual_host_preflight.py --server target/remove-device-http/after-server --output docs/performance/remove-device-http/config-preflight.json` | 5/5 PASS; loopback, IPv4/IPv6 TLS requirement, required-sink validation |
| `python3 -m unittest discover -s scripts/perf -p 'test_*.py'` | 5/5 PASS; Python AST and shell syntax checks also passed |

Final documentation/comment edits were followed by passing fmt/check/clippy, the
three official-client integration tests, and five performance-tool tests again.

[Preserved logs and gate JSON](performance/remove-device-http/validation/) include
the exact successful evidence. Local socket/process permissions were required for
the integration tests. The SDK smoke latency now starts after local MQTT enqueue;
it is not a server EventAccepted timestamp and is not compared with the historical
HTTP-based SDK measurement.

Coverage includes plaintext/TLS HTTP rejection, management health/ready/status and
config mutation/admin isolation, split/malformed MQTT/TCP prefixes, fixed deadlines,
connection cap/byte RAII cleanup, live commands, MQTT reconnect, bounded SDK command
queue overflow without PUBACK, oversized SDK publish rejection, `ConfigAck`, UDP
ACK authentication/retry/replay/10,000 duplicates, auth cache 10,000-publish invariant,
required-sink rollback/slow-sink isolation, spool failures, stable-ID replay and the
three-event SIGKILL loss-window test. Those restart tests now upload over real TCP.
No core recovery test was deleted to avoid replacing its former HTTP setup.

Fuzz is a short bounded smoke campaign, not a proof of parser security. The four
usual ignored tests were not indiscriminately enabled: only the relevant restart
soak was explicitly run. No multi-hour soak, separate-host capacity, power-loss test,
or new syscall/context-switch profiling was performed.

## Dependencies, artifact size and code surface

Native normal dependency tree (unique package/version pairs, SDK root excluded):
**117 → 95 (-22)**. Direct SDK `reqwest` and now-unused direct `serde` were removed;
`url` became a direct dependency but was already transitive. Removed SDK transitive
packages include Hyper, HTTP/body helpers, reqwest and Tower. Full before/after
`cargo tree -p netbaiot-device-sdk --edges normal --prefix none` output and package
lists are in [build metadata](performance/remove-device-http/after-build.json).
This does not claim those dependencies disappeared from the server/workspace.

| Release artifact, Rust 1.88.0, macOS arm64 | Before bytes | After bytes | Delta |
|---|---:|---:|---:|
| `netbaiot-server` | 9,305,488 | 9,251,664 | -53,824 (-0.58%) |
| `netbaiot-device-sdk` rlib | 429,448 | 306,864 | -122,584 (-28.54%) |
| Current `netbaiot-loadgen` executable | 5,891,040 | 5,853,840 | -37,200 (-0.63%) |

Artifacts were built with the same locked release command, host and toolchain.
Rlib is the SDK's own archive, not the size of a statically linked application or
its full transitive build directory. Sizes do not measure peak build memory or
build time. Implementation commit: **20 files, +425 / -977 lines**. Documentation,
benchmark tooling and raw evidence are additional changes.

## Performance

The machine-readable [results](remove-device-http-results.json), 30 raw run records,
plans, binary hashes and environment files are in
[the evidence directory](performance/remove-device-http/README.md).
Five protocol modes were each measured 3 × 30 seconds before and after, with
5-second warmup and 1-second worker ramp. Same Apple M4/10-core macOS loopback host;
10 server Tokio workers and **4 actual generator workers**. Stream modes offer
45,000 events/s with 32 client workers; UDP offers 80,000/s with 8 client workers.
Payload target is 256 bytes; the bounded client window is 128. Full limits are in
raw records. The required sink is the immediate development AuditSink.

The exact same frozen baseline loadgen executable is used on both sides. It retains
historical HTTP code, but the five plans never invoke it; the current loadgen source
and newly built executable have removed HTTP support. No compiler, tests or other
load run overlapped these performance trials. Trials were serialized before then
after, not randomized/interleaved; host drift remains a limitation.

Medians of three runs (P99 column is the median of per-run P99s):

| Mode | Before ACK/s | After ACK/s | Delta | Before/after receipt P99 ms | After accepted/offered | Before/after CPU cores | Before/after peak RSS KiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| mqtt | 43,687.2 | 43,586.8 | -0.23% | 2.15 / 2.28 | 96.86% | 2.01 / 1.99 | 8,064 / 8,656 |
| tcp | 43,744.8 | 43,445.2 | -0.68% | 2.14 / 2.52 | 96.54% | 1.91 / 1.85 | 8,016 / 8,208 |
| udp | 79,941.7 | 79,930.1 | -0.01% | 8.24 / 12.69 | 99.91% | 1.24 / 1.24 | 5,952 / 5,936 |
| mqtts | 43,733.5 | 43,827.2 | +0.21% | 1.91 / 1.80 | 97.39% | 2.00 / 2.01 | 9,920 / 11,232 |
| tls-tcp | 43,830.5 | 43,823.3 | -0.02% | 1.86 / 1.80 | 97.39% | 1.86 / 1.93 | 10,304 / 9,936 |

Accepted/s counts actual MQTT PUBACK, framed TCP acceptance, or authenticated NBA1,
not UDP sends or socket writes. The denominator is the fixed 30-second measurement
window; the raw final record also includes actual elapsed measurement time. P99 is
for successful receipts only. Stream trials experience bounded admission rejection
and reconnection under this offered load; accepted/attempted and accepted/offered
are both retained. These short windows do not establish a loss-free production
capacity or justify a throughput promotion.

Throughput medians vary from -0.68% to +0.21%; the task is retained for its smaller
protocol/SDK surface and passing correctness/resource checks, not a speed claim.
TCP receipt P99 rises from 2.14 to 2.52 ms; UDP's median rises from 8.24 to 12.69 ms.
UDP run P99 ranges are [1.68, 12.52] ms before and
[11.16, 25.25] ms after. One after run drops 56,452 offered
slots at the bounded client window; every actually attempted datagram in these
UDP runs receives a valid NBA1, with zero no-ACK or invalid-ACK counts. Short,
unrandomized loopback trials cannot attribute the tail-latency variation to this
change or establish a latency SLA. The UDP implementation is unchanged. These
regressions/variations are reported without tuning limits to hide them.

All **30/30** raw runs pass hash/receipt/window verification and finish with zero
events, event bytes, pending required deliveries and device connections. Runtime
tasks and numeric FDs return to their own idle values; every server exits gracefully
without force. Only the MQTT recovery image remains, not pending EventBus spool.
Peak RSS is observed process memory during the window, not per-connection cost or
an upper bound for production.

## Worker metadata correction and residual search

Review found the previous mixed audit's generator count was wrong: its main Tokio
macro explicitly fixes four workers. Requested `TOKIO_WORKER_THREADS=2` was ignored.
The old alleged 2→4 diagnostic therefore varied no actual worker setting; the 4.64%
observed UDP difference cannot support a worker-scaling conclusion. The historical
report now has an [explicit erratum](mixed-ingress-capacity-audit.md#worker-count-erratum-device-http-removal-review).
Raw old measurements are preserved. This task's before records also initially
inherited that incorrect metadata; [correction metadata](performance/remove-device-http/metadata-corrections.json)
records four actual workers on both sides and the common executable hash. After
records report four directly. Only metadata/guard/default-path changes were made to
the harness between treatments; timing, server limits and driver are the same.

Final searches cover `device_http`, `HttpRole`, `http_endpoint`, `/v1/device/`,
`Transport::Http`, `HttpRequests`, old product descriptions and translated variants.
The [file-by-file residual audit](performance/remove-device-http/residual-audit.json)
has no unclassified hits. Remaining hits are categorized as:

- Negative regression inputs: rejected HTTP on device ingress; rejected legacy
  config key; rejected serialized `http` transport. These must remain to test removal.
- `ManagementHttpRequests` and management HTTP code/dependencies: retained control
  plane, not device ingress. Business webhook/provider HTTP is retained too.
- This migration/report and public protocol compatibility notes: describe removal,
  never advertise a working legacy endpoint.
- Historical reports and original benchmark JSON: marked historical, tied to their
  original revisions. They are not current feature or capacity documentation.
- New evidence/tool filenames containing `remove-device-http`: task naming only.

## Known limitations

Device HTTP compatibility and active device config pull are intentionally removed.
UDP remains sessionless and unencrypted; only MQTT/TCP provide live bidirectional
commands. Management remains a separate control plane but shares global connection
and source-IP limits; this is not an absolute reserved capacity guarantee. There is
no new persistence or offline command queue. Abrupt failure retains the documented
bounded in-memory loss window. Measurements are local, short and workload-specific.
