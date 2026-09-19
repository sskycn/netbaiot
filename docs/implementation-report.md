# NetbaIoT implementation report

This is the first-milestone historical report. The subsequent
[correctness/resource/reliability audit](correctness-resource-reliability-audit.md)
records confirmed defects, fixes, expanded tests, crash injection and measured limits.

The first milestone now has a working shared ingestion and command path with an
embedded MQTT server. This report describes the tested foundation and its limits;
it does not claim production readiness or full MQTT conformance.

## 1. Architecture implemented

All four transports authenticate into a trusted DeviceKey, select a synchronous
versioned codec, and feed one typed DeviceMessage ingress service. Persistence,
business delivery and command routing are transport-independent. See
[architecture.md](architecture.md).

## 2. Workspace / module structure

Six packages: core, codecs, runtime, storage, transports, and the server composition
root. The core has no Tokio/network/database dependency. Codecs do not own sessions,
spawn tasks or call external services. Cargo.lock and a stable toolchain file are
included.

## 3. MQTT broker architecture

NetbaIoT owns the listener, incremental parser, explicit state machine, authentication,
clean sessions, exact-topic subscription registry, ingress routing and downlink.
One connection task owns the stream and bounded protocol state. No external broker
or MQTT broker crate is used.

## 4. MQTT protocol features actually supported

MQTT 3.1.1 CONNECT/CONNACK, PUBLISH QoS0/1, PUBACK, SUBSCRIBE/SUBACK,
UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP and DISCONNECT. Exact authorized topics,
CleanSession=1, nonzero keepalive, partial packets and pipelined packets are covered.
MQTT5 is explicitly refused. See [mqtt.md](mqtt.md) for exact refusal behavior.

## 5. HTTP/TCP/UDP status

HTTP provides ingestion, command pull, execution ACK, authenticated metrics and a
separately authorized business command route. Generic TCP uses a replaceable
length-prefix framer and supports ingestion/receipts/downlink. UDP accepts small
HMAC-authenticated uplinks with replay protection and has no session, response or
command downlink. All share JSON v1 and ingress.

## 6. Authentication model

An async trait returns server-provisioned identity, credential version, codec and
permissions. The initial provider is an immutable bounded credential configuration;
PostgreSQL provisioning stores device metadata and verifiers. MQTT/TCP/HTTP use
random-key credentials; UDP authenticates its complete envelope with HMAC-SHA256.
Authentication has a timeout. Public stream binds require TLS. Admin credentials
are separate. Runtime database credential lookup/rotation is not implemented.

## 7. Session model

MQTT/TCP sockets remain local. Replacement sessions increment generation and cancel
the old owner. Session and subscription cleanup check generation, including the
explicit delayed-old-disconnect regression. HTTP/UDP retain only bounded last-seen
presence, not fake connected sessions.

## 8. Subscription/topic routing design

An exact-topic hash index permits only a device's own down/up_ack subscriptions and
up/down_ack publications. Identity characters/length are validated independently
of topics. Counts are bounded by connection/device, tenant and node. Wildcards are
unsupported; there is no all-client scan during publish delivery.

## 9. QoS semantics

QoS1 packet identifiers are bounded session state, never durable message IDs.
Uplink PUBACK follows configured acceptance; application up_ack separately carries
the receipt. PostgreSQL receipts follow commit. Neither means downstream business
completion. QoS2 and retained publication are rejected rather than acknowledged
with invented semantics.

## 10. Command/downlink semantics

A common DeviceCommand is stored before routing. Active MQTT/TCP and HTTP pull share
bounded leases, retries, expiry and ownership checks. Sent, Received and device
execution are separate. MQTT PUBACK leaves execution Unknown. Execution ACKs update
state atomically with ingress. Device-side command-ID deduplication is required.

## 11. Persistence and deduplication semantics

PostgreSQL message + outbox + optional command ACK changes commit together. Same
application source ID/content returns the original receipt; differing content
conflicts. Deduplication expires after 24 hours by default. Outbox claims use
SKIP LOCKED, owner/attempt-protected leases, TTL, finite attempts and jittered retry.
A volatile adapter supports tests/development and labels every receipt accordingly.
See [delivery-semantics.md](delivery-semantics.md).

## 12. Resource budget table

| Resource | Default |
|---|---|
| Connections | 256 node / 64 tenant / 32 IP / 2 device |
| Stream packet/body | 64 KiB; headers 8 KiB / 32 |
| UDP packet | 1200 bytes |
| Network reservation | 512 KiB/connection, 128 MiB global |
| Ingress | 16 global / 4 tenant / 1 device; 2 MiB bytes |
| Outbound | 32 items, 256 KiB/connection; 2 MiB/tenant; 8 MiB/global |
| QoS1 IDs | 32/connection |
| Subscriptions | 2/device; 128/tenant; 512 global |
| Commands | 16/device; 128/tenant; 1024 global; 16 KiB each |
| Charged ingress storage | 2 MiB/device; 16 MiB/tenant; 128 MiB/global |
| Replay | 2 boots/device; 256/tenant; 1024 global; 120 s TTL |
| External operation / shutdown | 5 s / 30 s |

The complete count, byte, timeout and retention table is in
[resource-budgets.md](resource-budgets.md). All 71 settings are exposed in
[resource-limits.json](../configs/resource-limits.json). These are defaults, not a
measured production capacity.

## 13. Backpressure behavior

Admission rejects rather than accumulating arbitrary futures. HTTP reports bounded
errors; MQTT/TCP close when necessary; UDP silently drops. Command queue saturation
leaves a bounded durable lease for retry. QoS in-flight state holds permits until
ACK/disconnect. Storage quotas include terminal retained records. The resource
budget document maps producer, consumer, capacities, overflow and shutdown for every
transition.

## 14. Security protections

Bounded lengths and checked arithmetic, strict UTF-8/identifiers, JSON structure and
field preflight, exact namespace ACLs, constant-time key verification, UDP HMAC,
timestamp/replay windows, connection/rate quotas, incomplete-read deadlines, TLS,
separate admin authorization and sanitized error/log values. Workspace unsafe is
forbidden. This has not received an independent security or transitive dependency
audit.

## 15. Graceful shutdown design

Enter draining, stop listeners/new work, finish bounded admitted operations, cancel
workers, release local sessions/subscriptions/queues, and abort/join remaining owned
tasks by deadline. Unfinished durable leases can recover after restart. SIGINT and
SIGTERM are handled. Integration tests cover admitted ingress drain, worker timeout
and cancellation, connection cleanup, server listener shutdown, and executable
SIGTERM shutdown.

## 16. Tests added

43 passing default workspace tests: core/codec/resource/session/storage/parser tests,
15 real-socket transport scenarios and four server/config/TLS tests. The separately
run PostgreSQL contract passed, including forced outbox failure rollback, leases,
command transitions and concurrent deduplication. The actual executable durable
HTTP/MQTT/business-sink/command smoke script also passed.

## 17. Fuzz targets added

Six: MQTT fixed header, Remaining Length, packet decoder, TCP framing, UDP envelope,
and JSON codec. Each completed 5,000 AddressSanitizer smoke iterations. These are
short smoke runs, not exhaustive fuzzing or a memory-under-load proof.

## 18. Benchmarks added

Seven local release microbenchmarks: MQTT decode/encode, exact topic ACL,
subscription lookup, JSON codec, TCP frame decode and ingress admission. Each records
throughput and P50/P95/P99 across 20,000 iterations. Recorded results and measurement
limits are in [validation.md](validation.md).

## 19. Commands actually run and their results

Fmt, strict workspace Clippy, workspace tests, explicit PostgreSQL contract, server
build, actual executable durable smoke, default-limit generation, six ASan fuzz
smokes and all seven benchmarks passed. See [validation.md](validation.md) for exact
commands and initial sandbox/dependency/loader failures. No GitHub CI run is claimed.
The temporary PostgreSQL test cluster was stopped after testing.

## 20. Known limitations / intentionally unsupported features

- No MQTT5, QoS2, wildcard/shared subscriptions, persistent MQTT sessions, retained
  messages, LWT, MQTT clustering/bridges/WebSockets/MQTT-SN, or external broker.
- UDP is authenticated but unencrypted and uplink-only; its replay bitmap is local
  and resets on restart, with durable source-ID deduplication providing the second
  line of protection within the retention window.
- Static bounded credentials, one codec, one local stream route per device,
  HTTP/1 single-request connections, one delivery worker, and bounded polling for
  commands. No dynamic provisioning API, cluster socket routing or application
  fragmentation.
- PostgreSQL storage admission serializes through an advisory lock. Physical disk,
  WAL, allocator/RSS and kernel/TLS memory need deployment controls and measurement;
  logical byte reservations do not constitute an exact physical-resource guarantee.
- Metrics provide counters, queue gauges and accumulated latency; no latency
  histogram, distributed tracing backend or dashboard is included.
- A subsequent audit ran a short 64-connection RSS/load study and Paho 2.1.0
  interoperability tests. Sustained load/allocation profiling, a broad client
  compatibility matrix, long fuzzing and full conformance/security certification
  remain unperformed. See the focused audit for current evidence and limits.

## Significant file inventory

All implementation files added or completed for this task are listed below. The
user's concurrent AGENTS.md dependency-path update and LICENSE/NOTICE additions
were preserved and are not claimed as agent-authored changes.

- [.github/workflows/ci.yml](../.github/workflows/ci.yml)
- [.gitignore](../.gitignore)
- [Cargo.lock](../Cargo.lock)
- [Cargo.toml](../Cargo.toml)
- [README.md](../README.md)
- [apps/netbaiot-server/Cargo.toml](../apps/netbaiot-server/Cargo.toml)
- [apps/netbaiot-server/src/lib.rs](../apps/netbaiot-server/src/lib.rs)
- [apps/netbaiot-server/src/main.rs](../apps/netbaiot-server/src/main.rs)
- [apps/netbaiot-server/tests/server.rs](../apps/netbaiot-server/tests/server.rs)
- [benches/foundation.rs](../benches/foundation.rs)
- [configs/development.json](../configs/development.json)
- [configs/resource-limits.json](../configs/resource-limits.json)
- [crates/netbaiot-codecs/Cargo.toml](../crates/netbaiot-codecs/Cargo.toml)
- [crates/netbaiot-codecs/src/lib.rs](../crates/netbaiot-codecs/src/lib.rs)
- [crates/netbaiot-core/Cargo.toml](../crates/netbaiot-core/Cargo.toml)
- [crates/netbaiot-core/src/lib.rs](../crates/netbaiot-core/src/lib.rs)
- [crates/netbaiot-runtime/Cargo.toml](../crates/netbaiot-runtime/Cargo.toml)
- [crates/netbaiot-runtime/src/auth.rs](../crates/netbaiot-runtime/src/auth.rs)
- [crates/netbaiot-runtime/src/commands.rs](../crates/netbaiot-runtime/src/commands.rs)
- [crates/netbaiot-runtime/src/ingress.rs](../crates/netbaiot-runtime/src/ingress.rs)
- [crates/netbaiot-runtime/src/lib.rs](../crates/netbaiot-runtime/src/lib.rs)
- [crates/netbaiot-runtime/src/limits.rs](../crates/netbaiot-runtime/src/limits.rs)
- [crates/netbaiot-runtime/src/metrics.rs](../crates/netbaiot-runtime/src/metrics.rs)
- [crates/netbaiot-runtime/src/quota.rs](../crates/netbaiot-runtime/src/quota.rs)
- [crates/netbaiot-runtime/src/sessions.rs](../crates/netbaiot-runtime/src/sessions.rs)
- [crates/netbaiot-runtime/src/store.rs](../crates/netbaiot-runtime/src/store.rs)
- [crates/netbaiot-runtime/src/worker.rs](../crates/netbaiot-runtime/src/worker.rs)
- [crates/netbaiot-storage/Cargo.toml](../crates/netbaiot-storage/Cargo.toml)
- [crates/netbaiot-storage/src/lib.rs](../crates/netbaiot-storage/src/lib.rs)
- [crates/netbaiot-storage/src/memory.rs](../crates/netbaiot-storage/src/memory.rs)
- [crates/netbaiot-storage/src/postgres.rs](../crates/netbaiot-storage/src/postgres.rs)
- [crates/netbaiot-storage/tests/semantics.rs](../crates/netbaiot-storage/tests/semantics.rs)
- [crates/netbaiot-transports/Cargo.toml](../crates/netbaiot-transports/Cargo.toml)
- [crates/netbaiot-transports/src/common.rs](../crates/netbaiot-transports/src/common.rs)
- [crates/netbaiot-transports/src/http.rs](../crates/netbaiot-transports/src/http.rs)
- [crates/netbaiot-transports/src/lib.rs](../crates/netbaiot-transports/src/lib.rs)
- [crates/netbaiot-transports/src/mqtt/mod.rs](../crates/netbaiot-transports/src/mqtt/mod.rs)
- [crates/netbaiot-transports/src/mqtt/packet.rs](../crates/netbaiot-transports/src/mqtt/packet.rs)
- [crates/netbaiot-transports/src/mqtt/topics.rs](../crates/netbaiot-transports/src/mqtt/topics.rs)
- [crates/netbaiot-transports/src/tcp.rs](../crates/netbaiot-transports/src/tcp.rs)
- [crates/netbaiot-transports/src/udp.rs](../crates/netbaiot-transports/src/udp.rs)
- [crates/netbaiot-transports/tests/end_to_end.rs](../crates/netbaiot-transports/tests/end_to_end.rs)
- [docs/architecture.md](../docs/architecture.md)
- [docs/delivery-semantics.md](../docs/delivery-semantics.md)
- [docs/dependencies.md](../docs/dependencies.md)
- [docs/device-protocol.md](../docs/device-protocol.md)
- [docs/implementation-plan.md](../docs/implementation-plan.md)
- [docs/implementation-report.md](../docs/implementation-report.md)
- [docs/mqtt.md](../docs/mqtt.md)
- [docs/resource-budgets.md](../docs/resource-budgets.md)
- [docs/validation.md](../docs/validation.md)
- [fuzz/Cargo.lock](../fuzz/Cargo.lock)
- [fuzz/Cargo.toml](../fuzz/Cargo.toml)
- [fuzz/README.md](../fuzz/README.md)
- [fuzz/fuzz_targets/json_codec.rs](../fuzz/fuzz_targets/json_codec.rs)
- [fuzz/fuzz_targets/mqtt_fixed_header.rs](../fuzz/fuzz_targets/mqtt_fixed_header.rs)
- [fuzz/fuzz_targets/mqtt_packet.rs](../fuzz/fuzz_targets/mqtt_packet.rs)
- [fuzz/fuzz_targets/mqtt_remaining_length.rs](../fuzz/fuzz_targets/mqtt_remaining_length.rs)
- [fuzz/fuzz_targets/tcp_frame.rs](../fuzz/fuzz_targets/tcp_frame.rs)
- [fuzz/fuzz_targets/udp_envelope.rs](../fuzz/fuzz_targets/udp_envelope.rs)
- [migrations/0001_foundation.sql](../migrations/0001_foundation.sql)
- [rust-toolchain.toml](../rust-toolchain.toml)
- [tests/fixtures/README.md](../tests/fixtures/README.md)
- [tests/fixtures/localhost-cert.pem](../tests/fixtures/localhost-cert.pem)
- [tests/fixtures/localhost-key.pem](../tests/fixtures/localhost-key.pem)
- [tests/smoke_postgres.py](../tests/smoke_postgres.py)
