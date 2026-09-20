# Pure EventBus database-free refactor implementation report

This report is the acceptance record for the database-free gateway refactor. It
describes the current source tree, not the retired PostgreSQL design. Detailed
protocol evidence is also available in the
[MQTT implementation report](mqtt-3.1.1-implementation-report.md), while the
authoritative default limits remain in
[resource-limits.json](../configs/resource-limits.json).

## 1. Baseline commit

The requested baseline was `ca06963eeb5291b5ae7c165ff2c86570242c1ea4`.
Development continued on top of it without resetting newer work. The final SHA is
reported by the task response because a commit cannot contain its own SHA.

## 2. Final architecture

The runtime is a memory-first, database-free IoT gateway:

```text
HTTP / embedded MQTT / TCP / UDP
  -> bounded transport framing
  -> authentication or bound AuthContext
  -> versioned DeviceCodec
  -> transport-neutral DeviceEvent
  -> atomic bounded EventBus routing
  -> independently bounded confirmed/best-effort sinks
```

The reverse path is business command -> live local session -> bounded connection
queue -> MQTT/TCP device. Transport, codec, runtime routing, business egress,
commands, and planned-restart recovery are separate modules. There is no database,
filesystem, remote-auth, or remote-config operation in the normal MQTT/TCP message
hot path.

## 3. PostgreSQL/database code removed

The `netbaiot-storage` implementation, PostgreSQL and memory repositories,
`runtime/store.rs`, durable worker/outbox code, SQL migrations, PostgreSQL smoke
tests, query-plan tests, SQL load tooling, database cleanup, persistent command
state, and database quota/reservation paths were deleted. A source and manifest
search finds no runtime `sqlx`, PostgreSQL, SQLite, Redis, RocksDB, Kafka, NATS,
RabbitMQ, LMDB, or sled dependency.

The only filesystem persistence is the narrowly scoped local restart spool and the
MQTT planned-restart snapshot. Neither is a normal-path event database.

## 4. Runtime dependencies removed/added

Removed runtime dependencies include SQLx and its PostgreSQL transitive graph.
The refactor uses existing Tokio, `bytes`, serde, `thiserror`, `tracing`, SHA-256,
HMAC, rustls, Hyper, and Reqwest building blocks. No storage engine or external
broker dependency was added. Public protocol/client/device-SDK crates were added
later without introducing server/runtime coupling into `netbaiot-protocol`.

## 5. DeviceEvent model

`DeviceEvent` contains a stable `event_id`, source message ID, strong `DeviceKey`,
receive/occurrence timestamps, and a typed `DeviceEventKind`. Kinds include
telemetry, heartbeat, device event, presence, configuration ACK, and command ACK.
MQTT packets/topics, HTTP headers, TCP sockets, and UDP state do not enter the
business event model. Large/shared event ownership uses `Arc<DeviceEvent>`.

## 6. EventAccepted exact definition

`EventAccepted` is reached only after authentication, authorization, protocol
validation, codec decode, route selection, global count/byte admission, atomic
reservation for every required sink, and enqueue of every required responsibility.
It returns the stable event ID, acceptance timestamp, and required/best-effort
delivery counts. Before this boundary the producer owns retry; after it, a planned
successful exit must either observe every required ACK or fsync the responsibility
to the restart spool.

## 7. Required vs best-effort sink semantics

`ConfirmedRequired` sinks participate in atomic admission, require an explicit
application ACK, retry within bounded policy, and are covered by graceful-restart
recovery. `BestEffort` sinks have independent limits and may drop with metrics; they
do not falsely block or revoke an already valid required acceptance. A socket write
alone is never a confirmed ACK.

## 8. EventBus/router design

The EventBus is a facade over route matching, one shared active-event record,
per-sink queues, and fixed worker sets. Routes and sink definitions are validated
before installation. Sink IDs are held in ordered maps/sets, giving deterministic
reservation order. One event is shared across fanout rather than cloned per sink.
There is no unbounded global broadcast queue and no task spawned per event/retry.

## 9. Resource admission design

Global active events and every sink queue are charged by count and serialized byte
estimate. Required fanout is checked as one locked transaction before any enqueue;
failure leaves no partial delivery. Permits/accounting are released on ACK,
terminal best-effort outcome, enqueue failure, recovery completion, or shutdown.
Fanout, sinks, routes, retries, worker concurrency, waiters, and all externally
controlled frame/body sizes have hard caps.

## 10. Auth cache design

`AuthCache` is separate from configuration. Its key stores credential ID, a SHA-256
credential/message fingerprint, and authentication mode rather than retaining raw
passwords as ordinary keys. Values are positive identities or short-lived negative
results. The cache tracks entry and estimated-byte usage, maintains eviction order,
publishes low-cardinality metrics, and never logs secrets.

MQTT and TCP bind the returned `Arc<AuthenticatedDevice>` once. Subsequent packets
and frames reuse it without calling the provider.

## 11. Auth cache TTL/capacity/invalidation

Defaults are 4,096 entries, 4 MiB estimated bytes, 256 miss waiters, five-minute
positive TTL, and five-second negative TTL. Expiry and capacity eviction release
state. Explicit invalidation supports all/device/product/tenant and credential or
auth-generation fences as represented by the public invalidation model; affected
live sessions are disconnected rather than remotely reauthenticated per packet.

## 12. Auth provider single-flight

The first miss owns the provider future. Identical concurrent misses subscribe to a
bounded watch completion and retry local lookup after the leader completes. The
waiter semaphore and authentication deadline bound waiting work. Provider timeout,
overload, or unavailability is not cached as authorization success and cache misses
fail closed. Static development and bounded external HTTP providers are supported.
External auth requires HTTPS except HTTP on a literal/hostname loopback address,
refuses redirects, bypasses ambient proxies, caps response bytes, has a timeout,
and limits concurrency.

The invariant test performs one successful authentication followed by 10,000 MQTT
publishes and observes exactly one provider call. The same bound-context property is
also tested for active TCP frames.

## 13. Config cache design

`ConfigCache` owns an immutable `Arc<SnapshotIndex>` behind a short replacement
lock. Products and device configurations are shared by `Arc`; lookup does not clone
payloads. Snapshots are count/byte bounded, revisioned, fully validated before swap,
and support device invalidation and serialized route replacement. Defaults are
4,096 product+device entries and 16 MiB.

## 14. Control-plane bootstrap

The external control plane is authoritative for credentials, products, device
configuration, routes, and sinks. Startup validates the configured static
`ControlSnapshot`, creates sinks/routes/caches, restores committed recovery state,
and only then marks lifecycle `RUNNING`. Runtime snapshot mutation is revisioned and
atomic. Auth/config caches are intentionally cold/rebuilt after restart and are not
spooled. This revision supports static snapshot bootstrap plus external HTTP auth;
a continuously streaming remote configuration client is not implemented.

## 15. HTTP device API

The separately bound device listener provides:

| Endpoint | Contract |
|---|---|
| `POST /v1/device/data` | `202` only after EventAccepted |
| `GET /v1/device/config` | revision/ETag response or `304` |
| `POST /v1/device/config/ack` | typed ConfigAck event |
| `POST /v1/device/heartbeat` | typed heartbeat event |
| `POST /v1/device/commands/ack` | typed command-result event |

Headers, header count, body, concurrency, deadlines, and content encoding are
bounded. Upload success means bounded gateway acceptance, not business persistence.

## 16. HTTP management API

The independently authorized management listener provides health, readiness,
status, metrics, paginated connections, one-device connection/config operations,
live command send, auth/config invalidation, atomic control snapshot and route
replacement, and drain under `/api/v1`. Device credentials cannot authorize it.
Connection listing is paginated and capped at 256; device lookup uses the complete
strong `DeviceKey` rather than an ambiguous raw device ID.

## 17. Embedded MQTT behavior

NetbaIoT implements MQTT 3.1.1 directly. The current broker supports validated
CONNECT, CleanSession and persistent sessions, exact/`+`/`#` subscriptions, retained
messages, LWT, QoS0/1/2 state machines, packet-ID lifecycle, offline QoS1/2 limits,
topic ACLs, session-generation fencing, keepalive, and planned-restart snapshot
recovery. MQTT 5, shared subscriptions, bridging, `$SYS`, clustering, and multi-node
session migration remain out of scope. See the dedicated MQTT reports for the full
conformance matrix.

## 18. MQTT PUBACK exact meaning

For inbound QoS1, PUBACK is written only after the canonical payload crosses
`EventAccepted`. It means the event and every required responsibility are admitted
to bounded gateway ownership. It does not mean a business database persisted it or
that an application completed processing. After PUBACK, planned shutdown must drain
or spool any still-pending required delivery.

## 19. TCP/UDP behavior

Generic TCP uses four-byte length framing with incremental small buffers, checked
lengths, absolute frame deadlines, split/coalesced-frame support, codec separation,
bound AuthContext, and ACK only after EventAccepted. UDP remains connectionless and
uses a small datagram cap, signature identity, timestamp/sequence replay checks,
and bounded replay tables; it does not manufacture TCP-style sessions. Accepted UDP
events receive the same graceful-restart ownership, while datagrams during process
handover remain deployment-level best effort.

## 20. Business sink protocols

The confirmed webhook sends the full normalized event and an `Idempotency-Key`
equal to `event_id`; configured 2xx is the ACK. Pooling, redirects, timeout,
response-body streaming cap, concurrency, retry, and optional bearer auth are
bounded.

The high-rate streaming option is a versioned four-byte-framed TCP/RPC protocol:
`hello`, `subscribe`, `ready`, `event`, and matching `ack`. It has hard frame and
deadline limits and serial confirmed flow control for one active subscriber. Native
gRPC and WebSocket adapters are not implemented; a future WebSocket without an
application ACK must be best-effort.

## 21. Command live-session semantics

Commands contain a caller-stable `command_id` and route only to the current local
MQTT/TCP generation through count/byte/TTL-bounded queues. An offline device returns
the structured `device_offline`/unavailable result; nothing is stored for later.
Transport sent, device received, and device executed remain distinct. Device ACKs
return as normal events and business systems own durable workflow/retry state.

## 22. Per-connection memory design

Streams start with at most 4 KiB and grow incrementally to the hard packet/frame
limit. An empty buffer retaining more than 16 KiB is replaced after processing, so
a rare large packet does not permanently pin capacity. Idle outbound channels do
not preallocate their byte allowance. Repeated identity/config data is `Arc` shared.
There is one owner task and one FD per active plaintext MQTT/TCP connection in the
measured design; timers are awaited inside that owner rather than implemented as
extra per-connection tasks. Conservative logical admission reserves 512 KiB per
connection against a 128 MiB default global budget; this is a safety ceiling, not
actual RSS.

## 23. Graceful lifecycle

The explicit lifecycle is `STARTING -> RUNNING -> QUIESCING -> DRAINING -> SPOOLING
-> DRAINED -> EXIT`. Quiesce first makes readiness false, closes a race-safe ingress
gate, waits for active admission guards, stops listeners and connection owners, and
prevents new commands/config mutations. Required work drains to its deadline. Zero
pending work exits without an EventBus spool; otherwise shutdown snapshots uncertain
pending responsibility before success.

## 24. Restart spool format

An EventBus segment begins with `NBSP` and big-endian version 1. Each record is
`u32 length | bounded JSON SpoolRecord | SHA-256(payload)`. The record preserves the
complete event and stable ID, only the still-pending sink IDs, routing revision,
acceptance time, and attempt counters. Defaults: 100,000 records, 256 MiB total,
64 MiB per segment, and 1 MiB per record. Decode checks magic/version/length,
checked arithmetic, checksum, JSON, total bytes, and count before allocation/use.

## 25. Spool fsync/commit behavior

Commit creates a private 0600 temp file in a 0700 directory, writes and checks every
bounded record, `sync_all`s the file, atomically renames to `.spool`, then fsyncs the
directory where supported. Existing committed segments count toward global record
and byte budgets. A write, permission, capacity, checksum, fsync, or rename failure
returns a non-successful shutdown; accepted pending work is never silently discarded
to make exit look healthy.

## 26. Recovery semantics

Startup discovers only committed `.spool` segments, validates them as hostile input,
restores the same events and pending sink responsibilities into bounded EventBus
state, and replays. Committed files are removed and the directory synced only after
recovered required work drains. Corruption or an incompatible version fails loudly.
Normal traffic never writes the EventBus spool.

## 27. Duplicate replay semantics

Delivery is at least once. If a sink processes an event but its ACK is lost before
shutdown, the uncertain responsibility is spooled and can replay. The `event_id`
does not change; `delivery_id` for a stream attempt may change. Consumers requiring
durable correctness must deduplicate/idempotently apply by `event_id`.

## 28. SIGKILL loss semantics

SIGKILL, process/OS crash, power loss, or machine loss can discard the bounded
non-spooled memory window. A real child-process SIGKILL test accepted three events
against an unavailable sink and observed no committed spool, making the measured
loss window three accepted events for that scenario. This is intentional and is not
described as graceful-restart failure or crash durability.

## 29. Resource budget table

| Resource | Default bound / behavior |
|---|---:|
| Connections | 256 node / 64 tenant / 32 IP / 2 device |
| Logical connection memory | 512 KiB reservation / 128 MiB global |
| MQTT, HTTP body, TCP frame | 64 KiB each |
| UDP datagram | 1,200 B |
| Active ingress | 16 / 2 MiB; tenant 4; device 1 |
| Ingress waiters | 16 / 2 MiB / 25 ms |
| Auth cache | 4,096 / 4 MiB / 256 waiters |
| Config cache | 4,096 / 16 MiB |
| Sinks/routes/fanout | 32 / 256 / 8 per event |
| Active events | 16,384 / 64 MiB |
| Per-sink delivery | 4,096 / 16 MiB / concurrency 8 |
| Sink policy | 5 s / 5 attempts / one-hour max age |
| Commands | 16 device / 128 tenant / 1,024 global; 16 KiB each |
| Outbound bytes | 256 KiB connection / 2 MiB tenant / 8 MiB global |
| Restart spool | 100,000 / 256 MiB total / 64 MiB segment / 1 MiB record |

The complete MQTT session/offline/retained budgets are in
[resource-budgets.md](resource-budgets.md). Configured ceilings are not presented as
measured capacity.

## 30. Tests

Workspace unit/integration coverage includes hostile parser input, incremental TCP
and MQTT framing, ACL isolation, QoS state, generation races, count/byte admission,
required rollback, cancellation cleanup, slow sink isolation, cache TTL/capacity/
invalidation/single-flight, config snapshot atomicity, HTTP bounds and ETags, UDP
replay, live-only commands, lifecycle gates, spool corruption/capacity/fsync failure,
duplicate replay, real subprocess recovery, and SIGKILL semantics.

The completion pass additionally ran and passed the real external-auth outage test,
the normal multi-generation restart test, and the ignored 60-second restart soak.
Final fmt, clippy, and full workspace test results are recorded in the task response.

## 31. Fuzz results

Nine nightly `cargo fuzz` targets received 1,000 smoke runs each: MQTT fixed header,
Remaining Length, fragmented packet decoder, stateful MQTT broker, TCP frame, UDP
envelope, JSON codec, restart spool, and business stream. The recorded run found no
panic, unchecked allocation failure, integer overflow, or stuck decode. The final
completion pass reruns the restart spool and business stream targets after the last
source changes.

## 32. Connection memory results

Process measurements distinguish actual RSS from the 512 KiB logical reservation:

| Case | Connections | RSS delta/connection | Tasks/connection | FD/connection |
|---|---:|---:|---:|---:|
| MQTT CleanSession plaintext | 1,000 | 19,152.9 B | 1.0 | 1.0 |
| MQTT CleanSession TLS | 1,000 | 27,344.9 B | 1.0 | 1.0 |
| MQTT persistent plaintext | 1,000 | 23,445.5 B | 1.0 | 1.0 |
| MQTT persistent + two subscriptions | 1,000 | 26,230.8 B | 1.0 | 1.0 |
| TCP plaintext | 1,000 | 18,956.3 B | 1.0 | 1.0 |
| TCP plaintext | 3,000 | 18,300.9 B | 1.0 | 1.0 |

For the 3,000 TCP run, base/loaded RSS was 8,352/61,968 KiB and FDs 17/3,017.
TLS, Tokio, allocator capacity, credentials/cache entries, and kernel socket buffers
are included in process RSS and are not falsely attributed as exact Rust heap bytes.

## 33. Performance/load results

The final release-build run requested 10,000 MQTT events/s for 20 seconds through
codec, EventBus, and a confirmed HTTP sink. It published, PUBACKed, and delivered
199,972/199,972/199,972 events with no errors. PUBACK P50/P95/P99 was
0.14/0.30/0.65 ms. Sink end-to-end P50/P95/P99 was 1/5/25 ms. Sampled pending work
was 0-52, RSS settled near 9.5 MiB, CPU was about 53-56% during load, and graceful
server exit succeeded.

Current microbenchmarks measured auth-cache hit P50/P95/P99 at
0.917/1.042/1.084 us, a unique-key miss plus local provider/cache insert at
98.292/139.125/147.792 us, and config-cache hit/miss at 41/42/42 ns. At 10,000
subscriptions, bounded trie lookup was 167/209/250 ns. These are single-host
microbenchmarks, not network or production-capacity claims. Native HTTP upload and
TCP business-stream network latency were functionally tested but not isolated into
separate production-capacity claims.

## 34. Slow sink results

With a 10 ms confirmed webhook at 1,000 events/s for 15 seconds, all 14,999 events
were published, PUBACKed, and ultimately received. Publisher P50/P95/P99 remained
0.12/0.20/0.31 ms. Pending required work reached a sampled peak of 5,359 events
(about 2.14 MiB), stayed inside the explicit queue/global budgets, and drained to
zero during graceful shutdown. Sink P50/P95/P99 was 4,184/8,257/8,639 ms; RSS peaked
near 17.5 MiB. The independent fast/slow required-sink unit test proves the fast
worker progresses while the slow worker is blocked and all accounting returns to
zero after release.

## 35. Control-plane outage results

A real server and real HTTP auth provider test passed this sequence without server
restart: authenticate MQTT device A (provider calls = 1), make the provider return
503, publish from the already-bound A session and receive PUBACK with calls still 1,
attempt new device B and observe fail-closed connection termination (calls = 2),
restore the provider, then authenticate B successfully (calls = 3). Provider
unavailability is not converted into an authorization success or cached indefinitely.

## 36. Repeated restart results

The subprocess no-loss test first forces three accepted events into pending state,
drains to a committed spool, starts a new process, replays the same IDs to a healthy
confirmed sink, and removes the committed segment only after ACK. It then runs three
healthy generations without a spool. All accepted IDs were observed and no healthy
generation left a segment.

The explicit ignored soak extended the healthy phase to 12 generations with a
five-second dwell each (61.68 seconds total including forced recovery). Every
generation accepted a new event, waited for its ID at the sink, drained, exited
successfully, and left no EventBus spool file. Missing accepted IDs: zero.

## 37. Soak results

A separate 60-second steady 5,000 events/s run recorded 299,995 publishes,
PUBACKs, and confirmed sink receipts with bounded/stable queues and RSS. The final
completion pass repeated the 20-second 10K/s load, the 15-second slow-sink drain,
and the 61.68-second repeated-restart soak. These are development-host results;
they establish bounded behavior under the stated runs, not a production SLA.

## 38. Remaining limitations

The architecture is deliberately not crash durable and does not provide exactly
once delivery. Real sockets and command routing are node-local; there is no cluster
consensus, inter-node live-session routing, or automatic blue/green supervisor.
Native gRPC and WebSocket sinks are absent; the framed TCP/RPC stream is the current
high-rate confirmed option. Only static bootstrap/control snapshot APIs and external
HTTP authentication are implemented, not a durable/continuous remote config log.
MQTT retained wildcard replay scans the explicitly bounded retained store. HTTP
upload and TCP sink latency have no isolated standalone performance claim. All
reported performance and RSS numbers are single-host measurements and must be
remeasured with production TLS, credentials, payloads, limits, and business sinks.
