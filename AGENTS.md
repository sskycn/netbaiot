# AGENTS.md

## Product

NetbaIoT is a high-performance, low-memory, database-free, event-driven IoT
protocol gateway and real-time event router written in Rust.

Supported device transports are embedded MQTT, generic TCP, and UDP.
NetbaIoT implements MQTT directly; do not introduce an external broker.

Priority order:

> Correctness > Resource Safety > Reliability > Maintainability > Performance

The runtime responsibilities are exactly:

```text
CONNECT -> AUTHENTICATE -> DECODE -> NORMALIZE -> ROUTE -> SEND
```

Business systems own historical telemetry, business data, analytics, workflows,
offline commands, and durable command history. The gateway runtime must not depend
on PostgreSQL, SQLite, Redis, RocksDB, Kafka, NATS, RabbitMQ, LMDB, sled, or another
database/message store.

The only runtime persistence allowed is the local restart recovery spool described
below. It is not a hot-path queue or a general event store.

## Architecture

Keep transport, device protocol, authentication/control state, event routing,
business egress, command routing, and restart recovery strictly separated.

```text
Device
  -> Transport Adapter
  -> Authentication / bound AuthContext
  -> versioned DeviceCodec
  -> DeviceEvent
  -> bounded EventBus / Router
  -> independently bounded business sinks
```

Transport adapters own connections, framing, network I/O, transport lifecycle,
and transport metadata only. Vendor parsing belongs in synchronous, replaceable,
versioned codecs. MQTT packets/topics, raw sockets, and UDP socket
state must not leak into core business events.

All MQTT/TCP/UDP uplinks converge on `DeviceEvent`. Every event has a stable
`event_id`; retries and restart replay must preserve it.

## EventAccepted

One exact boundary applies to every transport. An event is accepted only after:

1. authentication and authorization;
2. protocol and codec validation;
3. route selection;
4. count and byte resource admission;
5. all required sink capacity is reserved atomically; and
6. all required sink deliveries are enqueued.

Required-sink fanout admission is all-or-nothing and uses deterministic `SinkId`
ordering. A QoS1 MQTT PUBACK, TCP acceptance receipt or signed UDP NBA1 means only
that this boundary was crossed. It does not mean business persistence or processing.

Required sinks need explicit acknowledgement. Best-effort sinks may drop according
to their bounded policy and do not normally block acceptance. Every sink has an
independent count/byte queue, concurrency, timeout, retry policy, and failure policy.
One slow sink must not create an unbounded backlog or block unrelated sinks.

At-least-once delivery and possible replay duplicates are expected. Business
consumers must process `event_id` idempotently. Exactly-once is not claimed.

## Authentication and gateway control

MQTT/TCP authenticate once and bind an immutable `Arc<AuthenticatedDevice>` to the
connection. Normal packets/frames must not call an auth or control-plane service.

The auth cache and gateway product/routing control state are separate and bounded
by count and bytes. Device business configuration belongs to external applications.
Auth caching includes positive/negative TTLs, eviction, safe credential fingerprints,
explicit invalidation, and bounded single-flight misses. Raw credentials must never
be logged or used as metric labels. Cache misses fail closed when the provider is
unavailable; valid sessions and unexpired positive entries may continue.

Control-plane snapshots are revisioned, validated completely, and atomically
replaced. Gateway control snapshots are shared through `Arc`; do not clone large
product/routing state per connection. Auth caches and gateway control state are
rebuilt after restart and never written to the restart spool. The gateway must not
store device desired/reported configuration, compare business revisions, or perform
configuration reconciliation. Applications may send configuration as ordinary
DeviceCommand payloads; no special command name, permission or transport is added.

## Embedded MQTT

The supported broker profile is MQTT 3.1.1 with QoS0/1/2, CleanSession 0/1,
persistent sessions, retained messages, LWT, exact/`+`/`#` subscriptions, and
planned-restart recovery. MQTT 5, MQTT-SN, shared subscriptions, and broker
clustering remain out of scope. Do not remove implemented MQTT 3.1.1 behavior as
though it were unsupported.

Preserve incremental parsing, hard Remaining Length limits, strict UTF-8, canonical
topic namespaces, authenticated identity checks, packet deadlines, bounded packet
IDs, and session-generation fencing. Never trust identity parsed from a topic.

## TCP and UDP

TCP is a byte stream: framing must handle split headers/payloads, multiple frames,
oversized/invalid frames, partial disconnects, slow readers, and bounded writes.
Framing remains separate from `DeviceCodec`.

UDP is connectionless. Preserve hard datagram limits, HMAC authentication, timestamp
and replay protection, and spoofing/amplification protections. Do not create a
long-lived UDP session.

## Commands and sessions

Commands route only to a currently connected local MQTT/TCP session. If no live
deliverable session exists, return `DEVICE_OFFLINE`/`Error::Unavailable`. Never
persist or silently queue an offline command. Business systems own retry and offline
storage.

Every command has `command_id`. Keep transport `SENT`, device receipt, and device
execution distinct. A socket write is not execution. Device command ACK/results
return through the normal `DeviceEvent` path.

Command queues are bounded per device/connection, tenant, and process by count and
bytes, with TTL. Old session disconnects must not invalidate a newer generation.
Real sockets and command handles remain node-local; multi-node routing is not solved.

## Bounded resources

Every runtime resource needs count and byte limits where payload size varies:
connections, buffers, admissions, queues, sinks, events, fanout, tasks, commands,
caches, retries, subscriptions, and replay state. Waiting work is itself bounded.

Do not hide overload behind spawned tasks waiting on a semaphore/channel. Do not use
unbounded channels. Do not preallocate maximum packet/frame or outbound queue sizes
per idle connection. Readers start small, grow only to a hard maximum, and should
release abnormally large retained capacity after measured need.

Every task has an owner, lifetime, shutdown path, concurrency bound, and failure
policy. Avoid blocking I/O on Tokio workers and locks across `.await`.

Treat all network/control/spool input as hostile. Validate lengths before allocation
and use checked arithmetic/conversions. Production request paths must not use
`unwrap`, `expect`, `panic!`, `todo!`, or `unimplemented!` for recoverable failures.
Avoid `unsafe`; if unavoidable, document invariants and add targeted tests.

## Graceful lifecycle and restart spool

The lifecycle is:

```text
STARTING -> RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED -> EXIT
```

Quiesce first makes readiness false, closes the ingress admission gate, waits for
active admission guards, stops new connections/uploads/commands/control mutation,
then drains accepted required deliveries.

Before a successful planned exit, every pending required delivery must either:

1. receive its sink acknowledgement; or
2. be written to the local restart spool, fsynced, atomically renamed, and followed
   by a directory fsync where supported.

Spool records preserve the event, stable `event_id`, pending sink IDs, routing
revision, and needed attempt metadata. The format is versioned, length bounded, and
checksummed. Decode all spool lengths as hostile. Spool count, record, segment, and
total bytes are hard limited. Restrict file permissions. Do not delete committed
spool state until the corresponding required work is acknowledged.

Inflight delivery without observed ACK is uncertain and must be spooled; replay may
duplicate it. If spool write, capacity, checksum, fsync, or rename fails while
accepted required work remains, the process must remain alive and unready, retain
ownership, and retry at a bounded cadence. It must not claim a successful graceful
exit until the durable commit succeeds.

Abrupt process/machine/power failure may lose a bounded amount of non-spooled memory
traffic. This is intentional and must be documented honestly; do not claim crash
durability.

## HTTP boundaries

Management HTTP uses an independent listener and admin authorization under
`/api/v1/...`. It is not a device transport. Device ingress classifies MQTT/TCP only
and closes HTTP input without a response or fallback. Device credentials never
authorize management operations.

HTTP bodies, headers, concurrency, response bodies, and deadlines are bounded.
Gateway control snapshots use explicit routing revisions. Device configuration
persistence and offline reconciliation are entirely external. CommandAck reports
device command execution; business applications decide convergence and retries.

Webhook success is a configured 2xx ACK. Confirmed TCP/RPC streams require an
application `ACK event_id`; a socket write alone is not confirmation.

## Security and observability

Preserve protections against malformed/oversized packets, invalid UTF-8, topic
spoofing, cross-device access, connection floods, slowloris, partial-frame attacks,
UDP replay/spoofing, callback amplification, and malformed ACKs. Public stream
transports should use TLS.

Never log passwords, secrets, access tokens, authorization headers, HMAC keys, raw
credentials, or full sensitive spool contents. Metrics use a closed low-cardinality
label vocabulary; device/event/command/client IDs, revisions, and sink URLs belong
in logs/traces, never labels.

## Testing and measurement

Protocol parsers cover valid, truncated, malformed, oversized, boundary, split, and
multi-frame inputs. Arbitrary bytes must not panic or allocate without a checked
bound. Preserve fuzz targets and add them for new spool/business framing decoders.

Tests must cover auth caches and gateway control snapshots, required admission rollback, count/byte cleanup,
slow-sink isolation, live-session command routing, lifecycle gate races, restart
recovery, corruption, spool failure, duplicate replay, and the invariant:

```text
1 successful authentication + 10,000 MQTT publishes = 1 provider call
```

Connection memory, throughput, latency, slow sinks, restart cycles, SIGKILL loss,
and soak behavior require actual measurement. Never present a configured maximum,
microbenchmark, burst, or historical database-era result as current production
capacity.

Before completing changes run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run relevant fuzz, subprocess restart, slow-sink, outage, memory, load, and soak
tests for affected paths. State exactly what was and was not run.

## Public protocol and SDK invariants

`netbaiot-protocol` contains public protocol types only. It must not depend on
Tokio, HTTP clients/servers, the broker, sessions, runtime, or server composition.
Wire protocol versioning is explicit and distinct from crate SemVer. Public API and
wire changes require compatibility review and focused serialization tests.

`netbaiot-client` must not depend on server/runtime internals. `netbaiot-cli` must
use `netbaiot-client` instead of duplicating HTTP or stream implementations.
`netbaiot-device-sdk` is optional and uses standard MQTT; ordinary MQTT
3.1.1 clients remain first-class and must never require the SDK.

Public clients expose structured errors. Tokens and device credentials never appear
in `Debug`, logs, or error messages. Event ACK occurs only after the application
chooses to ACK unless it explicitly requests immediate mode. Duplicate replay is
expected: `event_id` remains stable while `delivery_id` may change.

Client buffers are bounded by count and bytes. The confirmed stream has a bounded
ACK path and never creates a task per event or ACK. Reconnect loops use bounded
backoff, preserve subscription semantics, honor cancellation, and do not create
unlimited queued work. Dropping/shutting down a client must stop its owned tasks.

Command clients preserve caller-supplied `command_id`, do not blindly retry, and
report offline devices explicitly. Business configuration ownership stays external;
CommandAck is distinct from transport SENT and does not itself prove desired/reported
convergence. Client libraries create no hidden runtime, database, or unbounded
offline queue.

## MQTT 3.1.1 invariants

MQTT 3.1.1 is implemented internally. MQTT packet/session/QoS state remains separate
from IoT `DeviceEvent` business state. Persistent sessions, subscriptions, offline
queues, retained messages, Will payloads, packet buffers, and QoS inflight state are
count- and byte-bounded.

Persistent session ownership is `(Authenticated DeviceKey, ClientId)`; ClientId is
never trusted identity. CONNECT authenticates once and binds the AuthContext. No
normal PUBLISH, SUBSCRIBE, UNSUBSCRIBE, or QoS packet may trigger per-message remote
authentication.

QoS2 uses explicit inbound/outbound state. MQTT Packet Identifier is protocol state,
not EventId, and is reusable only after its lifecycle completes. MQTT QoS2 is not a
business exactly-once promise.

Every outbound acknowledgement must match one exact state transition: PUBACK only
completes `AwaitPuback`, PUBREC only advances `AwaitPubrec`, and PUBCOMP only
completes `AwaitPubcomp`. A wrong acknowledgement must not mutate accounting or
packet-ID order. Inbound QoS2 EventAccepted ownership uses an operation token that
survives same-session connection takeover; connection generation alone is not a
valid fence for finishing already accepted work.

Persistent MQTT state also carries a monotonic session incarnation and complete
authorization/profile provenance: credential version, auth generation, permissions,
codec ID, and codec version. CleanSession=1 creates a new incarnation; stale work must match the
session incarnation, packet identifier, and operation token. CleanSession=0 takeover
keeps the incarnation. Any credential version, auth generation, or permission change
resets the old persistent session, and management invalidation removes matching
bounded persistent sessions as well as live sessions.

Authentication invalidation, live-session registration, and the final MQTT broker
attachment share one synchronization boundary. Lock order is auth-registration gate,
AuthCache, Sessions, then MqttBroker; the reverse order is forbidden. A result returned before any device/product/tenant/version/generation/all
invalidation may not register afterward. MQTT attachment and Will responsibility
are RAII-owned across every post-registration early return, including failed
CONNACK writes.

Tenant inflight release must wake bounded pending work for other active sessions in
the tenant; ACK handlers must not scan all sessions. Recovery limits must cover
compact NBMQ v3 records for every admitted legal state. Writes are streaming and
bounded per record and finish with an authoritative record-count/byte-count/whole-stream
digest trailer; NBMQ v1 and v2 remain read-only compatible under separate ceilings. A structural recovery
failure is not retryable, but EventBus required work must still drain or spool before
the process remains alive and unready.

Routing to matching persistent QoS1/QoS2 subscriptions is an atomic admission
decision: preflight every target and retained mutation before the first commit. A
producer acknowledgement is forbidden if any required persistent responsibility
cannot be owned. QoS0 may still be shed under the documented best-effort policy.

Will topic/payload/QoS/retain and retained state are validated, authorized, and
bounded. CONNECT reserves one broker-owned Will responsibility. Subscriber pressure
moves an abnormal-disconnect Will into a bounded pending queue; capacity release retries it
without a task or busy loop, and planned recovery persists it. Only MQTT DISCONNECT suppresses
that Will. Route preflight stores compact per-target decisions and tenant projections; it must
never clone complete session payload state or rescan all sessions once per target. Reconnect must authenticate before restoring state. Abrupt crash may lose
recent in-memory MQTT state. No runtime broker or database is required.

MQTT 3.1.1 behavior changes require conformance regression against raw
state-machine tests and at least one mature external MQTT client. Mosquitto is a
test/reference implementation only and must never become a NetbaIoT runtime
dependency.

## Git completion workflow

Unless the user explicitly requests otherwise, finish repository change tasks by
merging the task branch into `main`, committing the result, and pushing `main` to
`origin` after the required validation passes. Then remove the merged local task
branch and its clean worktree. Preserve unrelated branches and user changes.
