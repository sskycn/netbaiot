# Implementation plan

The initial repository contains only AGENTS.md. This task explicitly selects an
embedded MQTT server and overrides the preference for an external broker.

## Boundaries and implementation order

1. `core`: validated domain identifiers, typed payloads, commands, receipts,
   synchronous codec interface. No network, database, or Tokio dependency.
2. `codecs`: bounded JSON v1 decoding and encoding; immutable versioned registry.
3. `runtime`: validated limits, authentication and storage interfaces, bounded
   ingress admission, command routing, generation-protected local sessions,
   connection/rate quotas, delivery worker, and low-cardinality metrics.
4. `storage`: PostgreSQL transactions and a bounded non-durable test store.
5. `transports`: HTTP, embedded MQTT 3.1.1, independent length-prefixed TCP,
   authenticated connectionless UDP. Shared authentication/codec/ingress path.
6. `server`: configuration, TLS, PostgreSQL, listeners, and shutdown composition.

Implement and compile in those layers, then add real socket integration tests,
parser property/fuzz smoke tests, benchmarks, and operational documentation.

## Ownership and lifecycle

One server-owned JoinSet supervises listeners/workers. Each stream listener owns
a bounded JoinSet of connections. One task owns each MQTT/TCP stream and all of
its protocol state. No task is spawned per packet. Cancellation stops admission;
bounded active work may finish before connection cleanup. A hard shutdown
deadline aborts and joins remaining tasks. RAII releases quotas and sessions.
HTTP/UDP record last-seen metadata without creating connected sessions.

## Resource design

Start conservatively: 256 global connections, 32/IP, 64/tenant; 64 KiB stream
packets/bodies, 1200-byte UDP datagrams; 16 ingress operations; 32 queued messages
and 256 KiB/connection, additionally bounded globally; 16 commands/device,
128/tenant, 1024 globally. Bound credentials/devices, subscriptions, replay and
storage by both configured capacities and retention. All limits are centrally
validated. Reject overload rather than queueing unknown amounts of work.
Reserve stream memory before spawning connection tasks. Enforce per-device and
per-tenant admission as well as node admission.

## MQTT state machine

Accepted -> AwaitConnect -> Authenticating -> Connected -> Draining -> Closed.
Only CONNECT can leave AwaitConnect. Authentication creates the trusted identity.
Second CONNECT, malformed packets, unsupported QoS/retain, and ACL violations
close the connection; failed subscriptions receive SUBACK failure. MQTT 5 gets
an unsupported-version response. CleanSession=1 only; no persistent sessions,
retained messages, LWT, wildcard subscriptions, or QoS2 in this milestone.
Exact canonical topic indexes avoid scanning connected clients. Generation
tokens ensure delayed cleanup cannot remove replacement sessions.

## Acceptance and command boundaries

PostgreSQL atomically inserts normalized message and delivery job. A stable
application source ID deduplicates within a documented retention period; a
conflicting payload is rejected. Application receipts follow commit. PUBACK is
protocol progress, not business execution. Command transport states and execution
states are separate. Persist commands before routing; offline commands expire
within bounded capacities. Delivery leases use bounded batches, timeouts, TTL,
bounded attempts, exponential backoff and jitter; no transaction spans delivery.

## Verification

Unit/property tests cover parsers, quotas, codecs, ACLs, packet IDs, session races,
deduplication, replay, command state and deadlines. Real socket tests cover all
four transports, MQTT lifecycle/downlink and shutdown. PostgreSQL integration
tests require a dedicated test database and will report explicitly if unavailable.
Run fmt, clippy, workspace tests, fuzz smoke tests and decoder/admission benches.

## Dependency decisions

There are no existing dependencies. Use Tokio/bytes for bounded async I/O,
serde/serde_json for bounded JSON, thiserror for errors, tracing for diagnostics,
UUID for application IDs, async-trait for object-safe runtime ports, SQLx for
PostgreSQL transactions/pooling, Hyper for HTTP parsing, Rustls for TLS, and
HMAC/SHA-256 for high-entropy credential verification and UDP authentication.
Implement the small MQTT packet subset internally: no MQTT broker or packet crate
is needed, and broker/session/routing semantics remain entirely in this repository.
Dependency details and limitations will be recorded after resolving the lockfile.
