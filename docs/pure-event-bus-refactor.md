# Pure EventBus database-free refactor

Baseline `ca06963eeb5291b5ae7c165ff2c86570242c1ea4` was refactored from a
database-first runtime into a memory-first gateway. PostgreSQL/SQLx, migrations,
storage repositories, durable outbox workers, and database-era performance tooling
were removed from the runtime dependency graph.

The accepted uplink path is now transport framing → bound authentication → codec →
strong `DeviceEvent` → bounded EventBus → independent business sinks. Required sink
admission is atomic across count and byte reservations. `EventAccepted` is returned
only after every required responsibility is reserved and enqueued. Best-effort
queues may drop with metrics; required overload rejects without a false success.

Each sink owns a bounded worker set, timeout, retry count, retry age, exponential
backoff, jitter, and acknowledgement contract. HTTP webhook acknowledgement is a
configured 2xx. The framed TCP/RPC sink requires `ACK event_id`; a socket write is
not business confirmation. Slow/failing sinks are isolated from healthy sinks.

Authentication uses positive/negative TTL, entry/byte capacity, safe fingerprinted
keys, waiter bounds, invalidation generation, and single-flight misses. Active MQTT
and TCP connections bind the authenticated context once; cache expiry affects only
new authentication attempts. The device configuration cache is immutable,
revisioned, count/byte bounded, and atomically replaced.

Commands are explicitly live-only. Session generation fences delayed disconnects,
and count/byte bounded device queues release their accounting on every exit path.
Offline devices return unavailable; there is no hidden business command database.

Lifecycle admission is race-safe. Planned shutdown closes readiness and admission,
stops listeners/owners, drains required deliveries, and commits uncertain work to a
local bounded restart spool if needed. Spool records preserve stable EventId and
pending sink responsibility with version, checked lengths, checksum, private
permissions, file fsync, atomic rename, and directory fsync. A failed commit makes
shutdown fail. Replay is at-least-once and requires idempotent consumers.

The device and management HTTP surfaces use separate listeners and credentials.
HTTP bodies/headers/concurrency/deadlines are bounded. UDP retains replay and
signature defenses without inventing connection semantics. TCP/MQTT use incremental
buffers, whole-frame deadlines, one connection owner, and bounded output.

Measured evidence on the implementation included a 60-second 5,000 event/s soak
with 299,995/299,995 acknowledgements and sink receipts, 10,000 event/s runs with
sub-millisecond PUBACK latency, a 20,000 event/s overload run that reached the hard
queue ceiling and closed publishers without false ACKs, slow-sink backlog and drain,
real multi-generation restart replay, forced spool failure, and an explicit SIGKILL
loss-window test. Current MQTT-specific measurements are recorded in
[mqtt-3.1.1-implementation-report.md](mqtt-3.1.1-implementation-report.md).

The architecture is intentionally not crash durable. SIGKILL, machine failure, or
power loss can lose the bounded in-memory window. No performance result changes that
contract, and no configured limit is presented as measured capacity.
