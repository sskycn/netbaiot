# Design philosophy

NetbaIoT is an ingress gateway and real-time event router. It connects devices to
business systems that already own application data and workflows.

## Why no runtime database?

The gateway is not the owner of historical telemetry, customer records, analytics,
or workflow state. Keeping those responsibilities in business systems lets each
application choose its own storage, retention, and processing model. NetbaIoT does
not need PostgreSQL, Redis, or a general-purpose message store on its normal event
path.

The only runtime persistence is local restart recovery. It records accepted work
that remains pending when a planned shutdown cannot drain it. It is bounded and is
not a hot-path queue or event history. See [restart recovery](restart-spool.md).

## Why memory-first?

Connection state, authentication cache entries, gateway routing state, MQTT broker
state, and event delivery queues are held in memory during normal operation. This
keeps the data path direct and makes resource ownership visible. It also means the
gateway must state plainly which state is recoverable and which is not. A restart
spool does not turn all memory state into durable state.

## Business systems own durable state

Applications should own:

- durable business data and its retention policy;
- command intent, offline command queues, and command history;
- domain idempotency and workflow state;
- desired/reported device configuration and reconciliation;
- analytics, dashboards, and business-specific alerting.

The gateway can route an online `DeviceCommand` to a connected MQTT/TCP device and
route a `CommandAck` back as a `DeviceEvent`. It does not retain commands for an
offline device or decide whether application state has converged.

## Why bound every resource?

Every queue, cache, connection pool, packet buffer, session store, retry lane,
subscription index, replay window, retained store, command queue, and recovery file
needs explicit count and byte limits. A count limit alone does not constrain
variable-sized payloads. Waiting work also consumes capacity, so waiting tasks and
pending admissions must be bounded too.

When a bound is reached, the system must reject, delay within a bounded budget, or
apply the configured best-effort drop policy. Hiding overload in unbounded queues
or tasks only postpones the failure and makes it harder to isolate a slow sink.

## Why not exactly-once?

An event may be delivered more than once when an acknowledgement is lost, a sink
times out after processing, or accepted work is replayed after a planned restart.
The gateway preserves `event_id` across retry and recovery. Consumers should
deduplicate that ID in the same transaction as their business update. This is
at-least-once delivery, not an exactly-once transaction across the gateway and a
customer database.

MQTT QoS2 is an MQTT protocol handshake. It does not change the business delivery
contract into exactly-once processing.

## Planned restart recovery vs crash durability

During a planned stop, the lifecycle closes ingress, waits for active admission,
drains required deliveries, and commits remaining required work to a bounded,
checksummed local spool when needed. Commit includes file sync, atomic rename, and
directory sync where supported. A successful planned shutdown does not discard
accepted required work.

An abrupt process exit, operating-system crash, power loss, or hardware failure can
lose bounded traffic and MQTT state that had not reached a committed snapshot.
NetbaIoT does not claim crash durability. Auth cache and gateway control snapshots
are rebuilt after restart and are never written to the spool.
