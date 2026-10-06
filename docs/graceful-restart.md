# Graceful restart

The explicit lifecycle is `STARTING`, `RUNNING`, `QUIESCING`, `DRAINING`, optional
`SPOOLING`, then `DRAINED`. Readiness is true only in `RUNNING`; liveness remains true
until process exit.

`begin_admission` first checks `RUNNING`, increments an active guard, then checks
again. Quiesce atomically closes the state and waits for every pre-existing guard.
Therefore work racing with shutdown either completes the `EventAccepted` boundary
and is tracked, or fails without a success acknowledgement.

Quiesce rejects new device events and commands and stops listeners/connections.
MQTT connection owners publish their Will unless the client sent MQTT DISCONNECT;
the MQTT 3.1.1 Will contract applies when the server closes the Network Connection.
After all owners detach, one consistent broker snapshot records
persistent sessions, subscriptions, offline messages, inbound/outbound QoS state,
packet allocator position, retained messages, and bounded Wills awaiting delay or subscriber
capacity. The snapshot must commit before a
successful exit.
Required sink workers continue during the drain window. If pending required count
reaches zero, the process exits without a new spool. Otherwise the process enters
`SPOOLING` while workers retain ownership and may still recover. Only after the exact
pending responsibility is durably committed are workers stopped and graceful exit
allowed.

An MQTT snapshot or EventBus spool failure blocks voluntary shutdown. A structural
MQTT recovery error is recorded as critical and is not retried, but it no longer
short-circuits EventBus safety: required events first drain or enter the fsynced
restart spool. The process then stays alive and unready with management available.
Retryable storage failures continue at a bounded cadence. An external SIGKILL while
blocked remains part of the explicitly lossy abnormal-crash contract.

At startup the MQTT broker first validates/restores its snapshot, then EventBus spool
segments are restored with their original IDs, sinks/listeners are initialized, and
only then readiness is enabled. Recovered EventBus segment files are removed only
after required work drains. MQTT reconnect still authenticates before session
resume; restored state never contains credentials.

MQTT recovery writes NBMQ v6 incrementally as bounded typed records plus a final
record-count, byte-count, and whole-stream-digest trailer. It reads v1 through v6;
the larger legacy v1 ceiling is selected only after the prefix identifies v1.
Recovery validates topic/filter syntax, packet identifiers, legal QoS per
state, non-QoS0 offline backlog, retained consistency, ordering, duplicates, and
authorization/codec provenance and session ACL ownership before the replacement
broker state becomes visible.

Control mutations acquire lifecycle admission before their first side effect and
retain it through completion: HTTP auth invalidation, control/routes replacement,
Business RPC V2/V3 auth sync/invalidation, provider-offline revocation, commands and
MQTT/TCP session establishment. Quiesce waits for admitted operations before broker
recovery starts. New mutations return HTTP 503 `draining` / RPC `unavailable`;
command retries also obey this fence. Diagnostic reads remain available and repeated
drain requests remain idempotent. Admission closes before stopping MQTT owners;
existing QoS/Will cleanup finishes before their tasks are joined and snapshot begins.

Startup constructs a paused EventBus and validates both recovery domains, management
credentials, TLS (including business TLS), and all listener bindings before starting
sink workers or publishing Running. Restored events cannot deliver during that
preparation. Existing `EventBus::new` continues to start workers for library callers,
who must call `stop_workers`; composition roots should use `new_paused` followed by
`start_owned_workers`. Its owner joins on normal shutdown and cancels/aborts on Drop.
The gateway also owns detached Tokio handles through abort-on-drop guards and listener
JoinSets. Dropping a startup/run future releases prepared sockets, cancels listeners,
aborts owned workers and releases the directory lock after blocking I/O owners finish.
This emergency cancellation path does not promise graceful drain or spool commit.
