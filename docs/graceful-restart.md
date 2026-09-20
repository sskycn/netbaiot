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
packet allocator position, and retained messages. The snapshot must commit before a
successful exit.
Required sink workers continue during the drain window. If pending required count
reaches zero, the process exits without a new spool. Otherwise the process enters
`SPOOLING` while workers retain ownership and may still recover. Only after the exact
pending responsibility is durably committed are workers stopped and graceful exit
allowed.

An MQTT snapshot or EventBus spool failure blocks voluntary shutdown. The process
stays alive and unready, keeps accepted in-memory responsibility, leaves the
management health/readiness surface available, and retries at a bounded cadence.
Repairing storage or restoring the required sink lets shutdown finish. An external
SIGKILL while blocked remains part of the explicitly lossy abnormal-crash contract.

At startup the MQTT broker first validates/restores its snapshot, then EventBus spool
segments are restored with their original IDs, sinks/listeners are initialized, and
only then readiness is enabled. Recovered EventBus segment files are removed only
after required work drains. MQTT reconnect still authenticates before session
resume; restored state never contains credentials.
