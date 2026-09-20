# Graceful restart

The explicit lifecycle is `STARTING`, `RUNNING`, `QUIESCING`, `DRAINING`, optional
`SPOOLING`, then `DRAINED`. Readiness is true only in `RUNNING`; liveness remains true
until process exit.

`begin_admission` first checks `RUNNING`, increments an active guard, then checks
again. Quiesce atomically closes the state and waits for every pre-existing guard.
Therefore work racing with shutdown either completes the `EventAccepted` boundary
and is tracked, or fails without a success acknowledgement.

Quiesce rejects new device events and commands and stops listeners/connections.
MQTT connection owners detach without publishing Will because this is an intentional
server shutdown. After all owners detach, one consistent broker snapshot records
persistent sessions, subscriptions, offline messages, inbound/outbound QoS state,
packet allocator position, and retained messages. The snapshot must commit before a
successful exit.
Required sink workers continue during the drain window. If pending required count
reaches zero, the process exits without a new spool. Otherwise workers are stopped,
so inflight uncertainty remains pending, and the exact pending responsibility is
committed to the restart spool before successful exit.

Spool failure makes `run` return an error; it never logs `shutdown complete` and the
process does not claim graceful success. A supervisor may still force-kill it, which
falls under the explicitly lossy abnormal-crash contract.

At startup the MQTT broker first validates/restores its snapshot, then EventBus spool
segments are restored with their original IDs, sinks/listeners are initialized, and
only then readiness is enabled. Recovered EventBus segment files are removed only
after required work drains. MQTT reconnect still authenticates before session
resume; restored state never contains credentials.
