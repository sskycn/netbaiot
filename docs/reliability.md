# Reliability and recovery

This page summarizes the runtime's delivery and shutdown guarantees. It does not
promise that an arbitrary process or machine failure is lossless.

## `EventAccepted`

An event crosses `EventAccepted` only after authentication and authorization,
protocol/codec validation, route selection, count and byte admission, atomic
reservation for every required sink, and enqueue to those sinks. Required fanout
is all-or-nothing. A producer receipt marks this boundary; it is not proof of
business processing or database commit.

Required sinks acknowledge explicitly. A configured HTTP webhook uses a 2xx
response. A confirmed TCP/RPC consumer acknowledges the matching `event_id`.
Best-effort sinks may drop under their bounded policy and do not normally prevent
acceptance. Sink queues, concurrency, timeouts, retries, and failure behavior are
independent and bounded.

## Retries and duplicates

Delivery is at-least-once. If a sink processes an event and its acknowledgement is
lost, the gateway may retry the same event. If pending work is replayed after a
planned restart, it keeps the same stable `event_id`. Consumers should make their
business update idempotent by that ID.

One slow sink cannot create an unbounded process backlog. When a required sink is
full, admission is rejected or backpressured within configured limits; required
fanout does not partially commit. Best-effort behavior is intentionally different.

## Graceful shutdown

The planned lifecycle closes readiness and ingress admission, waits for active
admissions, stops new device work, then drains required deliveries. Pending or
inflight-uncertain required work is written to the local restart spool. The process
must remain alive and unready if that durable commit fails while accepted work is
still owned. It exits successfully only after required work is acknowledged or
committed for replay.

The independent MQTT recovery snapshot preserves bounded broker protocol state,
including eligible persistent sessions, retained messages, QoS state, and pending
Wills. Authentication cache and gateway control snapshots are rebuilt after
restart. Business commands and long-term history are not stored by the gateway.

## Limits of recovery

The spool is local, bounded restart recovery, not a database, hot-path queue, or
general event log. A process crash, OS crash, power loss, or hardware failure may
lose work that remained only in memory since the last successful planned snapshot.
MQTT protocol recovery does not promise business exactly-once processing.

More detail: [delivery semantics](delivery-semantics.md),
[EventBus](event-bus.md), [restart spool](restart-spool.md),
[MQTT session recovery](mqtt-session-recovery.md), and
[operations guide](operations-guide.md).
