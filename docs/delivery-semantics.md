# Delivery semantics

## EventAccepted

An event is accepted only after authentication/authorization, protocol and codec
validation, route selection, count/byte admission, and atomic enqueue reservation
for every confirmed-required sink. Before that point the producer remains
responsible for retry. After it, planned shutdown must ACK or restart-spool every
pending required responsibility.

MQTT QoS1 PUBACK, TCP acceptance and signed UDP NBA1 mean exactly this boundary. None means a
business application processed or durably stored the event.

## Normal and restart delivery

Normal operation is memory-first. Required sinks explicitly acknowledge; best-effort
sinks may drop at their bounded overflow/failure policy. Retries preserve `event_id`,
are count/state/rate bounded, use exponential full jitter, and do not spawn one task
per retry. Once the normal attempt/age budget is exhausted, confirmed-required work
moves to a bounded low-frequency degraded retry lane; it stays spoolable and can
recover in-process. Delayed retries are selected by readiness rather than queue
position, so an older backoff cannot block a later ready event.

A planned restart drains required work. Any still-pending or inflight-uncertain
delivery is fsynced into the local restart spool and replays with the same ID. This
provides graceful-restart-safe, at-least-once delivery. Duplicate delivery is
possible, including when business processed an event but its ACK was lost.

If that fsync/atomic commit fails, the process remains alive and unready with worker
and in-memory responsibility intact, and retries at bounded cadence. Planned exit is
not permitted until required work drains or the authoritative snapshot commits.

SIGKILL, process/OS crash, power failure, or hardware failure can lose the bounded
set of accepted events still only in memory. NetbaIoT does not claim crash-durable or
exactly-once delivery. Consumers requiring durable correctness must deduplicate
`event_id` and persist data in the business system.

## Commands

Commands are volatile live-session operations. Admission means a currently
connected local MQTT/TCP session accepted the command into its count-and-byte queue.
An offline device returns unavailable. Transport write (`SENT`), MQTT PUBACK/device
receipt, and device execution ACK are distinct. Execution results return as
`DeviceEventKind::CommandAck` with the same `command_id`.

Dedup receipts report the latest process-local transport state using the existing
`queued`, `dispatching`, `sent`, `received`, `expired`, and `failed` values. An unsent
queued command expires at its effective deadline even when dedup retention is longer;
expiry increments `CommandFailed` once. `Dispatching` starts before the socket write,
`Sent` means the write completed, and MQTT `Received` requires the matching positive
QoS ACK. None proves device execution. TTL cannot expire an already-started transfer.
A pre-admission dispatch failure releases the ID for retry. A later transport failure
keeps the accepted ID/status until retention ends; no automatic retry is introduced.
Late updates from an evicted dispatch cannot change a newer dispatch with the same ID.
No command progress or dedup history is written to recovery. Business applications
retain durable history, idempotency and offline retry decisions.

The public `EventBus::publish` API rejects an EventId already carrying active
responsibility with `Conflict` before fanout or accounting changes. It does not
keep a permanent history: reuse after all owned deliveries finish is permitted.
Custom `DeviceCodec` implementations must generate stable, distinct IDs for their
accepted events; the default JSON codec generates IDs in the gateway, so this
boundary does not imply that ordinary remote devices choose arbitrary EventIds.

Recovery batches are preflighted under the EventBus state mutex before any enqueue.
Every record must have nonempty, unique, existing required sinks; batch/active IDs
must be unique and cumulative global/per-sink count and byte limits must fit.
Accepted times are nonnegative Unix milliseconds. Routing revision is retained as
historical u64 metadata and need not match current routes. Attempt values retain
u32 history (including saturation), with at most `max_sinks` entries; completed or
removed historical sink keys are allowed. Invalid batches leave live state unchanged.
