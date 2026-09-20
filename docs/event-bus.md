# Event bus

`EventBus` is a facade over routing, atomic resource admission, independent sink
queues, bounded workers, acknowledgement tracking, and restart snapshots. There is
no global broadcast channel and no database outbox.

For each `DeviceEvent`, the router builds a deduplicated, sorted `SinkId` set. Under
one short state lock it validates global event count/bytes and every required sink's
count/byte capacity before changing any queue. If one required reservation fails,
nothing is enqueued. Best-effort sinks are then admitted when capacity remains or
dropped with a metric. Fanout and routing-filter counts are hard limited.

Each sink owns a `VecDeque`, byte/count accounting, a notification, and one bounded
runner with at most the configured delivery concurrency. Delivery calls have a
timeout. Retry uses bounded exponential full-jitter scheduling and no per-retry
owner task. Required work that exhausts its retry policy stays owned for explicit
shutdown spooling; best-effort failure releases its resources.

The bus stores one shared `Arc<DeviceEvent>` for fanout. Per-sink accounting charges
the serialized event bytes, while global accounting charges each active event once.
ACK, drop, cancellation, and queue failure paths release accounting deterministically.

`spool_records()` returns only still-required responsibilities: event, stable
`event_id`, pending sink IDs, route revision, accepted time, and attempts. Inflight
delivery without an observed ACK remains pending.
