# MQTT session and planned-restart recovery

## Ownership and lifecycle

The key is `(Authenticated DeviceKey, ClientId)`. Authentication completes before
lookup. A restored record contains identity, ClientId, subscriptions and QoS state,
but never credentials; a reconnecting client must authenticate as the same
DeviceKey. Stored authorization provenance contains only credential version, auth
generation and permissions. A mismatch resets the session. Active attachment has a
monotonically increasing generation; persistent state also has a separate monotonic
session incarnation. Cleanup from an old connection cannot remove a newer
generation, and old async QoS2 work cannot mutate a new incarnation.

CleanSession=1 removes only that key's old state. CleanSession=0 detaches the socket
while preserving the compact `StoredSession`. Disconnected state contains no
socket, TLS object, reader buffer, Tokio task, command receiver, or cancellation
owner. The 24-hour default idle policy is enforced during attach, with global and
tenant count/byte ceilings always providing a hard bound.

## Snapshot contents

`mqtt-runtime.state` contains one compact NBMQ v2 record stream with:

- format version and snapshot/broker generation;
- persistent SessionKey and last-seen time;
- subscription filter and granted QoS;
- bounded offline QoS1/2 messages;
- inbound QoS2 AwaitPubrel and EventAccepted/pending-route records;
- outbound AwaitPuback/AwaitPubrec/AwaitPubcomp records;
- next packet identifier;
- retained topic/payload/QoS/owner state.

Will belongs to the active Network Connection. Before a planned restart snapshots
broker state, closing a live Network Connection publishes its Will unless that
client already sent DISCONNECT. The published message (including retained state or
offline subscriber delivery) is part of the snapshot; the dead connection's Will
itself is not restored. A reconnect creates a new Will contract.

## Atomicity and validation

New wire files are:

```text
NBMQ | version=2 | generation | header SHA-256
record type | checked length | binary payload | record SHA-256
```

Shutdown stops listeners, closes owners, waits for detach, then streams one coherent
lock-held view to a private temporary file, fsyncs it, atomically renames it, and
fsyncs the directory. The encoder allocates at most one bounded record and keeps
binary payloads raw. The decoder reads v2 incrementally and retains read compatibility
with the legacy NBMQ v1 JSON envelope. File and record limits are checked before
allocation. Restore recomputes logical counters and rejects impossible QoS/topic/
packet-ID/order/authorization state instead of trusting serialized counters.

The EventBus spool and MQTT snapshot are independent replay-safe responsibilities,
not a general transaction/database. If either required commit fails, planned
shutdown fails.

## Resume behavior

- AwaitPuback: resend QoS1 PUBLISH with the same ID and DUP=1.
- AwaitPubrec: resend QoS2 PUBLISH with the same ID and DUP=1.
- AwaitPubcomp: resend PUBREL using mandatory fixed flags `0010`.
- AwaitPubrel inbound: accept duplicate PUBLISH as the same flow. PUBREL crosses the
  IoT acceptance point once, persists `EventAccepted`, then atomically consumes the
  retained-capacity reservation, routes subscriber responsibility, and releases the
  packet state before PUBCOMP. A restart from `EventAccepted` skips IoT ingestion and
  resumes broker routing, preventing a duplicate `DeviceEvent`.
- Offline QoS1/2: promote messages into bounded inflight slots as ACKs complete.
- Subscriptions and retained state are restored before readiness.

The reconnecting socket itself is new. Server-generated ClientId is used only for a
CleanSession=1 connection and therefore is not resumable.

## Failure contract

This is planned-restart recovery, not crash durability. SIGKILL, OS crash, or power
loss may lose recent retained updates, sessions, offline messages, QoS state, and
EventBus deliveries that were not in an earlier committed image. Continuous disk
checkpointing was intentionally not added to the hot path.
