# MQTT session and planned-restart recovery

## Ownership and lifecycle

The key is `(Authenticated DeviceKey, ClientId)`. Authentication completes before
lookup. A restored record contains identity, ClientId, subscriptions and QoS state,
but never credentials; a reconnecting client must authenticate as the same
DeviceKey. Active attachment has a monotonically increasing generation. Cleanup
from an old connection cannot remove or mutate a newer generation.

CleanSession=1 removes only that key's old state. CleanSession=0 detaches the socket
while preserving the compact `StoredSession`. Disconnected state contains no
socket, TLS object, reader buffer, Tokio task, command receiver, or cancellation
owner. The 24-hour default idle policy is enforced during attach, with global and
tenant count/byte ceilings always providing a hard bound.

## Snapshot contents

`mqtt-runtime.state` contains one JSON snapshot with:

- format version and snapshot/broker generation;
- persistent SessionKey and last-seen time;
- subscription filter and granted QoS;
- bounded offline QoS1/2 messages;
- inbound QoS2 AwaitPubrel records;
- outbound AwaitPuback/AwaitPubrec/AwaitPubcomp records;
- next packet identifier;
- retained topic/payload/QoS/owner state.

Will belongs to the active Network Connection. Planned restart intentionally closes
that connection without publishing Will and does not restore a dead connection's
Will. A client reconnects with a new CONNECT and new Will contract.

## Atomicity and validation

The wire file is:

```text
NBMQ | version u32 | generation u64 | JSON length u32 | JSON | SHA-256
```

Shutdown stops listeners, closes owners, waits for detach, builds one locked broker
snapshot, writes a private temporary file, fsyncs it, atomically renames it, and
fsyncs the directory. Internal state such as an inflight packet and its packet ID is
therefore not split across records. Snapshot/file/record/segment limits are checked
before write and before allocation during read. Restore recomputes all logical byte
counters instead of trusting serialized counters. Unknown version, checksum,
generation, syntax, duplicate-key, or resource-bound failure aborts startup.

The EventBus spool and MQTT snapshot are independent replay-safe responsibilities,
not a general transaction/database. If either required commit fails, planned
shutdown fails.

## Resume behavior

- AwaitPuback: resend QoS1 PUBLISH with the same ID and DUP=1.
- AwaitPubrec: resend QoS2 PUBLISH with the same ID and DUP=1.
- AwaitPubcomp: resend PUBREL using mandatory fixed flags `0010`.
- AwaitPubrel inbound: accept duplicate PUBLISH as the same flow; PUBREL crosses the
  IoT acceptance point once, releases state, and returns PUBCOMP.
- Offline QoS1/2: promote messages into bounded inflight slots as ACKs complete.
- Subscriptions and retained state are restored before readiness.

The reconnecting socket itself is new. Server-generated ClientId is used only for a
CleanSession=1 connection and therefore is not resumable.

## Failure contract

This is planned-restart recovery, not crash durability. SIGKILL, OS crash, or power
loss may lose recent retained updates, sessions, offline messages, QoS state, and
EventBus deliveries that were not in an earlier committed image. Continuous disk
checkpointing was intentionally not added to the hot path.
