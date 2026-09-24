# MQTT session and planned-restart recovery

## Ownership and lifecycle

The key is `(Authenticated DeviceKey, ClientId)`. Authentication completes before
lookup. A restored record contains identity, ClientId, subscriptions and QoS state,
but never credentials; a reconnecting client must authenticate as the same
DeviceKey. Stored authorization provenance contains only credential version, auth
generation, permissions, codec ID, and codec version. A mismatch resets the
session. Active attachment has a monotonically increasing generation; persistent
state also has a separate monotonic session incarnation. Cleanup from an old
connection cannot remove a newer
generation, and old async QoS2 work cannot mutate a new incarnation.

CleanSession=1 removes only that key's old state. CleanSession=0 detaches the socket
while preserving the compact `StoredSession`. Disconnected state contains no
socket, TLS object, reader buffer, Tokio task, command receiver, or cancellation
owner. The 24-hour default idle policy is enforced during attach, with global and
tenant count/byte ceilings always providing a hard bound.

## Snapshot contents

`mqtt-runtime.state` contains one compact NBMQ v5 record stream with:

- format version and snapshot/broker generation;
- persistent SessionKey and last-seen time;
- subscription filter, granted QoS, and MQTT 5 subscription options;
- bounded offline QoS1/2 messages;
- inbound QoS2 AwaitPubrel and EventAccepted/pending-route records;
- outbound AwaitPuback/AwaitPubrec/AwaitPubcomp records;
- a started-transfer flag for outbound QoS state, so expiry cannot erase an
  exchange after its first PUBLISH transfer begins;
- next packet identifier;
- retained topic/payload/QoS/owner and publisher session origin;
- bounded pending Will responsibilities, including MQTT 5 delay deadline,
  cancellation identity, and message expiry interval;
- protocol version, MQTT 5 session expiry, and bounded publish properties.

Will begins as a bounded responsibility reserved at successful CONNECT. MQTT 5 DISCONNECT reason 0x00 suppresses it. Every other connection end transfers it to broker-owned
publication. If atomic subscriber routing is overloaded, the Will remains in the
bounded pending queue and is retried once when ACK/removal capacity changes; there
is no retry task or busy loop. Pending state is included in planned restart recovery.

## Atomicity and validation

New wire files are:

```text
NBMQ | version=4 | generation | header SHA-256
record type | checked length | binary payload | record SHA-256
NEND | record count | total record bytes | whole-stream SHA-256
```

Shutdown stops listeners, closes owners, waits for detach, then streams one coherent
lock-held view to a private temporary file, fsyncs it, atomically renames it, and
fsyncs the directory. The encoder allocates at most one bounded record, hashes
incrementally, and keeps binary payloads raw. The decoder reads v4/v5 incrementally and
retains read compatibility with NBMQ v2 records and the legacy NBMQ v1 JSON envelope.
v1 uses the immediately previous release's 1,342,177,280-byte read ceiling; v2/v3/v4/v5
use the compact configured ceiling. File and record limits are checked before
allocation. Restore recomputes logical counters and rejects impossible QoS/topic/
packet-ID/order/authorization/codec/ownership state instead of trusting serialized
counters.

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
