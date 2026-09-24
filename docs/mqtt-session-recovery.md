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

`mqtt-runtime.state` contains one compact NBMQ v6 record stream with:

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
  cancellation identity, message expiry interval, and original publisher SessionKey;
- protocol version, MQTT 5 session expiry, and bounded publish properties.

Will begins as a bounded responsibility reserved at successful CONNECT. MQTT 5 DISCONNECT reason 0x00 suppresses it. Every other connection end transfers it to broker-owned
publication. If atomic subscriber routing is overloaded, the Will remains in the
bounded pending queue and is retried once when ACK/removal capacity changes; there
is no retry task or busy loop. Pending state is included in planned restart recovery.

## Atomicity and validation

New wire files are:

```text
NBMQ | version=6 | generation | header SHA-256
record type | checked length | binary payload | record SHA-256
NEND | record count | total record bytes | whole-stream SHA-256
```

Shutdown stops listeners, closes owners, waits for detach, then streams one coherent
lock-held view to a private temporary file, fsyncs it, atomically renames it, and
fsyncs the directory. The encoder allocates at most one bounded record, hashes
incrementally, and keeps binary payloads raw. The decoder reads v4/v5/v6 incrementally and
retains read compatibility with NBMQ v2 records and the legacy NBMQ v1 JSON envelope.
v1 uses the immediately previous release's 1,342,177,280-byte read ceiling; v2/v3/v4/v5/v6
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

## Upgrade and rollback across NBMQ v6

Before upgrading, finish a planned shutdown and retain a verified copy of the
committed v5 snapshot together with its EventBus restart spool. Start the v6
binary with the original recovery directory. It reads v1–v5 and writes v6 on
the next successful planned shutdown. Keep the v5 copy separate: the v6 file
is the authoritative state after the new binary has run.

A v1–v5 pending Will has no explicit publisher origin field. For a delayed Will,
the reader uses the recorded `cancel_on_resume` SessionKey as its origin. For an
immediate pending Will without that key, origin stays unknown (`None`); the
reader does not invent a ClientId. A legacy immediate Will can therefore be
forwarded to a matching No Local subscription after recovery. v6 records retain
the origin explicitly, including when a recovered state is written again.

NBMQ v2 also lacks codec authorization provenance. A v2 session can be read and
its QoS state carried into a v6 image, but the v6 writer marks that profile as
unknown rather than inventing a codec ID/version. On the next authenticated
reconnect, the existing conservative profile check resets that session; do not
promise seamless resume of such legacy QoS exchanges. v3 and later carry codec
provenance and retain their normal matching-profile resume behavior.

An older binary rejects v6 rather than silently interpreting it. Direct
downgrade with a v6 snapshot is unsupported; there is no v6-to-v5 converter that
preserves the new Will-origin semantics. If the upgraded process has made **no**
broker or business state change at all, an operator may evaluate restoring a
verified pre-upgrade v5 snapshot and its paired EventBus spool in an isolated
copy before restarting the old binary. This requires checking timers, Will
publication, accepted events, subscriptions, ACKs, and retained changes, not
merely checking that no client is currently connected. If the new version has
processed business or protocol state, the old snapshot is stale and is **not**
a lossless rollback. Preserve the v6 state and roll forward or use a separately
validated migration procedure. Never delete or edit the v6 recovery file to
make the old binary start.
