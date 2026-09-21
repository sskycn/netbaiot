# MQTT 3.1.1 implementation report

## 1. Baseline commit

`b337c1127d1640b32f0518e38e016872dede6653`.

## 2. Final commit

The final commit is the local commit containing this report; its SHA is recorded in
the task response because a commit cannot embed its own content-derived SHA.

## 3. MQTT architecture

TCP/TLS feeds a bounded incremental packet codec, connection state machine,
connection-level authentication, generation-fenced `MqttBroker`, session store,
subscription trie, retained store, explicit QoS state, and then the IoT binding.
MQTT state stops at that boundary; normalized `DeviceEvent` and commands contain no
MQTT packet identifiers or protocol states.

## 4. Packet decoder design

`BytesMut` begins at 4 KiB, handles arbitrary fragmentation/coalescing, and does not
reserve from Remaining Length. Fixed flags, canonical one-to-four-byte Remaining
Length, total maximum, checked arithmetic, MQTT UTF-8, topic/filter syntax, packet
IDs and packet-specific bodies are validated before split/allocation. Whole-packet
deadline remains independent of keepalive.

## 5. CONNECT/auth model

CONNECT validates protocol name/level, all flag combinations, ClientId, keepalive,
Will, username and password. Authentication uses AuthCache once, then binds an
`Arc<AuthenticatedDevice>`. A real connection with 10,000 QoS1 publishes observed
exactly one provider call.

## 6. Session key/ownership model

`SessionKey = (Authenticated DeviceKey, ClientId)`. Lookup occurs only after
authentication. Same ClientId under another DeviceKey creates a distinct session
and cannot evict, resume, clean, or recover the first identity's state.

## 7. CleanSession=1 behavior

The authenticated key's old stored session is removed, Session Present is zero,
state is connection-lifetime only, and detach deletes it. Empty ClientId is accepted
only here and receives a generated ephemeral ID.

## 8. CleanSession=0 behavior

Socket detach preserves bounded subscriptions, offline QoS messages, inbound QoS2,
outbound QoS1/2, packet allocator and last-seen metadata. It retains no socket, TLS
state, task, parser buffer or business history. Default idle policy is 24 hours.

## 9. Session Present behavior

It is one only when an actual resumable session for the authenticated key existed.
New, CleanSession=1, expired/removed and cross-identity ClientId cases return zero.
Unit, Paho reconnect and real subprocess restart tests cover this.

Connection ownership is an `Attachment` RAII guard. If the success CONNACK cannot
be written, Drop clears active ownership exactly once. CleanSession=1 removes its
temporary stored state; CleanSession=0 preserves resumable state but not a live
owner. A real duplex write-failure regression verifies the next reconnect observes
Session Present 0 and 1 respectively.

## 10. Subscription router

A level trie supports insertion, replacement, removal/pruning, exact children, `+`
and `#`. Lookup does not scan every subscription. Duplicate matches merge by
SessionKey and retain the maximum granted subscription QoS.

## 11. Wildcard semantics

`+` and `#` must occupy whole levels; `#` is final. Publish Topic Names reject both.
Root `#`/`+` do not match `$...`; explicit `$` filters do. `$SYS` service topics are
not exposed. Filters are authorized only below the bound device root.

## 12. Retained store

RETAIN stores/replaces the exact topic, zero payload deletes, existing subscribers
receive RETAIN=0 and future matching subscribers receive RETAIN=1. Exact lookup is
hash based. Wildcard replay scans the hard-bounded retained store; this is the main
known router scaling limitation.

## 13. LWT

Will Topic, binary payload, QoS0/1/2, retain, size and ACL are validated at CONNECT.
EOF/error/timeout/replacement and planned server shutdown publish once. Only an MQTT
DISCONNECT deletes the Will without publication. Raw socket tests validate abnormal,
takeover, normal-DISCONNECT, and planned-shutdown/restart behavior; Mosquitto CLI
validates QoS0/1/2 retained Wills.

An authenticated CONNECT reserves retained capacity before the success CONNACK and
then arms a `WillGuard`. Abnormal completion, including CONNACK write failure,
publishes through Drop; MQTT DISCONNECT explicitly suppresses it. Publication and
reservation release occur under one broker lock, so later retained pressure cannot
discard an accepted Will. The optional IoT binding happens afterward and cannot
duplicate or suppress MQTT delivery.

## 14. Inbound QoS0

The bound ACL is checked, canonical IoT payload crosses codec/EventBus admission,
and broker routing follows without PUBACK. Overload closes/sheds according to the
bounded subsystem that rejected work.

## 15. Inbound QoS1

PUBACK is written only after `EventAccepted`; packet ID is session protocol state,
not EventId. DUP may yield at-least-once IoT delivery as MQTT QoS1 permits.

## 16. Inbound QoS2

PUBLISH stores `AwaitPubrel` and returns PUBREC. Duplicate identical PUBLISH returns
PUBREC without another application delivery. PUBREL crosses EventAccepted once,
fences/removes state, routes once and returns PUBCOMP; duplicate PUBREL returns
PUBCOMP without a second DeviceEvent. Recovery preserves AwaitPubrel.

PUBREL first claims a bounded operation token and changes the stored state to
`Delivering`. EventAccepted completion is fenced by that token, not by connection
generation, so a same-session takeover cannot strand accepted work. Restore maps an
interrupted Delivering state back to AwaitPubrel; EventAccepted remains routable by
the replacement session.

## 17. Outbound QoS0

Active delivery has no packet ID or inflight record. A full active sender sheds QoS0
rather than growing memory or an offline queue.

## 18. Outbound QoS1

The allocator assigns 1..65535, stores AwaitPuback, sends PUBLISH and releases bytes
on PUBACK. Persistent reconnect resends the same ID with DUP=1.

## 19. Outbound QoS2

AwaitPubrec sends PUBLISH; PUBREC moves to AwaitPubcomp and sends PUBREL; PUBCOMP
releases state. Duplicate PUBREC resends PUBREL. Reconnect before PUBREC resends
PUBLISH with DUP; reconnect before PUBCOMP resends PUBREL.

Completion is an exhaustive state×ACK match: PUBACK completes only AwaitPuback,
PUBREC advances only AwaitPubrec, and PUBCOMP completes only AwaitPubcomp. Wrong
PUBACK/PUBCOMP leave the map, order, byte accounting, and packet identifier intact.

## 20. Packet Identifier lifecycle

Zero is rejected. Allocation is bounded to a 65,535 search and skips all active
outbound IDs. ID reuse occurs only after PUBACK/PUBCOMP release. Snapshot preserves
the next allocator value and every active ID.

## 21. DUP handling

Initial outgoing PUBLISH has DUP=0; recovered QoS1/QoS2 PUBLISH has DUP=1 with the
same ID. PUBREL always uses MQTT-required flags `0010`; retransmission is represented
by state rather than setting an illegal fixed-header bit.

## 22. Offline message behavior

Disconnected CleanSession=0 subscriptions queue eligible QoS1/2 only. Reconnect
promotes messages into available per-session and per-tenant inflight slots. The
business command API remains live-only and returns `DEVICE_OFFLINE`.

When a tenant inflight ceiling queues work for another active session, a bounded
deduplicated per-tenant ready queue records that session. PUBACK/PUBCOMP release
wakes eligible work directly through the existing bounded connection channel; it
does not scan all sessions and creates no task per message.

## 23. Session resource bounds

Defaults: 4,096 sessions/node, 512/tenant, 32 subscriptions/session, 64/device,
128/tenant, 512/node, QoS1/QoS2 32/session and 4,096/tenant each, 2 MiB/session,
32 MiB/tenant, 128 MiB/node, 24-hour idle policy. Startup rejects inconsistent
hierarchies.

## 24. Retained resource bounds

Defaults: 4,096 messages and 64 MiB/node; 512 and 8 MiB/tenant; 64 KiB per message.
Replacement accounts delta; delete releases bytes.

## 25. Per-tenant bounds

Tenant fences cover connections, sessions, subscriptions, inflight QoS1/2, offline
count/bytes, retained count/bytes and aggregate MQTT session bytes. Checks occur
before mutation; restore recomputes and revalidates them.

## 26. Restart recovery architecture

`mqtt-runtime.state` is one `NBMQ` versioned/generation snapshot with checked length,
JSON and SHA-256. It is written 0600 under a 0700 directory using temp file, fsync,
atomic rename and directory fsync. Restore rejects unknown/corrupt/over-limit state
and contains no credentials. EventBus spool remains an independent replay-safe
responsibility in the same recovery directory.

The JSON format remains backward compatible, but its configured file bound is now
derived rather than equated with logical state bytes. Six times the global session
plus retained logical ceilings covers worst-case JSON string escaping and byte-array
expansion; bounded per-session/retained wrapper allowances and the 52-byte envelope
are added with checked arithmetic. The default is 1.25 GiB. Startup rejects a lower
bound, high-byte payloads are committed/recovered in tests, and structural overflow
during shutdown returns an invariant error instead of retrying forever.

## 27. Graceful restart tests

Real child processes quiesce work listeners, detach owners, commit MQTT state, drain
or spool EventBus responsibilities, exit successfully, restart and authenticate
again. A forced spool failure keeps the child alive and unready until the directory
is repaired, after which restart replays the same event ID. SIGKILL while blocked is
the separately documented abnormal-loss case.

## 28. Persistent-session restart results

PASS: CleanSession=0 subscription restored, reconnect CONNACK had Session Present=1,
and routing remained active. CleanSession=1 reset is independently tested.

## 29. QoS1 inflight restart results

PASS: restart after outbound PUBLISH/before PUBACK resent the same packet identifier
with DUP=1; PUBACK then released state.

## 30. QoS2 restart results

PASS: four real generations covered outbound before PUBREC, outbound PUBREL before
PUBCOMP, and inbound PUBREC before PUBREL. Duplicate PUBREL produced PUBCOMP without
another routed publish. Unit snapshot tests cover the same explicit states.

## 31. Retained restart results

PASS: retained publish survived graceful restart and a new wildcard subscriber
received payload with RETAIN=1. Recovery checksum/version corruption tests pass.

## 32. Interoperability results

Mosquitto 2.1.2 clients, explicitly using `-V mqttv311`, passed authentication,
QoS0/1/2, exact/`+`/`#`, unsubscribe, retained replay/replace/delete, persistent
offline QoS1/2, and retained QoS0/1/2 Wills. Verified TLS passed with the test CA and
an untrusted self-signed endpoint was rejected. The isolated Mosquitto 2.1.2 broker
differential matrix passed CONNECT, session state, QoS0/1/2, subscribe/re-subscribe/
unsubscribe, retained, Will, duplicate ClientId, keepalive, malformed flags, and
unknown-ID classifications. Paho is not installed in the current environment; the
historical Paho 2.1.0 evidence remains non-current. No external broker is a runtime
dependency.

## 33. Conformance checklist

The evidence-linked checklist is [mqtt-3.1.1-conformance.md](mqtt-3.1.1-conformance.md).
All 125 applicable server normative statements are PASS under the documented
NetbaIoT topic/identity profile. Sixteen client-only, WebSocket, bridge, or outbound-
broker statements are NOT_APPLICABLE. MQTT 5 and listed extensions remain excluded.

## 34. Fuzz results

Nightly `cargo fuzz`, 1,000 runs each, passed nine targets: MQTT fixed header,
Remaining Length, full fragmented packet decoder, stateful broker sequence, TCP
frame, UDP envelope, JSON codec, restart spool and business stream. No crash,
unbounded allocation assertion, integer overflow or stuck decode was observed.

## 35. Memory results

At 1,000 active connections: CleanSession=1 plaintext RSS delta 19,152.9 B/conn;
TLS 27,344.9 B/conn; CleanSession=0 plaintext 23,445.5 B/conn; persistent plus two
subscriptions 26,230.8 B/conn. Each active connection added one task and FD.
Disconnected persistent RSS growth was 24,576 B/session at 1K and 22,634.5 B/session
at 10K, with zero retained tasks/FDs. Broker logical session accounting with one
subscription was 181.9 B/session at 1K and 185.9 B/session at 10K; process RSS also
includes auth cache, hash-table capacity and allocator overhead.

## 36. Throughput/latency results

Real MQTT → codec → EventBus → confirmed HTTP sink at requested 10,000 events/s for
20 seconds produced/published/PUBACKed/received 199,999 events with no errors.
PUBACK P50/P95/P99 was 0.13/0.21/0.25 ms; sink ACK was 1/1/1 ms. Queue samples stayed
0–20, RSS stabilized around 9.3 MiB, and server exit was successful.

Router microbenchmark at 100/1K/10K subscriptions: P50 167 ns, P95 208/209/209 ns,
P99 209 ns, about 5.2M lookups/s. Retained exact lookup at 1K/4K: P50 41 ns,
P95 42 ns, P99 42/83 ns. Bounded wildcard retained scan: at 1K P50/P95/P99
69.3/75.5/81.5 µs; at 4K 287.2/303.0/333.7 µs.

## 37. Slow consumer results

A 10 ms confirmed sink at 1,000 events/s for 15 seconds accepted and delivered all
14,999 events. Publisher PUBACK P50/P95/P99 was 0.11/0.20/0.31 ms; bounded pending
required work peaked at 5,639 then drained. Sink end-to-end P50/P95/P99 was
4,794/9,478/9,879 ms. MQTT subscriber queues independently use bounded
cancel/offline/shedding policy and never spawn per-message tasks.

## 38. Known limitations

The broker is memory-first and not crash durable; SIGKILL may lose changes since the
last planned snapshot. Retained wildcard replay scans the bounded retained store.
No `$SYS` service, MQTT 5, shared subscriptions, WebSocket, bridging, clustering or
multi-node migration is implemented. Publishing is intentionally limited to
canonical IoT topics, and the IoT live-session binding selects one current command
endpoint per DeviceKey even though multiple persistent ClientIds may be stored.
Exhausting explicit subscriber/session limits sheds delivery by documented policy.
Performance results are single-host measurements, not production capacity claims.
