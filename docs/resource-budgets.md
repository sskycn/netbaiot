# Resource budgets and backpressure

These are engineering defaults, not a tested capacity claim. The full authoritative
configuration snapshot is [configs/resource-limits.json](../configs/resource-limits.json).
Generate it with `cargo run -p netbaiot-server -- --print-default-limits`.
`Limits::validate` rejects zero/overflow-prone values, excessive packet sizes,
inconsistent hierarchies and invalid timeout/retention relationships at startup.

| Resource | Device / connection | Tenant | Node / global |
|---|---:|---:|---:|
| Stream connections | 2 authenticated/device; 32/IP | 64 | 256 |
| Stream memory reservation | 512 KiB/connection | connection quota applies | 128 MiB |
| MQTT packet / HTTP body / TCP payload | 64 KiB | admission quota applies | reservation applies |
| HTTP headers | 8 KiB, 32 headers | connection quota | connection quota |
| HTTP handlers | one/connection; request stage 1/device | 4 throughout handler/body | 16 handlers |
| UDP datagram | 1200 B | authenticated quotas | one sequential owner |
| Ingress operations | 1/device | 4 | 16; 2 MiB payload bytes |
| MQTT protocol/admission rate | 16/s | 128/s | 512/s |
| Connection/HTTP/UDP rate | 32/s/IP | authenticated admission | 512/s |
| Subscriptions | 2/device and connection | 128 | 512 |
| Filters per SUBSCRIBE/UNSUBSCRIBE | 16; exact own topics only | — | — |
| Topic | 256 bytes / 8 levels | — | — |
| In-flight MQTT QoS1 | 32 | connection quota applies | connection quota applies |
| Command queue incl. in-flight command | 32 items / 256 KiB encoded bytes | 2 MiB | 8 MiB |
| Retained command records | 16 × at most 16 KiB | 128 | 1024 |
| Stored ingress records | 1000 | 10,000 | 100,000 |
| Charged ingress storage bytes | 2 MiB | 16 MiB | 128 MiB |
| Provisioned credentials / presence | one credential/device | 128 devices | 1024 devices |
| UDP replay entries | 2 boots/device; 64-sequence bitmap | 256 | 1024 |
| Source-IP rate table | one-second expiry | — | 1024 entries |
| Codec registry | immutable | — | 64 entries |
| PostgreSQL connections | — | — | 8 |
| Delivery worker / command worker | — | — | one each; no per-item tasks |
| DB maintenance / command batch | — | — | 16 records / device keys |
| Delivery claim | — | — | one record |

Storage charge is `16 * canonical_bytes + 8192` per message/outbox pair, a
conservative service-memory reservation. It also provides a logical PostgreSQL
capacity gate. It is not a promise about PostgreSQL physical disk, indexes, WAL,
autovacuum, allocator fragmentation, TLS/kernel socket buffers, or process RSS.
Those require deployment-level disk/memory controls and load measurement. A byte
quota may reject before a count quota is reached. Command count × max encoded
size bounds stored command payloads; queue metadata is separately bounded by
count. Queued commands retain encoded bytes and identifiers, not a second decoded
command payload. Superseded connection owners retain permits until cleanup. Tenant budget identities
remain shared across old and new sessions while any endpoint or byte permit survives;
the weak-identity registry itself is bounded by max_devices.

| Deadline / retention | Default |
|---|---:|
| CONNECT / TLS handshake | 10 s |
| Authentication | 5 s |
| Incomplete packet/frame | 30 s |
| Stream write | 10 s |
| HTTP handler/body | 15 s |
| HTTP full connection | CONNECT + request + write budgets |
| MQTT nonzero keepalive | 1.5 × client interval |
| Server idle / outbound PUBACK inactivity | 120 s |
| External DB/business request | 5 s |
| Worker lease | 30 s |
| Worker idle/maintenance poll | 200 ms |
| Delivery attempts | 5 |
| Retry exponential full jitter | base 1 s, cap 30 s |
| Delivery TTL | 1 h |
| Ingress deduplication / disconnected presence | 24 h |
| Command maximum future expiry | 5 min |
| Command record retention | expiry + 5 min |
| UDP clock skew / replay TTL | 30 s / 120 s |
| Server shutdown | 30 s |

## Transition policies

| Producer → consumer | Count / byte capacity | Overflow | Shutdown |
|---|---|---|---|
| OS socket → connection owner | connection hierarchy + memory reservation | close new socket | stop accept, drain owned tasks |
| Stream → parser | max frame/packet + fixed header; one reader | close malformed/oversized/slow stream | cancel read; RAII cleanup |
| UDP socket → owner | one 1201-byte receive buffer | drop oversized, no response | finish one bounded operation then stop |
| HTTP → handler | 16 permits + authenticated 4/tenant, 1/device + reservation/body limit | 429 or connection close; 413 for body | reject new work; graceful response completion |
| Packet → auth | one inline auth/owner, 5 s; source rates | refuse/close/drop | no new ingress after drain |
| Codec → ingress | one message; device/tenant/node permits and bytes | HTTP 429; MQTT/TCP close; UDP drop | admitted operations finish with deadlines |
| Ingress → PostgreSQL | 8 pool connections, bounded callers; storage quotas | reject, no application ACK | transaction commits or rolls back |
| DB outbox → delivery worker | one leased item, TTL/attempt limits | stay durable until due/terminal/expired | finish current bounded call; recover leases after restart |
| DB commands → session channel | 32 items and three byte permits | enqueue fails; durable retry within limits | stop claims; drop queue; preserve durable record |
| Session channel → device | one write at a time, 10 s | disconnect on timeout; no Sent claim on failure | close/drop and release all permits |
| Outbound QoS1 → PUBACK | 32 non-reusable IDs, bounded retained command permits | disconnect when full/timed out | clean-session protocol state discarded |

No unbounded channels, background futures, retry queues, offline MQTT sessions,
retained MQTT payloads, or replay caches are used. Authentication provisioning is
static and capacity-checked; no growing auth cache exists. Replay eviction only
removes expired entries; an otherwise full cache rejects new boots. Presence and
rate-table expirations run on bounded collections. Delivery and command attempts
are persisted and bounded; device execution must remain idempotent across retries.

The focused audit and measured physical-memory limitations are recorded in
[correctness-resource-reliability-audit.md](correctness-resource-reliability-audit.md).
