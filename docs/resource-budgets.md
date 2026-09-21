# Resource budgets and backpressure

Defaults are engineering ceilings, not measured production capacity. The complete
authoritative snapshot is [resource-limits.json](../configs/resource-limits.json).
Startup rejects zero, inconsistent hierarchy, excessive length, invalid TTL, and
spool relationship values.

| Resource | Default bound |
|---|---:|
| Connections | 256 node / 64 tenant / 32 IP / 2 device |
| Logical connection memory | 512 KiB reservation / 128 MiB global |
| MQTT/HTTP/TCP maximum | 64 KiB |
| Initial stream read buffer | at most 4 KiB; grows incrementally |
| UDP datagram | 1,200 B |
| Ingress active | 16 / 2 MiB; tenant 4; device 1 |
| Ingress wait | 16 / 2 MiB / 25 ms, inside existing owner task |
| Outbound device command | 16/device, 128/tenant, 1,024/global |
| Outbound bytes | 256 KiB/connection, 2 MiB/tenant, 8 MiB/global |
| MQTT persistent sessions | 4,096 global / 512 tenant / 24 h idle policy |
| MQTT subscriptions | 32/session, 64/device, 128/tenant, 512 global |
| MQTT inflight | QoS1/QoS2 32/session and 4,096/tenant each |
| MQTT offline queue | 128 + 1 MiB/session; 4,096 + 32 MiB/tenant; 16,384 + 128 MiB global |
| MQTT retained | 4,096 + 64 MiB global; 512 + 8 MiB/tenant; 64 KiB/message |
| MQTT session state | 2 MiB/session / 32 MiB/tenant / 128 MiB global |
| MQTT Will | 64 KiB payload; at most 256 node / 64 tenant responsibilities, sharing MQTT session byte ceilings |
| Auth cache | 4,096 / 4 MiB / 256 miss waiters |
| Config cache | 4,096 / 16 MiB |
| Presence registry | bounded by configured devices / 1 h offline TTL / oldest-offline eviction |
| Sinks/routes/fanout | 32 sinks / 256 filters / 8 per event |
| Global active events | 16,384 / 64 MiB |
| Per-sink delivery | 4,096 / 16 MiB / concurrency 8 |
| Sink timeout/retry | 5 s / 5 attempts / max age 1 h |
| Restart spool | 100,000 records / 256 MiB total |
| Spool segment/record | 64 MiB / 1 MiB |
| MQTT recovery image | 202,178,660 B; compact NBMQ v3 bound including profiles, pending-Will owners, and integrity trailer |

Every sink queue is independently count and byte charged. Global event accounting
charges the shared event once; each sink charges its delivery responsibility.
Required overload rejects upstream before EventAccepted. Best-effort overload drops
with a metric. Count/byte permits release on ACK, best-effort terminal failure,
queue failure, session replacement, receiver closure, or owner drop.

Stream parsers allocate at most 4 KiB initially, never the maximum frame. If a
processed large frame leaves an empty buffer above 16 KiB capacity, it is replaced
with the small initial allocation. Idle command channels do not preallocate payload
byte limits.

| Producer → consumer | Overflow behavior |
|---|---|
| OS accept → connection owner | reject/close by IP, tenant, node, logical bytes |
| stream → incremental parser | close malformed, oversized, slow, or incomplete input |
| HTTP → handler/body | 429/413/timeout; no hidden waiting task |
| codec → event router | bounded wait then reject; no success ACK |
| router → required sink | all-or-nothing reject before acceptance |
| router → best-effort sink | drop and metric |
| command → live session | reject overloaded/offline; never persist |
| MQTT route → active subscriber | QoS0 may shed; QoS1/2 is preflighted and retained as outbound state |
| MQTT route → persistent offline subscriber | bounded QoS1/2 queue; whole publication rejects atomically at limit |
| accepted Will → overloaded persistent subscriber | bounded pending ownership; retry on capacity release and planned-restart persistence |
| graceful drain → spool | remain alive/unready and retry bounded commits while accepted work remains |

TLS, allocator-retained pages, Tokio, and kernel socket buffers are not exactly
represented by logical accounting and require process-level measurement.
