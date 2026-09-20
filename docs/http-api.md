# HTTP API

Device and management HTTP bind independently. Both enforce bounded headers, body,
handler concurrency, timeouts, and reject content encoding. Device bearer format is
`credential_id:secret`; management uses a separate 64-character secret configured
through `NETBAIOT_ADMIN_SECRET`.

## Device listener

| Method/path | Meaning |
|---|---|
| `POST /v1/device/data` | Decode and route collected data; `202` means EventAccepted |
| `GET /v1/device/config` | Return revisioned config; supports `If-None-Match`/304 |
| `POST /v1/device/config/ack` | Route typed `ConfigAck` as a normal event |
| `POST /v1/device/heartbeat` | Route a heartbeat event |
| `POST /v1/device/commands/ack` | Route typed command execution ACK |

Upload success does not mean business persistence or application processing.

## Management listener

| Method/path | Meaning |
|---|---|
| `GET /api/v1/health` | Process liveness |
| `GET /api/v1/ready` | Ready only in RUNNING |
| `GET /api/v1/status` | Lifecycle and bounded cache/event usage |
| `GET /api/v1/metrics` | Low-cardinality Prometheus text |
| `GET /api/v1/connections?offset=&limit=` | Paginated local live sessions; max 256 |
| `POST /api/v1/devices/commands` | Send immediately to a live local session |
| `POST /api/v1/auth/invalidate` | Invalidate scope and disconnect affected sessions |
| `POST /api/v1/config/invalidate` | Remove one cached device config |
| `PUT /api/v1/control/snapshot` | Validate and atomically replace revisioned snapshot |
| `PUT /api/v1/routes` | Validate and replace routes by revision |
| `POST /api/v1/drain` | Request graceful shutdown |

Connection queries are bounded. The command body contains the authoritative full
`DeviceKey`; unavailable devices receive 503 and are not queued offline.
