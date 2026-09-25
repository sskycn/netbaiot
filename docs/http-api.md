# Management HTTP API

Management HTTP has its own listener and supports the legacy bootstrap token,
API Keys, RS256 JWT, and explicitly mapped management mTLS identities. See
[management authentication](management-auth.md) for configuration and permission rules.
Headers, bodies, handler concurrency, responses
and deadlines are bounded; content encoding is rejected. Device credentials cannot
authorize these operations. The device ingress does not dispatch HTTP.

## Management listener

| Method/path | Meaning |
|---|---|
| `GET /api/v1/health` | Process liveness |
| `GET /api/v1/ready` | Ready only in RUNNING |
| `GET /api/v1/status` | Lifecycle and bounded cache/event usage |
| `GET /api/v1/metrics` | Low-cardinality Prometheus text |
| `GET /api/v1/connections?offset=&limit=` | Paginated local live sessions; max 256 |
| `POST /api/v1/devices/connection` | Query non-sensitive connection/presence by `DeviceKey` |
| `POST /api/v1/devices/commands` | Send immediately to a live local session |
| `POST /api/v1/auth/invalidate` | Invalidate scope and disconnect affected sessions |
| `PUT /api/v1/control/snapshot` | Validate and atomically replace revisioned snapshot |
| `PUT /api/v1/routes` | Validate and replace routes by revision |
| `POST /api/v1/drain` | Request graceful shutdown |

Connection queries are bounded. The command body contains the authoritative full
`DeviceKey`; unavailable devices receive 503 and are not queued offline.
The connection response includes the active transport, `connected_at`, `last_seen`,
and session generation without exposing socket state. `connected_at` is present
only while the current MQTT/TCP session remains connected.

Errors use the stable `ApiError` JSON shape with `code`, safe `message`, optional
`request_id`, and optional `required_scope`. In particular, an offline command uses
`device_offline`, not an opaque internal 500. Device and management authorization
remain separate. Missing scopes return 403 with `required_scope`; resource denials
return 403 without exposing whether a device exists.
