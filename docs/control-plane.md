# Gateway control plane

Gateway control owns credentials, trusted identities, permissions, auth generations,
product/codec profiles, routes and installed sink definitions. `GatewayControl`
shares immutable `Arc` snapshots, separately bounded from the authentication cache.
`ControlSnapshot` contains only `revision`, `products` and `routes`. No per-device
business desired/reported state is stored.

Startup validates bootstrap control data, constructs sinks/routes and recovers
committed restart work before becoming ready. Optional external authentication
requests have finite deadlines and concurrency; established MQTT/TCP sessions use
their bound identity without per-message provider calls.

Control replacement checks revision, unique product keys, nonzero profile/codec
versions, product count and serialized bytes. Management updates serialize route
validation against installed sinks and fanout bounds before replacing either
control or router state. Route replacement preserves product profiles; a stale or
oversized update leaves existing state intact. Limits are `control_max_products`
(4,096), `control_max_bytes` (16 MiB), and `max_routing_filters` (256). Auth state
and control snapshots are rebuilt after restart and never enter the delivery spool.

Profile metadata does not override a live session's immutable authenticated codec
binding. Auth invalidation remains the mechanism for revoking trusted sessions.

Device configuration persistence, desired/reported revisions, history, retries,
rollout/rollback and offline reconciliation belong to business applications.
Online changes use ordinary `DeviceCommand` over MQTT/TCP and return `CommandAck`.
The gateway neither interprets command names nor compares application revisions.
See [migration](remove-device-config.md).

Control mutations acquire lifecycle admission before their first side effect and
retain it through completion: HTTP auth invalidation, control/routes replacement,
Business RPC V2/V3 auth sync/invalidation, provider-offline revocation, commands and
MQTT/TCP session establishment. Quiesce waits for admitted operations before broker
recovery starts. New mutations return HTTP 503 `draining` / RPC `unavailable`;
command retries also obey this fence. Diagnostic reads remain available and repeated
drain requests remain idempotent. Admission closes before stopping MQTT owners;
existing QoS/Will cleanup finishes before their tasks are joined and snapshot begins.
