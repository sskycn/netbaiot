# Authentication cache

MQTT and generic TCP authenticate once during connection setup and bind an immutable
`Arc<AuthenticatedDevice>` containing identity, credential version, auth generation,
permissions, and codec binding. Every later packet/frame uses this context and does
not call the provider.

The separate auth cache is bounded by 4,096 entries, 4 MiB estimated logical bytes,
and 256 concurrent miss waiters by default. Positive TTL is five minutes; negative
TTL is five seconds. Entries evict oldest cache order when count or bytes would be
exceeded. Raw secrets/tags are not retained: keys use credential ID plus SHA-256
fingerprints and are never logged or labeled.

Identical simultaneous misses share one provider operation through a race-safe watch
completion channel. Provider calls have a five-second default timeout and bounded
concurrency. Provider outage behavior is fail closed for an unknown/expired miss;
an unexpired positive entry or already-bound long-lived session continues.

Management invalidation supports device, product, tenant, credential version, auth
generation, or all entries. Affected active sessions are canceled immediately.
