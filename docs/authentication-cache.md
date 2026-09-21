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
completion channel. A leader owns an RAII inflight lease: timeout, task cancellation,
panic unwind, or any early return removes the inflight entry and wakes followers so
one of them can retry. Miss wait permits are released on every exit. Provider calls
have a five-second default timeout and bounded concurrency. Provider outage behavior
is fail closed for an unknown/expired miss; an unexpired positive entry or
already-bound long-lived session continues.

UDP uses a distinct positive cache entry keyed by credential identity. A provider
lookup returns an opaque `DeviceVerifier` containing the authenticated identity and
256-bit HMAC verification material. Every datagram is still signature-checked
locally, then credential version and replay window are checked; the signed message
and tag are not used as the remote-cache key. Thus 10,000 valid packets within TTL
perform one provider lookup, not 10,000. The external provider contract uses
`{"kind":"verifier","credential_id":...}` and returns
`{"identity":...,"verifier_key_hex":...}` over the already-required HTTPS (or
loopback HTTP) channel. Verification keys are never serialized to recovery files,
logged, or exposed through `Debug`.

Management invalidation supports device, product, tenant, credential version, auth
generation, or all entries. Every invalidation advances an auth epoch. A provider
result begun in an older epoch is rejected and cannot repopulate the cache after
invalidation. Active sessions are matched against their bounded, immutable bound
identity and canceled independently of positive-cache contents, so TTL expiry or
eviction cannot defeat revocation.

The final MQTT establishment boundary is fenced by the same short gate. Lock order
is `auth_registration -> AuthCache -> Sessions -> MqttBroker`: candidate freshness,
live registration, and `MqttBroker::attach` complete before release. Invalidation
uses that order for cache removal, active cancellation, and persistent MQTT-session
removal, so a stale candidate cannot recreate state after revocation completes.
Management results separately report cache entries, network connections, and
persistent MQTT sessions; offline state is not a disconnected connection.
