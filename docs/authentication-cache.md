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

Signed UDP ACKs reuse the verifier from the one lookup/HMAC verification. The raw
32-byte key remains private, with no Debug/serialization. An epoch-fenced synchronous
completion signs and tries to send NBA1 under the auth-cache lock, so invalidation
cannot race an old signer past that boundary. Unrelated invalidations conservatively
suppress outstanding receipts too; accepted work remains committed and may be
confirmed by an authenticated retry.

## HTTP authority service authentication

Set `NETBAIOT_AUTH_PROVIDER_TOKEN` in the gateway process environment (or inject it
from a secret manager). Both secret authentication and UDP verifier resolution send
`Authorization: Bearer <token>` through the same request builder. Do not put this
secret in JSON configuration, device credentials, logs or recovery files. Values
must be nonempty, at most 4096 bytes and contain only visible ASCII without spaces.
An invalid or non-Unicode environment value fails startup with a generic error.
The header is marked sensitive; the provider has no credential-bearing Debug output.

The token is optional for backwards compatibility, including loopback development.
A non-loopback HTTPS authority without a token emits a fixed warning; production
should require and validate a separate service token. HTTPS (or loopback HTTP),
disabled redirects, no_proxy, request deadlines, response-size and concurrency bounds
remain unchanged. Environment changes take effect on gateway restart. Future mTLS
can extend this client construction without changing device authentication semantics.
