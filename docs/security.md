# Security overview

For undisclosed vulnerabilities, follow the [Security Policy](../SECURITY.md)
and use the repository's private reporting form. Do not post exploit details in
a public issue.

## Device authentication

MQTT and TCP authenticate at connection setup and bind an immutable device
identity, credential version, permissions, and codec selection to that connection.
Normal packets/frames do not make a remote auth-provider call. Authentication
caching is count- and byte-bounded and supports explicit invalidation. Cache misses
fail closed when the configured provider is unavailable.

UDP is connectionless. Each NBI1 datagram carries a credential ID, credential
version, timestamp, sequence, payload, and HMAC-SHA256. The gateway checks the
credential, timestamp tolerance, signature, and bounded replay window. The
datagram body is not encrypted: **authenticated UDP is not confidential UDP**.
Use a protected network when payload confidentiality is required.

## TLS and listener boundaries

Non-loopback device TCP requires TLS. MQTT and generic framed TCP share the device
TCP listener after the TLS handshake. Development mode permits plaintext only on
loopback. Management HTTP uses a separate listener and authorization service;
device credentials never grant management access. Non-loopback management also
requires TLS and an enabled authorization provider.

The webhook and business RPC/TCP listeners have their own configured transport
and authentication settings. Protect those paths according to the deployment
configuration; a device credential is not their service identity.

## Secrets and authorization state

Do not put passwords, tokens, HMAC keys, raw credentials, authorization headers, or
full sensitive spool records in logs, metric labels, or support output. Credentials
belong in protected secret injection, not committed tutorial or production
configuration. The repository's tutorial keys are public development fixtures.

Management supports a legacy full-scope bootstrap token, scoped API Keys, RS256
JWT with configured JWKS, and optional management mTLS identity mapping. The
bootstrap token has Global access and should be disabled after migration to scoped
identities. Management authorization is independent of device authentication.

Auth cache and gateway control snapshots are separate, bounded, and rebuilt after
restart. Authentication invalidation fences stale session registration and also
removes matching bounded persistent MQTT session state.

## Resource and parser protections

Network, management, MQTT, codec, spool, and sink inputs are treated as hostile.
Lengths are checked before allocation; buffers, requests, queues, caches, retries,
connections, subscriptions, retained data, replay entries, and recovery records
have hard count and byte ceilings. MQTT uses incremental parsing, strict UTF-8,
packet deadlines, and a Remaining Length limit. TCP framing, UDP datagrams, and
management HTTP have independent limits.

See [device protocol](device-protocol.md), [MQTT profile](mqtt.md),
[management authentication](management-auth.md), and
[operations guide](operations-guide.md) for configuration details.
