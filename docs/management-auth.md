# Management authentication and authorization

Management HTTP uses its own listener and authorization service. Device credentials and the device `AuthCache` never authorize management calls. No provider or failed authentication means 401 `unauthenticated`; an authenticated principal without permission receives 403 `forbidden`. Missing scopes include `required_scope` in the existing `ApiError` response. Resource denials use a generic message.

## Credentials

| Method | Request | Configuration |
|---|---|---|
| Bootstrap static token | `Authorization: Bearer <64 hex>` | `NETBAIOT_ADMIN_SECRET` (legacy, enabled by default) |
| API Key | `Authorization: ApiKey <key_id>.<64 hex secret>` | `management_auth.api_keys` with `secret_env` |
| JWT access token | `Authorization: Bearer <three JWT segments>` | `management_auth.jwt` with HTTPS JWKS URL |
| Management mTLS | Verified client certificate, no Authorization header | `management_tls` and an explicit certificate SHA-256 mapping |

One request may present only one credential. A client certificate and an Authorization header together are rejected. A failed provider is never retried through another provider. The legacy static token produces `bootstrap-admin`, `admin.*`, and Global resource access; it can authorize every management operation. The server warns when it remains enabled alongside a usable scoped provider. After migration, set `"legacy_static_token_enabled": false` under `management_auth`. A non-loopback management listener requires a usable provider; disabled or expired API Keys do not count.

API Key example (the environment variable holds the secret, never the JSON file):

```json
{
  "management_auth": {
    "api_keys": [{
      "key_id": "backend-prod",
      "secret_env": "NETBAIOT_API_KEY_BACKEND",
      "subject": "service:backend",
      "scopes": ["connection.read", "device.command"],
      "resources": {"tenants": ["tenant-a"]},
      "global": false,
      "expires_at": null,
      "auth_generation": 1,
      "enabled": true
    }]
  }
}
```

`secret_env` must resolve at startup to 64 hexadecimal characters. `expires_at` is Unix milliseconds. Disabled and expired keys fail authentication and cannot satisfy the public-listener provider check. `global: true` is explicit; an empty resource list grants no resource access. Key IDs, subjects, scopes, and resource sets are validated and bounded at startup. Treat API Keys as high entropy random values. The retained `auth_generation` field is metadata only; changing it does not revoke an API Key at runtime. Disable or remove the key and restart to revoke it.

JWT example:

```json
{
  "management_auth": {
    "jwt": {
      "issuer": "https://identity.example.com/realms/iot",
      "audience": "netbaiot",
      "jwks_url": "https://identity.example.com/realms/iot/protocol/openid-connect/certs",
      "subject_claim": "sub",
      "scope_claim": "scope",
      "roles_claim": "roles",
      "tenant_claim": "tenants",
      "role_scopes": {"iot-operator": ["runtime.read", "connection.read", "device.command"]},
      "global_roles": []
    }
  }
}
```

Only RS256 is supported in this phase. The verifier requires a valid signature, configured issuer and audience, `exp`, and `nbf`. It rejects token supplied JWK/URL overrides. JWKS is fetched only from the configured HTTPS URL, with a 2 second connect timeout, 5 second overall timeout, no redirects, a byte and key count ceiling, a TTL, and a minimum refresh interval. Cache misses are serialized without an unbounded waiter queue. Valid unexpired cached keys continue during an outage; expired keys fail closed. Invalid JWT credentials return 401. A JWKS outage, timeout, unusable keyset, or refresh contention without a verifiable key returns 503; no token is accepted in that state. Unknown `kid` values in a valid refreshed keyset return 401 and cannot drive unlimited refreshes. JWT scopes may be a space separated string or an array. Role mapping is explicit; only a configured `global_roles` entry grants Global resource access. Tenant claims otherwise create a restricted principal.

For mTLS, configure a separate management certificate and a trusted client CA:

```json
{
  "management_tls": {
    "certificate": "certs/management.pem",
    "private_key": "certs/management-key.pem",
    "client_ca": "certs/client-ca.pem",
    "require_client_certificate": true
  },
  "management_auth": {
    "mtls_identities": [{
      "certificate_sha256": "<64 lowercase or uppercase hex characters>",
      "subject": "service:iot-platform",
      "scopes": ["runtime.read"],
      "global": true
    }]
  }
}
```

The TLS handshake validates client CA, certificate validity and the chain before matching the leaf DER SHA-256 fingerprint. An unmapped certificate is rejected. Certificate Subject CN is never trusted as identity. The existing `tls` configuration still works for the device listener and is separately loaded for management when `management_tls` is omitted. `require_client_certificate: true` requires at least one mapped `mtls_identities` entry and makes the management listener mTLS-only: every connection must present a certificate, and Authorization credentials cannot be combined with it. This does not change MQTT/TCP clients. URI/DNS SAN identity mapping is not implemented yet; explicit certificate fingerprint mapping is supported.

## Authorization

Scopes: `runtime.read`, `metrics.read`, `connection.read`, `device.command`, `auth.invalidate`, `auth.invalidate.all`, `control.read`, `control.write`, `routes.read`, `routes.write`, `runtime.drain`, and `admin.*` (all scopes). Resource access is Global or a bounded union of tenant IDs, `(tenant_id, product_id)` pairs, and full `DeviceKey` values. `admin.*` does not override resource restrictions.

`control.read` and `routes.read` are reserved names with no corresponding read API today; granting them has no effect.

| API | Scope | Resource |
|---|---|---|
| health, ready, status | `runtime.read` | none |
| metrics | `metrics.read` | none |
| connections, device connection | `connection.read` | filtered list or full DeviceKey |
| device command | `device.command` | full DeviceKey |
| auth invalidate device/product/tenant | `auth.invalidate` | matching resource |
| auth invalidate credential version/generation | `auth.invalidate` | Global |
| auth invalidate all | `auth.invalidate.all` | Global |
| control snapshot | `control.write` | Global |
| routes | `routes.write` | Global |
| drain | `runtime.drain` | Global |

`/connections` filters the bounded live registry **before** offset/limit pagination. The gateway checks scope before parsing a bounded body, then checks the resource before mutating runtime state.

## Migration and use

Existing API paths, `NETBAIOT_ADMIN_SECRET`, client `.token()`, CLI `--token`, and `NETBAIOT_TOKEN` remain valid. Add API Keys or JWT configuration, grant the narrowest scopes and resource sets, then disable the bootstrap token when clients have migrated. Use TLS for non-loopback management. Do not put secrets in JSON, logs or metric labels. mTLS identity fingerprints and JWT subjects are never metric labels.

```bash
curl -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET" http://127.0.0.1:9090/api/v1/status
curl -H "Authorization: ApiKey $NETBAIOT_API_KEY" https://gateway.example.com/api/v1/status
curl -H "Authorization: Bearer $ACCESS_TOKEN" https://gateway.example.com/api/v1/status
```

HMAC request signing, OAuth2 token introspection, administrative audit persistence, and URI/DNS SAN mapping remain future work. The gateway is not a user or session store.

## Resource ceilings

`limits` exposes `management_auth_max_subject_bytes` (128), `management_auth_max_scopes` (32), `management_auth_max_scope_bytes` (512), `management_auth_max_resource_entries` (128), `management_auth_max_resource_bytes` (8192), `management_api_key_max_entries` (128), `management_api_key_max_bytes` (32768), `management_jwks_max_keys` (32), `management_jwks_max_bytes` (65536), `management_jwks_ttl_ms` (300000), `management_jwks_refresh_min_interval_ms` (30000), and `management_jwt_max_bytes` (16384). Defaults are shown in parentheses. A management HTTP request has at most the configured header/body limits and one handler slot.

Connection admission and management request rates use separate bounded windows with the existing `requests_per_second` and `requests_per_ip_second` settings. Each accepted HTTP request consumes one request quota.
