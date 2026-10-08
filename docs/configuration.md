# Configuration and project setup

```bash
netbaiot init my-gateway
cd my-gateway
netbaiot config check --config netbaiot.json
netbaiot doctor --config netbaiot.json
netbaiot serve --config netbaiot.json
```

Init creates only netbaiot.json, .env.example, README.md and var/. The development
config uses loopback, an example HTTP receiver on 127.0.0.1:18080/events and the
existing development credential. Start your own receiver before serve, or use
`netbaiot demo` for the self-contained experience. No management secret is generated
or printed. .env.example contains variable names with empty values and is not
loaded automatically. Supply secrets from protected sources.

`netbaiot init --production my-production` creates a parser-readable skeleton with
TLS placeholders, HTTPS provider/sink placeholders, no device credential and no
fixed production token. It is **not ready to run**. Fill TLS/auth/sink values then
run config check. No public plaintext gateway can start from the skeleton.

Init refuses to replace any of its managed files by default. `--force` replaces
only these three regular files; it rejects symlinks/special files and never removes
the whole directory or unrelated files. Writes are staged, synced and atomically
published per file. This is not a transaction across three files: a late filesystem
failure can leave an explicitly reported partial generation. The generated recovery
path is absolute to this project; update it after relocation. Config file paths
otherwise retain the server's current-working-directory semantics.

## IDE schema association

[Generated JSON Schema](schema/netbaiot-config.schema.json) comes from the actual
Rust Config/serde types through the optional `schema` feature. It respects
serde defaults, enum spellings and deny_unknown_fields. Resource scalar limits
share the runtime's positive/u32 range; secret_hex is writeOnly and 64 hex digits.
Production server/client binaries do not enable schema generation by default.
`netbaiot config schema` prints the committed generated artifact without a compiler.

Config rejects unknown fields, including `$schema`. Do not insert `$schema` into
your gateway JSON. For VS Code, associate it in editor settings:

```json
{
  "json.schemas": [{
    "fileMatch": ["netbaiot.json"],
    "url": "./netbaiot-config.schema.json"
  }]
}
```

Save `netbaiot config schema` output beside your config using that filename, or use
a suitable absolute editor schema path. The schema is also included in binary archives.

Schema validation does not replace `netbaiot config check`. Cross-field listener
security, identity uniqueness, role/permission constraints, environment secret
sources, real PEM/key matching and runtime ownership/ports remain separate checks.
The [generated field/default reference](configuration-fields.md) is rebuilt by
`cargo xtask config-reference`; [operations](operations-guide.md) contains the
handwritten security/deployment guidance.

## Per-tenant event backlog

`limits.event_queue_max_count_per_tenant` and
`limits.event_queue_max_bytes_per_tenant` bound outstanding EventBus events for
each tenant, including queued, retrying and inflight deliveries. Serialized event
bytes are counted once, regardless of fanout. Ownership ends only after the last
sink responsibility completes. Required sink failures continue to own the quota.

Both limits default to the existing global event limits (16384 events, 67108864
bytes). Old configuration files remain parseable and default capacity is unchanged.
Previously increased global limits need explicit tenant settings if a tenant must
own more than these defaults. Set them lower to reserve global capacity
for other tenants. Global and sink limits still apply independently; setting a
tenant limit above the global limit does not increase capacity. Admission and
restart replay preflight all global, tenant and required sink quotas before commit.
