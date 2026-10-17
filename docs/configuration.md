# Configuration and project setup

Device payloads support JSON V1, CBOR V1, MessagePack V1 and Protobuf V1. See [codec wire formats, selection and extension](codecs.md).

The optional `payload_codecs` array compiles vendor binary protocols at startup.
See [vendor codec configuration and native registration](vendor-codecs.md) and
[the complete development example](../configs/vendor-codec.json).

[中文](configuration.zh-CN.md)

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
are positive and normally bounded by u32. Aggregate byte ceilings may exceed
4 GiB, within Tokio's semaphore representation and JSON's exact integer range;
individual packet/allocation limits retain their tighter bounds. `secret_hex`
is writeOnly and 64 hex digits.
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

## Connection memory admission

`limits.connection_memory_reservation` defaults to 16 KiB and reserves a conservative
fixed allowance when the socket is admitted. It does not allocate that many
bytes or represent exact heap, RSS, TLS or kernel socket memory. MQTT session,
auth, command and event state retain their independently bounded accounts.

Reader allocation capacity is charged on demand against
`max_read_buffer_per_connection`, `max_connection_bytes_per_tenant` and
`global_connection_logical_bytes`. The default reader ceiling is 65,540 bytes,
including a maximum 65,536-byte TCP frame and its four-byte length envelope.
Decoded frames that share an allocation keep its charge until their last alias
is dropped. The per-connection ceiling applies to all stable retained capacity.
An exclusive synchronous replacement may temporarily hold up to twice that
ceiling; both old and new allocations must fit the unchanged global and tenant
budgets before either is changed. A shared old allocation receives no replacement
exemption. Prefixes of at most eight bytes can use bounded stack staging after
reserving the final capacity, avoiding two simultaneous heap buffers.
Admission fails immediately when a count or byte ceiling is exhausted.

The global connection account combines fixed allowances and live reader capacity;
the tenant account does the same after authentication. Pre-authentication bytes
still consume the global account and must fit the tenant account when the
connection authenticates. Increasing the maximum connection count requires sizing
both byte ceilings, the per-protocol/IP/tenant counts, and other protocol state
limits. `max_tls_handshakes` independently bounds concurrent TLS handshakes
(default 64, maximum 65,536); excess connections close without waiting for a slot.
Deployment capacity requires measured process and kernel memory.

The fixed connection admission allowance has a validated 16 KiB minimum, independent of maximum frame size. Actual reader capacities are admitted on demand; logical quotas are not RSS or kernel socket measurements. See [connection density audit](performance/connection-density-20261013.md).

## Vendor numeric fields

Since `v0.3.4`, integer value fields use string parameters:
`"conversion":{"mode":"decimal_exact","scale":"0.1","bias":"0"}`. Omit
`conversion` for exact identity; never include legacy integer `scale/bias`, even
`null`, `1` or `0`. Float `f32/f64` fields keep numeric `scale/bias`; explicit null
still means omission. Decimal output must fit both 64 public bytes and
`CodecLimits.field_bytes`. `source_message_id` is independent identifier metadata,
with full unsigned `u64` support or the public 64-byte identifier rules, rather
than Scalar output-length rules. See [vendor codecs](vendor-codecs.md) and
[migration](migration/integer-decimal.md).

`config check` reports recognized obsolete integer members as `NBI-CFG-016` with
the member path and migration help. Schema describes structure; deserialization
rejects obsolete members and compilation proves layout/numeric/resource rules.

## Current Business RPC configuration

`business_tcp` accepts Business RPC V4 only and requires explicit `business_rpc`.
Current fields include `limits`, `send_ahead`, `experiment_socket_send_buffer_bytes`,
TLS/identities, development token environment and connection/auth limits. Old
`version`, `allow_v1`, `v3`, `v3_send_ahead` and
`v3_experiment_socket_send_buffer_bytes` are unknown fields. Production uses mTLS
with mapped principals; plaintext is restricted to loopback development tokens.
See [upgrade requirements](migration/current-protocol-only.md) and
[Business RPC V4](business-rpc.md).
