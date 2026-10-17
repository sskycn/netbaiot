# Documentation status and authority

[中文](documentation.zh-CN.md)

Checked on **2026-10-17** against `origin/main` at `a26ab86`. The workspace in
[Cargo.toml](../Cargo.toml) remains `0.3.3`; the latest public release is
[v0.3.3](https://github.com/sskycn/netbaiot/releases/tag/v0.3.3), published that day.
Documentation and examples follow `main`, which already contains unreleased
integer exact-Decimal work after that tag (`028b358` through `a26ab86`). The
configuration diagnostic fix documented here is also unreleased. Do not infer
release contents from the Cargo version alone. See [integer migration](migration/integer-decimal.md).

## Reading order and authoritative references

| Topic | Reference | Related guide |
| --- | --- | --- |
| First use and project setup | [Quick Start](quick-start.md), [configuration](configuration.md) | [Tutorial (Chinese)](getting-started.md), [CLI](cli.md) |
| Runtime boundaries | [Architecture](architecture.md), [delivery semantics](delivery-semantics.md) | [User guide (Chinese)](user-guide.md) |
| Device wire contracts | [Protocol matrix](protocol-support.md), [device framing](device-protocol.md), [MQTT](mqtt.md) | [Device integration (Chinese)](device-integration-guide.md) |
| Payloads and exact numbers | [Built-in codecs](codecs.md), [vendor codecs](vendor-codecs.md) | [Integer migration](migration/integer-decimal.md) |
| Business and online commands | [Business RPC V3](business-rpc-v3.md), [public protocol](public-protocol.md), [client](client.md) | [Business integration](business-integration.md) |
| Configuration fields/defaults | [Generated Schema](schema/netbaiot-config.schema.json), [generated reference](configuration-fields.md) | [Runnable fixtures](../configs/development.json), [vendor fixture](../configs/vendor-codec.json) |
| Security and operations | [Management auth](management-auth.md), [security](security.md), [resource budgets](resource-budgets.md) | [Operations (Chinese)](operations-guide.md), [troubleshooting (Chinese)](troubleshooting.md) |
| Recovery and upgrades | [Restart spool](restart-spool.md), [MQTT recovery](mqtt-session-recovery.md), [protocol migration](migration/current-protocol-only.md) | [Graceful restart](graceful-restart.md) |
| Tests and measurements | [Maintenance](maintenance.md), [benchmarks](benchmarks.md), [production validation](production-readiness.md) | [Evidence policy/index](performance/README.md), [reproduction scripts](../scripts/perf/README.md) |

Paired English/Chinese references share contracts and link to each other. Some
longer guides retain Chinese content under unqualified filenames; this is a
filename convention, not a second English specification. Short topic overviews
link to the detailed references above instead of defining different behavior.

## Preserved history and reproducible checks

All release notes in `docs/releases/`, compatibility guides in `docs/migration/`,
and measurements/manifests/validation records in `docs/performance/` are retained.
Their versions, source hashes and measured windows define their scope. Statements
such as “current” inside a dated report mean its recorded revision, not today's
`main`. Do not rewrite raw evidence or replace its hashes when editing current
prose. No historical files were deleted in this audit.

```bash
python3 scripts/check_docs.py --inventory target/docs-audit/inventory.json
cargo xtask schema --check
cargo xtask config-reference --check
cargo xtask check evidence
cargo xtask check release --part preflight
```

The inventory enumerates every root/docs/configs/scripts/.github Markdown file
and every JSON fixture under `configs/`, classifying current references, generated
reference, compatibility guidance and historical evidence. The checker verifies
local Markdown targets and heading anchors; it does not validate external URLs,
remote branch contents or semantic claims. Preflight runs this check and its unit
tests. Run runnable server examples through `netbaiot config check`; resource-only,
standalone codec and complete loadgen fixtures use their respective typed parsers,
as in the CLI Schema and load-tool tests. Command-readiness profiles are harness
inputs rather than standalone configs; see the [fixture inventory](../configs/README.md). Regenerate Schema/reference only when Rust
configuration descriptions or structure actually change.

A successful CI job, short smoke, loopback benchmark and completed long soak are
separate evidence categories. None alone certifies production capacity. Required
sink ACK, device EventAccepted, planned-restart replay and abrupt-crash loss remain
distinct. Commands require a live local session; multi-node routing is not provided.
