# Documentation

[中文](README.zh-CN.md) · [Project overview](../README.md)

Start with [Quick Start](quick-start.md), then use the reference for the boundary
you integrate. Configuration fields and defaults are generated from Rust; the
[Schema](schema/netbaiot-config.schema.json) and [field reference](configuration-fields.md)
are authoritative for configuration structure.

| Task | References |
| --- | --- |
| Run and configure | [Quick Start](quick-start.md), [configuration](configuration.md), [CLI](cli.md), [doctor](doctor.md), [config fixtures](../configs/README.md) |
| Understand the runtime | [Architecture](architecture.md), [delivery semantics and EventBus](delivery-semantics.md), [resource budgets](resource-budgets.md) |
| Connect devices | [Protocol matrix](protocol-support.md), [MQTT](mqtt.md), [device framing](device-protocol.md), [device SDK](device-sdk.md) |
| Decode payloads | [Built-in codecs](codecs.md), [vendor codecs](vendor-codecs.md) |
| Integrate business systems | [Business integration](business-integration.md), [Business RPC V4](business-rpc.md), [Rust client](client.md), [public protocol](public-protocol.md) |
| Manage and secure | [HTTP API](http-api.md), [management auth](management-auth.md), [auth cache](authentication-cache.md), [control plane](control-plane.md), [security](security.md), [UDP key provisioning](device-key-security.md) |
| Deploy and recover | [Platform support](platform-support.md), [graceful restart](graceful-restart.md), [restart spool](restart-spool.md), [MQTT recovery](mqtt-session-recovery.md) |
| Validate and maintain | [Maintenance](maintenance.md), [MQTT conformance](mqtt-3.1.1-conformance.md), [benchmarks](benchmarks.md), [production validation](production-readiness.md), [measurement evidence](performance/README.md), [reproduction scripts](../scripts/perf/README.md) |

The [end-to-end tutorial](getting-started.md), [device integration guide](device-integration-guide.md),
[operations guide](operations-guide.md) and [troubleshooting](troubleshooting.md)
are Chinese guides under unqualified filenames. Paired English/Chinese references
share the same contracts.

## Versions and upgrades

Documentation follows `main`; the workspace version is `0.4.0`. The
[v0.4.0 release](releases/v0.4.0.md) requires Business RPC V4 and version-independent
Rust API, configuration and monitoring names. Upgrade business clients together
with the gateway; V3 and earlier handshakes are rejected.

Exact integer Decimal conversion and configuration diagnostics were introduced in
[v0.3.4](releases/v0.3.4.md) and remain part of `v0.4.0`. When upgrading from
`v0.3.3` or earlier, follow the [integer conversion migration](migration/integer-decimal.md)
before loading an existing vendor codec configuration.

Only Business RPC V4, MQTT recovery NBMQ v6 and EventBus recovery NBSP v3 are
supported. Old or unknown versions fail explicitly without fallback or migration;
read the [upgrade guide](migration/current-protocol-only.md). MQTT 3.1.1/5.0 and
management `/api/v1` remain supported.

The current [release notes](releases/v0.4.0.md), preceding
[v0.3.4](releases/v0.3.4.md) and [v0.3.3](releases/v0.3.3.md) notes, and
[release template](release-template.md) remain in the tree. Other release history
is available through Git and [GitHub Releases](https://github.com/sskycn/netbaiot/releases).

## Keeping documentation small

Keep one reference per topic and link to it from guides. Keep runnable examples,
generated configuration references, compatibility guidance and measurement
provenance. Remove duplicate overviews, completed task reports and obsolete prose;
Git history preserves removed material. Retain source hashes, manifests and
validation records supporting published measurements, and interpret dated results
only at their recorded revisions. Raw new logs belong under ignored `target/`.

Run the [documentation checks](maintenance.md#documentation-checks) after edits.
They validate local links/anchors and generated-reference drift; external URLs,
semantic claims and production capacity require separate review and evidence.
