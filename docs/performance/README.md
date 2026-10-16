# Performance evidence policy

Keep reviewed measurements with their source revision, workload, duration,
configuration, environment and executable SHA256. Record what was actually run
and what remains unverified. A configured limit or short regression is not a
production capacity result; see [measurement requirements](../benchmarks.md).

This directory retains compact [codec comparisons](codec-audit/README.md) and
[capacity/stability results](../production-readiness.zh-CN.md). Superseded experiments,
process logs and task reports are available in Git history rather than the active
documentation tree.

Current MQTT measurements: [publish and Trie](mqtt-publish-20261015/README.md),
[uplink routing and Retained](mqtt-retained-routing-20261016/README.md).

Current TCP measurements: [binary EventAccepted ACK v1](tcp-ack-20261016/README.md).

Current UDP measurements: [profiling, capacity and optimization rollbacks](udp-20261016/README.md).

Write raw logs, per-request samples, traces and heap/perf captures under ignored
`target/performance/` or `target/evidence/`. CI artifacts have limited retention;
use reviewed release assets when permanent external evidence is required.

Before committing evidence, remove local paths with `scripts/scrub_paths.py` and
review it for secrets. Path scrubbing does not redact credentials. Never publish
live gateway configurations, device payloads or private recovery records.

`cargo xtask check evidence` enforces the repository evidence budget: JSON files
at most 256 KiB, individual files at most 1 MiB, and tracked evidence at most
20 MiB. The required `evidence-allowlist.json` has no current exceptions. Keep
source/environment manifests beside their summaries; remove superseded copies.
