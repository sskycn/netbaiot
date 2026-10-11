# Codec comparison evidence

These compact measurements cover JSON V1, CBOR V1, MessagePack V1 and Protobuf V1
at their recorded source revisions. They are subsystem and short socket regression
results, not current production capacity.

| Campaign | Results | Environment | Source and binary provenance |
| --- | --- | --- | --- |
| macOS ARM64 comparison | [summary](summary.json) | [environment](environment.json) | [manifest](manifest.json) |
| Linux x86_64 follow-up | [summary](linux-followup-summary.json) | [environment](linux-followup-environment.json) | [manifest](linux-followup-manifest.json) |

Compare matching harness/configuration and workloads within a campaign. Network
results include protocol receipts and a confirmed test webhook; codec microbenchmarks
and allocation probes use separate definitions. Summary percentiles are medians of
each run's percentiles, not pooled percentiles. They cannot be added across stages.

The Linux follow-up corrects short-run CPU sampling to use procfs clock ticks. Do
not mix it with earlier whole-second Linux `ps TIME` results. Server and generator
share a host, and sampled RSS is not allocation peak or per-connection memory.

Use [the reproduction tools](../../../scripts/perf/README.md) for current commands
and [measurement requirements](../../benchmarks.md) before deployment decisions.
Raw runs remain outside Git; manifests identify their original bytes. Historical
implementation and step-by-step audit documents are available in Git history.
