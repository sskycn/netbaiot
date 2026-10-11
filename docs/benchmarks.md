# Benchmarks and measurements

[中文](benchmarks.zh-CN.md)

The [2026-10-08 engineering optimization report](engineering-optimization.md)
records incremental EventBus responsibility accounting, borrowed HTTP JSON
serialization, bounded HTTP outage recovery and a MQTT session matrix. Its
curated measurements are in `docs/performance/engineering-optimization/`.

The repository preserves measured performance, resource, and reliability
experiments. They are evidence for the stated setup only. Several measurements
belong to historical commits and must not be read as capacity for the current
revision or a production deployment.

## Engineering hotspot campaign

The [engineering optimization report](performance/engineering-hotspots/report.md)
records route correctness, presence/rate-table complexity, MQTT borrowing/shared
payloads, EventBus ready/deadline queues, session scan evaluation, and a physical
broker module split. It includes release A/B matrices, allocation/lock/RSS data,
MQTT/UDP and timeout/recovery checks, and an explicit list of unrun work. The
fixed-load measurements are regression evidence for that setup, not production
capacity. Session usage and expiry scans remain unchanged after evaluation.

## Published baseline context

The primary historical MQTT baseline identifies this setup:

| Item | Recorded setup |
| --- | --- |
| Hardware | Mac mini `Mac16,10`, Apple M4 10-core CPU, 16 GiB RAM |
| OS | macOS 26.6.2, Darwin 25.6.0, arm64 |
| Build | Locked Cargo release build; Rust 1.88.0 baseline toolchain |
| Network | One host, IPv4 loopback; server and load generator share the host |
| MQTT publishers | 64 for the primary QoS throughput points |
| Payload | 256-byte generated JSON payload |
| Sink | In-process required audit sink for the primary MQTT table; no external HTTP sink |
| Duration | 5 s warm-up, 20 s measured interval, 2 s cooldown; three repetitions |
| TLS | The primary QoS table does not label a TLS profile. TLS connection-memory and separate throughput experiments are labeled in the detailed report. |
| Latency | QoS1 PUBACK and QoS2 PUBCOMP percentiles are reported separately; QoS0 has no protocol ACK latency. |

The primary table reports median completed rates of 24,159.9/s for QoS0,
19,956.4 PUBACK/s for QoS1, and 19,992.8 PUBCOMP/s for QoS2 on its measured
baseline commit. These are historical same-host results, not service-level
objectives or production limits. The report records the commit, repetitions,
tail latency, CPU, RSS, pending work, and later experiments.

## What these numbers do not mean

- They are not a production capacity guarantee or a promise for another revision.
- The load generator shares CPU and loopback with the server, so the test does not
  isolate a remote server ceiling.
- Localhost network behavior does not represent a physical network, WAN, packet
  loss, NAT, or a multi-host deployment.
- TLS, payload size, publisher count, QoS, retained routing, fanout, and business
  sink latency change CPU, memory, and tail latency.
- An in-process audit sink is not a measurement of a customer's database, webhook,
  or remote RPC service. The separately measured Python HTTP sink can become the
  bottleneck.
- A burst result, microbenchmark, configured maximum, or historical database-era
  result is not a current production capacity claim.

## Known measurement limits and hotspots

The latest separate-host audit in the baseline document was preparation plus
single-host controls; no new dual-host capacity result was established. The
historical shared-host load generator also approached its own CPU limit at higher
offered rates. The measured Python webhook path is sink-bound before the gateway
path in its tested configuration. MQTT wildcard retained replay scans the bounded
retained store. The detailed reports state what was not measured, including
several larger fanout, dual-host, and deployment-specific combinations.

## Detailed records

- [Performance baseline and historical results](performance-baseline.md)
- [Separate-host audit preparation and controls](performance-separate-host-audit.md)
- [Mixed ingress capacity audit](mixed-ingress-capacity-audit.md)
- [Reproduction scripts](../scripts/perf/README.md)

Before publishing or using a number, read the relevant report's measurement SHA,
transport/TLS mode, load, sink, time window, and caveats. For a deployment decision,
rerun the workload on the target hardware and network with the intended TLS,
authentication provider, event fanout, and business sink.
