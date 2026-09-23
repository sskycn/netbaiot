# Goal

> Historical measurements from the revision stated below. Device HTTP has since
> been removed; see the [current migration and verification report](remove-device-http.md).

Determine the highest repeatable stable QoS1 ingress rate with exactly 256 MQTT
application payload bytes, plaintext, normal EventAccepted semantics, a minimal
downstream path, and independent server/load-generator hosts.

**BENCHMARK BLOCKED BY PRODUCTION ISSUE.** This is an intentional production
configuration constraint, not a discovered runtime correctness defect. The
current server forbids non-loopback plaintext MQTT and restricts the development
AuditSink configuration to loopback listeners. The requested combination cannot
start through the production composition entry point. The task explicitly
requires stopping if production changes would be necessary; no policy was
changed or bypassed.

**SEPARATE-HOST MEASUREMENT NOT EXECUTED.** There is no new capacity baseline,
server knee, overload plateau, measured first saturated resource, or matched
same-host distortion percentage. The requested capacity validation remains
incomplete. Date: 2026-09-22.

# Topology

Candidate Host A is physical Mac `X`; candidate Host B is the reachable
`vm-dev` cloud VM, `mail.gostartkit.com`. They are different machines and not
on the same LAN. A uses Wi-Fi, not its inactive Ethernet port. B has virtual
network interfaces. A routed Internet data path would be required.

| Reachability requirement | Actual evidence |
|---|---|
| A can reach B control plane | SSH inventory succeeded through the SOCKS proxy in `/opt/local/bin/vm-dev` |
| A can reach B directly | Not validated in this task |
| B can reach A | Not validated; no inbound data path or firewall rule was established |
| B can reach NetbaIoT MQTT on A | Not validated; compliant remote plaintext configuration is rejected before bind |

SSH uses `afxcn@45.76.70.90` with
`ProxyCommand=nc -x 127.0.0.1:1080 %h %p`. That loopback address is a local
control-plane proxy, not a benchmark destination. No SSH data tunnel, plaintext
forwarder, TLS substitute, reverse proxy, or global firewall change was used.
The only server started by the preflight used loopback and received no MQTT
publish workload. Therefore there are **no primary test IPs**.

# Hardware

| Property | Candidate server A | Candidate loadgen B |
|---|---|---|
| Host | `X`, Mac mini `Mac16,10` | `mail.gostartkit.com` |
| Placement | Physical Mac | Full VM, Microsoft hypervisor reported |
| CPU | Apple M4 | Intel Broadwell, no TSX, IBRS, about 2394 MHz guest clock |
| Cores | 10 physical/logical, 4 performance + 6 efficiency | 1 vCPU, guest reports 1 core/1 thread; physical host topology unknown |
| RAM | 17,179,869,184 B (16 GiB) | 474,640,384 B (452.65 MiB) |
| OS | macOS 26.6.2, build 25G83 | Rocky Linux 8.10 |
| Kernel/architecture | Darwin 25.6.0, arm64 | 4.18.0-553.144.1.el8_10.x86_64, x86_64 |
| Active NIC | `en1`, Wi-Fi | `enp1s0`, virtual; `enp8s0`, private virtual |
| IPv4 | `192.168.2.128/24` | `45.76.70.90/23`; private `10.5.96.3/20` |
| Observed global IPv6 (one stable address) | `2408:8456:c33:c544:20da:2280:e439:3` | `2001:19f0:6001:dd:5400:ff:fe58:e76` |
| Link speed | Unknown; `en0` Ethernet inactive | Unknown; both sysfs speed values are `-1` |

VM memory available during refreshed inventory was approximately 318 MiB.
Neither dedicated VM CPU allocation nor network throughput/headroom was proven.
Small VM resources are a limitation to investigate, not measured saturation.

# Software

The authoritative starting local HEAD was
`286da293b30a06c77ed017d5466f1d093186c25e`, with a clean working tree. Work is
on `codex/dual-host-qos1-validation`, created from that HEAD. No reset,
cherry-pick, or merge of the reference SHAs was performed.

| Reference | SHA |
|---|---|
| Correctness | `ce07b5b126b04f7ae95749c46e0263c7514fe491` |
| Initial performance | `70b855f1a97ebe1222dc0212fe0847e91b07cf4b` |
| Broker-route runtime | `cbbad11d827d3d8e04160819ab72378f6d9ae9a9` |
| EventBus evidence, REVERT | `43d76c3ccb7271b6b82b1c058c3f879c383ebd22` |
| EventId evidence, REVERT | `0a50ee04c45c5f7fe0f9790db08b0c0a67d327e1` |
| Separate-host preparation/open-loop evidence | `d718c0c50bcaa7f9640f16ad36a85f1b318e73bc` |

The open-loop reference is on another local branch/worktree and is not an
ancestor of this starting HEAD. It was inspected as historical evidence, not
silently treated as the current load generator. No loadgen was deployed or run
on B in this task; there is no remote loadgen build SHA or binary checksum.

Host A ran `cargo build --release --locked` successfully with Rust 1.97.1
(`8bab26f4f`), Cargo 1.97.1 (`c980f4866`), LLVM 22.1.6, default workspace
features, empty `RUSTFLAGS`, and no special CPU flags. The preflight server uses
unchanged production source at starting HEAD. Its SHA256 is
`31f877c61e6e48165bcc187be273ae36de1e108d202933088ae36c8eb1baeaa9`.
The build is only evidence for configuration validation, not throughput.
B reports Python 3.6.8; no remote benchmark build was attempted.

# Server configuration

`Config::validate` in `apps/netbaiot-server/src/lib.rs` enforces three relevant
boundaries before constructing listeners or sinks:

1. Development mode requires every device/management listener to be loopback.
2. Non-loopback device HTTP, MQTT or TCP requires TLS.
3. Production mode requires an HTTP delivery URL or confirmed business TCP sink.

The existing regression
`public_streams_require_tls_and_volatile_store_requires_loopback` also protects
this policy. The minimal AuditSink is a valid loopback development path; selecting
production mode alone does not provide a remotely accessible plaintext version.

The release reproduction starts from `configs/development.json`, changes its
five listener ports to 24000..24004, uses a temporary owned restart directory,
and leaves all resource limits at their defaults. Only the specified fields
below differ between cases. Management remains loopback in every case.

| Startup case | Changes from loopback development fixture | Observed result |
|---|---|---|
| Positive control | None | Owned process logged `runtime ready`; graceful termination returned 0 |
| Remote development plaintext | MQTT `0.0.0.0:24002` | Exit 1, `Error: Configuration` |
| Remote production plaintext, IPv4 | Development false, MQTT `0.0.0.0:24002`, HTTP sink configured | Exit 1, `Error: Configuration` |
| Remote production plaintext, IPv6 | Same, MQTT `[::]:24002` | Exit 1, `Error: Configuration` |
| Production without required sink | Development false, all listeners loopback, no sink | Exit 1, `Error: Configuration` |

The configured HTTP URL is `http://127.0.0.1:24006/events`; it is not contacted,
because invalid configuration returns before sink construction. This isolates
the TLS requirement from the missing-sink requirement. `RUST_LOG=info` is used
only to observe startup readiness, with no capacity workload or per-message
logging. Both IPv4 and IPv6 wildcard binds are rejected; using a specific
non-loopback address is subject to the same `is_loopback` predicate.

Reproduce from this checkout:

```bash
cargo build --release --locked
python3 scripts/perf/dual_host_preflight.py
```

The tool serializes its own fixed-port startup control, observes readiness from
the owned child, bounds startup/log capture, stops owned children, and writes
JSON. It never reserves a port then releases it for another process. A port
conflict makes the positive control fail instead of being reported as readiness.

Compact committed evidence: [preflight JSON](performance-dual-host-qos1-preflight.json).
Raw configs/logs are included in
`target/perf-audit/dual-host/preflight.json`; hardware inventory and validation
logs share that directory. Configs contain only existing public demo fixtures.

# Loadgen validation

Not run on B. The requested 20k/30k/40k/50k/60k/75k/100k pacing accuracy,
missed slots, maximum accurate attempted rate, CPU and RSS are all unknown.
The intended primary configuration would be 256 publishers with a finite
32-per-publisher inflight limit (8192 aggregate), but it was **not executed**.
No current payload serialization, topic size or packet byte count was measured.
256 B is a requested payload size, not a measurement from this task.

# Same-host controls

No new matched same-host throughput controls were run after the primary
configuration blocker was confirmed. Historical evidence at `d718c0c...` used
64 publishers, 256 actual payload bytes, 30 s warmup, 60 s measurement and 15 s
cooldown, with three QoS1 20k repetitions: median accepted 19,983.58/s, server
0.686 CPU core, PUBACK P50/P95/P99 0.17/0.30/0.39 ms, pending 0 and no errors.
That is **not** a matched 256-publisher control for an unexecuted dual-host run.

# Dual-host sweep

| Offered/s | Attempted/s | Accepted/s | P50/P95/P99 | Server/loadgen CPU | Result |
|---|---|---|---|---|---|
| 10,000 | N/A | N/A | N/A | N/A | NOT RUN: configuration blocked |
| 20,000 | N/A | N/A | N/A | N/A | NOT RUN: configuration blocked |
| 30,000 | N/A | N/A | N/A | N/A | NOT RUN: configuration blocked |
| 40,000 | N/A | N/A | N/A | N/A | NOT RUN: configuration blocked |
| 50,000 and higher | N/A | N/A | N/A | N/A | NOT RUN: configuration blocked |

No point is labeled HEALTHY, KNEE, OVERLOAD, LOADGEN-LIMITED or NETWORK-LIMITED:
those classifications require traffic evidence.

# Healthy points

No dual-host healthy point exists. Historical local evidence is preserved above.

# Saturation knee

Unknown. No last healthy point, first unhealthy rate or three-repeat knee
validation exists. The startup rejection is a configuration blocker, **not** a
CONFIG-LIMITED throughput result or measured server capacity.

# Overload point

Not run. Bounded queues, RSS, explicit overload behavior and plateau were not
measured in this task. No production limits were raised.

# Server metrics

No workload CPU, per-core utilization, RSS, FDs, accepted/rejected events,
EventBus pending, MQTT rate or accepted-events/server-CPU-second measurements.
Startup readiness alone does not demonstrate healthy load handling.

# Loadgen metrics

No attempted/completed rates, pacing lag, inflight high-water, ACK percentiles,
timeouts, connection errors, CPU or RSS measurements on B. The VM's 1 vCPU is
not evidence that load generation would saturate before the server.

# Network metrics

No workload RX/TX, retransmits, socket errors, bandwidth or headroom measurements.
Link speeds are unknown. SSH success does not prove a direct MQTT path or
bidirectional reachability. One-way timing was not calculated; any future ACK
latency must be measured entirely at the generator.

# Profiles

None captured: the task requires establishing the true knee first. No old
wall-stack proportions are relabeled as true-knee mutex, EventBus, kernel/Tokio,
allocation/JSON or EventId/getentropy shares.

# Same-host distortion

Unknown. The numerator (dual-host accepted throughput/knee) does not exist, so
neither a capacity ratio nor a distortion percentage can be calculated.

# Previous 25–30k classification

**NOT REPRODUCED in this task; cause remains undetermined.** This does not mean
an equivalent workload succeeded or disproved the earlier observation. The
earlier open-loop preparation found local generator pacing limitations and
invalidated treating 25–30k/s as a proven server-only knee. No new evidence here
supports SERVER-SIDE, LOADGEN-SIDE or MIXED as the true isolated-server answer.

# First saturated resource

Unknown. No resource was saturated. The measured obstacle is startup policy,
not SERVER_CPU, SERVER_SERIALIZATION, EVENTBUS_BACKLOG, LOADGEN, NETWORK,
DOWNSTREAM_SINK or another saturated resource. A 30-minute run at 70–80% of an
unknown knee was not attempted; there is no RSS/queue drift or soak claim.

# Updated bottleneck ranking

No new ranking: #1/#2/#3 all remain unestablished for the requested topology.
Historical rankings apply only to their recorded workloads. Broker zero-subscriber
fast path remains KEEP; EventBus scheduling experiment and EventId RNG experiment
remain REVERT. No production Rust source, semantics, resource limits, retries or
runtime workers changed.

# Next recommended task

**D. Benchmark infrastructure still insufficient.** Resolve the experiment's
incompatibility with the existing supported listener/security and sink policies
before spending time on a capacity sweep. A separately authorized TLS experiment
with an explicit required sink would be a different workload and needs its own
baseline; weakening production validation is not part of this audit. Then prove
the direct data path in both directions and validate generator pacing on the VM
before claiming any server knee. No EventBus, allocation/copy or kernel/runtime
optimization is justified by this blocked experiment.

The definition of done for trusted dual-host capacity has **not** been met. This
commit records the blocked validation and reproducible preflight only; it does
not claim to establish capacity.

## Correctness validation

Rust 1.88.0 and stable 1.97.1 each passed `fmt --all -- --check`, locked
workspace/all-targets/all-features Clippy with `-D warnings`, and locked
workspace/all-features tests (138 passed on each toolchain). MQTT core
`python3 tests/mqtt_conformance/run.py --netbaiot-only` passed all 31 cases.
The release startup preflight passed all five cases. Commands and exit codes
are retained in the accompanying JSON evidence and raw logs.

The full release gate was not run because production code did not change.
Nothing was pushed.
