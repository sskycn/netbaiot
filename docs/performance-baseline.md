# Performance baseline

## Latest experiment: EventId (2026-09-22)

**EXPERIMENT REVERTED — insufficient end-to-end gain.** Starting HEAD was
`43d76c3ccb7271b6b82b1c058c3f879c383ebd22`. A thread-local, OS-seeded ChaCha12
candidate preserved UUIDv4 and reduced direct single-thread generation from
643.87 to 14.19 ns/ID (97.80%). Fifty million IDs across five thread topologies,
100 process starts and a separate two-million-ID overlap test found no duplicates.

Fresh primary QoS1 20k medians were 19,998.80 -> 19,998.65 accepted/s,
14.67 -> 13.78 CPU seconds, and 0.43 -> 0.32 ms P99. However, three alternating
confirmation pairs gave 19,998.20 -> 19,953.60/s, 14.27 -> 13.70 CPU seconds, and
0.47 -> 1.04 ms P99. The nominal CPU threshold passes, but the no-tail-regression
condition does not. The 25--30k knee did not materially improve. The candidate
was removed; no EventId optimized baseline or production capacity claim exists.

[The full report](performance-eventid-experiment.md) includes the contract/security
review, all repetitions, CPU/profile definitions, collision bounds and gates.
Final runtime still uses `Uuid::new_v4` per EventId. Baseline terminology remains
correctness `ce07b5b...`, initial performance `70b855f...`, broker-route runtime
`cbbad11d...`, EventBus evidence `43d76c3...`, plus this EventId evidence commit.

## Latest experiment: EventBus (2026-09-22)

**REVERT — insufficient measurable gain.** The single failed-poll/deadline-scan
fusion candidate reduced QoS1 20k state acquisitions from 4.526 to 3.766/event
(16.8%), but full-state wait sum fell only 3.9% and wait P99 stayed <=100 us.
Accepted throughput was 19,969.35 -> 19,965.00/s; PUBACK P99 improved from
1.60 to 1.38 ms. The 25--30k/s shared-host knee did not improve. Sampled peak
CPU rose 6.3%; there is no retained EventBus optimization or new capacity claim.

Fresh comparisons used identical opt-in instrumentation on the starting
`cbbad11d827d3d8e04160819ab72378f6d9ae9a9` behavior and the candidate, Rust
stable 1.97.1 release builds, three 20-second primary repetitions, 64 publishers,
256-byte payloads and concurrency eight. These are separate from the historical
Rust 1.88 numbers below. [The experiment report](performance-eventbus-experiment.md)
contains complete lock/wake definitions, auxiliary workloads, and gate results.

Baseline names remain: correctness `ce07b5b...`, initial performance `70b855f...`,
broker-route optimized `cbbad11d...`. No EventBus optimized baseline was created.

## Scope and result status

This document records a release-build, single-host loopback baseline for commit
`ce07b5b126b04f7ae95749c46e0263c7514fe491`, measured on 2026-09-21. The audit
changes add measurement support only; the final commit SHA is intentionally left
to the Git history rather than substituted into the measurement SHA.

The headline result is a conservative, repeatable healthy region of 24.16k/s for
MQTT QoS0, 19.96k/s for QoS1, and 19.99k/s for QoS2 with 64 publishers and 256-byte
JSON payloads. The single-host saturation knee is approximately 25--30k/s. These
are not production capacity claims: the server and load generator shared one Mac,
one loopback interface, and the load generator consumed 70--93% of one core at the
healthy points.

Raw results and the macOS `sample` profile are under `target/perf-audit/` and are
not committed. Historical files under `docs/performance/` describe the retired
database architecture and were not used as evidence.

## Environment

| Item | Value |
|---|---|
| Measurement SHA | `ce07b5b126b04f7ae95749c46e0263c7514fe491` |
| Host | Mac mini `Mac16,10` |
| CPU | Apple M4, 10 cores (4 performance + 6 efficiency), no SMT |
| RAM | 16 GiB |
| OS | macOS 26.6.2 (25G83), Darwin 25.6.0, arm64 |
| Filesystem | APFS, local journaled system volume |
| Rust MSRV toolchain | rustc/cargo 1.88.0, LLVM 20.1.5, `aarch64-apple-darwin` |
| Current stable | rustc/cargo 1.97.1, LLVM 22.1.6 |
| Build | Cargo `release`, locked workspace dependencies |
| Network | One host, IPv4 loopback; no physical-NIC or remote-load-host result |
| TLS | `tokio-rustls` 0.26, `ring`, TLS 1.2 enabled; RSA-2048 fixture certificate |
| FD limits | process soft limit 1,048,575; `kern.maxfilesperproc=61,440` |
| TCP limits relevant here | ephemeral range 49,152--65,535 (16,384 ports), `somaxconn=128` |

## Method

The major stable throughput points used 5 s warmup, 20 s measured traffic, 2 s
cooldown, three repetitions, 64 publishers, no subscribers, 256-byte generated
JSON payloads, and the in-process required audit sink. The table reports the
median run; rate variation is `(best - worst) / median`. Discovery sweeps used
10--15 s intervals. This is shorter than a production capacity certification and
is deliberate: this document is an initial baseline with explicit confidence
bounds, not an SLA.

Connection probes used three fresh server processes per repeatable point and held
connections long enough to sample stable RSS. RSS/connection is the delta from
that process's pre-connection RSS, not an object-size claim. macOS allocator and
kernel high-water behavior remain in the measurement.

Lock histograms use fixed microsecond buckets (`10, 25, 50, 100, 250, ...`), so
reported quantiles are bucket upper bounds, not interpolated values. Timing was
recorded only in successful publish paths and has no labels or per-event logging.
The performance drivers opt in with `NETBAIOT_PERF_LOCK_METRICS=1`; production
defaults to no broker/EventBus lock-clock reads.

## Correctness and build baseline before measurement

The following all passed before the campaign:

- Rust 1.88.0 format check, workspace/all-target/all-feature Clippy with warnings
  denied, and workspace/all-feature tests.
- Current-stable workspace/all-target/all-feature Clippy and workspace/all-feature
  tests.
- NetbaIoT-only MQTT conformance: 31/31 cases.
- Locked release workspace build.

The required post-change gate is recorded in the final section after it is rerun.

## Idle connection scaling

All values except the one 10k row are medians of three successful runs. CPU and
connect-latency histograms were not emitted by this connection-memory probe and
are marked unavailable rather than inferred.

| Transport/state | Requested | Active | Loaded RSS | Delta RSS/conn | FDs (base -> loaded) | Tokio tasks (base -> loaded) | Errors | Confidence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| MQTT plaintext | 1,000 | 1,000 | 30,688 KiB | 24,805 B (24.22 KiB) | 17 -> 1,017 | 7 -> 1,007 | 0 | 3/3 |
| MQTT plaintext | 3,000 | 3,000 | 77,232 KiB | 23,424 B (22.88 KiB) | 17 -> 3,017 | 7 -> 3,007 | 0 | 3/3 |
| MQTT plaintext | 10,000 | 10,000 | 252,688 KiB | 23,737 B (23.18 KiB) | 17 -> 10,017 | 7 -> 10,007 | 0 | 1/3 usable |
| MQTT TLS | 1,000 | 1,000 | 39,552 KiB | 32,916 B (32.14 KiB) | 17 -> 1,017 | 7 -> 1,007 | 0 | 3/3 |
| MQTT TLS | 3,000 | 3,000 | 100,512 KiB | 31,064 B (30.34 KiB) | 17 -> 3,017 | 7 -> 3,007 | 0 | 3/3 |

The second and third immediate 10k plaintext attempts established only 6,000 and
318 sockets because prior loopback connections occupied the host's 16,384-port
ephemeral range/TIME_WAIT pool. They are excluded from per-connection statistics.
For the same reason 30k and 50k cannot be tested from one loopback source address.
The maximum verified point is therefore 10k plaintext (one successful run), while
the maximum repeatable point is 3k plaintext and TLS. This is a host test-topology
limit, not a measured NetbaIoT connection ceiling.

The configured connection logical admission reservation was 512 KiB/connection at
1k and 3k. The 10k probe used 64 KiB/connection so the aggregate configuration fit
the runtime's bounded representation. These are admission ceilings, not allocated
RSS and not measured live object sizes.

### TLS idle increment

TLS added a median 8,110 B/connection at 1k and 7,640 B/connection at 3k relative
to plaintext. The 3k value is the better scale-matched estimate: about 7.46 KiB per
idle connection.

## Publisher-count scaling at fixed aggregate load

These are single diagnostic runs at nominal 10k QoS1 publishes/s, 15 s measured,
with no external sink. They establish scaling direction, not a capacity percentile.

| Publishers | Connected | Completed/s | PUBACK P50/P95/P99 | Server peak CPU | Loadgen peak CPU | Peak RSS | Notes |
|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1 | 780 | 0.07/0.08/0.14 ms | 1.7% | 1.5% | 5,408 KiB | Client window 4 limits one publisher |
| 100 | 100 | 9,999 | 0.12/0.23/0.47 ms | 85.7% | 53.3% | 8,688 KiB | Full offered rate |
| 1,000 | 1,000 | 9,999 | 0.12/0.20/0.31 ms | 40.8% | 21.5% | 31,984 KiB | Full offered rate; one run |

The initial 1,000-publisher attempts stopped at exactly 512 authenticated broker
sessions because the benchmark config retained the default persistent-session
ceiling. Scaling that ceiling made all 1,000 connect. This is why benchmark limits
must be reported with results.

The 1,000-publisher active run peaked at 31,984 KiB versus the 30,688 KiB median
loaded RSS of the 1,000-idle probe: a 1,296 KiB process difference, or about
1.30 KiB/connection. Treat this only as a diagnostic upper-bound comparison;
the two harness configurations and allocator histories are not identical, so it is
not a precise active-state object cost.

## MQTT throughput

The required audit sink remains part of the EventAccepted path, but there is no
external HTTP sink in this table. `Pending max` is sampled required work, not a
monotonically growing backlog.

| QoS | Offered/s | Median completed/s | Best | Worst | Variation | Protocol latency P50/P95/P99 | Server peak CPU | Loadgen peak CPU | Peak RSS | Pending max | Errors/rejects |
|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---|
| 0 | 25,000 | 24,159.9 publishes | 24,185.0 | 24,098.0 | 0.36% | no protocol ACK | 150.1% | 71.1% | 8,128 KiB | 42 | 0 |
| 1 | 20,000 | 19,956.4 PUBACKs | 19,985.3 | 19,925.6 | 0.30% | 0.30/0.99/1.53 ms | 131.8% | 78.6% | 8,192 KiB | 25 | 0 |
| 2 | 20,000 | 19,992.8 PUBCOMPs | 19,993.6 | 19,991.8 | 0.009% | 0.58/1.48/2.32 ms | 163.6% | 90.4% | 8,320 KiB | 29 | 0; 7 window-full schedules in one run |

QoS0 has no broker protocol acknowledgement, so generator schedule-lag
(median run P50/P95/P99 1.25/2.06/2.36 ms) is not presented as server ACK latency.
QoS2 latency covers PUBLISH through PUBCOMP and therefore two protocol round trips.

### Saturation sweep

| Scenario | Result |
|---|---|
| QoS0 10k | 9,999/s completed |
| QoS0 offered 25k | 24.19k/s discovery; 24.16k/s stable median |
| QoS0 offered 50k | 28.04k/s |
| QoS0 offered 75k | 31.39k/s |
| QoS1 10k / 20k | Full offered load |
| QoS1 offered 30k | 27.09k/s |
| QoS1 offered 40k | 29.12k/s |
| QoS2 5k / 10k / 20k | Full offered load |
| QoS2 offered 30k | 27.83k/s; 82 client-window-full schedules |

The knee is 25--30k/s on this shared host. The overload values are absolute
single-host loopback plateaus, not stable production throughput. The generator's
bounded windows and shared CPU cause offered scheduling to flatten; this campaign
did not force an unbounded server queue or produce server admission rejection.
Consequently the measured stable points are conservative lower bounds, and a
separate load host is required to find the server-only ceiling.

## Latency decomposition and lock contention

At 19.9k QoS1/s (199,287 successful publishes over 10 s):

| Stage/lock | Acquisitions/s | Mean wait | Wait P50/P95/P99 | Mean hold | Hold P50/P95/P99 | Highest occupied bucket |
|---|---:|---:|---|---:|---|---|
| Admission accounting | 19.94k | 0.70 us | <=10/<=10/<=25 us | 0.044 us | <=10/<=10/<=10 us | wait <=2.5 ms, hold <=1 ms |
| Broker route state | 19.93k | 5.06 us | <=10/<=25/<=250 us | 0.81 us | <=10/<=10/<=10 us | wait <=5 ms, hold <=1 ms |
| EventBus state | 19.93k | 5.05 us | <=10/<=25/<=250 us | 0.47 us | <=10/<=10/<=10 us | wait <=10 ms, hold <=250 us |

The wait tail is much larger than the protected work. At a 500/s load with a
10 ms sink, EventBus wait averaged 0.002 us while hold averaged 0.70 us, confirming
that the high-load wait is contention rather than intrinsically long EventBus work.
The broker instrumentation covers the `route` critical section only. It does not
claim measurements for attach, subscribe, QoS state transitions, or recovery.

Sessions/Auth were not given equivalent lock histograms. Independent microbench
evidence is: sessions lookup P50 42 ns and P99 <=84 ns from 1 through 256 registry
entries; auth positive-cache hit P50 833 ns, P95 1,083 ns, P99 1,084 ns; local
provider miss P50 103 us. The throughput runs authenticated once per connection,
and no normal publish made a provider call. `Sessions::touch` appeared in 17
top-of-stack profile samples, so it is visible but not proven material.

## Foundation microbenchmarks

Values below are representative of three low-variance release runs except where
noted. The harness is a sampling microbenchmark, so these are distributions rather
than Criterion confidence intervals.

| Operation | P50 | P95 | P99 |
|---|---:|---:|---:|
| EventBus publish | 1,250 ns | 1,541 ns | 2,291 ns |
| MQTT decode, representative payload | 125 ns | 167 ns | 167 ns |
| MQTT encode | 83 ns | 125 ns | 125 ns |
| Topic ACL | 42 ns | 84 ns | 84 ns |
| Subscription lookup | 209 ns | 250 ns | 292 ns |
| QoS1 broker route + ACK transition | 958 ns | 1,000 ns | 1,167 ns |
| QoS2 inbound accept route | 667 ns | 709 ns | 833 ns |
| JSON v1 codec, representative payload | 1,208 ns | 1,250 ns | 1,500 ns |
| Ingress admission | 333 ns | 334 ns | 417 ns |

Size sweeps:

- MQTT decode P50: 125 ns small, 167 ns medium, 1,250 ns maximum payload.
- JSON codec P50: 1.50 us small, 33.3 us medium, 175.9 us maximum. JSON becomes
  important for large payloads even though it is secondary at 256 B.
- Subscription router P50 remained 167 ns at 100, 1,000, and 10,000 entries;
  P99 was 209--250 ns in the tested exact/mixed lookup.
- Retained wildcard scan was 74.9 us at 1,000 and 298.2 us at 4,000 retained
  messages. This linear path matters for wildcard retained replay/query, not the
  no-subscriber uplink hot path.

## Fanout / route preflight

This isolated broker benchmark plans a route; it does not enqueue and drain real
subscriber sockets and therefore has no defensible msg/s or CPU-capacity number.

| Targets | Median plan latency | Best/worst | Compact plan bytes | Approx. bytes/target |
|---:|---:|---:|---:|---:|
| 100 | 22 us | 22/90 us (cold first run) | 10,090 B | 100.9 B |
| 1,000 | 173 us | 160/179 us | 101,890 B | 101.9 B |
| 2,000 | 343 us | 340/361 us | 204,890 B | 102.4 B |

The current production configuration default bounds fanout much lower than these
diagnostic targets. Full 1/10/100/500/1,000 subscriber delivery throughput and
outbound queue growth were not measured in this campaign.

## Persistent and QoS state memory

Disconnected-session RSS retains credentials, auth-cache entries, hash-table and
allocator capacity as well as broker state. Broker logical accounting isolates the
payload-bearing records.

| State | Result |
|---|---:|
| 1,000 disconnected persistent sessions | median delta 25,068 B/session RSS |
| Same plus one subscription/session | median delta 27,722 B/session RSS |
| Increment attributable to subscription experiment | 2,654 B/session RSS |
| Broker logical disconnected session state | about 182 B/session |
| Retained logical bytes | about 146 B/message in tested topic/payload fixture |

Payload-state charges (bytes/record):

| Payload | Offline queued | Outbound QoS1 | Outbound QoS2 | Inbound QoS2 |
|---:|---:|---:|---:|---:|
| 64 B | 147 | 147 | 147 | 145 |
| 256 B | 339 | 339 | 339 | 337 |
| 1 KiB | 1,107 | 1,107 | 1,107 | 1,105 |
| 8 KiB | 8,275 | 8,275 | 8,275 | 8,273 |
| 32 KiB | 32,851 | 32,851 | 32,851 | 32,849 |

These are exact runtime logical-accounting charges for the benchmark topic, not
RSS/transaction or allocation counts. CPU/state-transition and near-bound offline
rejection tests were not run.

## Business HTTP sink

Three repetitions used one confirmed local HTTP webhook, 64 QoS1 publishers,
256-byte payloads, offered 10k/s, 5 s warmup, 20 s measurement, and 3 s cooldown.

| Completed | PUBACK P50/P95/P99 | Event received-to-webhook receipt P50/P95/P99 | Pending max | Server CPU | Loadgen CPU | Sink CPU | RSS |
|---:|---|---|---:|---:|---:|---:|---:|
| 9,999.45/s median | 0.12/0.20/0.23 ms | 1/2/4 ms | 20 | 55.4% | 20.0% | 76.6% | 9,424 KiB |

All 199,988--199,989 events/run received configured 204 ACKs. At this point the
Python webhook consumed more CPU than NetbaIoT, so this is a 10k/s end-to-end
baseline, not the Rust HTTP sink ceiling. The sink's clock-derived latency is
millisecond-granularity. Connection-reuse behavior was not separately counted.

Slow-sink checks completed all accepted work: 5k/s with configured 1 ms delay had
maximum pending 12; 500/s with configured 10 ms delay had maximum pending 6. The
instrumented 10 ms case recorded EventAccepted-to-SinkAck mean 10.659 ms and all
quantiles within the coarse <=25 ms bucket. The final sink-side `rejected=1` in one
10 ms run happened during teardown; server rejection metrics remained zero.

## TLS throughput overhead

Paired current-binary runs used 64 QoS1 publishers, 10k/s, 256 B, no external
sink, 5 s warmup, 15 s measurement, 2 s cooldown, three repetitions.

| Mode | Completed median | Connect P50/P95/P99 median | PUBACK P50/P95/P99 median | Server peak CPU median | Loadgen peak CPU median | Peak RSS median |
|---|---:|---|---|---:|---:|---:|
| Plaintext | 9,999.3/s | 0.28/0.34/0.42 ms | 0.23/0.55/0.86 ms | 82.5% | 52.2% | 7,984 KiB |
| TLS | 9,999.2/s | 3.16/4.16/4.37 ms | 0.26/0.63/1.04 ms | 88.2% | 57.0% | 9,776 KiB |

TLS preserved throughput at 10k/s. Median sampled server peak rose 5.7 CPU points
(6.9% relative), steady PUBACK P99 rose 0.18 ms, and the small-process peak RSS
rose about 1.75 MiB. Connect P50 rose about 2.88 ms. The TLS connect distribution
varied materially (one run P50 0.81 ms; two were 3.16--3.44 ms); it is not a TLS
handshake-capacity result.

## Mixed production-like workload

The committed driver uses 1,000 connections: 70% idle, 20% at 1 msg/s, 9% at
10 msg/s, and 1% alternating quiet/bursty. Its intended average event mix is 60%
QoS0, 30% QoS1, and 10% QoS2. It includes persistent-session reconnects, low-rate
commands/downlink, and confirmed HTTP webhook delivery. Low-frequency retained
updates are not generated by the current load tool.

The final result is in `target/perf-audit/mixed-baseline-30m-final.json`. It
established all 1,000 initial connections, completed 120 persistent-session
resumptions and 180 commands/downlink ACKs, and delivered every accepted event.

| Duration | Connections | Events/s | QoS mix | ACK latency | Server/loadgen/sink CPU | RSS | Errors | Status |
|---|---:|---:|---|---|---|---:|---|---|
| 20 s validation | 1,000 | 1,190 | 60.5/30.3/9.2% | QoS1 P99 not aggregated in this short row | not capacity evidence | not retained | 0 | passed |
| 30 min baseline | 1,000 | 1,199.93 | 60.003/30.001/9.996% | QoS1 0.13/0.22/0.63 ms; burst QoS2 1.04/1.76/2.12 ms | max 25.0% / 8.7% / 37.3% | stable 30,160--33,536 KiB | 0 runtime/client errors | passed |

The run accepted and sink-ACKed 2,159,882 events with zero event rejection,
sink retry, sink failure, or queue rejection. Stable-window required work was
0--12 events (maximum 4,835 B). FDs stayed 1,021--1,025 and tasks 1,012--1,024;
the small ranges reflect four persistent reconnects per minute. Stable RSS began
at 33,040 KiB and ended at 30,192 KiB, with a 30,160--33,536 KiB range. This is no
monotonic growth over 30 minutes, not proof of leak freedom. The sink's final
`rejected=104` counts cancellation of active keep-alive handlers during teardown;
NetbaIoT recorded every event ACK and zero sink failure.

An earlier diagnostic deliberately exposed a loadgen contract error: when the
client had not subscribed for application ACK events, it still waited for such an
ACK after publishing a device command ACK. The benchmark-only fix now waits only
when `subscribe=true`; transport PUBACK behavior is unchanged.

## CPU profile and system overhead

`target/perf-audit/mqtt-q1-30k.sample.txt` is a macOS `sample` call-tree captured
near the QoS1 knee. `sample` combines running and blocked wall-clock stacks, so the
following percentages are shares of 6,263 non-park top-of-stack samples after
removing 97,138 condition-variable parks and 9,262 `kevent` waits from 112,663
total samples. They are not exclusive CPU percentages.

| Rank | Top-of-stack symbol/category | Samples | Share of non-park samples |
|---:|---|---:|---:|
| 1 | `__psynch_mutexwait` | 2,187 | 34.92% |
| 2 | `sendto` | 644 | 10.28% |
| 3 | `recvfrom` | 385 | 6.15% |
| 4 | `mach_absolute_time` | 338 | 5.40% |
| 5 | `swtch_pri` | 300 | 4.79% |
| 6 | `getentropy` | 227 | 3.62% |
| 7 | `malloc_tiny` | 194 | 3.10% |
| 8 | `free` | 142 | 2.27% |
| 9 | `__psynch_mutexdrop` | 131 | 2.09% |
| 10 | JSON v1 decode | 91 | 1.45% |
| 11 | `pthread_cond_signal`/`cvsignal` | 58 | 0.93% |
| 12 | serde JSON escaped serialization | 58 | 0.93% |
| 13 | `clock_gettime` | 56 | 0.89% |
| 14 | Tokio notify | 52 | 0.83% |
| 15 | `memmove` | 51 | 0.81% |
| 16 | `memcmp` | 44 | 0.70% |
| 17 | allocator small-block dedup path | 40 | 0.64% |
| 18 | `memset` | 40 | 0.64% |
| 19 | ingress ingest | 33 | 0.53% |
| 20 | SipHash write | 31 | 0.49% |

Mutex blocking, syscalls, timekeeping/scheduling, and allocation are visible. The
call tree also showed EventBus and broker-route waits. Packet decoding had only five
top-of-stack samples; broker route-locked had ten; EventBus publish had thirteen;
Sessions touch had seventeen. No Instruments Time Profiler or allocation trace was
captured, so these figures cannot be converted into exclusive CPU time,
allocations/publish, or bytes allocated/publish.

A second 10 s `sample` call tree was captured during the 1,000-device mixed
workload (`target/perf-audit/mixed-60s.sample.txt`). Of 77,530 top-stack samples,
70,476 were condition-variable parks and 6,333 were `kevent`; only 721 were
non-park. The largest non-park tops were mutex wait 126 (17.5%), `recvfrom` 125
(17.3%), `writev` 81 (11.2%), tiny malloc 41 (5.7%), `memcmp` 39 (5.4%), `sendto`
37 (5.1%), `mach_absolute_time` 28 (3.9%), Sessions touch 26 (3.6%), EventId
`getentropy` 21 (2.9%), free 18 (2.5%), and broker `route_locked` 15 (2.1%). This
low-rate profile is dominated by idle connection/runtime parking and corroborates,
rather than replaces, the message-heavy profile.

## Copy/clone source audit

| Location/object | Source-audited bytes/event at 256 B | Calls/event | Profile attribution | Ownership note |
|---|---:|---:|---|---|
| Decoded MQTT payload `to_vec()` -> `BrokerMessage` | 256 B | 1 | not separately resolved | owns bytes beyond read buffer |
| `BrokerMessage::clone()` before broker route | >=256 B payload plus topic string | 1 | not separately resolved | clone is dropped in no-subscriber path |
| Codec JSON parse -> dynamic event values | unknown | 1 | JSON decode 1.45% of non-park top stacks | required by current public event model |
| EventBus JSON size accounting | serialized event size unknown | 1 | serde escaped serialization 0.93% | resource admission accounting |
| `Arc<DeviceEvent>` fanout | pointer clone, not payload copy | per sink | not resolved | shares accepted event |

The defensible source-audited lower bound is 512 payload bytes copied per 256-byte
publish, plus at least one topic-string clone. This is not a measured byte-allocation
count and is labelled accordingly. Exact allocations/publish, allocated
bytes/publish, peak live allocations, and BrokerMessage clone CPU contribution are
unknown pending an allocation trace/counting-allocator benchmark.

## Backpressure and resource classification

- The first two 1,000-device long attempts exposed benchmark-harness backpressure:
  the Python webhook wrote a status line every second to an undrained pipe. Around
  five minutes the pipe filled and blocked its asyncio event loop. After about
  417 s, required work reached the exact 50,000-event bound (20.1 MiB), RSS peaked
  at 117,136 KiB, 301 events were rejected, and MQTT clients were closed. One run
  ended at 494,029 accepted, 444,029 sink ACKs, 2,808 sink retries, and exactly
  50,000 pending; queue-reject count remained zero. The server exited successfully
  after planned shutdown, but the harness fetched metrics before shutdown and
  deleted its temporary spool, so the run did not distinguish final drain from
  restart-spool commit. This harness-induced overload is still valid evidence of
  bounded runtime backpressure, not stable capacity. Removing periodic sink output
  then passed an eight-minute check with 575,911/575,911 ACKs and pending <=11.
- At the tested overload points the bounded client window/scheduler flattened the
  offered rate. Required-work samples stayed bounded and server rejection counters
  remained zero. No unbounded RSS or queue growth was observed in these short runs.
- This does **not** validate the exact server-side MQTT overload response. A remote
  open-loop generator capable of exceeding the server is still needed to observe
  connection close/no-ACK/admission behavior at the real server ceiling.
- At the shared-host knee the system is primarily lock/scheduling and syscall
  constrained, with the generator also consuming a material core fraction.
- The profile directly attributes 3.62% of non-park top stacks to
  `JsonV1::decode -> Uuid::new_v4 -> getentropy`, making EventId generation a
  measured secondary CPU cost.
- At 10k confirmed HTTP delivery the Python sink is externally sink-bound before
  the Rust process.
- Hashing (0.49% of non-park top stacks), packet parsing (five top samples and
  125 ns representative microbench), exact topic lookup (sub-microsecond), and
  session lookup (<=84 ns P99) are not justified optimization targets now.

## Explicitly unmeasured / limits of this baseline

The following requested questions do not have valid measurements and must not be
filled with estimates:

- allocations/publish, allocated bytes/publish, peak live allocations;
- active RSS/connection isolated from credential/config scaling and allocator
  high-water behavior;
- 10k TLS connections, 30k/50k connections, and 50k/100k stable messages/s;
- full subscriber delivery fanout, route commit time, and per-target allocations;
- near-bound offline-queue throughput/rejection and RSS recovery after drain;
- BrokerState critical sections other than route;
- direct Sessions/Auth mutex wait/hold histograms;
- confirmed TCP business-sink absent/reconnect/filter-mismatch occupancy;
- a TLS CPU profile and true flamegraphs (macOS `sample` call trees exist for the
  message-heavy and mixed workloads);
- an allocation profile;
- a four-hour mixed soak.

For a four-hour run using the committed mixed driver:

```bash
cargo build --release --locked -p netbaiot-server -p netbaiot-loadgen
python3 scripts/perf/mixed_load.py --duration 14400 --warmup 60 \
  --cooldown 30 --sample-every 60 > target/perf-audit/mixed-soak-4h.json
```

The driver emits RSS, CPU, FD, task, EventBus count/bytes, and pending-required
samples. It does not yet emit every MQTT offline/inflight/retained byte counter
requested for a full soak certification.

## Regression baselines to keep

Record, but do not gate CI on, these current low-variance microbenches: MQTT packet
decode, topic lookup, compact route planning, EventBus publish, ingress admission,
and JSON codec size sweep. Establish cross-host variance before choosing thresholds.
Future optimization work should change one thing, rerun the identical scenario and
correctness gate, and revert when the gain is within benchmark noise.

## Post-change correctness

All required post-change gates passed:

- `cargo +1.88.0 fmt --all -- --check`;
- Rust 1.88.0 locked workspace/all-target/all-feature Clippy with warnings denied;
- Rust 1.88.0 locked workspace/all-feature tests;
- current-stable locked workspace/all-target/all-feature Clippy with warnings denied;
- current-stable locked workspace/all-feature tests;
- NetbaIoT-only MQTT conformance, 31/31;
- locked release workspace build;
- ignored 60-second multi-generation graceful-restart soak (62.78 s).

The restart test is lifecycle/recovery evidence, not a four-hour performance soak.

## Broker route experiment (2026-09-22)

The focused experiment at starting SHA
`70b855f1a97ebe1222dc0212fe0847e91b07cf4b` was **KEPT**. A derived atomic
subscription-count hint removes the broker mutex acquisition for valid,
non-retained publishes only when the authoritative broker has no subscriptions.
Subscribed and retained routing continue through the original locked atomic
preflight/commit path; the lock type and ownership/state-machine semantics did not
change.

At 20k offered QoS1/s, three-run median throughput moved from 19,978.55 to
19,991.05/s (+0.063%), PUBACK P99 from 1.50 to 1.36 ms, server peak CPU from
140.9% to 134.3%, and peak RSS from 8,192 to 8,208 KiB. Broker-route acquisitions
and wait sum fell 100% (one acquisition/publish to zero) in this no-subscriber
workload. This satisfies the experiment's >=25% lock-wait threshold, but the
25--30k/s saturation knee did not move materially.

QoS0 25k and QoS2 20k throughput remained within 0.04% and 0.006%. Compact fanout
plan bytes remained exactly 10,090/101,890/204,890 at 100/1k/2k targets. The
hot-publisher fairness scenario completed every low-rate message with unchanged
0.13 ms P99, bounded overload remained stable, all Rust/MQTT/restart gates passed,
and no second bottleneck was optimized. Full methodology and source accounting are
in `docs/performance-broker-route-experiment.md`.
