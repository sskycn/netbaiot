# Separate-host capacity audit: preparation and same-host controls

## Goal and status

**SEPARATE-HOST MEASUREMENT NOT EXECUTED.** The final user instruction was to
perform preparation and same-host controls. `vm-dev` was inventoried, but no
remote benchmark was started. Its isolated build preparation was stopped.
Therefore the highest repeatable stable **isolated-server** QoS1 throughput and
its first saturated resource remain **unknown**. The work below supplies a
repeatable harness and actual local controls, not a new server capacity baseline.

No server production code or limits implementation changed. The broker route
fast path remains **KEEP**. EventBus local scheduling and EventId RNG experiments
remain **REVERT**. This audit implements no next optimization.

## Hardware and network topology

| Property | Tested server | Tested load generator |
|---|---|---|
| Placement | Same physical Mac, native process | Same physical Mac, separate native process |
| Host/model | `X`, Mac mini `Mac16,10` | Same |
| CPU | Apple M4, 10 physical/logical cores: 4 performance + 6 efficiency; no SMT | Same |
| Memory | 16 GiB | Shared 16 GiB |
| OS/kernel | macOS 26.6.2 (25G83), Darwin 25.6.0, arm64 | Same |
| Test address/interface | `127.0.0.1`, `lo0`, MTU 16384 | Same |
| Physical network used | None | None |
| Physical NIC/link speed | Ethernet `en0` inactive; Wi-Fi `en1` active, negotiated speed unavailable | Same |
| Other address | Wi-Fi `192.168.2.128`, not used for workload traffic | Same |

There is no 1/2.5/10 GbE capacity measurement. Loopback has no negotiated physical
link speed. Interface counters include other local traffic and both ends of the
benchmark. They must not be treated as dedicated server RX/TX.

The unused `vm-dev` target is `mail.gostartkit.com`, Rocky Linux 8.10, kernel
`4.18.0-553.144.1.el8_10.x86_64`, one Broadwell vCPU under a Microsoft hypervisor,
474,640,384 bytes RAM and approximately 1.9 GiB swap. Interfaces are `enp1s0`
(public IPv4 `45.76.70.90`) and `enp8s0` (`10.5.96.3`); both report unknown link
speed. Existing services were running. Its physical placement and shared-host
contention are unknown; it is not a verified dedicated wired-LAN peer. The
`vm-dev` SSH command uses a local SOCKS proxy. SSH control connectivity does not
prove a benchmark data path. No bandwidth or throughput claim uses this VM.

## Software versions and provenance

| Reference | SHA |
|---|---|
| Correctness baseline | `ce07b5b126b04f7ae95749c46e0263c7514fe491` |
| Initial performance baseline | `70b855f1a97ebe1222dc0212fe0847e91b07cf4b` |
| Broker-route runtime baseline | `cbbad11d827d3d8e04160819ab72378f6d9ae9a9` |
| EventBus evidence, reverted experiment | `43d76c3ccb7271b6b82b1c058c3f879c383ebd22` |
| EventId evidence, reverted experiment | `0a50ee04c45c5f7fe0f9790db08b0c0a67d327e1` |
| Authoritative starting local HEAD | `286da293b30a06c77ed017d5466f1d093186c25e` |
| Measured release build source | `cd774e0435ca053f1b6a311337b755bd185ddd83` |

The EventId evidence reference is not an ancestor of the starting checkout; it
was not cherry-picked. Work occurs in an isolated `codex/separate-host-capacity-audit`
worktree, preserving the original checkout and newer work.

Both measured binaries use Rust stable 1.97.1, normal Cargo release, locked
workspace dependencies, default features and empty `RUSTFLAGS`. Commands were
`cargo build --release --locked` and the package-specific
`cargo build --release --locked -p netbaiot-loadgen`. The same binaries are reused
for the matrix and preserved under `target/perf-audit/separate-host/frozen-bin/`. Later Python collector fixes do not rebuild either binary;
raw metadata records the source status. SHA256: server
`ad01b4bd049288963c99627ae69aaf97eb5a0de14698fc88e8d813ba443dce80`;
load generator `fd1c1d8f5fd3e46be6ef592e4fc003f6a210a171517407dcaeb4e57c49372f99`. Logs use `RUST_LOG=warn` and existing
opt-in `NETBAIOT_PERF_LOCK_METRICS=1`. Diagnostic profile runs are separate from
unprofiled primary repeats.

## Benchmark methodology

Primary controls use 64 publishers, QoS1, 256 actual MQTT payload bytes, 30 seconds
of traffic warmup, 60 seconds measured traffic and 15 seconds cooldown. Offered
10k, 20k and 30k/s each have three repetitions, with the order reversed in the
second pass. QoS0/2 receive representative 20k/s controls. Secondary matrices
are shorter diagnostic points, not repeatable capacity certification.

The audit generator establishes every MQTT connection and checks CONNACK before
releasing a common warmup start. It sends during warmup, then emits explicit
measurement start/end markers. Server readiness uses `/api/v1/ready`. TCP
consumer healthy mode additionally waits for a completed stream handshake.
Fixed configured ports replace bind-close-rebind port selection; local jobs
serialize through an owned file lock and actual listener binding fails closed
if another service owns a port.

Primary configuration keeps EventBus and required sink bounds at 50,000 events /
64 MiB each, sink concurrency eight, MQTT inflight 32 per session, and 4,096
inflight per tenant. Connection/admission count caps are 128 for the 64-publisher
core controls; per-device ingress concurrency is four and ingress byte ceilings
remain the runtime defaults. Connection-count diagnostics scale the matching connection/device/cache admission
caps. The 10k idle-only point explicitly uses 65,536 bytes per connection of
logical reservation, following the existing connection-memory harness; see the
configuration rejection and corrected point below. Other points retain 524,288. Full exact configs are retained per run.
No queue bound was raised to salvage an overload point.

Each publisher has an absolute rate schedule independent of ACK completion.
A late wake sends at most one message and explicitly counts discarded schedule
slots, rather than moving the schedule or creating an unbounded catch-up burst.
At an inflight limit, the attempt is counted as local backpressure. The original
legacy generator mode remains available for existing harnesses and idle holds.

The hard MQTT inflight limit is 32 per publisher (2,048 total at 64 publishers),
with at most one active socket write per task. There is no extra publish/send
queue or ACK queue. The parser starts at 4 KiB with a 65,536-byte packet ceiling;
connections/tasks are bounded at 10,000 in audit mode. A 5-second ACK/write
failure is explicit. Setup waits and root task lifetime are bounded. Histograms
have fixed bins; errors retain at most eight 128-character samples. JSON output
contains cumulative counters plus counters restricted to sends originating in
the measurement interval. Final measurement ACK counts may include completion
during cooldown; server rates use the server's own monotonic sampling interval.

PUBACK/PUBREC/PUBCOMP latency is measured entirely on the generator's monotonic
clock. No cross-machine one-way latency is reported. Histogram quantiles are
bucket upper bounds (10 microseconds below 100 ms; 1 ms thereafter).

The lowest requested payload (64 B) cannot fit the harness's valid JSON envelope
and unique source ID. Results report actual average serialized bytes, never
pretend this is a valid 64-byte telemetry measurement.

### Stable-point rule and limitations

A candidate healthy point must attempt and accept at least 99% of intended rate,
have no unexpected errors/rejections, no growing required backlog, bounded tails
and stable RSS. The script's `HEALTHY` is provisional until time series are
reviewed. Backlog screening compares first/last third medians, with a tolerance
of max(10 messages, 1% of offered/s); raw series remain authoritative. Missing
schedule slots are never silently removed from the offered denominator.

`LOADGEN-LIMITED` means the offered-rate validity criterion failed. It does not
prove all loss originates inside the generator: shared host scheduling or server
backpressure may contribute. `OVERLOAD` includes a growing sink backlog. Neither
classification by itself identifies a server CPU bottleneck. A stable saturation
knee requires repeated qualifying points and isolated load generation; none is
claimed here. A maximum observed accepted rate is not an overload plateau unless
a plateau is actually established.

The frozen measurement generator reset latency histograms at the warmup boundary.
A final review found that this could erase the first few measurement ACK samples:
0–30 samples per primary run (at most 0.0018%); the largest fraction across short
diagnostics was 0.0067%. Throughput/ACK counters were not reset. Published latency
values are the actual retained histograms, not reconstructed missing samples.
The final tool removes this race by recording measurement-origin ACKs and pacing
samples directly without clearing histograms. The historical matrix was not
rerun with that later tool change; its source SHA and limitations remain explicit.
A final smoke check verifies published = ACKed = latency histogram count and
attempted + missed = scheduled slots. It is functional validation, not a new
capacity point.

Host samples occur about once per second. Process CPU seconds are integrated
separately for server, generator and optional sink; CPU cores = CPU seconds /
wall second, so 1.0 means 100% of one core. RSS, numeric FD count and threads are
sampled separately. `/api/v1/status` exposes event count/bytes, pending required
work, connection counts, cache usage and runtime tasks. Broker session bytes,
offline/retained occupancy, actual per-core utilization, socket queue depths and
server QoS inflight occupancy are not exposed by this collector and remain
unavailable. Client inflight has a hard bound and observed peak.

The macOS `netstat -s -p tcp` files returned all-zero counters despite active
traffic. Retransmit and kernel socket-error counts are therefore **unavailable**,
not evidence of zero retransmissions. `nettop` provides timestamped per-process
socket-byte totals in later collector runs; early untimestamped or buffered
files remain unavailable for window-specific byte rates. Management traffic is
included in server process totals. These are not physical Ethernet wire bytes.

Boundary telemetry is not atomic: HTTP counters are fetched first, then process
statistics. The server denominator uses server monotonic timestamps. Same-host
boundary receipt delay is retained where supported. One-second sampling can miss
brief queue/RSS peaks. Existing histogram metrics provide EventAccepted and
sink-ACK timings; macOS stack samples are wall samples, not CPU-cycle attribution.

## Open-loop generator validation

A local 1k/s smoke test verified connection readiness, warmup traffic, measurement
boundaries, measured ACK counts, actual payload bytes and clean completion.
Unit tests verify missed-slot accounting without schedule drift and reject
incompatible/unbounded audit configurations. Controls record schedule lag,
missed slots, attempted/published/completed messages, inflight peaks, CPU, RSS
and network counters. Mean CPU alone is insufficient to establish 20–30%
generator headroom: rate tracking is also mandatory.


| QoS1 offered/s | Median missed slots/s | Schedule-lag P99 median / worst ms | Inflight observed peak (bound 2,048) | Window-full / timeouts |
|---:|---:|---|---:|---|
| 10,000 | 3.33 | 2.17 / 3.75 | 144 | 0 / 0 |
| 20,000 | 15.73 | 2.18 / 2.36 | 201 | 0 / 0 |
| 30,000 | 1,289.17 | 2.37 / 2.48 | 376 | 0 / 0 |

The highest **target** validated at >=99% attempted rate in these core controls
was 20k/s. The 50k offered profile attempted 39.83k/s, which is an observed rate,
not a validated 50k-capable generator. CPU headroom at a true knee is unknown.

## Same-host control and QoS summary

All rows are **same-host core ingress**. Medians are across runs, not pooled latency histograms. CPU is cores consumed, not percent of the ten-core machine.

| Scenario | Offered/s | Attempted/s | Accepted/s | P50 / P95 / P99 ms | Server CPU | Loadgen CPU | Server RSS KiB | Pending peak | Result |
|---|---:|---:|---:|---|---:|---:|---:|---:|---|
| QoS0 20k (1 run) | 20,000 | 20,000.20 | 19,999.40 | N/A (no ACK) | 0.627 | 0.313 | 8,144 | 31 | HEALTHY |
| QoS1 10k (3 runs) | 10,000 | 9,996.67 | 9,996.18 | 0.11 / 0.18 / 0.23 | 0.387 | 0.253 | 8,176 | 15 | HEALTHY |
| QoS1 20k (3 runs) | 20,000 | 19,984.27 | 19,983.58 | 0.17 / 0.30 / 0.39 | 0.686 | 0.438 | 8,224 | 26 | HEALTHY |
| QoS1 30k (3 runs) | 30,000 | 28,710.45 | 28,709.27 | 0.22 / 0.47 / 1.10 | 0.940 | 0.594 | 8,512 | 197 | LOADGEN-LIMITED |
| QoS2 20k (1 run) | 20,000 | 19,983.00 | 19,982.03 | 0.38 / 0.72 / 1.56 | 1.158 | 0.733 | 8,448 | 17 | HEALTHY |

| QoS1 offered/s | Median accepted/s | Best | Worst | (best−worst)/median | PUBACK P99 range ms |
|---:|---:|---:|---:|---:|---|
| 10,000 | 9,996.18 | 9,996.83 | 9,953.21 | 0.436% | 0.22–2.33 |
| 20,000 | 19,983.58 | 19,984.10 | 19,871.82 | 0.562% | 0.35–1.38 |
| 30,000 | 28,709.27 | 28,866.68 | 28,678.22 | 0.656% | 0.51–1.31 |

The 20k QoS1 control is repeatably eligible, but is **not a knee**. All primary
runs had zero client errors, zero rejection counters, no window-full drops and
zero pending required events at measurement end. QoS1 20k RSS first-to-last
changes were +16, +16 and +96 KiB. Its generator CPU range was 0.413–0.495 cores;
this cannot certify headroom at an unmeasured server knee. At 30k, actual
attempts were only 28.68–28.87k/s and the server tracked them. Offered workload
validity failed even though the server did not show a persistent backlog.

QoS2 PUBREC P50/P95/P99 was 0.18/0.40/0.80 ms; PUBCOMP was
0.38/0.72/1.56 ms. QoS0 has no producer protocol ACK, so no client acceptance
latency is invented. Server-side EventAccepted counts are its throughput source.

At 20k, accepted events/server CPU second were about 31,905 (QoS0), 29,131 (QoS1 median), and 17,251 (QoS2). These are local workload efficiencies, not host-independent capacity.



Timestamped process socket-byte samples at representative primary points (MB = 1,000,000 bytes):

| Point | Server RX MB/s | Server TX MB/s | Loadgen TX MB/s | Loadgen RX MB/s |
|---|---:|---:|---:|---:|
| q0-20000-r1 | 5.617 | 0.017 | 5.617 | 0.000 |
| q1-20000-r3 | 5.653 | 0.097 | 5.653 | 0.080 |
| q2-20000-r1 | 5.732 | 0.177 | 5.732 | 0.160 |
| q1-30000-r3 | 8.165 | 0.133 | 8.165 | 0.115 |

Server totals include management sampling; sender/receiver sampling boundaries
can differ by up to about one second. No physical link utilization percentage
is inferred. Per-point JSON/CSV preserves missing early timestamped counters.

## Separate-host QoS0, QoS1 and QoS2

**SEPARATE-HOST MEASUREMENT NOT EXECUTED.** Offered/accepted/latency/CPU/RSS knees
for all three QoS levels are unavailable. No separate/same ratio or percentage
delta can be calculated. The historical 25–30k/s shared-host knee remains a
historical observation, not a confirmed server-side limit.

## Publisher concurrency, payload matrix and TLS

Each publisher/payload diagnostic used 10 s warmup, 30 s measurement and 5 s cooldown. These are single runs.

| Publishers | Offered/s | Attempted/s | Accepted/s | PUBACK P99 ms | Server / loadgen CPU cores | Result |
|---:|---:|---:|---:|---:|---|---|
| 1 | 20,000 | 3,332.40 | 3,332.08 | 0.10 | 0.097 / 0.076 | LOADGEN-LIMITED |
| 10 | 20,000 | 8,394.50 | 8,393.99 | 0.19 | 0.329 / 0.226 | LOADGEN-LIMITED |
| 100 | 20,000 | 19,879.43 | 19,877.96 | 1.00 | 0.666 / 0.459 | HEALTHY |
| 1,000 | 20,000 | 20,000.00 | 19,998.57 | 0.36 | 0.735 / 0.414 | HEALTHY |

The 1/10-publisher results fail pacing accuracy despite low average CPU. They are generator/workload observations, **not server single-connection limits**.

| Requested payload B | Actual mean B | Offered/s | Accepted/s | PUBACK P99 ms | Server CPU cores | Result |
|---:|---:|---:|---:|---:|---:|---|
| 64 | 123.99 | 2,000 | 1,999.92 | 0.15 | 0.078 | HEALTHY |
| 256 | 256.00 | 2,000 | 1,999.92 | 0.16 | 0.086 | HEALTHY |
| 1,024 | 1,039.99 | 2,000 | 1,999.82 | 0.18 | 0.085 | HEALTHY |
| 8,192 | 8,328.99 | 2,000 | 1,999.84 | 0.26 | 0.199 | HEALTHY |
| 32,768 | 32,768.00 | 2,000 | 1,999.91 | 0.56 | 0.381 | HEALTHY |

No exact 64-B, exact 1-KiB or exact 8-KiB telemetry claim is made where the envelope exceeded the request. Larger payloads use a bounded number of JSON fields plus padding; this is not arbitrary production JSON complexity. A separate 1-KiB saturation sweep was not executed.

QoS1 TLS used the same 30/60/15 s timing and minimal AuditSink: 19,998.94 accepted/s, P50/P95/P99 0.17/0.30/0.35 ms, server/loadgen CPU 0.753/0.509 cores and server peak RSS 9,872 KiB. Server CPU was 9.7% above the plaintext median. Accepted-rate difference was below 0.1%; TLS P99 lies within the plaintext repeat range. A single TLS point does not establish a capacity delta or prove faster TLS latency.


## Business HTTP sink

The HTTP sink is the repository's bounded Python sink on the **same Mac**. Its
CPU and server sink ACKs are captured separately. These are required-sink path
measurements; Python throughput limitations are not core broker capacity.

| Offered/s | Accepted/s | Sink ACK/s | PUBACK P99 ms | EventAccepted→sink ACK P99 upper ms | Server / sink CPU cores | Pending sampled peak / end | Result |
|---:|---:|---:|---:|---:|---|---|---|
| 2,000 | 1,999.82 | 1,999.82 | 0.16 | 1 | 0.157 / 0.232 | 4 / 0 | HEALTHY |
| 10,000 | 9,999.20 | 9,996.00 | 0.26 | 25 | 0.757 / 0.722 | 276 / 92 | HEALTHY |

Both are 10/30/5 s diagnostics with zero client errors/rejections and no sustained backlog growth. The 10k point ended its measurement with 92 required deliveries, then drained during cooldown. No HTTP sink saturation knee was searched or established.


## Confirmed TCP sink

`capacity_consumer.py` implements the actual version-1 Hello/Subscribe/Event/ACK
stream, with one connection, one bounded frame and one outstanding ACK at a
time. Scenarios include healthy service, initial absence followed by recovery,
disconnect/reconnect, filter mismatch followed by recovery, and delayed ACKs.
The server's existing confirmed sink concurrency is one and is not changed.
Consumer receipt→ACK-write timing excludes any claim that a socket write already
means server-observed ConsumerAccepted; `netbaiot_sink_acks_total` is the server
confirmation counter. Partial frames survive polling deadlines.

| Scenario | Offered/s | EventAccepted/s | ConsumerAccepted/s (server ACKs) | Producer PUBACK P99 ms | Pending sampled peak / end | Retries | Recovery after consumer ready |
|---|---:|---:|---:|---:|---|---:|---|
| tcp-healthy | 1,000 | 997.47 | 997.47 | 0.13 | 2 / 0 | 0 | not applicable |
| tcp-absent-recover | 500 | 499.97 | 666.67 | 0.13 | 9,006 / 0 | 2 | 3.78 s |
| tcp-reconnect | 500 | 499.95 | 499.95 | 0.13 | 2,320 / 0 | 1 | 1.83 s |
| tcp-filter-recover | 500 | 499.95 | 666.65 | 0.14 | 9,610 / 0 | 2 | 3.93 s |
| tcp-slow | 500 | 499.97 | 82.84 | 0.13 | 16,353 / 16,680 | 0 | not recovered |

Initial absence lasted 20 s from consumer launch; reconnect disconnected after
18 s of connection lifetime and waited 5 s; filter mismatch lasted 20 s before
reconnecting with the matching filter. Recovery is sampled time from the final
successful consumer handshake until pending <= 1% of offered/s (at least one
message), with roughly one-second resolution. It is not outage duration.
Recovered fault points are **fault/recovery evidence**, not steady healthy sink
capacity points, regardless of the script's provisional healthy rate check.
ACK/s can exceed current ingress while draining warmup/outage backlog.

The healthy 1k/s consumer point has EventAccepted→sink-ACK P99 <=1 ms.
Absence/filter-recovery tails exceed the metric's largest finite 5-second
bucket, so an exact P99 is unavailable. Consumer receipt→ACK-write latency is
separately retained in raw consumer JSON; it omits time already spent in EventBus.
Observed consumer outstanding count peaked at one, consistent with the existing
server sink concurrency of one. Dedicated server sink-slot occupancy is not
exposed; it is not inferred as a continuous measured gauge.

The intentional 10 ms ACK delay admitted about 500/s while confirming about
83/s. Required pending work grew to 16,680 at measurement end (16,353 peak
in the periodic samples): **OVERLOAD in
the sink path**, despite timely producer PUBACKs. It remained below the unchanged
50,000-event / 64-MiB bounds, management sampling remained responsive, and
shutdown retained remaining responsibilities in the normal recovery spool.
RSS grew from 15,744 to 39,008 KiB; it did not stabilize. The logical event-byte
sample peak was 6,529,916 bytes. The count/byte ceilings were not reached, so this
is not a test of behavior at a completely full queue. Shutdown wrote a
7,625,309-byte EventBus recovery spool. This is neither core server saturation
nor a throughput optimization result.


## Fanout

Full 1/10/100/500/1,000 online subscriber delivery was not executed. The current
public device model restricts subscriptions to that authenticated device's
canonical topic prefix (`session_subscribe_acl`), and registering a new live
session for the same `DeviceKey` cancels the previous one (`Sessions::register`).
Thus many ClientIds sharing one credential do not create many simultaneously
live subscribers. Cross-device subscriptions would violate the ACL. Changing
those semantics or substituting an internal route microbenchmark would not
satisfy this requested end-to-end workload. A future authorized benchmark-only
composition must define a valid subscriber role first. No production behavior
was changed to make this matrix possible.

## Connection scaling and host limits

| Idle mode | Requested / observed connections | Full-count host samples | RSS hold median KiB | RSS delta KiB/conn | Client setup P50 / P95 / P99 ms | Errors |
|---|---|---:|---:|---:|---|---:|
| idle-plain-1000 | 1,000 / 1,000 | 19 | 30,944 | 24.43 | 0.17 / 0.28 / 0.39 | 0 |
| idle-plain-3000 | 3,000 / 3,000 | 19 | 76,144 | 22.12 | 0.31 / 0.55 / 0.78 | 0 |
| idle-plain-10000-valid | 10,000 / 10,000 | 19 | 251,168 | 22.66 | 0.76 / 1.99 / 3.48 | 0 |
| idle-tls-1000 | 1,000 / 1,000 | 19 | 39,984 | 32.43 | 0.70 / 0.80 / 0.97 | 0 |
| idle-tls-3000 | 3,000 / 3,000 | 19 | 100,688 | 29.97 | 0.85 / 1.17 / 1.52 | 0 |

These holds used a controlled 200 connections/s ramp, 3 s warmup, 15 s hold and
3 s cooldown, with zero publish rate. They do not establish maximum connection
establishment rate. TLS added approximately 8.00 KiB/connection at 1k and
7.85 KiB/connection at 3k in these single RSS-delta probes.

The first 10k fixture was rejected **before the server became ready**: its
`(10,000+32) × 524,288` global logical byte budget exceeded the runtime's u32
configuration ceiling. No connection capacity result came from that attempt.
Following the pre-existing `connection_memory.py` idle convention, the corrected
10k point explicitly set `--connection-reservation 65536` (global budget
657,457,152 bytes). This is a benchmark configuration exception, not a runtime
change or a recommendation to use that reservation for active large-payload
production connections. The 1k/3k points retained the default 512-KiB reservation.
No OS FD, ephemeral port, socket-buffer or TIME_WAIT tuning was performed.


Local limits: process soft FD 1,048,575; kernel per-process max files 61,440;
ephemeral ports 49,152–65,535 (16,384); TCP MSL 15,000 ms; default TCP send/receive
space 131,072 bytes. A previous 10k repetition's ephemeral-port/TIME_WAIT limit
is not a NetbaIoT connection ceiling. Idle RSS deltas include allocator high-water
and TLS/library initialization, not exact object memory. Client setup latency (TCP connect, optional TLS handshake, then MQTT CONNACK)
is retained in generator histograms; this is not a dedicated connections/s
capacity sweep.

## Saturation analysis, profiles and first saturated resource

Profiles used `sample PID 10 1` during separate 15-second measurement points (10 s warmup / 5 s cooldown), with the same frozen release binaries.

| Profile offered/s | Attempted/s | Accepted/s | PUBACK P99 ms | Server / loadgen CPU cores | Classification |
|---:|---:|---:|---:|---|---|
| 10,000 | 10,000.00 | 9,998.68 | 0.31 | 0.397 / 0.275 | HEALTHY |
| 30,000 | 29,171.47 | 29,167.02 | 0.68 | 1.046 / 0.694 | LOADGEN-LIMITED |
| 50,000 | 39,830.00 | 39,822.63 | 0.73 | 1.394 / 0.937 | LOADGEN-LIMITED |

The highest observed accepted rate was **39,822.63/s in one profiled 15-second point**. It is neither a stable capacity nor an established overload plateau: intended 50k/s was not delivered, and there was no refinement/repeated isolated knee search.

Raw collapsed top-of-stack counts (including parks):

| Rank | 10k offered | 30k offered | 50k offered |
|---:|---|---|---|
| 1 | `__psynch_cvwait`: 68,454 | `__psynch_cvwait`: 58,669 | `__psynch_cvwait`: 56,581 |
| 2 | `kevent`: 6,840 | `kevent`: 5,614 | `kevent`: 5,038 |
| 3 | `__psynch_mutexwait`: 24 | `__psynch_mutexwait`: 2,590 | `__psynch_mutexwait`: 2,709 |
| 4 | `__sendto`: 14 | `__sendto`: 675 | `__sendto`: 984 |
| 5 | `mach_absolute_time`: 8 | `__recvfrom`: 562 | `mach_absolute_time`: 849 |
| 6 | not reported | `mach_absolute_time`: 488 | `__recvfrom`: 816 |
| 7 | not reported | `getentropy`: 258 | `getentropy`: 414 |
| 8 | not reported | `_xzm_xzone_malloc_tiny`: 204 | `_xzm_xzone_malloc_tiny`: 237 |
| 9 | not reported | `_xzm_free`: 179 | `_xzm_free`: 215 |
| 10 | not reported | `__psynch_mutexdrop`: 151 | `__psynch_mutexdrop`: 197 |

| Same-host offered/s | Non-park samples | Mutex wait | send/recv | Allocation | Copy | JSON codec/serde | getentropy |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 30,000 | 7,261 | 35.67% | 17.04% | 7.59% | 0.61% | 3.03% | 3.55% |
| 50,000 | 9,551 | 28.36% | 18.85% | 6.97% | 0.72% | 3.39% | 4.33% |

These are **wall-stack sample shares, not exclusive CPU shares**. Non-park
means thread-root counts minus `__psynch_cvwait` and `kevent`; omitted symbols
below five samples make grouped named costs lower bounds. The 10k profile has
only 111 non-park samples and five reported collapsed symbols, too sparse for a
credible top-ten hotspot ranking. No missing symbols were fabricated.
`__psynch_mutexwait` includes all mutexes, not just EventBus. `getentropy` is not
exclusive EventId attribution. BrokerMessage clone had no named collapsed entry
above the reporting threshold, which does not establish zero copy cost.

Per-core utilization and frequency were unavailable. A process aggregate near
1.39 CPU cores cannot distinguish one serialized worker plus auxiliary work from
other scheduling patterns. These local profiles preserve candidates for a later
isolated audit; they do not justify reopening the rejected EventId experiment or
implementing EventBus architecture changes now.


No true server knee was measured. Profile points are labeled by **same-host
offered rate**, not “healthy / true knee / true server overload.” Mutex, kernel,
allocation/JSON and EventId shares at a true knee therefore remain unavailable.
Historical EventBus and EventId experiments remain reverted. No architecture or
allocation optimization is justified by simply repeating those old rankings.

## Stable soak and mixed workload

A 30-minute run at 70–80% of a newly established knee was **not executed** because
no isolated knee exists in this scoped run. No 4-hour run is claimed. Short-run
RSS/backlog observations do not substitute for soak evidence. The audit mode
intentionally accepts fixed-rate single-QoS workloads only; realistic mixed QoS,
persistent reconnect, commands and retained traffic remain a separate workload,
not an unverified mixture of independently measured ceilings.

Once an isolated knee and appropriate sink workload are established, choose an
explicit rate (do not infer it from the local 30k offered point) and run the same
prepared `serve`/`load` workflow with `--duration 1800`, or `--duration 14400` for
four hours. The `serve --max-seconds` must also include setup, 30 seconds warmup
and cooldown; its default 14,400 seconds is insufficient for a full four-hour
measurement plus setup. Run a representative mixed workload separately using the
repository mixed harness, with its own offered-rate verification.

## Reproducible two-host workflow

This is preparation, **not evidence of execution**. Use Python 3.9 or newer for the coordinator (the unused VM's default Python 3.6
is insufficient). Run the same committed audit
revision on both machines and record `git rev-parse HEAD`, `git status --short`,
`rustc --version`, CPU/OS/NIC/link and binary SHA256 on both. Build once with normal
`cargo build --release --locked`, and explicitly build `-p netbaiot-loadgen` on
Host B. Do not compare binaries from different revisions or compile during runs.

Public runtime configuration requires TLS on non-loopback stream/HTTP listeners
and an explicit required business sink. The loopback development AuditSink is
therefore not directly deployable as a plaintext LAN core baseline. The commands
below use TLS plus the required local HTTP sink. Match that exact configuration
in the same-host comparator. The business sink should later be moved to a third
host and measured separately; the local Python sink can become the bottleneck.
An SSH data tunnel would be a different measured topology and must be labeled as
such. Only management sampling is forwarded below.

Host A (substitute the actual interface, reachable IPv4 and certificate paths;
the test certificate must validate both the chosen data hostname and localhost
management endpoint):

```bash
cargo build --release --locked
python3 scripts/perf/capacity_audit.py prepare target/perf-audit/two-host/q1-20k \
  --host <HOST_A_IPV4> --bind-host 0.0.0.0 --interface <HOST_A_NIC> \
  --tls --tls-server-name <CERTIFICATE_NAME> \
  --certificate <CERTIFICATE_PEM> --private-key <PRIVATE_KEY_PEM> \
  --placement separate-host --sink-mode webhook --rate 20000 --connections 64 --qos 1 --payload 256 \
  --warmup 30 --duration 60 --cooldown 15 --label two-host-q1-20k
python3 scripts/perf/capacity_audit.py serve target/perf-audit/two-host/q1-20k \
  --max-seconds 180
```

Copy **only** `load.json`, `manifest.json` and the public CA/certificate to Host B.
Do not copy server private keys. In Host B's copied `load.json`, replace `tls_ca`
with the absolute path of its public CA/certificate. The fixture credential is
for an isolated benchmark environment; never substitute production credentials
into committed result files. Forward the loopback-only sampler and run:

```bash
# Run this SSH tunnel in a separate terminal for the point.
ssh -N -L 24008:127.0.0.1:24008 <HOST_A_SSH_ALIAS>

cargo build --release --locked -p netbaiot-loadgen
python3 scripts/perf/capacity_audit.py load <COPIED_BUNDLE> \
  --sampler-port 24008 --interface <HOST_B_NIC> \
  --output target/perf-audit/separate-host/q1-20k-r1
```

Declare `--placement separate-host` only after verifying actual placement;
`prepare` otherwise records `unverified`, while `local` always records same-host.
Different hostnames alone do not prove different physical machines. A bundle
identity and source-SHA check reject the wrong sampler or checkout.

Start `load` only after `serve` reports ready. Its own explicit MQTT readiness
barrier controls warmup. Repeat with fresh output/bundle paths at least three
times near a healthy point, candidate knee and overload point. Use 10/20/25/30/
35/40/50/60/75/100k offered as discovery, then refine only while attempted rate
tracks target. An inadequate generator or network invalidates a server knee.
Record server RX/TX, client TX/RX, retransmits, loss, queues and CPU independently.
The loopback sampler stays off the public network; the data connection goes
directly over TLS. No unsynchronized one-way latency is derived.

For a matching local comparator use `local` with `--tls --sink-mode webhook`,
the same certificate, QoS, publishers, payload, offered rate and durations.
For the executed core ingress controls use:

```bash
python3 scripts/perf/capacity_audit.py local target/perf-audit/separate-host/repeat/bundle \
  --output target/perf-audit/separate-host/repeat --label local-q1-20k \
  --rate 20000 --connections 64 --qos 1 --payload 256 \
  --warmup 30 --duration 60 --cooldown 15
```

Representative diagnostic flags for the same `local` command are
`--sink-mode tcp --rate 500 --tcp-connect-delay 20 --cooldown 20`,
`--sink-mode tcp --rate 500 --tcp-disconnect-after 18 --tcp-reconnect-delay 5 --cooldown 20`,
`--sink-mode tcp --rate 500 --tcp-filter-mismatch-seconds 20 --cooldown 20`, and
`--sink-mode tcp --rate 500 --sink-delay-ms 10 --cooldown 30`.
Use `--rate 0 --connections 10000 --connection-reservation 65536 --warmup 3
--duration 15 --cooldown 3` only for the explicitly labeled idle-memory probe.

An exact four-hour **future** preparation command, after choosing a rate from an
isolated knee, is below. It has not been executed:

```bash
: "${RATE:?Set RATE to 70–80 percent of the measured isolated knee}"
python3 scripts/perf/capacity_audit.py prepare target/perf-audit/two-host/soak \
  --host <HOST_A_IPV4> --bind-host 0.0.0.0 --interface <HOST_A_NIC> \
  --placement separate-host --tls --tls-server-name <CERTIFICATE_NAME> \
  --certificate <CERTIFICATE_PEM> --private-key <PRIVATE_KEY_PEM> \
  --sink-mode webhook --rate "$RATE" --qos 1 --connections 64 --payload 256 \
  --warmup 30 --duration 14400 --cooldown 30 --label future-four-hour
python3 scripts/perf/capacity_audit.py serve target/perf-audit/two-host/soak --max-seconds 14600
# Host B: copy public load bundle / forward sampler as above, then:
python3 scripts/perf/capacity_audit.py load <COPIED_SOAK_BUNDLE> \
  --sampler-port 24008 --interface <HOST_B_NIC> --output target/perf-audit/four-hour
```

Use distinct fixed base-port blocks for intentionally different campaigns;
otherwise run serially. Do not raise EventBus or inflight bounds to improve a
headline. Preserve raw JSONL/JSON/network/profile data under `target/perf-audit/`;
only compact summaries and this report belong in Git.

## Same-host versus separate-host comparison

| Scenario | Same host | Separate host | Delta |
|---|---|---|---|
| QoS1 healthy control | See measured control table | Not executed | Unavailable |
| QoS1 stable server knee | Not established | Not executed | Unavailable |
| QoS1 knee P99 | Unavailable | Not executed | Unavailable |
| Server CPU at knee | Unavailable | Not executed | Unavailable |
| Loadgen CPU at knee | Unavailable | Not executed | Unavailable |

The amount by which colocating the generator distorted historical capacity
cannot be quantified without the isolated comparison. Current controls expose
missed schedule slots; they do not establish whether the old 25–30k limit was
server-side, generator-side or a mixture. `pmset -g therm` reported no recorded thermal/performance warning or CPU power
status; frequency counters were not available. This does not prove thermal
stability; interleaved repeats reveal wall-rate variation but cannot causally
attribute it to thermal throttling.

## Updated bottleneck ranking and recommended next experiment

1. **Measurement limit:** no isolated generator/server result; actual offered
   rate can fail to track target. Resolve this before treating local plateaus
   as server saturation.
2. **Unresolved shared-host scheduling/kernel/server interaction:** lower total
   CPU does not exclude a serialized thread or timer/pacing limit.
3. **Workload-specific required sink:** judge HTTP/TCP queue and sink-ACK evidence
   separately; do not generalize a Python consumer limit to core ingress.

These are an evidence/measurement priority order, **not a new proven server
hotspot ranking**. Recommendation **E: loadgen/network still limits measurement**
(the network part remains unmeasured). Repeat the prepared matched two-host
workload with verified generator headroom and link capacity. EventBus architecture,
allocation/copy and kernel/runtime tuning are not justified by this audit yet.

## Correctness validation and repository state

All requested gates passed after the final benchmark-code cleanup:

| Gate | Result |
|---|---|
| Rust 1.88.0 `fmt --all -- --check` | PASS |
| Rust 1.88.0 locked workspace/all-targets/all-features Clippy, `-D warnings` | PASS |
| Rust 1.88.0 locked workspace/all-features tests | PASS, 140 passed |
| Stable 1.97.1 format / locked all-targets all-features Clippy / workspace tests | PASS, 140 tests passed |
| MQTT `--netbaiot-only` core gate | PASS |
| Python audit regression suite | PASS, five tests |
| Full `--release-gate` | Not required and not run: production runtime untouched |

Stable Clippy initially rejected a collapsible nested `if` inside the new audit
mode. It was rewritten as an equivalent let-chain. The histogram boundary race
described above was subsequently fixed; both toolchains and the MQTT core gate
were rerun successfully after that correction. This happened **after** measurements and
did not replace their frozen release binaries. Initial failure logs are retained
alongside final gate logs. No server/runtime production source changed.

Added/updated tooling: `capacity_audit.py`, `capacity_consumer.py`,
`capacity_summary.py`, `capacity_profiles.py`, focused Python regression tests,
and the existing Rust `netbaiot-loadgen` audit mode. Final release smoke checks
verified exact publish/ACK/histogram conservation: 5,000 measurement-origin
messages for QoS1 plaintext and 5,001 for QoS2 over TLS with the confirmed TCP
sink; both had zero client errors. These counts are observations within the
finite measurement window, not a claim of an exactly 1,000.000/s schedule. Compact committed evidence is
[the JSON summary](performance-separate-host-results.json) and
[the CSV point table](performance-separate-host-results.csv). Raw JSONL, bundle
configs, CPU/network samples and profiles remain in
`target/perf-audit/separate-host/` and are not committed.

Unexecuted measurements: actual separate-host sweeps and isolated knees; full
QoS0/QoS2 and 1-KiB saturation sweeps; physical network/link saturation;
per-core CPU; trustworthy kernel retransmit counts; actual multi-subscriber
fanout; dedicated connection-establishment capacity; 30-minute/4-hour soak and
realistic mixed workload. These omissions must not be represented as passes.

The audit is committed locally on the isolated audit branch. Nothing is pushed;
the original checkout and concurrent/user changes are preserved.

