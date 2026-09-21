# Performance bottleneck audit

## Executive conclusion

On the measured Apple M4 single-host loopback workload, NetbaIoT's no-subscriber
MQTT uplink path is not limited by packet parsing, hashing, session lookup, or
exact topic matching. The first material constraint is contention around the
serialized broker route and EventBus publish states. Kernel I/O plus runtime
scheduling is the second broad cost center. Per-event UUID entropy is a smaller
measured CPU cost. Allocation/copy/JSON work is visible but is not yet quantified
well enough to justify a structural rewrite.

The safe first optimization experiment is a narrowly scoped reduction of work or
acquisition frequency in the broker `route` critical section, preserving atomic
route preflight/commit and all MQTT semantics. It must be evaluated alone against
the same 64-publisher QoS1 sweep. Do not begin with sharding, lock-free structures,
an alternate hasher, or a new codec.

The evidence, methodology, and all absolute baseline tables are in
`docs/performance-baseline.md`. Percentages from macOS `sample` below are shares of
non-park top-of-stack samples, not exclusive CPU percentages.

## Ranking method

The ordering considers observed CPU/profile presence, frequency, latency and
scaling impact, feasibility, and correctness risk. It separates the primary
saturation mechanism from external sink, memory, and tail-latency constraints.
Only measured candidates are ranked; unavailable allocation counts are not
silently assigned scores.

## #1 Serialized broker route and EventBus state access

**Class:** primary saturation and tail-latency bottleneck.

### Evidence

- The healthy QoS1 point completes 19.96k/s. At offered 30k it completes 27.09k/s,
  and the shared-host plateau is about 27.8--31.4k/s across QoS modes.
- At 19.9k QoS1/s, broker route lock wait averages 5.06 us while hold averages
  only 0.81 us. Wait P50/P95/P99 is <=10/<=25/<=250 us. The highest occupied wait
  bucket is <=5 ms.
- EventBus wait averages 5.05 us while hold averages 0.47 us. Wait
  P50/P95/P99 is <=10/<=25/<=250 us. The highest occupied bucket is <=10 ms.
- Both locks are acquired at approximately 19.9k/s in this scenario. Their combined
  mean wait is about 10.1 us/publish, compared with about 1.28 us/publish of
  measured protected hold time.
- Admission wait averages only 0.70 us and its P95 remains <=10 us, making it a
  smaller serial point at the same load.
- `__psynch_mutexwait` is 2,187 of 6,263 non-park top-of-stack profile samples
  (34.9%). The profile call tree contains both broker route and EventBus waits.
- At 500/s, EventBus wait falls to 0.002 us mean while hold remains 0.70 us. This
  separates contention under load from inherently expensive EventBus work.

The `sample` mutex symbol aggregates process mutexes, so 34.9% must not be assigned
entirely to these two locks. The dedicated histograms are the attribution evidence.

### Why it matters

Every accepted uplink traverses both serial sections. The long wait tail appears
well before their protected work is individually expensive, making publisher
parallelism turn into contention rather than proportional throughput. The same
tail feeds protocol ACK latency.

### Expected optimization direction

Start with the broker route section because its measured hold mean is 72% larger
than EventBus hold mean. Audit which route preparation can be completed from an
immutable snapshot before acquiring the state lock, or whether an existing
per-message acquisition can be removed. Preserve deterministic preflight, atomic
fanout ownership, tenant accounting, retained mutation, packet-ID ordering, and
session-incarnation fencing.

This is an experiment direction, not authorization for a `Mutex` replacement or
broker sharding. If the small critical section cannot be shortened without
duplicating validation or weakening atomicity, stop and retain the current code.

### Risk

High correctness risk. This boundary owns MQTT session/QoS state and atomic
subscriber responsibility. Races can create lost messages, duplicate accounting,
stale-generation mutation, or invalid producer acknowledgement.

### Do not optimize yet

Do not simultaneously alter EventBus locking, hash maps, payload ownership, or
fanout structures. One isolated change is needed to attribute the result.

## #2 Kernel I/O, timekeeping, and Tokio wake/scheduling overhead

**Class:** secondary CPU/scaling bottleneck; partly workload/topology dependent.

### Evidence

- `sendto` and `recvfrom` account for 10.3% and 6.2% of non-park top-stack samples.
- `mach_absolute_time` is 5.4%, priority switching is 4.8%, `clock_gettime` is
  0.9%, and Tokio notify is 0.8%.
- The server peaks around 1.3--1.6 cores at the stable 20--25k/s points rather than
  saturating all ten host cores, consistent with serialized/wake-heavy processing.
- QoS2 at 20k/s uses 163.6% sampled server CPU and 90.4% loadgen CPU versus QoS1's
  131.8% and 78.6%, reflecting its additional protocol round trip and state work.
- The colocated generator itself consumes 70--93% of one core at stable high-rate
  points. A single-host plateau cannot be assigned solely to the server.

### Why it matters

Even after lock contention is reduced, small MQTT packets require socket read/write,
ACK generation, timer checks, and task wakeups. The relative cost increases for
QoS1 and QoS2 and can cap gains from purely in-process work reduction.

### Expected optimization direction

First move the load generator to a separate host and obtain a true CPU Time
Profiler trace. If server-side syscall cost remains material, inspect bounded write
coalescing and redundant timer reads/wakeups. Retain packet deadlines and bounded
writes; batching must not delay ACKs or break shutdown ownership.

### Risk

Medium to high. I/O batching and wake policy changes can increase tail latency,
weaken backpressure, retain oversized buffers, or delay shutdown.

### Do not optimize yet

The present profile is wall-stack sampling on a shared host. Do not redesign Tokio
task ownership or socket buffering from these percentages alone.

## #3 Per-event EventId entropy

**Class:** secondary CPU bottleneck.

### Evidence

- `getentropy` accounts for 227 of 6,263 non-park top-of-stack samples (3.62%),
  sixth among the observed top-stack symbols.
- The sampled call tree directly traces these stacks through
  `JsonV1::decode -> Uuid::new_v4 -> getentropy`; this is not attribution by symbol
  name alone.
- `JsonV1` creates one `EventId::generate()` per decoded device event, and the
  public protocol implementation uses `Uuid::new_v4()`.
- This path is therefore exercised at accepted-event frequency in the measured
  workload. An isolated EventId generation latency/syscall benchmark was not run.

### Why it matters

OS randomness is a nontrivial per-event cost and can interact with syscall and
lock pressure at high rates. Unlike packet parsing, it has direct profile evidence
above the materiality threshold.

### Expected optimization direction

First benchmark EventId generation alone and confirm the number of OS entropy
calls with Instruments. If the attribution holds, evaluate a standard,
cryptographically secure userspace UUIDv4 generator seeded from the OS at bounded
intervals, without changing the UUID wire representation or stable-ID semantics.

### Risk

High security and correctness risk. Weak, repeated, predictable, or fork-unsafe
IDs can collide and break business idempotency. A new identifier/wire format is
out of scope.

### Do not optimize yet

Do not introduce a custom PRNG. Require a maintained standard implementation,
collision analysis, restart/fork behavior tests, and a measured material gain.

## #4 Allocation, payload copying, and JSON/event materialization

**Class:** secondary CPU and memory-pressure candidate; measurement incomplete.

### Evidence

- `malloc_tiny` is 3.10%, `free` 2.27%, and another allocator small-block path
  0.64% of non-park top-stack samples. `memmove` and `memset` add 0.81% and 0.64%.
- JSON decode is 1.45% and escaped JSON serialization is 0.93% of those samples at
  the representative 256-byte workload.
- Source audit establishes at least two 256-byte payload copies per publish:
  decoded bytes into `BrokerMessage`, then `BrokerMessage::clone()` before route.
  The clone also copies the topic string. The lower bound is >=512 payload bytes
  per 256-byte event, not including JSON value construction or size serialization.
- JSON codec P50 is 1.5 us for a small payload, 33.3 us for the medium size, and
  175.9 us at the maximum benchmark size. Its importance is payload-dependent.
- Exact allocations/publish, allocated bytes/publish, live allocation peak, and
  BrokerMessage clone CPU contribution were not measured.

### Why it matters

This work occurs per event and can amplify allocator contention and memory bandwidth
after the serialized lock bottleneck is reduced. Large JSON payloads shift the
balance much more strongly toward codec cost.

### Expected optimization direction

The next action is measurement: add a benchmark-only counting allocator or capture
an Instruments Allocations trace, broken down by payload size and fanout. Only then
consider eliminating the no-subscriber `BrokerMessage` clone or sharing immutable
payload storage. Preserve strict packet bounds and ownership across async queues.

### Risk

Medium for local clone removal, high for changing payload/event public types.
Lifetime mistakes can retain large network buffers, bypass byte accounting, or
couple public protocol types to runtime internals.

### Do not optimize yet

Do not convert all payloads to `Bytes`, replace JSON, add a slab/custom allocator,
or change public event types without allocation and end-to-end CPU evidence.

## #5 Per-connection and persistent-state memory

**Class:** connection-scale memory bottleneck, not the message-rate bottleneck.

### Evidence

- Median plaintext idle RSS delta is 24.22 KiB/connection at 1k and
  22.88 KiB/connection at 3k. The one valid 10k run is 23.18 KiB/connection.
- TLS median is 32.14 KiB/connection at 1k and 30.34 KiB/connection at 3k, an
  incremental 7.46 KiB/connection at the scale-matched 3k point.
- Each active connection adds exactly one observed runtime task and one FD.
- After disconnect, 1,000 persistent sessions plus config/auth state retain a
  median 24.48 KiB/session RSS. Adding one subscription each increases the median
  experiment by 2.59 KiB/session.
- Exact broker logical disconnected-session state is only about 182 B/session;
  therefore process RSS is dominated by credentials/configuration, cache/table
  capacity, runtime objects, and allocator behavior rather than that record alone.

### Why it matters

At the observed 23 KiB plaintext delta, 50k idle connections would require over a
GiB of incremental process RSS before credentials, queues, kernel buffers, or active
traffic. That arithmetic illustrates priority but is not a capacity extrapolation.

### Expected optimization direction

Use `vmmap`/allocation tracing to decompose connection, credential, auth-cache, and
allocator-retention cost in separate processes. Then target the largest live class.
Keep count and byte admission reservations independent from actual lazy allocation.

### Risk

Medium. Shrinking buffers or consolidating tasks can harm partial-frame handling,
slow-reader isolation, deadlines, and shutdown ownership.

### Do not optimize yet

Do not infer that the 512 KiB logical reservation is resident memory, and do not
attribute RSS/connection solely to a connection struct.

## #6 External business sink capacity and sink-latency occupancy

**Class:** workload-dependent external throughput bottleneck.

### Evidence

- The confirmed HTTP path completed 9,999.45/s with every event ACKed.
- At that point NetbaIoT sampled 55.4% CPU, the loadgen 20.0%, and the Python
  webhook 76.6%. The external sink was the largest CPU consumer.
- End-to-end sink receipt P50/P95/P99 was 1/2/4 ms; required pending work peaked
  at 20 and did not grow.
- With configured 10 ms ACK delay at 500/s, EventAccepted-to-SinkAck averaged
  10.659 ms and pending work peaked at 6. With 1 ms delay at 5k/s, it peaked at 12.
- In the first two long mixed attempts, the benchmark webhook eventually blocked
  while writing periodic status to an undrained pipe. Required work then reached
  its exact 50,000-event bound after ~417 s, RSS peaked at 117 MiB, 301 events were
  rejected, and clients were closed. Removing periodic output eliminated this
  artificial bottleneck in an eight-minute validation (575,911/575,911 ACKed,
  zero retry/rejection, pending <=11).
- Confirmed TCP business-sink absence/reconnect/filter mismatch was not measured.

### Why it matters

Required sinks retain bounded EventBus ownership and concurrency until explicit
ACK. A real slow or absent business system can become the capacity limit even when
the MQTT ingress path is healthy.

### Expected optimization direction

Benchmark with a compiled sink on a separate host, then exercise 1/4/8 required
sinks and failure/retry. For confirmed TCP, specifically measure concurrency-slot
occupancy while no eligible stream exists. Tune configured concurrency and timeout
only from those results; do not alter acknowledgement semantics.

### Risk

High if changes weaken the exact EventAccepted boundary, sink acknowledgement, or
restart-spool ownership. Low for improving the benchmark sink itself.

### Do not optimize yet

The current 10k result is Python-sink-bound. It cannot justify rewriting NetbaIoT's
HTTP client or changing delivery confirmation.

## Material but not ranked as current hot-path bottlenecks

### Compact high-fanout route planning

Planning scales approximately linearly: median 22 us/10,090 B at 100 targets,
173 us/101,890 B at 1,000, and 343 us/204,890 B at 2,000. This is material for
large subscriber fanout, but the measured uplink capacity scenario has no
subscribers and the production limits bound fanout. Full delivery/commit and queue
cost are still unknown.

### TLS

At 10k QoS1/s TLS preserves throughput but adds about 6.9% relative sampled server
peak CPU, 0.18 ms PUBACK P99, and 7.46 KiB per idle connection at 3k. TLS is a
material security cost, not the first plaintext throughput bottleneck. It must not
be disabled to meet performance targets.

### Retained wildcard scan

Wildcard retained scan grows from 74.9 us at 1,000 messages to 298.2 us at 4,000.
It is a focused retained replay/query scalability concern, not a normal no-retain
uplink bottleneck.

## Proven poor optimization targets

These conclusions apply to the measured 256-byte, minimal-fanout uplink workload:

- **MQTT packet decoding:** representative P50 125 ns and five profile top stacks.
- **Session lookup:** P50 42 ns, P99 <=84 ns through 256 registry entries.
- **Exact/mixed subscription lookup:** P50 209 ns, P99 292 ns; router P50 167 ns
  from 100 through 10,000 entries in the tested fixture.
- **Hashing:** SipHash write was 31 non-park top-stack samples (0.49%). Replacing
  the hasher is not justified.
- **Topic ACL:** P50/P95/P99 42/84/84 ns.
- **Ingress admission bookkeeping:** P50/P95/P99 333/334/417 ns. Admission lock
  wait is materially below broker/EventBus wait.
- **Auth provider calls on publish:** none. Authentication occurs once per
  connection; normal publishes used the bound immutable authentication context.

## Candidate optimization backlog

Priorities are performance priorities, not correctness severities.

### P0-perf

#### Isolated broker-route critical-section experiment

- **Evidence:** 5.06 us mean wait, <=250 us P99 bucket, 0.81 us mean hold at
  19.9k/s; largest measured application lock hold.
- **Expected gain:** target at least 10% QoS1 knee throughput or at least 25%
  reduction in broker wait sum/P99 at unchanged load. This is a target, not a claim.
- **Complexity:** medium to high.
- **Correctness risk:** high.
- **Benchmark required:** identical 64-publisher QoS1 10/20/30/40k sweep, route
  lock histograms, raw MQTT state-machine/conformance tests, slow sink, restart.
- **Rollback:** revert if throughput/CPU improvement is within 0.36% observed
  stable-rate variation, P99 regresses, or any semantic test changes.

#### Separate-host open-loop capacity and overload rig

- **Evidence:** colocated loadgen reaches 70--93% of one core and bounded windows
  flatten offered load without server rejection.
- **Expected gain:** measurement quality, not runtime gain; establish server-only
  stable ceiling and exact overload behavior.
- **Complexity:** medium infrastructure work.
- **Correctness risk:** low.
- **Benchmark required:** physical network, synchronized metadata, server and
  generator CPU, open-loop offered rate, bounded queue/RSS telemetry.
- **Rollback:** discard results if generator cannot sustain at least 2x the server
  accepted rate or clocks/metadata are incomplete.

### P1-perf

#### EventId generator measurement and standard CSPRNG experiment

- **Evidence:** `getentropy` is 3.62% of non-park top-stack samples and one UUIDv4
  EventId is generated per decoded event.
- **Expected gain:** eliminate repeated kernel entropy calls and reduce CPU/event
  by at least 3% before the change is worth retaining.
- **Complexity:** medium.
- **Correctness risk:** high because collisions break idempotency guarantees.
- **Benchmark required:** EventId ns/op and syscall count, multi-thread uniqueness,
  restart/fork simulation, full serialization compatibility, same MQTT sweep.
- **Rollback:** any collision/predictability concern, wire change, or <3% CPU gain.

#### EventBus critical-section experiment

- **Evidence:** 5.05 us mean wait and <=250 us P99 bucket; 0.47 us mean hold.
- **Expected gain:** >=20% EventBus wait-sum reduction at 20k/s without changing
  acceptance latency or semantics.
- **Complexity:** medium.
- **Correctness risk:** high because admission/fanout ownership is atomic.
- **Benchmark required:** required-sink rollback, byte/count cleanup, slow-sink
  isolation, drain/spool, duplicate replay, same MQTT sweep.
- **Rollback:** any acceptance semantic change, queue leak, or gain within noise.

#### Allocation trace and one-copy experiment

- **Evidence:** >=512 payload bytes source-audited copies/event; allocator/free
  symbols total at least 6% of non-park top stacks, but counts are unknown.
- **Expected gain:** measurement phase first; only retain a later change if it cuts
  allocated bytes/event >=20% and CPU/event >=5% at unchanged tails.
- **Complexity:** low for measurement, medium for ownership change.
- **Correctness risk:** medium.
- **Benchmark required:** counting allocator/Allocations trace at 64 B--64 KiB,
  fanout 0/1/8, queue retention, disconnect/shutdown.
- **Rollback:** retained-buffer growth, accounting mismatch, or CPU gain in noise.

#### Connection-memory decomposition

- **Evidence:** 22.9--24.2 KiB plaintext RSS/connection; broker logical session
  record is only ~182 B.
- **Expected gain:** identify a live class capable of >=15% RSS/connection reduction.
- **Complexity:** medium.
- **Correctness risk:** low for profiling, medium for implementation.
- **Benchmark required:** `vmmap`/allocations at 0/1k/3k/10k and after disconnect.
- **Rollback:** any buffer-bound regression or <10% repeatable RSS gain.

#### Real required-sink and confirmed-TCP matrix

- **Evidence:** Python sink is already the largest CPU user at 10k/s; confirmed TCP
  occupancy is unknown.
- **Expected gain:** measurement and configuration guidance first.
- **Complexity:** medium.
- **Correctness risk:** low for the rig, high for later sink semantics changes.
- **Benchmark required:** 1/4/8 sinks, 0/1/10 ms, unavailable/retry/reconnect/filter.
- **Rollback:** discard any result that lacks explicit sink ACK and queue telemetry.

### P2-perf

#### Large-payload JSON investigation

- **Evidence:** codec P50 rises from 1.5 us small to 175.9 us maximum; only 1.45%
  of non-park top stacks at 256 B.
- **Expected gain:** payload-size-specific CPU reduction, not default-path gain.
- **Complexity:** low to medium for parser-level improvements.
- **Correctness risk:** medium; public codec compatibility is frozen.
- **Benchmark required:** valid/malformed 64 B--64 KiB, allocation profile, exact
  serialization compatibility.
- **Rollback:** any wire change or small-payload regression.

#### Retained wildcard lookup investigation

- **Evidence:** 74.9 us at 1k versus 298.2 us at 4k.
- **Expected gain:** improve retained replay/query only.
- **Complexity:** medium.
- **Correctness risk:** medium.
- **Benchmark required:** exact/`+`/`#` correctness and memory at configured bounds.
- **Rollback:** missed/extra retained match or memory growth.

Hash replacement, generic packet parser work, and session-lookup changes are not in
the backlog because current evidence says they are below material threshold.

## Recommended first optimization task

Create one branch that changes only broker route critical-section structure. Begin
with a precise source/trace inventory of work done under the lock, then move only
pure, bounded preparation that does not depend on mutable broker state outside it.
Do not change the lock type, split/shard ownership, or alter fanout/QoS semantics in
the first experiment.

Acceptance target:

- at least 10% improvement in the shared-host QoS1 saturation-knee throughput **or**
  at least 25% reduction in broker route wait sum and P99 bucket at the same 20k/s;
- no regression beyond run variance in PUBACK P99, EventBus wait, CPU/event, or RSS;
- the full correctness gate, raw state-machine tests, conformance, required fanout,
  restart/recovery, and slow-sink tests remain green.

If the gain is within the 0.36% stable-throughput variation, revert it. If moving
work outside the lock weakens a state-dependent validation or duplicates large
route state, stop rather than compensate with a broader rewrite.

## Measurement gaps before later optimization

No optimization claim should be made yet for:

- exact allocation count/bytes and BrokerMessage clone CPU;
- attach/subscribe/unsubscribe/QoS ACK/offline-promotion broker lock sections;
- full subscriber fanout commit/delivery;
- confirmed TCP absence/reconnect occupancy;
- server-only open-loop overload response;
- four-hour memory drift;
- TLS profile and handshake capacity.

Those gaps are first-class audit findings. They are not zeroes.
