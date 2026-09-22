# EventBus route preparation lock experiment

The original A/B round below is historical. The follow-up **Experiment C: batch
worker dequeue** is appended at the end, with its own exact measurement baseline.

BASELINE_SHA: `8a94e62d90c2f19d3a19de480a9711e1bde4775b`.
Initial checkout: `codex/dual-host-qos1-validation`, clean. No reset is used.
Baseline source is an exact `git archive` of that commit, built into an independent
target directory under `/tmp/netbaiot-eventbus-lock`. Results below will refer to
the tested candidate, even if it is subsequently reverted.

## Source accounting before implementation

Audit: every production `State` access uses `lock_state`; direct `state.lock()`
appears only in test fixtures. `State` contains routes/revision, per-sink queues,
counts/bytes/inflight, active ownership, global bytes and the admission gate.
Let F = selected sink count, S = configured sinks, R = routes plus their sink
entries, A = active events, Q = records in the selected sink queue. T = delivery
attempts/event (across sinks), E = unsuccessful worker takes/event.

| Path | Acquisitions | Read/write; work held | Allocation / clone | Traversal / containers | Metrics / clock | Growth |
| --- | --- | --- | --- | --- | --- | --- |
| publish | 1/event | W; gate, routing, global and atomic required preflight, commit all queues and active owner | target/remaining/required sets, notify Vec, possible queue/HashMap growth; SinkId/Arc clones | all routes; two target passes; BTreeMap lookups, VecDeque push, active insert | probe; drop metric; wall clock and Instant per sink; timing | O(R + F log S), amortized active insert |
| take_ready | T+E/event | W; find earliest due record, remove, increment inflight | none; moves record | sink lookup, full queue scan, VecDeque remove | probe, Instant | O(Q) |
| next_ready_delay | normally E/event | R; earliest remaining delay | none | sink lookup, full queue scan | probe, Instant | O(Q) |
| successful completion | 1/attempt | W; decrement inflight, store attempt, release sink count/bytes, remove sink ownership, possibly remove active | attempts BTreeMap insert/SinkId clone; free final owner | active lookup twice/removal, sink lookup twice, set removals | probe, wall clock, ACK histogram/counter | O(log F + log S), expected O(1) active |
| retry / terminal required failure | 1/attempt | W; decrement inflight, record attempt, requeue still-owned responsibility | attempt entry/SinkId clone, possible queue growth | active lookup, sink lookups, VecDeque push | probe, wall clock, Instant, retry/failure counter and notify under lock | O(log F + log S) |
| terminal best-effort failure | 1/attempt | W; same release path as success | same as success | same as success | drop counter; drain notification after unlock | same as success |
| restore | 1/record | W; duplicate/global/fanout and required preflight, queue and active ownership commit | sets/Vec/queue/HashMap; SinkId/Arc/attempt clones | pending sinks twice; active lookup/insert | probe, Instant per sink | O(F log S) per record |
| spool_records | 1/snapshot | R; snapshot every pending required event, including inflight | full DeviceEvent clone and pending/attempt/record collections | active scan, sort output by ID | probe only | O(A F + output log output + payload bytes) |
| usage / drain | 1/call or drain wake | R; active len/bytes and required sum | none | all active events | probe only | O(A) |
| route replace / validate | 1/call | W/R; validate references/limits/revision; replace routes | temporary unique sets; replacement frees old routes | all proposed routes and sink lookups | probe only | O(R log S) |
| close_admission | 1/call | W; accepting=false | none | none | probe only | O(1) |
| worker startup | 1 + S total | R; collect IDs, clone definition/notify per worker | ID Vec and clones | all sinks then one lookup per worker | probe only | O(S log S) |
| stop/cancellation | no direct State lock | cancel fixed workers, abort/join bounded inflight tasks; active required ownership remains for spool | handle Vec moved | worker handles | no State metrics | O(S + inflight) |

The worker uses bounded JoinSet delivery tasks already present at baseline; this
experiment must not add tasks. A no-retry one-sink event needs publish + take +
completion, plus failed takes/deadline queries. No lock implementation change is
proposed. Existing opt-in `EventBusLockWait/Hold` measures successful publish;
`EventBusStateWait/Hold` covers all State acquisitions including observers. Probe
counters distinguish publish/take/complete/deadline/other. Histograms are integer
microseconds with coarse upper-bound quantiles, not exact P95/P99 samples.

## First hypothesis

Moving route matching and deterministic target-set construction before mutable
State admission reduces serialized publish work. Keep one consistent immutable
revision/routes snapshot; preserve admission linearization by validating its
identity under the State lock, falling back to the current route snapshot on a
concurrent update. No optimistic retry loop, rollback, per-sink lock, or new task.
Required capacity preflight and all ownership/accounting commits stay together.

First experiment: REVERT. Three paired 20k runs gave State wait-sum medians
5,615,896 -> 5,142,212 us (-8.43%), below 20%. Full raw evidence, binary hashes,
and the rejected patch are in `eventbus-lock-evidence/route-snapshot/`. Snapshot
wait is recorded separately; all mutex acquisitions actually increase by one per
publish. The snapshot's runtime changes were removed before experiment two.
Its runtime suite passed including concurrent revision/target consistency.

## Second hypothesis: final completion retirement

Baseline completion allocates an attempts-map entry and clones SinkId even when
the same critical section immediately destroys the last ActiveEvent. Skip that
unobservable metadata allocation only on final release, and retire the removed
ActiveEvent outside the mutex. Still write the exact attempt metadata whenever
any responsibility remains, on every retry and every terminal required failure.
All route selection, admission, queueing, scheduling and lock count are unchanged.
The final ACK/release still linearizes under the original State mutex, and a
snapshot can see either the pending owner before completion or its removal after
completion. Resource counters are updated before unlocking; only destructor work
is deferred. No other completion can access the removed owner. No new lock,
waiting work, task, timer, cache, or rollback path is introduced.

## Method and reproducibility

Machine: Apple M4 Mac mini (Mac16,10), 10 cores (4 performance + 6 efficiency),
16 GiB RAM; macOS 26.6.2 (25G83), Darwin 25.6.0 aarch64.
`rustc 1.97.1 (8bab26f4f 2026-07-14)`, LLVM 22.1.6. Default release profile,
locked dependencies, no runtime/allocator/CPU-affinity changes. Shared desktop,
loopback generator and server on this same host; these are not production capacity
measurements. No benchmark ran alongside a Rust build or test suite.

Baseline construction:

```sh
mkdir -p /tmp/netbaiot-eventbus-lock/baseline-src
git archive 8a94e62d90c2f19d3a19de480a9711e1bde4775b | tar -x -C /tmp/netbaiot-eventbus-lock/baseline-src
CARGO_INCREMENTAL=0 cargo build --locked --release \
  --manifest-path /tmp/netbaiot-eventbus-lock/baseline-src/Cargo.toml \
  --target-dir /tmp/netbaiot-eventbus-lock/baseline-target \
  -p netbaiot-server -p netbaiot-loadgen -p netbaiot-runtime --example eventbus_probe --bins
```

Each experiment directory retains a patch against BASELINE_SHA, and manifests
with exact commands and SHA-256s of before/after/loadgen binaries. Apply only one
patch (`gzip -dc candidate.patch.gz | git apply`) to baseline and build with `cargo build --locked --release -p netbaiot-server
-p netbaiot-runtime --example eventbus_probe --bins`. The shared measurement
scripts in this change run both binaries; the load generator is always the exact
baseline binary. No historical report's numbers enter the comparison.

```sh
python3 scripts/perf/eventbus_lock_pairs.py \
  --before /tmp/netbaiot-eventbus-lock/baseline-target/release/netbaiot-server \
  --after target/release/netbaiot-server \
  --loadgen /tmp/netbaiot-eventbus-lock/baseline-target/release/netbaiot-loadgen \
  --output /tmp/eventbus-results
# Run the same command with --secondary for knee and publisher scaling.
```

Primary: QoS1, no subscribers, required in-process audit sink (`--sink-mode none`
means no webhook, not no sink), 64 publishers, 256-byte payload, 20k offered/s,
5s configured warmup, 20s measurement, 2s cooldown. Existing loadgen warmup is
connected idle settling after ramp, not 5s of discarded publishing. Counters
therefore cover the measured publishes. Three repetitions per side, alternating
B/A, A/B, B/A. The initial baseline-only pilot is excluded from medians.
Secondary knee runs use 20s; 1/100/1000 publishers use 10s. All timings explicitly
enable `NETBAIOT_PERF_LOCK_METRICS=1`; instrumentation remains off by default.

`ps time` adds cumulative server CPU seconds before shutdown, including startup
and warmup, to the existing sampled CPU/RSS. CPU/event divides that value by
accepted events; it is more meaningful than peak CPU/rate but includes fixed setup
cost. Pending/RSS/CPU peaks are sampled at ~1s, not exact maxima. Integer-us
histograms truncate sub-us samples; quantiles shown as upper bucket bounds.
The first sandboxed network invocation failed to bind loopback before running;
authorized benchmark processes were then run outside that restriction.

## Source accounting after

Experiment one changes only route preparation on the common path: one additional
short snapshot acquisition, route traversal/target allocation outside State; a
concurrent revision change falls back to the original work under State, with no
retry loop. It adds no per-event payload clone. Other rows of the source table
remain identical. Its additional snapshot wait is included in the evidence.

Experiment two changes only final completion: State acquisitions stay identical;
the final attempts-map insert/SinkId clone is skipped and removed ActiveEvent
destruction happens after unlock. For a successful one-sink event this removes
one temporary BTreeMap-node allocation plus one SinkId string clone; for 4/8 sinks
it avoids the final SinkId clone (tree allocation depends on existing map state).
These are source allocation counts, not measured allocator traces. Publish
allocations, full-payload sharing, scans, queue operations and notifications are
unchanged. No inference of lower total allocations from latency alone is made.

## Decision: REVERT both candidates

The route-snapshot candidate's primary State wait-sum reduction is 8.43%, below
20%; including its additional snapshot lock reduces even that small benefit.
The completion-retirement candidate increases primary median State wait sum
4,103,835 -> 4,390,096 us (+6.98%), while publish-only wait sum falls just 5.33%.
Its accepted rate is essentially flat (19,994.50 -> 19,993.15/s, -0.007%), but
PUBACK P99 worsens 1.34 -> 1.49 ms (+11.19%), beyond the 5% tolerance, and CPU/event
rises 46.10 -> 47.68 us (+3.42%). Neither candidate qualifies for KEEP.

No third experiment or sharding is attempted: the evidence does not justify
multi-lock atomic fanout complexity. The runtime production code is restored to
BASELINE_SHA; retain only regression tests, measurement helpers, raw evidence and
this report. No evidence supports updating `performance-bottleneck-audit.md` as a
successful optimization. Historical reports remain intact.

Single-run secondary measurements are diagnostic, not confidence intervals.
Baseline medians differ across the two sequential campaigns (5.62s versus 4.10s
State wait sum), demonstrating substantial shared-host/scheduling variability.
Within the first candidate its third P99 is much lower than its first two; report
all repetitions and use the specified median instead of selecting that run.
These measurements establish no capacity increase or significant contention
reduction. Serialized EventBus State access remains an unresolved synchronization
candidate; no newly dominant subsystem is established without a new profile.
## Measured results: route-snapshot

Before/After below always refer to the candidate experiment, not the final runtime.

| Run | Accepted/s | PUBACK/s | P50/P95/P99 ms | CPU us/event | CPU peak % | RSS KiB | Pending peak |
| --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 19,973.650 | 19,973.650 | 0.330/1.020/1.540 | 52.669 | 129.9 | 7952 | 49 |
| after 1 | 19,987.050 | 19,987.050 | 0.330/1.000/1.560 | 54.285 | 132.9 | 8000 | 32 |
| before 2 | 19,991.850 | 19,991.850 | 0.340/1.040/1.530 | 56.223 | 131.6 | 7984 | 50 |
| after 2 | 19,988.450 | 19,988.450 | 0.310/0.950/1.490 | 52.355 | 124.7 | 7824 | 33 |
| before 3 | 19,938.300 | 19,938.300 | 0.300/0.790/1.400 | 44.562 | 108.0 | 8384 | 45 |
| after 3 | 19,999.550 | 19,999.550 | 0.220/0.480/0.700 | 45.601 | 107.4 | 7936 | 19 |

| Run | State acquisitions | Wait sum us | Wait mean us | Wait P95/P99 <=us | Hold sum us | Hold mean us | Hold P95/P99 <=us | Publish wait sum us | Publish wait mean us | Publish hold sum us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 1747578 | 5615896 | 3.214 | 10.0/100.0 | 836797 | 0.479 | 10.0/10.0 | 2679788 | 6.708 | 172802 |
| after 1 | 1826008 | 5425889 | 2.971 | 10.0/100.0 | 759242 | 0.416 | 10.0/10.0 | 2578538 | 6.451 | 118847 |
| before 2 | 1768798 | 6105727 | 3.452 | 10.0/100.0 | 934127 | 0.528 | 10.0/10.0 | 3014423 | 7.539 | 194057 |
| after 2 | 1807690 | 5142212 | 2.845 | 10.0/100.0 | 747877 | 0.414 | 10.0/10.0 | 2438258 | 6.099 | 124656 |
| before 3 | 1667443 | 4573725 | 2.743 | 10.0/100.0 | 689924 | 0.414 | 10.0/10.0 | 2453621 | 6.153 | 93552 |
| after 3 | 1882611 | 3423463 | 1.818 | 10.0/50.0 | 627598 | 0.333 | 10.0/10.0 | 1682807 | 4.207 | 66256 |

| Median metric | Before | After | Delta |
| --- | --- | --- | --- |
| Accepted/s | 19,973.650 | 19,988.450 | +0.07% |
| PUBACK/s | 19,973.650 | 19,988.450 | +0.07% |
| PUBACK P50 ms | 0.330 | 0.310 | -6.06% |
| PUBACK P95 ms | 1.020 | 0.950 | -6.86% |
| PUBACK P99 ms | 1.530 | 1.490 | -2.61% |
| CPU us/event | 52.669 | 52.355 | -0.60% |
| RSS peak KiB | 7,984.000 | 7,936.000 | -0.60% |
| Pending peak | 49.000 | 32.000 | -34.69% |
| State locks/event | 4.375 | 4.568 | +4.42% |
| State wait sum us | 5,615,896.000 | 5,142,212.000 | -8.43% |
| State wait mean us | 3.214 | 2.845 | -11.48% |
| State hold sum us | 836,797.000 | 747,877.000 | -10.63% |
| State hold mean us | 0.479 | 0.414 | -13.60% |
| Publish wait sum us | 2,679,788.000 | 2,438,258.000 | -9.01% |
| Publish hold mean us | 0.433 | 0.297 | -31.27% |

Additional snapshot-lock wait (candidate median): 109,890.000 us; mean 0.275 us. It adds exactly one mutex acquisition/publish; this is not hidden in State acquisition counts.

## Measured results: completion-retirement

Before/After below always refer to the candidate experiment, not the final runtime.

| Run | Accepted/s | PUBACK/s | P50/P95/P99 ms | CPU us/event | CPU peak % | RSS KiB | Pending peak |
| --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 19,999.300 | 19,999.300 | 0.230/0.530/0.840 | 46.102 | 111.9 | 7936 | 16 |
| after 1 | 19,995.950 | 19,995.950 | 0.250/0.710/1.290 | 48.110 | 120.9 | 7904 | 17 |
| before 2 | 19,994.500 | 19,994.500 | 0.320/0.890/1.340 | 47.113 | 114.6 | 8000 | 45 |
| after 2 | 19,993.150 | 19,993.150 | 0.290/0.850/1.490 | 45.891 | 106.1 | 8016 | 44 |
| before 3 | 19,979.550 | 19,979.550 | 0.240/0.710/1.420 | 44.120 | 116.0 | 8080 | 19 |
| after 3 | 19,967.100 | 19,967.100 | 0.310/1.060/1.830 | 47.678 | 115.7 | 7952 | 44 |

| Run | State acquisitions | Wait sum us | Wait mean us | Wait P95/P99 <=us | Hold sum us | Hold mean us | Hold P95/P99 <=us | Publish wait sum us | Publish wait mean us | Publish hold sum us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 1824480 | 4002426 | 2.194 | 10.0/100.0 | 760796 | 0.417 | 10.0/10.0 | 2101026 | 5.253 | 104372 |
| after 1 | 1880234 | 4162863 | 2.214 | 10.0/100.0 | 621386 | 0.330 | 10.0/10.0 | 1829069 | 4.574 | 117458 |
| before 2 | 1728471 | 5012731 | 2.900 | 10.0/100.0 | 724395 | 0.419 | 10.0/10.0 | 2549090 | 6.374 | 120391 |
| after 2 | 1769946 | 4390096 | 2.480 | 10.0/100.0 | 599360 | 0.339 | 10.0/10.0 | 2041131 | 5.105 | 97744 |
| before 3 | 1796155 | 4103835 | 2.285 | 10.0/100.0 | 692881 | 0.386 | 10.0/10.0 | 2155960 | 5.395 | 93821 |
| after 3 | 1767997 | 4840317 | 2.738 | 10.0/100.0 | 664201 | 0.376 | 10.0/10.0 | 2269694 | 5.684 | 123072 |

| Median metric | Before | After | Delta |
| --- | --- | --- | --- |
| Accepted/s | 19,994.500 | 19,993.150 | -0.01% |
| PUBACK/s | 19,994.500 | 19,993.150 | -0.01% |
| PUBACK P50 ms | 0.240 | 0.290 | +20.83% |
| PUBACK P95 ms | 0.710 | 0.850 | +19.72% |
| PUBACK P99 ms | 1.340 | 1.490 | +11.19% |
| CPU us/event | 46.102 | 47.678 | +3.42% |
| RSS peak KiB | 8,000.000 | 7,952.000 | -0.60% |
| Pending peak | 19.000 | 44.000 | +131.58% |
| State locks/event | 4.495 | 4.427 | -1.51% |
| State wait sum us | 4,103,835.000 | 4,390,096.000 | +6.98% |
| State wait mean us | 2.285 | 2.480 | +8.56% |
| State hold sum us | 724,395.000 | 621,386.000 | -14.22% |
| State hold mean us | 0.417 | 0.339 | -18.79% |
| Publish wait sum us | 2,155,960.000 | 2,041,131.000 | -5.33% |
| Publish hold mean us | 0.261 | 0.294 | +12.56% |

| Scenario | Side | Accepted/s | P50/P95/P99 ms | State wait sum us | Wait mean us | CPU us/event | RSS KiB | Pending peak |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| q1-25k | before | 24,205.000 | 0.330/0.960/1.780 | 6,446,447.000 | 3.195 | 44.536 | 7888 | 62 |
| q1-25k | after | 24,137.550 | 0.310/1.040/2.000 | 5,784,871.000 | 2.835 | 44.205 | 8000 | 38 |
| q1-30k | before | 26,708.750 | 0.360/1.090/1.940 | 7,723,794.000 | 3.568 | 45.135 | 8320 | 73 |
| q1-30k | after | 26,493.950 | 0.330/1.000/1.870 | 6,361,166.000 | 2.945 | 43.029 | 8112 | 84 |
| publishers-1 | before | 8,314.100 | 0.060/0.080/0.110 | 105.000 | 0.000 | 20.567 | 5280 | 1 |
| publishers-1 | after | 9,112.000 | 0.060/0.080/0.110 | 84.000 | 0.000 | 18.547 | 5280 | 1 |
| publishers-100 | before | 19,963.000 | 0.420/1.140/1.920 | 2,824,708.000 | 3.626 | 44.582 | 8864 | 0 |
| publishers-100 | after | 19,985.100 | 0.240/0.980/2.010 | 2,018,636.000 | 2.185 | 45.034 | 8832 | 23 |
| publishers-1000 | before | 19,998.100 | 0.250/0.620/1.090 | 1,697,076.000 | 1.789 | 53.405 | 31824 | 40 |
| publishers-1000 | after | 19,995.900 | 0.310/7.740/12.620 | 1,357,191.000 | 1.351 | 39.058 | 32528 | 50 |

## Fanout, delayed/retried ACK and slow-sink evidence

Three alternating independent process repetitions per side of the completion experiment.
Latency columns are medians of per-run quantiles (not pooled quantiles). These short,
bounded direct EventBus probes are diagnostic bursts, not network capacity or an SLA.

| Scenario | Side | Completed events/s | Publish P50/P95/P99 us | State wait mean us | State wait P99 <=us | State locks/event |
| --- | --- | --- | --- | --- | --- | --- |
| 1 sinks / 0ms / retry=False | before | 222654.5 | 2.000/9.291/14.500 | 0.781 | 10 | 3.578 |
| 1 sinks / 0ms / retry=False | after | 263667.6 | 1.583/8.583/15.417 | 0.766 | 10 | 3.184 |
| 4 sinks / 0ms / retry=False | before | 50674.2 | 6.667/45.000/72.750 | 6.853 | 100 | 9.023 |
| 4 sinks / 0ms / retry=False | after | 50673.3 | 6.459/45.291/71.459 | 6.861 | 100 | 9.034 |
| 8 sinks / 0ms / retry=False | before | 22434.7 | 9.209/88.833/142.250 | 17.137 | 250 | 17.017 |
| 8 sinks / 0ms / retry=False | after | 22605.5 | 8.958/89.792/145.042 | 16.948 | 250 | 17.064 |
| 1 sinks / 1ms / retry=False | before | 3533.8 | 1.125/16.209/29.750 | 1.072 | 25 | 3.010 |
| 1 sinks / 1ms / retry=False | after | 3505.5 | 1.041/17.417/29.667 | 1.029 | 25 | 3.009 |
| 1 sinks / 10ms / retry=False | before | 672.5 | 1.125/18.208/28.500 | 1.114 | 25 | 3.012 |
| 1 sinks / 10ms / retry=False | after | 672.6 | 1.166/16.625/26.750 | 1.087 | 25 | 3.009 |
| 1 sinks / 0ms / retry=True | before | 18822.5 | 2.333/17.000/30.875 | 0.751 | 25 | 6.208 |
| 1 sinks / 0ms / retry=True | after | 18831.7 | 2.208/15.125/25.500 | 0.719 | 25 | 6.020 |

Raw per-run publish, completion, depth, wait/hold sums and quantiles, and probe counters
are retained in `completion-retirement/*-micro-*.jsonl` and `micro-summary.json`.
No allocator trace was run; temporary allocation analysis is source-based above.

| Blocked required sink plus fast required sink | Accepted | Rejected | Slow backlog | Event bytes | Fast ACK/s | Idle worker select returns |
| --- | --- | --- | --- | --- | --- | --- |
| before | 1024 | 1024 | 1024 | 241492 | 164037.9 | 0 |
| after | 1024 | 1024 | 1024 | 241492 | 138911.8 | 0 |

Both isolation processes assert that every fast delivery completes while the slow sink
is blocked, both overload count and bytes remain bounded, and all EventBus accounting
returns to zero after release. The static blocked window is 100ms, not a long memory soak.
These probes preserve one shared Arc<DeviceEvent>; regression tests independently check
Arc pointer identity across 1/4/8 restored inflight deliveries.

## Interpretation and limits

At 25k offered/s the completion candidate achieves 24,137.55 versus 24,205.00/s;
at 30k it achieves 26,493.95 versus 26,708.75/s. The approximate 25–30k knee does
not improve, so 35k was not added. The one-publisher test is client-window and
round-trip limited; it does not reach its configured 20k offered target. At 1,000
publishers the single candidate run has a much worse PUBACK P99 (12.62 vs 1.09ms)
despite lower measured State wait. This cannot be attributed confidently from one
shared-host pair, but clearly prevents any claim of robust tail improvement.

Every network run has accepted = published = PUBACK = sink ACK, every requested
publisher connected, no client error samples, no unacknowledged publish at
disconnect, no queue/ingress/protocol/event rejects, no retries or sink failures,
and exit status 0. Every final sampled pending count is 0. Exact checks are retained
in `eventbus-lock-evidence/measurement-checks.txt`. The primary pending peak rises
19 -> 44 for candidate two, but does not show persistent accumulation. A sampled
zero peak (one 100-publisher baseline run) means the observer missed transient
work, not that no required work existed. Finite runs with all generated publishes
acknowledged show no lost accepted work; aggregate counters do not establish a
per-publisher latency bound or prove scheduler fairness.

The candidate's shorter aggregate State hold time (-14.22%) did not translate into
lower aggregate wait or better primary tails. Direct one-sink microbenchmark
throughput improves, while 4/8-sink throughput is essentially unchanged. Neither
is grounds to override the failed network acceptance criteria.

No per-sink sharding, lock replacement, broker/UUID/codec/runtime/network/sink
optimization or hot-path persistence was introduced. No allocator trace, physical
network/remote-generator campaign, integrated CPU profile, timing-disabled
comparison, new fuzz run, or multi-hour memory soak was performed. No parser,
framing or spool format changed, so no new fuzz target is needed. The short delayed
and blocked-sink fixtures are not substitutes for long-duration outage/memory
testing; the required restart soak is reported separately below.

## Correctness and final working tree

FINAL working-tree state: production `event.rs` is byte-identical to BASELINE_SHA
(everything preceding `#[cfg(test)]`), and `metrics.rs` has no diff. The evidence
commit contains no performance implementation. Three retained regressions cover:

- 1/4/8 required sinks, shared Arc identity, exact global bytes/count, restored
  attempts/revision/accepted_at/event_id, inflight snapshot, partial ACK metadata,
  final per-sink count/byte/inflight/queue cleanup;
- full last required target by count and by bytes, with every queue/counter and
  active ownership unchanged on rejected fanout;
- cancellation of a live blocked delivery preserving accepted required ownership
  and stable event ID for spool.

The first candidate passed 38 runtime tests (one ignored); the second passed 39
(one ignored). Both candidate and baseline ran the existing release-only ignored
queue-depth probe. Existing regressions for best-effort overload, delayed retry,
retry exhaustion/recovery, sink panic, timeout, slow-sink isolation and scheduler
spin all passed. No test/assertion was weakened, timeout enlarged, default limit
increased, or benchmark offered rate lowered.

Final gates (Rust 1.97.1):

```sh
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
CARGO_INCREMENTAL=0 cargo test --locked --workspace --all-features
python3 tests/mqtt_conformance/run.py --release-gate --no-build
CARGO_INCREMENTAL=0 cargo test --locked -p netbaiot-server --test server \
  subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

Format and warning-denying Clippy passed. Workspace tests: **141 passed, 0 failed,
4 intentionally ignored**. This includes subprocess planned recovery, spool
failure/repair, SIGKILL bounded-loss, MQTT session/QoS recovery, EventBus confirmed
TCP isolation, and spool checksum/corruption/capacity tests. MQTT release gate:
**76/76 passed**, including raw state-machine checks, mature Mosquitto client and
reference interoperability, and **125/125** normative requirements with current
PASS evidence. Python measurement scripts were syntax-checked without writing
bytecode caches. Full gate outputs are in `eventbus-lock-evidence/final/`.

The ignored **60-second multi-generation planned-restart soak passed in 61.82s**.
It verifies no missing accepted required event IDs across restart generations;
duplicate replay remains permitted. SIGKILL tests verify the intentional bounded
memory-loss window, not abrupt-crash durability.

No push was performed. Final commit purpose: retain the rejected experiments'
reproducible evidence and independent EventBus regressions; do not ship either
unqualified performance implementation.

## Experiment C: measurement baseline (2026-09-23)

START_SHA: `fd734c02822a5b4b701e7907c219d72d230749b1`, clean checkout.
Before changing dispatch, add only opt-in measurement and its tooling in a
separate commit. That commit is the actual `BASELINE_SHA` for C and is built from
an exact Git archive into an independent target directory. This avoids calling
an uncommitted instrumentation patch an exact baseline build. A/B production
implementations remain reverted; `complete()` remains untouched throughout C.

The five fixed sites are publish, take_ready, next_ready_delay, complete, and
control_restore_spool (including usage/startup). Each records nanosecond wait and
hold histograms/count/sum using the same start/stop instants as legacy aggregate
timing. Hold includes mutex release; all histogram atomics execute after release.
No device, tenant, sink or event labels. Disabled metrics allocate no new timing
storage and perform no extra clock reads or histogram atomics; only fixed field
bookkeeping/enable checks remain. Enabled timing allocates a single fixed-size
metrics block, not per-event samples or queues.

Dequeue instrumentation records initial queue length, total selection-scan ns,
and records returned per acquisition, including empty calls. Nonempty batches
and mean records/nonempty batch are derived separately; P95 batch excludes empty
calls. Queue and batch buckets are fixed numeric bounds, not high-cardinality
labels. Baseline batches are exactly one record when nonempty. Selection timing
excludes VecDeque removal and captures just readiness/minimum selection. C must
use identical selection probes and keep the queue structure unchanged.

C BASELINE_SHA: `49e885e740ef5dd971f516821e88bc946517a46f`.

The shared checkout was concurrently changed by another task after baseline build.
All subsequent work runs in `/tmp/netbaiot-eventbus-c/worktree` on
`codex/eventbus-batch-dispatch`, based on the frozen instrumentation commit.
No unrelated staged work was modified. The independent baseline binary is unchanged.

### Measured prerequisite: 1/4/8 required sinks (before implementing C)

| Required sinks | All State locks/event | publish | take_ready | complete | next_ready_delay | control |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 3.7834 | 1.0000 | 1.3877 | 1.0000 | 0.3877 | 0.0080 |
| 4 | 9.0262 | 1.0000 | 4.0091 | 4.0000 | 0.0091 | 0.0080 |
| 8 | 17.0706 | 1.0000 | 8.0313 | 8.0000 | 0.0313 | 0.0080 |

These are runtime probe deltas for 10,000 accepted, acknowledged, no-retry events,
not static source estimates. The fixture bounds outstanding deliveries to 128 ×
sink count. Its observer adds 0.008 control acquisitions/event.
Raw discovery measurements are retained separately from the paired acceptance runs.

### Decision to test C, based on verified discovery

| Site | Locks/event | Wait sum s | Wait share | Hold sum s |
| --- | --- | --- | --- | --- |
| publish | 1.000000 | 2.455093 | 49.34% | 0.689613 |
| take_ready | 1.699460 | 1.520655 | 30.56% | 0.224219 |
| next_ready_delay | 0.699460 | 0.187804 | 3.77% | 0.030724 |
| complete | 1.000000 | 0.812221 | 16.32% | 0.338293 |
| control_restore_spool | 0.000073 | 0.000142 | 0.00% | 0.000046 |

The verified 20k discovery uses the frozen isolated measurement scripts. The earlier
network discovery is retained but excluded from comparisons because the shared
checkout changed during its launch. Dequeue is a material wait contributor, so C
is implemented only after this evidence and the fanout table above.

C uses one worker-owned reusable Vec of DeliveryRecord handles, grown only for
actual ready work and bounded by sink concurrency (no payload cloning). One State
acquisition selects at most available slots, also clamped to remaining sink inflight
capacity. Each selection recomputes now, scans the unchanged VecDeque, and chooses
the same earliest ready deadline with existing queue-order tie breaking. It does
not optimize scans or introduce a different retry order. Record removal and inflight
increments are atomic under State. Count/bytes and ActiveEvent ownership remain
unchanged until baseline complete(). The worker releases State before spawning
bounded deliveries; there is no await between removal and dispatch. Required
spool ownership survives cancellation. No completion batching is implemented.

Selection instrumentation accumulates scan ns per acquisition; both sides take
the same number of selection timestamps per scan. Baseline buffers return one
record; C reuses the bounded Vec across batches. `complete()` is byte-identical
to BASELINE_SHA (checked directly).

### C decision: REVERT

Three alternating pairs give total State wait (ns-precision) medians
6.286437 -> 6.347108 seconds (**+0.97%**), while acquisitions/event fall
4.404347 -> 4.058205 (**-7.86%**) and take_ready acquisitions/event fall
1.702137 -> 1.291566 (**-24.12%**). Nonempty batches average 1.344969 records,
P95 = 3, P99 = 7. The >=20% wait reduction gate fails even though actual batching
and fewer acquisitions are directly observed. Accepted throughput, primary P99,
CPU and RSS do not rescue a failed contention gate.

The hypothesis that dequeue acquisition frequency is the main removable source of
the representative workload's contention is not supported. This is narrower than
claiming repeated acquisitions never matter. Batch dequeue reduces its own wait,
but does not remove the dominant overall serialized contention; the measurements
do not establish an exclusive CPU bottleneck or a causal scheduler explanation.

C is completely reverted to the instrumentation-only baseline, including its
C-only helper/test. That test passed before reversion and is preserved in
`eventbus-batch-evidence/candidate.patch.gz`; no existing test was deleted or
weakened. Frozen candidate binaries finish secondary measurement independently of
the reverted working tree. Retain only default-off instrumentation, tooling,
raw evidence and this report. No A/B implementation was revisited.

**Experiment D is only a proposal:** measure and independently test bounded
completion batching as a possible way to reduce remaining repeated State access.
It would require explicit retry/ownership/ACK/drain proofs, unchanged required
fanout admission, and the same paired acceptance gates. No D code is implemented.
### C primary raw results and medians

All After values below are the rejected candidate, not the final production implementation.
Three paired 20k runs; each summary field is its own median, not a pooled distribution.
Independent site medians need not add to the median total. Nanosecond histograms retain
sub-microsecond samples; P95/P99 are bucket upper bounds, not exact quantiles.

| Run | Accepted/PUBACK per s | P50/P95/P99 ms | CPU us/event | RSS KiB | Pending peak | Locks/event | Wait sum s | Hold sum s | Mean nonempty batch |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 19,973.800000 | 0.32/1.06/1.67 | 55.347505 | 7920 | 39 | 4.419322 | 6.038254 | 1.538150 | 1.000000 |
| after 1 | 19,977.700000 | 0.32/1.03/1.69 | 53.084189 | 8160 | 28 | 4.075434 | 5.674935 | 1.465492 | 1.365268 |
| before 2 | 19,986.650000 | 0.39/1.04/1.36 | 58.363958 | 8016 | 37 | 4.363125 | 6.394763 | 1.546721 | 1.000000 |
| after 2 | 19,991.550000 | 0.37/1.02/1.36 | 59.049949 | 7936 | 45 | 4.058205 | 6.347108 | 1.593168 | 1.344969 |
| before 3 | 19,978.000000 | 0.31/1.08/1.69 | 57.237962 | 7968 | 48 | 4.404347 | 6.286437 | 1.588430 | 1.000000 |
| after 3 | 19,991.050000 | 0.35/1.14/1.69 | 57.150575 | 8016 | 28 | 3.971075 | 6.530664 | 1.603577 | 1.313321 |

| Site | Side | Locks/event | Count/run | Wait sum s | Wait mean us | Wait P95/P99 <=us | Hold sum s | Hold mean us | Hold P95/P99 <=us |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| publish | before | 1.000000 | 399560 | 2.936253 | 7.348717 | 50/250 | 0.821816 | 2.057235 | 10/25 |
| publish | after | 1.000000 | 399821 | 3.177689 | 7.947580 | 50/250 | 0.807203 | 2.018861 | 10/25 |
| take_ready | before | 1.702137 | 680106 | 2.021364 | 2.972131 | 0.5/100 | 0.281047 | 0.413240 | 1/5 |
| take_ready | after | 1.291566 | 516408 | 1.468805 | 2.844272 | 0.25/100 | 0.280777 | 0.550480 | 2.5/10 |
| next_ready_delay | before | 0.702137 | 280546 | 0.260216 | 0.927533 | 0.1/5 | 0.036497 | 0.130093 | 0.25/1 |
| next_ready_delay | after | 0.766564 | 306496 | 0.488845 | 1.725974 | 0.1/50 | 0.044468 | 0.152253 | 0.25/1 |
| complete | before | 1.000000 | 399560 | 1.068512 | 2.674222 | 0.25/100 | 0.420004 | 1.051166 | 2.5/10 |
| complete | after | 1.000000 | 399821 | 1.168308 | 2.922005 | 0.25/100 | 0.430195 | 1.075969 | 2.5/10 |
| control_restore_spool | before | 0.000073 | 29 | 0.000091 | 3.139345 | 25/100 | 0.000016 | 0.550241 | 2.5/10 |
| control_restore_spool | after | 0.000073 | 29 | 0.000002 | 0.081793 | 0.25/0.5 | 0.000016 | 0.558966 | 2.5/10 |

| Median metric | Before | After | Delta |
| --- | --- | --- | --- |
| State acquisitions/event | 4.404347 | 4.058205 | -7.86% |
| take_ready acquisitions/event | 1.702137 | 1.291566 | -24.12% |
| Total State wait s (ns precision) | 6.286437 | 6.347108 | +0.97% |
| Legacy total State wait us | 6,143,197.000000 | 6,205,342.000000 | +1.01% |
| Total State hold s (ns precision) | 1.546721 | 1.593168 | +3.00% |
| take_ready wait s | 2.021364 | 1.468805 | -27.34% |
| Accepted/PUBACK per s | 19,978.000000 | 19,991.050000 | +0.07% |
| PUBACK P99 ms | 1.670000 | 1.690000 | +1.20% |
| CPU us/event | 57.237962 | 57.150575 | -0.15% |
| RSS KiB | 7,968.000000 | 8,016.000000 | +0.60% |
| Pending peak | 39.000000 | 28.000000 | -28.21% |

| Run | Dequeue calls | Empty calls | Nonempty batches | Records | Records/batch | P95 batch | P99 batch | Size 2 batches | Size 4 batches | Size 8 batches |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before 1 | 682954 | 283478 | 399476 | 399476 | 1.000000 | 1 | 1 | 0 | 0 | 0 |
| after 1 | 516955 | 224299 | 292656 | 399554 | 1.365268 | 3 | 7 | 23703 | 4560 | 1839 |
| before 2 | 672161 | 272428 | 399733 | 399733 | 1.000000 | 1 | 1 | 0 | 0 | 0 |
| after 2 | 516408 | 219129 | 297279 | 399831 | 1.344969 | 3 | 7 | 24010 | 4266 | 1744 |
| before 3 | 680106 | 280546 | 399560 | 399560 | 1.000000 | 1 | 1 | 0 | 0 | 0 |
| after 3 | 506346 | 201911 | 304435 | 399821 | 1.313321 | 3 | 7 | 21632 | 3934 | 1632 |

### C saturation knee

| Offered | Side | Accepted/s | P99 ms | Wait sum s | CPU us/event | RSS KiB | Mean nonempty batch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| q1-25k | before | 24,089.850000 | 1.15 | 5.197725 | 42.694330 | 8048 | 1.000000 |
| q1-25k | after | 24,193.300000 | 1.39 | 6.496976 | 44.826460 | 8000 | 1.273288 |
| q1-30k | before | 26,634.400000 | 1.63 | 8.070167 | 47.570060 | 7968 | 1.000000 |
| q1-30k | after | 26,670.100000 | 1.74 | 8.222157 | 46.419024 | 8336 | 1.226578 |

Single paired secondary points: the approximate 25–30k/s knee is unchanged. At 25k
candidate P99 is worse (1.15 -> 1.39ms), as is the 30k point (1.63 -> 1.74ms).
There is no basis to call the tiny accepted-rate differences a capacity increase.

### Queue selection cost

| Side | Initial queue mean | Queue P95 <= | Queue P99 <= | Selection ns/call | Selection P95 <=ns | Selection P99 <=ns | Selection sum s | % take hold | % total State hold |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before | 5.143898 | 32.0 | 64.0 | 139.092515 | 500.0 | 1000.0 | 0.094598 | 33.658983 | 6.116013 |
| after | 5.279575 | 32.0 | 64.0 | 206.660371 | 1000.0 | 2500.0 | 0.106721 | 38.009191 | 6.698672 |

Selection sum measures only min-by-deadline scans (all scans in a batch), excluding
VecDeque removal. Before averages 139ns/selection call with small queues, about 6%
of total State hold time. Scanning is visible inside take_ready, but is not established
as the dominant primary-load critical-section cost. C still repeats the O(Q) scan
for each record; it changes neither the queue data structure nor next_ready_delay.
The separate ignored depth probe tests future queues up to 16,383 entries; its
whole-call latencies include locking and instrumentation (and C test helper Vec costs),
so they must not be interpreted as pure selection timings. Large backlog scan cost
is a possible later independent study; no queue optimization is included here.

Nanosecond units do not imply nanosecond hardware accuracy: timer quantization and
the scan timer's own overhead remain. The extra probes run only when explicitly
enabled and affect absolute timings; both compared binaries have the same probes.
No timing-disabled A/A calibration or CPU sampling profile was run in C.

### C fanout and delayed/retry microbenchmarks

Three separate paired process repetitions; 10,000 no-retry events per fast 1/4/8
sink case. Low-rate/delayed/retry cases use the existing bounded fixture. Microburst
throughput is not server capacity. Values are per-field medians across three runs.

| Scenario | Side | Events/s | All locks/event | publish | take_ready | complete | next delay | Mean batch | Take wait sum s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 sinks/0ms/retry=False | before | 224,632.024665 | 3.765400 | 1.000000 | 1.378700 | 1.000000 | 0.378700 | 1.000000 | 0.009855 |
| 1 sinks/0ms/retry=False | after | 218,485.513045 | 3.483900 | 1.000000 | 1.049200 | 1.000000 | 0.426700 | 1.182173 | 0.007167 |
| 4 sinks/0ms/retry=False | before | 50,650.233003 | 9.051600 | 1.000000 | 4.021800 | 4.000000 | 0.021800 | 1.000000 | 0.237310 |
| 4 sinks/0ms/retry=False | after | 46,677.840750 | 9.015800 | 1.000000 | 4.001200 | 4.000000 | 0.006000 | 1.000826 | 0.265185 |
| 8 sinks/0ms/retry=False | before | 23,310.285918 | 17.026000 | 1.000000 | 8.009000 | 8.000000 | 0.009000 | 1.000000 | 1.277751 |
| 8 sinks/0ms/retry=False | after | 21,848.248997 | 17.018800 | 1.000000 | 8.002500 | 8.000000 | 0.008400 | 1.000663 | 1.382970 |
| 1 sinks/1ms/retry=False | before | 3,532.450971 | 3.017500 | 1.000000 | 1.004500 | 1.000000 | 0.004500 | 1.000000 | 0.000287 |
| 1 sinks/1ms/retry=False | after | 3,523.938001 | 3.016000 | 1.000000 | 1.002000 | 1.000000 | 0.005500 | 1.002506 | 0.000362 |
| 1 sinks/10ms/retry=False | before | 673.691567 | 3.019500 | 1.000000 | 1.005500 | 1.000000 | 0.005500 | 1.000000 | 0.000407 |
| 1 sinks/10ms/retry=False | after | 673.738554 | 3.016000 | 1.000000 | 1.002000 | 1.000000 | 0.005500 | 1.002506 | 0.000391 |
| 1 sinks/0ms/retry=True | before | 18,562.353516 | 6.125500 | 1.000000 | 2.558500 | 2.000000 | 0.558500 | 1.000000 | 0.001567 |
| 1 sinks/0ms/retry=True | after | 19,043.009370 | 5.724500 | 1.000000 | 2.155500 | 2.000000 | 0.560500 | 1.173709 | 0.000869 |


### Slow required sink and scan-depth observations

| Side | Accepted | Explicit rejects | Slow backlog | Event bytes | Fast ACK/s | Worker select returns in blocked 100ms |
| --- | --- | --- | --- | --- | --- | --- |
| before | 1024 | 1024 | 1024 | 241492 | 132631.7 | 0 |
| after | 1024 | 1024 | 1024 | 241492 | 119347.3 | 0 |

Both fixtures assert all 1,024 fast deliveries finish while the slow required sink
is blocked, accounting remains bounded, and all counts/bytes return to zero after
release. This short blocked interval is not a long outage or memory-soak claim.

| Future queue depth | Side | take P50/P95/P99 ns | next delay P50/P95/P99 ns |
| --- | --- | --- | --- |
| 0 | before | 333/416/541 | 250/292/458 |
| 1000 | before | 1209/1459/1542 | 5292/6334/6458 |
| 10000 | before | 4375/5166/5209 | 25042/29291/30250 |
| 16383 | before | 6292/6417/7583 | 36625/37417/44959 |
| 0 | after | 333/417/542 | 250/375/500 |
| 1000 | after | 1250/1500/1541 | 5292/6334/6458 |
| 10000 | after | 4375/5167/5250 | 25000/29292/31208 |
| 16383 | after | 6333/6459/8166 | 36625/39292/45125 |

At 16,383 future records baseline take P50 is about 6.3us and next-ready-delay
P50 about 36.6us, versus hundreds of ns at depth zero. Thus linear scans can be
material with a large backlog; the representative primary queue (mean ~5, P99
<=64) does not demonstrate that large-backlog condition. Next-delay scanning
and completion are unchanged by C. These are isolated timings, not saturation evidence.

### C correctness, reproducibility and final answers

Candidate runtime tests: **41 passed, 0 failed, 1 ignored**, including the new
batch deadline/tie-order/concurrency/spool test. Both frozen revisions passed the
ignored release queue-depth test. Paired micro fixtures cover 1/4/8 required
fanout, delayed ACK, retry and blocked required sink isolation. Final source is
the instrumentation-only baseline; its gates passed:

```sh
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
CARGO_INCREMENTAL=0 cargo test --locked --workspace --all-features
python3 tests/mqtt_conformance/run.py --release-gate --no-build
CARGO_INCREMENTAL=0 cargo test --locked -p netbaiot-server --test server \
  subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

Final workspace: **142 passed, 0 failed, 4 intentionally ignored**. Existing
atomic admission/last-required-target rollback, best-effort full, 1/4/8 shared
fanout, slow sink, retry, panic, cancellation, restore and inflight spool tests
all pass. Subprocess recovery, failed-spool repair and SIGKILL bounded-loss tests
also pass. MQTT: **76/76**, including external Mosquitto interoperability and
**125/125** normative coverage. Restart soak: **61.88s**, passed. No timeout,
resource limit or assertion was relaxed. No parser/spool format changed; no new
fuzz campaign, long-duration outage/memory soak, physical-network test or allocator
trace was run. The 60-second restart soak is not abrupt-crash durability evidence.

Every network run has published = accepted = PUBACK = sink ACK = dequeued, zero
reject/retry/failure counters, zero client errors, final sampled pending = 0 and
normal server exit. Per-site acquisitions exactly sum to the aggregate count;
ns/us totals differ only by expected integer truncation. Checks are retained in
`eventbus-batch-evidence/measurement-checks.txt`. Pending and RSS peaks are sampled,
not exact maxima. `ps time` CPU/event includes setup, connected-idle warmup and
metrics rendering before shutdown. No benchmark ran alongside this task's Rust
builds or tests; unrelated shared-host work was not controlled. Single paired
knee points and microbursts cannot establish production capacity.

The C build uses the same M4/16GiB/macOS 26.6.2/Rust 1.97.1 host and release profile
described above. `provenance.json` records the exact BASELINE_SHA, binary hashes,
toolchain, isolated branch and unchanged complete() hash. Exact network commands
and execution order are in `paired/manifest-primary.json` and
`paired/manifest-secondary.json`. Reproduction from repository root:

```sh
mkdir -p /tmp/netbaiot-eventbus-c/baseline-src
git archive 49e885e740ef5dd971f516821e88bc946517a46f | tar -x -C /tmp/netbaiot-eventbus-c/baseline-src
CARGO_INCREMENTAL=0 cargo build --locked --release \
  --manifest-path /tmp/netbaiot-eventbus-c/baseline-src/Cargo.toml \
  --target-dir /tmp/netbaiot-eventbus-c/baseline-target \
  -p netbaiot-server -p netbaiot-loadgen -p netbaiot-runtime --example eventbus_probe --bins
# In a separate checkout of BASELINE_SHA, apply only candidate.patch.gz, then build:
# gzip -dc /path/to/eventbus-batch-evidence/candidate.patch.gz | git apply
# cargo build --locked --release -p netbaiot-server -p netbaiot-runtime --example eventbus_probe --bins
python3 scripts/perf/eventbus_lock_pairs.py --before BASELINE_SERVER \
  --after CANDIDATE_SERVER --loadgen BASELINE_LOADGEN --output RESULTS
# Repeat with --knee-only for 25k/30k.
python3 docs/eventbus-batch-evidence/reproduce_micro.py --before BASELINE_PROBE \
  --after CANDIDATE_PROBE --output RESULTS
python3 scripts/perf/eventbus_site_summary.py RESULTS > RESULTS/summary.json
```

1. **How many State acquisitions per accepted event?** Primary medians 4.404347
   before, 4.058205 for C. No-retry paired 1/4/8-sink micro medians before are
   3.7654 / 9.0516 / 17.0260 and after 3.4839 / 9.0158 / 17.0188. Observer/control
   calls are included and separately reported.
2. **Largest wait site?** Publish: median 2.936253s before and 3.177689s after per
   20-second run. Waiting caller attribution is not proof that this caller alone
   causes the contention; all sites share the same mutex.
3. **Actual batch size?** Primary mean nonempty batch 1.344969, P95 3, P99 7, with
   measured size-2/4/8 batches. Saturated 4/8-sink micros average only 1.000826 /
   1.000663: unchanged worker scheduling observes one completion then refills that
   slot, leaving little opportunity to batch in these cases. Their throughput
   does not improve. No hidden completion batching was added to manufacture gains.
4. **Did acquisitions become lower total wait?** No. Take acquisitions fall 24.12%
   and its own wait falls, but next-delay/publish and other waits offset this.
   Total ns-precision wait increases 0.97%; legacy us sum increases 1.01%.
5. **Did the knee move?** No measurable shift: both sides remain ~24.1k accepted/s
   at 25k offered and ~26.6k at 30k offered.
6. **Is a new dominant bottleneck proven?** No. Serialized State contention remains
   unresolved. Dequeue frequency is not supported as its main independently
   removable cause; small-queue min scans consume only ~6% of total State hold.
   Large-backlog scans and proposed completion batching require separate evidence.

Final decision **REVERT**. Final `event.rs` and `metrics.rs` are exactly those of
the instrumentation-only `49e885e` baseline. The final evidence commit is on
`codex/eventbus-batch-dispatch`, isolated from the concurrently changed shared
checkout. No push was performed.
