# EventBus critical-section and wake-pressure experiment

## Hypothesis and starting point

Starting local HEAD: `cbbad11d827d3d8e04160819ab72378f6d9ae9a9`; the working tree
was clean. Correctness baseline: `ce07b5b126b04f7ae95749c46e0263c7514fe491`.
Initial performance baseline: `70b855f1a97ebe1222dc0212fe0847e91b07cf4b`.
Broker-route baseline: the starting HEAD. No newer work was reset.

Hypothesis: repeated acquisitions of the single EventBus state mutex, redundant
queue scans, and worker wakes amplify contention despite short protected work.
Only one candidate is evaluated; MQTT broker/parser, IDs, codecs, transport I/O,
Tokio configuration, recovery formats, and business protocols are out of scope.

## Before architecture and source accounting

`State` owns routes, active events, pending required sink IDs, attempt metadata,
count/byte accounting, queues, and inflight counts. There is one worker per sink
and one global `std::sync::Mutex<State>`. Existing bounded delivery tasks run in
each worker's JoinSet, limited by the unchanged sink concurrency.

The lifecycle does **not** have a workload-independent acquisition count. For
`S` required sinks, `A` delivery attempts across those sinks, `E` unsuccessful
`take_ready` calls, and `U` explicit usage/drain/control calls, steady-state locks
are `1 + 2*A + 2*E + U`. Startup adds one sink inventory lock and one worker
initialization lock per sink. A full worker skips the delay query and waits only
for completion or cancellation.

| Operation | Before acquisitions/event | Lock-held work | Notify calls |
|---|---:|---|---:|
| publish | 1 | route union, required preflight, all enqueue/accounting commit | S worker notifications |
| successful take_ready | A | scan for earliest due record, remove, increment inflight | 0 |
| unsuccessful take_ready | E | scan entire queue for due record | 0 |
| complete | A | attempt, inflight, retry/release, active-event cleanup | one worker notify per requeue; one drain notify per final result |
| next_ready_delay | E | scan queue for minimum remaining delay | 0 |
| usage/drain wait | U | sum required IDs over active events | 0 |
| metrics render | 0 | atomic reads, no EventBus lock | 0 |
| stop_workers | 0 state locks | cancel; separate worker-handle lock; join | cancellation wakes, not Notify counter |
| spool snapshot | 1 per snapshot | bounded required-event snapshot | 0 |

Minimal continuously busy lifecycle counts, excluding unsuccessful polls and
observers: one sink ACK **3**, one sink retry once **5**, four sinks ACK **9**,
eight sinks ACK **17**. An isolated event on a sleeping concurrency-eight worker
measured **7** locks (plus two benchmark usage calls); an isolated retry-once
measured **15** (plus two usage calls). These include post-ACK return to idle.

### Answers established before selecting the candidate

1. Normal event: three useful transitions plus two locks per unsuccessful poll;
   isolated measurement seven, concurrent measurement workload dependent.
2. Retry once: five useful transitions plus two per unsuccessful poll; isolated
   measurement fifteen. Backoff remains event-ID jittered.
3. `complete` unlocks before the same worker loops into `take_ready`; other sink
   workers and publishers may contend between these operations.
4. Publish notifies only sinks actually enqueued, not all configured workers.
5. Notify produces some empty iterations, but coalesces most per-event calls.
   JoinSet completions can also return to an empty queue; count them separately.
6. Every unsuccessful take is followed by another lock and full queue scan in
   `next_ready_delay`, unless cancelled.
7. The earliest deadline is recomputed on every such iteration.
8. Different sinks share the same global mutex.
9. Completion's accounting is already merged into its own transition; no separate
   accounting lock exists to remove.
10. One notify permit already represents many enqueues safely: the worker fills
    available concurrency before waiting again.
11. Each non-full iteration constructs a sleep, including a one-hour fallback
    for an empty queue. This is per iteration, not necessarily per event.
12. Each delivery attempt generates one JoinSet completion. The counters establish
    that frequency; they do not prove exclusive scheduler CPU attribution.

## Measurement definitions

`NETBAIOT_PERF_LOCK_METRICS=1` enables the experiment counters and timings. Default
metrics leave these counters inactive. The original `event_bus_lock_*` histogram
still measures successful publish only, allowing comparison with previous audits.
New `event_bus_state_*` histograms cover all instrumented state acquisitions;
hold includes mutex release and excludes recording histogram samples. Fixed
microsecond buckets report upper bounds; sub-microsecond times truncate to zero.

A worker wake means a selected Notify/timer/JoinSet branch returning, **not** an
executor task poll or OS context switch. An empty wake means the next iteration
launched no delivery, including useful completion wakes that simply return to
idle. Notification counts separate worker `notify_one` and drain
`notify_waiters`; an absent drain waiter is still a notify call. Startup and
status polling are counted as `other`; microbench tables subtract startup.

The bounded microbench uses default sink concurrency eight, an aggregate outstanding-delivery window of
128 × sink count, 10,000 fast events or 2,000 delayed/retry events, and bounded per-event sample
storage. It reports publish/acceptance latency and per-sink completion latency.
The retry fixture returns Retryable exactly once per event (5 ms base, 10 ms cap).
Its yield-based producer is diagnostic and must not be presented as server capacity.

Queue-depth probes stop workers, enqueue bounded records with a future deadline,
and independently measure failed take, delay lookup, and final ACK completion.
They are overload-scan diagnostics, not steady-state throughput.

## Candidate selected from BEFORE evidence

Fuse the unsuccessful `take_ready` result with its earliest future deadline.
The same queue traversal chooses the earliest record; if it is not yet due, return
that deadline to the worker. The worker uses it for its existing wait instead of
calling `next_ready_delay`. A publish racing with this check still leaves a Notify
permit, and cancellation remains selected first. Absolute deadlines avoid adding
queue-check time to retry delay. No queue hint/counter owns responsibility.

This removes one mutex acquisition and one full queue scan per unsuccessful take.
It deliberately removes **no** notification or JoinSet completion; the measured
coalescing is already effective under sustained multi-sink load. Completion and
all retry/required-failure branches stay unchanged. No batching, new task, extra
queue, lock replacement, sharding, or runtime dependency is introduced.

The structural target is `1 + 2*A + E + U` acquisitions, compared with
`1 + 2*A + 2*E + U`. Thus the isolated ACK target is seven to five and isolated
retry-once fifteen to ten; at continuous full concurrency both remain at their
three/five useful-transition floor. This is a reason to test the network case,
not a claim that a 30% reduction is inevitable.

## Correctness invariants

`State` remains authoritative. Required targets are preflighted in SinkId order
before any mutation, and every required delivery is enqueued before EventAccepted.
Count/byte reservations, concurrency, explicit sink ACK, panic-to-Retryable
conversion, retry attempt/age/jitter/cap, permanently failing required ownership,
and best-effort behavior remain unchanged. The worker still fills only its
available slots and cancellation aborts/joins the same owned delivery tasks.
Spool snapshots continue to include inflight responsibility without an observed
ACK. The server quiesce/drain/spool/fsync/retry lifecycle is unchanged.
## Benchmark environment and reproducibility

Apple M4 Mac mini, 10 cores, 16 GiB, macOS 26.6.2 arm64; Rust stable 1.97.1, locked release builds. Both sides share one host and IPv4 loopback, identical credentials/limits, 64 publishers, 256-byte payloads, no MQTT subscribers, required in-process audit sink, sink concurrency eight. No Tokio/runtime/socket tuning was performed. Raw artifacts and frozen binaries are in `target/eventbus-experiment/{before,after}/` (ignored local artifacts). The committed campaign and summary scripts reproduce the measurements.

The exact starting-HEAD release binary was built and smoke-measured first (19,954.95/s, PUBACK P99 1.53 ms). Both comparison sides then used identical opt-in instrumentation. That one smoke run is not a statistical estimate of instrumentation overhead. The three primary repetitions use 5 s warmup, 20 s measurement, 2 s cooldown; auxiliary rates use 10–20 s. Only actual measurement duration divides event/ACK counts. No compilation or test workload overlapped a network or microbenchmark run.

## BEFORE / AFTER: primary QoS1 20k

**AFTER throughout this report means the tested candidate, subsequently reverted; it is not the final committed runtime.** Values below are medians of the three runs for each metric.

| Metric | Before | Candidate after | Delta |
| --- | --- | --- | --- |
| Accepted/s | 19,969.35 | 19,965.00 | -0.022% |
| Sink ACK/s | 19,969.35 | 19,965.00 | -0.022% |
| Sampled peak CPU % | 103.70 | 110.20 | +6.268% |
| Peak RSS KiB | 7,920.00 | 8,000.00 | +1.010% |
| PUBACK P50 ms | 0.28 | 0.27 | -3.57% |
| PUBACK P95 ms | 0.93 | 0.8 | -13.98% |
| PUBACK P99 ms | 1.6 | 1.38 | -13.75% |

| Run | Before accepted/s, P99 ms | After accepted/s, P99 ms |
| --- | --- | --- |
| 1 | 19969.35, 1.92 | 19924.80, 1.29 |
| 2 | 19995.30, 1.60 | 19983.95, 1.38 |
| 3 | 19957.55, 1.48 | 19965.00, 1.41 |

| Lock metric | Version | Mean us | P95 <=us | P99 <=us | Sum us / run |
| --- | --- | --- | --- | --- | --- |
| All-state wait | Before | 2.502 | 10.0 | 100.0 | 4,473,019 |
| All-state wait | After | 2.856 | 10.0 | 100.0 | 4,298,423 |
| All-state hold (including release) | Before | 0.370 | 10.0 | 10.0 | 653,422 |
| All-state hold (including release) | After | 0.417 | 10.0 | 10.0 | 614,760 |
| Publish-only wait | Before | 5.756 | 50.0 | 250.0 | 2,297,530 |
| Publish-only wait | After | 5.333 | 50.0 | 250.0 | 2,131,484 |
| Publish-only hold | Before | 0.268 | 10.0 | 10.0 | 107,367 |
| Publish-only hold | After | 0.256 | 10.0 | 10.0 | 101,969 |

| Per accepted event | Before | After |
| --- | --- | --- |
| State acquisitions (transition/scheduling) | 4.526169 | 3.765512 |
| Worker notify calls | 1.000000 | 1.000000 |
| Drain notify calls | 1.000000 | 1.000000 |
| Worker select returns | 1.290305 | 1.291564 |
| Notify branch returns | 0.290305 | 0.291564 |
| JoinSet completions | 1.000000 | 1.000000 |
| Empty iterations after wake | 0.565041 | 0.569487 |
| Timer returns | 0.000000 | 0.000000 |
| Observer/startup locks | 0.000073 | 0.000073 |

Fewer acquisitions remove many uncontended samples, so mean wait per remaining acquisition rises even though total wait falls slightly. The medians of total wait drop from 4,473,019 to 4,298,423 us (3.9%); normalizing each run by its event count gives the same insufficient direction. Peak CPU/accepted-rate ratio rises about 6.3%, but sampled peak CPU is not integrated CPU time/event.

## Mandatory source-level comparison

| Operation | Before locks/event | After locks/event | Before/after notify or wake calls | Before scans/event | After scans/event |
| --- | --- | --- | --- | --- | --- |
| publish | 1 | 1 | S / S worker notify | route selection unchanged | unchanged |
| successful take_ready | A | A | 0 / 0 | A | A |
| failed take_ready | E | E | 0 / 0 | E | E (also obtains deadline) |
| next_ready_delay | E | 0 | 0 / 0 | E | 0 |
| complete | A | A | retry notify and final drain notify unchanged | active-event cleanup unchanged | unchanged |
| timer select | 0 | 0 | runtime-dependent; primary 0 / 0 | 0 | 0 |
| JoinSet completion | 0 itself | 0 itself | A / A | 0 | 0 |
| primary measured total | 4.526 | 3.766 | 1.290 / 1.292 select returns | 2.526 queue scans | 1.766 queue scans |

No authoritative state moved. No derived persistent hint/counter was added by the candidate. The only derived value was the earliest deadline returned from the locked scan. One `next_ready_delay` acquisition and scan disappeared after each failed take. No notification or completion wake was structurally removed. After revert these scheduling acquisitions and scans are restored; only opt-in observational counters/timing remain.

## Saturation, QoS0 and QoS2

| Scenario | Before accepted/s | After accepted/s | Before P50/P95/P99 ms | After P50/P95/P99 ms |
| --- | --- | --- | --- | --- |
| q1-25k | 24067.13 | 24081.60 | 0.310/0.990/2.020 | 0.300/0.720/1.290 |
| q1-30k | 26379.87 | 26082.80 | 0.320/1.070/2.190 | 0.310/0.630/0.920 |
| q0-25k | 23970.47 | 23969.93 | N/A: no MQTT ACK | N/A: no MQTT ACK |
| q2-20k | 19985.85 | 19999.55 | 0.650/1.290/1.910 | 0.490/0.890/1.190 |

The offered-load knee remains approximately 25–30k/s; no >=10% improvement. Client windows and shared-host scheduling limit achieved offered load, so this is not server-only capacity. QoS0 accepted counts are server metrics, not fabricated protocol ACKs. QoS2 tails improved in its single representative run; it is not a repeated QoS2 confidence interval.

## Required-sink microbenchmarks

Three separate process repetitions per side, alternating order. Each row selects the median-throughput repetition for that scenario and reports its accompanying latency/counters. Fixed storage is bounded; the producer limits outstanding deliveries to 128 × sink count. Uneven sink progress can leave more than 128 active events (up to that delivery bound), explaining the larger multi-sink depths. End-to-end latency below uses the slowest sink quantile in the scenario, not a merged per-event all-sink latency distribution.

| Scenario | Version | Completed events/s | Publish events/s | Accepted P50/P95/P99 us | Completion P50/P95/P99 us | Locks/event | Wakes/event | Sampled max depth |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 sink fast | Before | 227825.5 | 240313.8 | 1.667/10.083/21.708 | 20.708/154.625/274.750 | 3.823 | 1.077 | 58 |
| 1 sink fast | After | 239670.0 | 253510.6 | 1.916/8.292/13.208 | 22.584/61.917/89.708 | 3.278 | 1.042 | 34 |
| 4 sink fast | Before | 47711.9 | 48238.6 | 6.208/48.167/77.792 | 3874.542/4504.333/4903.334 | 9.030 | 4.002 | 243 |
| 4 sink fast | After | 78419.6 | 79871.5 | 3.125/28.667/46.292 | 4990.291/6181.500/6335.167 | 9.393 | 4.044 | 480 |
| 8 sink fast | Before | 22425.5 | 22732.1 | 9.583/89.000/142.375 | 8306.000/9356.084/9648.500 | 17.019 | 8.002 | 228 |
| 8 sink fast | After | 33334.5 | 33630.1 | 7.667/53.625/85.375 | 14931.292/18120.542/18823.167 | 17.220 | 8.014 | 626 |
| 1 ms ACK | Before | 3526.9 | 3788.5 | 1.125/16.208/25.084 | 36453.959/36675.708/36865.041 | 3.009 | 1.001 | 128 |
| 1 ms ACK | After | 3548.3 | 3788.4 | 1.125/8.292/12.375 | 36408.167/36603.667/36727.042 | 3.006 | 1.003 | 128 |
| 10 ms ACK | Before | 673.4 | 717.4 | 1.125/15.083/25.459 | 190400.625/192216.292/192335.958 | 3.011 | 1.002 | 128 |
| 10 ms ACK | After | 674.5 | 718.1 | 1.167/7.916/12.708 | 190187.208/191986.083/192295.625 | 3.006 | 1.002 | 127 |
| retry once | Before | 18983.1 | 21144.0 | 2.791/17.459/28.250 | 6328.916/10541.708/11461.334 | 6.040 | 2.155 | 128 |
| retry once | After | 18836.8 | 21206.6 | 2.458/13.375/23.250 | 6228.417/10442.166/11251.666 | 5.589 | 2.168 | 128 |

| Scenario | Version | State wait mean us | Wait P99 <=us | Notify calls/event (worker+drain) | Empty/event | Timers/event |
| --- | --- | --- | --- | --- | --- | --- |
| 1 sinks, delay=0, retry=False | Before | 0.732 | 25.0 | 2.000 | 0.304 | 0.000 |
| 1 sinks, delay=0, retry=False | After | 0.772 | 10.0 | 2.000 | 0.199 | 0.000 |
| 4 sinks, delay=0, retry=False | Before | 7.384 | 100.0 | 8.000 | 0.013 | 0.000 |
| 4 sinks, delay=0, retry=False | After | 3.713 | 50.0 | 8.000 | 0.301 | 0.000 |
| 8 sinks, delay=0, retry=False | Before | 17.106 | 250.0 | 16.000 | 0.009 | 0.000 |
| 8 sinks, delay=0, retry=False | After | 10.103 | 100.0 | 16.000 | 0.164 | 0.000 |
| 1 sinks, delay=1, retry=False | Before | 1.048 | 25.0 | 2.000 | 0.004 | 0.000 |
| 1 sinks, delay=1, retry=False | After | 0.606 | 10.0 | 2.000 | 0.005 | 0.000 |
| 1 sinks, delay=10, retry=False | Before | 1.016 | 25.0 | 2.000 | 0.005 | 0.000 |
| 1 sinks, delay=10, retry=False | After | 0.551 | 10.0 | 2.000 | 0.005 | 0.000 |
| 1 sinks, delay=0, retry=True | Before | 0.844 | 25.0 | 3.000 | 0.443 | 0.034 |
| 1 sinks, delay=0, retry=True | After | 0.736 | 25.0 | 3.000 | 0.510 | 0.035 |

Fast 4/8-sink diagnostic throughput and lock P99 improve despite near-minimal acquisition counts. Their lock-P99 reductions meet the numerical A threshold in these microbenchmarks (100 -> 50 us and 250 -> 100 us), but the slowest-sink completion P99 worsens (4.90 -> 6.34 ms and 9.65 -> 18.82 ms), and the benefit does not carry through to the primary network workload. Retry-once remains two completions and three total notify calls/event; deadline waiting stays bounded, with no per-retry task introduced. The existing retry/timeout/panic/drain regressions additionally validate ownership and wake behavior.

## Queue-depth sensitivity

Single isolated release probe; P50/P95/P99 in nanoseconds. The AFTER zero delay column means the separate operation is absent, not a measured zero-cost call.

| Queued future records | Version | take_ready ns | next_ready_delay ns | complete ns |
| --- | --- | --- | --- | --- |
| 0 | before | 209/250/458 | 209/375/458 | 750/917/1083 |
| 1000 | before | 1125/1334/1375 | 5250/6292/6375 | 667/833/875 |
| 10000 | before | 4292/5041/5125 | 24958/29250/31000 | 375/417/500 |
| 16383 | before | 6250/6375/7458 | 36584/37708/43959 | 333/375/417 |
| 0 | after | 125/125/167 | 0/0/0 | 333/375/417 |
| 1000 | after | 1458/1500/1750 | 0/0/0 | 292/334/416 |
| 10000 | after | 13417/15417/19667 | 0/0/0 | 333/417/875 |
| 16383 | after | 21917/24625/31458 | 0/0/0 | 333/375/875 |

At 10k future records the combined scheduling P50 falls from 29.25 us to 13.42 us; at 16,383 from 42.83 us to 21.92 us. The standalone first scan is slower after fusion because it computes the minimum of all future records. This benefits large backlog scheduling but does not establish steady-state network gain. Completion timing in this tiny fixture is noisy and its implementation did not change.

## Many publishers and fairness

| Publishers at 20k offered | Before accepted/s; P99 ms; CPU % | After accepted/s; P99 ms; CPU % | Before/after state wait mean us |
| --- | --- | --- | --- |
| 1 | 8899.8; 0.11; 18.2 | 8610.2; 0.11; 18.8 | 0.000 / 0.000 |
| 100 | 19998.2; 1.38; 122.2 | 19997.9; 1.17; 103.7 | 2.367 / 2.229 |
| 1000 | 19997.9; 1.09; 115.6 | 19995.9; 1.54; 118.5 | 1.748 / 2.311 |

The one-publisher run is window/schedule limited. All published messages completed; the 1,000-publisher P99 worsened from 1.09 to 1.54 ms in its representative run, another reason not to keep the candidate.

| 100 low-rate publishers beside saturated publisher | Published | PUBACKed | P50/P95/P99 ms | Error samples |
| --- | --- | --- | --- | --- |
| before | 14999 | 14999 | 0.080/0.120/0.160 | 0 |
| after | 14999 | 14999 | 0.080/0.120/0.130 | 0 |

Every generated low-rate message completed. The nominal target was 15,000; both runs generated 14,999 due to interval-boundary scheduling. This is not a missing accepted event.

## Overload and sink isolation

| 50k offered network | Accepted/s | Pending max | Event bytes max | RSS max KiB | Reject counters | Retries |
| --- | --- | --- | --- | --- | --- | --- |
| before | 28200.9 | 69 | 27602 | 7968 | 0.0 | 0.0 |
| after | 28361.4 | 58 | 23180 | 8128 | 0.0 | 0.0 |

The bounded client windows flatten achieved load; this scenario does not force server rejection. A second fixture deliberately blocks a required sink and fills the exact 1,024-event capacity while a separate required fast sink continues.

| Blocked-sink fixture | Accepted | Explicit rejects | Slow backlog | Event bytes | Fast ACK/s | Fast ACK P50/P95/P99 us | Process peak RSS KiB | Select returns during 100ms idle |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before | 1024 | 1024 | 1024 | 241492 | 198406.4 | 17.583/37.000/43.916 | 4544 | 0 |
| after | 1024 | 1024 | 1024 | 241492 | 188715.2 | 18.792/45.250/53.125 | 4496 | 0 |

Both versions acknowledge all fast deliveries while the slow sink is blocked, hold count/bytes at the bound, then drain to zero after release. No timer/Notify/JoinSet activity appears in the 100 ms static blocked window. Fast sink timings are short diagnostic bursts, not a sink SLA; no cross-sink head-of-line block was observed. A 100 ms fixed-bound test and short network overload do not prove long-term RSS stability. `/usr/bin/time -l` could not read a sandboxed clock sysctl; isolated child `getrusage` supplied the process RSS above.

## Profile comparison

Same offered 30k workload and 10-second macOS `sample` capture. There are 1,613 BEFORE and 5,472 AFTER non-park samples after subtracting condvar and kevent parks from thread-root sample totals. These are wall-stack shares, not exclusive CPU percentages; the different non-park populations limit conclusions.

| Category | Before samples (non-park share) | After samples (non-park share) |
| --- | --- | --- |
| Mutex wait | 389 (24.12%) | 1714 (31.32%) |
| Mutex release | 43 (2.67%) | 90 (1.64%) |
| Notify | 18 (1.12%) | 71 (1.30%) |
| Condvar signal | 48 (2.98%) | 62 (1.13%) |
| Tokio scheduler | 41 (2.54%) | 106 (1.94%) |
| sendto | 158 (9.80%) | 545 (9.96%) |
| recvfrom | 138 (8.56%) | 369 (6.74%) |
| mach_absolute_time | 193 (11.97%) | 357 (6.52%) |
| EventId getentropy | 58 (3.60%) | 202 (3.69%) |
| malloc/free | 92 (5.70%) | 338 (6.18%) |
| JSON codec/serialization | 31 (1.92%) | 167 (3.05%) |

Collapsed symbols below five samples are omitted by `sample`, so grouped categories are lower bounds. Condvar parks were 64,283 / 66,342 samples and kevent parks 6,119 / 5,928; these are excluded from the denominator, not treated as CPU. Aggregate mutex wait includes other runtime locks; dedicated EventBus counters establish that the candidate did not remove the wait tail. No entropy, allocation, JSON, kernel or scheduler optimization was attempted.

## Decision: REVERT

**EXPERIMENT REVERTED — insufficient measurable gain.**

- A: at the primary QoS1 point, full-state wait sum decreased 3.9%, below 25%; wait P99 stayed <=100 us. Publish-only wait sum decreased about 7.2%, also below threshold. The 4/8-sink microbenchmarks do clear the numerical A threshold, but secondary tails worsen and this is not a network-wide gain.
- B: no >=10% saturation-knee gain; 30k achieved rate slightly decreased.
- C: acquisitions decreased 16.8%, below 30%, despite a 13.8% primary PUBACK P99 improvement.
- Secondary evidence: sampled peak CPU/event proxy worsened about 6.3%, and 1,000-publisher P99 worsened in the diagnostic run. RSS changed about +1.0% at the primary point.

The final source restores the original separate `take_ready` and `next_ready_delay`, relative timer waits, and all original scheduling/notification behavior. Retained changes are opt-in instrumentation, bounded benchmark/report helpers, and timeout/confirmed-TCP regression tests. No optimized EventBus baseline is named. No second candidate or hotspot was optimized.

## Retained bottleneck ranking

1. Serialized EventBus state access remains the first application synchronization candidate: global ownership, about 4.53 acquisitions/event, and persistent network wait tails. The hypothesis that this one redundant scan is its main cause is rejected.
2. Kernel send/receive, timekeeping and Tokio scheduling remain substantial shared-host costs. Separate-host measurement is needed before capacity attribution.
3. Allocation/JSON and EventId entropy form a secondary group: allocator top symbols exceed entropy in aggregate, while entropy alone remains ~3.6–3.7%. These profiles do not justify a precise exclusive-CPU ordering within the group.

This is a revised evidence assessment after a rejected candidate, not a claim that an EventBus optimization promoted another hotspot.

## Correctness, restart/drain, and final validation

The candidate passed 36 runtime tests, the new confirmed-TCP integration regression,
five subprocess lifecycle/recovery tests, and the 12-generation graceful-restart
soak (62.28 seconds test time). The candidate was then removed before the final
workspace and release gates. The retained runtime has the original scheduling.

Focused passing evidence includes:

- `required_admission_is_atomic_and_count_byte_bounded`;
- `slow_required_sink_does_not_block_fast_sink_and_accounting_returns_to_zero`;
- `best_effort_overload_drops_without_blocking_required_acceptance`;
- `event_bus_full_concurrency_waits_for_completion_without_ready_timer_spin`;
- `delayed_retry_does_not_block_later_ready_delivery` and
  `delayed_retry_wakes_with_another_delivery_inflight`;
- `required_delivery_recovers_after_normal_retry_exhaustion`;
- `eventbus_sink_panic_recovery_001` and the new
  `eventbus_timeout_retries_owned_event_and_drains`;
- the new `eventbus_tcp_absence_filter_change_and_reconnect_preserve_isolation`,
  plus the existing filter-mismatch and official-client reconnect tests;
- `subprocess_graceful_restart_spools_and_replays_every_accepted_event_id`;
- `subprocess_spool_failure_stays_alive_until_repaired_then_replays_same_event_id`;
- spool checksum/corruption/capacity/repeated-restart/failed-replacement tests.

The TCP regression exercises EventBus with an absent stream consumer and an
independent fast required sink, then a mismatching consumer, then an eligible
replacement that explicitly ACKs the same event ID. It checks retained required
ownership, zero timer returns/retries during the blocked interval, and final zero
accounting. It does not change `TcpStreamSink`.

The timing-sensitive
`subprocess_sigkill_exposes_the_documented_three_event_loss_window` passed in all
three executions: candidate subprocess suite, final Rust 1.88 suite, and final
stable suite. No timeout occurred, no retry was needed, and its original timing
and assertions were unchanged. This verifies the intentional non-durable memory
loss window; it is not a claim of abrupt-crash durability.

Final Rust gates used these exact command forms for both `1.88.0` and `stable`:

```sh
cargo +1.88.0 fmt --all -- --check
cargo +1.88.0 clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --workspace --all-features
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +stable test --locked --workspace --all-features
python3 tests/mqtt_conformance/run.py --netbaiot-only
python3 tests/mqtt_conformance/run.py --release-gate
cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

Both toolchains passed format and warning-denying Clippy. Each workspace test run
passed 138 tests with four intentionally ignored tests. MQTT core passed 31/31;
the full release gate passed 76/76, including external Mosquitto interoperability
and 125/125 normative-requirement evidence. The ignored queue-depth probe was run
separately in release on both variants; the ignored restart soak was also run
explicitly on the candidate and final source. The final soak passed in 61.94
seconds with missing EventAccepted required IDs = 0; duplicates remain allowed.

The first attempt to start final gates encountered disk exhaustion before running
any gate. Removing only 11 GiB of rebuildable `target/debug/incremental` cache
resolved it. Final gates used `CARGO_INCREMENTAL=0`; no source, benchmark binary,
or measurement output was removed. This build-storage incident was not a runtime
or SIGKILL test failure.

No new parser/spool decoder was introduced, so no new fuzz target or fuzz campaign
was run. No physical-network/remote-generator test, integrated CPU profiler,
allocation trace, multi-hour memory soak, or TLS performance campaign was run.
The bounded overload fixture and 60-second restart soak are not substitutes for
those measurements. Exact executor polls/OS wakeups remain unavailable; the
reported worker counters deliberately use the narrower select-return definition.

All changes are local to `codex/eventbus-contention-experiment`; the final commit
uses `perf: evaluate eventbus contention optimization`. No push was performed.
