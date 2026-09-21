# MQTT broker route critical-section experiment

## Decision

**KEEP.** At the representative 64-publisher, no-subscriber QoS1 20k/s point,
broker-route mutex acquisitions fell from one per publish to zero. The broker
wait sum therefore fell by 100%, which exceeds the experiment's 25% primary
threshold. Median throughput changed only +0.06%, the saturation knee remained
about 25--30k/s, and the result must not be described as a capacity increase.

The change is intentionally narrow. A non-retained publish may bypass the broker
mutex only while a derived atomic subscription count is zero. Retained publishes
and every route while any subscription exists continue through the unchanged
mutex-protected preflight and atomic commit. `BrokerState`, its `std::sync::Mutex`,
the subscription trie, MQTT sessions, QoS state, retained state, and accounting
remain authoritative and unchanged.

## Reference and environment

- Correctness baseline: `ce07b5b126b04f7ae95749c46e0263c7514fe491`.
- Performance baseline and starting local HEAD:
  `70b855f1a97ebe1222dc0212fe0847e91b07cf4b`.
- Measurement date: 2026-09-21--22.
- Host/toolchain/network: the same Apple M4, macOS 26.6.2, arm64, Rust 1.88
  release-build, single-host IPv4 loopback setup recorded in
  `docs/performance-baseline.md`.
- Primary runs: 64 publishers, 256-byte payload, in-process required audit sink,
  no business sink, no MQTT subscriptions, five-second warmup.

The before server was built from the exact starting SHA. The source tree was not
reset: an isolated detached worktree supplied the comparison binary. Benchmark
helpers accept explicit server/load-generator binaries so the two versions can be
compared without moving local HEAD.

## Hypothesis and source accounting before

The original no-subscriber route still acquired the broker mutex for every
non-retained PUBLISH. Under that lock it performed an empty trie lookup and built
empty route-planning containers. Protected work was short, but 64 publishers
contended on the acquisition.

For the measured no-subscriber, non-retained publish:

| Operation | Before/event | Location |
|---|---:|---|
| Validate topic/QoS | 1 | outside broker lock |
| BrokerState mutex acquisition | 1 | boundary |
| `SubscriptionTrie::matching` | 1 | lock held |
| Topic-level `Vec` materialization | 1 | lock held |
| Match `HashMap` construction | 1 | lock held |
| Relevant-tenant `HashSet` construction | 1 | lock held |
| Tenant-usage `HashMap` construction | 1 | lock held |
| `RoutePlan`/target `Vec` construction | 1 | lock held |
| Session/accounting lookup, packet-ID search, sender check | 0 | no targets |

For a subscribed route with `N` matches, the unchanged planner additionally does
one match-map insertion and `SessionKey` clone per unique match; one tenant-key
clone per match for tenant discovery; one scan across stored sessions with one
tenant-usage lookup for each relevant session; and, per target, a session lookup,
tenant projection lookup, active-sender lookup/capacity check, bounded outbound
state scan, and bounded packet-ID search. Commit performs one mutable session
lookup per QoS1/2 target, one active-sender lookup, and compact accounting updates.
The routed `BrokerMessage` is cloned once per target and once more into persistent
outbound QoS1/2 ownership. No full `StoredSession` or existing payload state is
cloned.

Retained preflight/update adds retained topic and tenant accounting lookups and
remains entirely under the broker lock. Those operations are not bypassed.

## Change and linearization

`MqttBroker` now carries an `AtomicUsize` derived from the authoritative
`BrokerState::subscription_count`. Subscribe, unsubscribe, clean-session removal,
authorization invalidation, expiry/attach cleanup, and recovery restore publish
the derived value while holding the broker mutex after the matching trie change.

A valid, non-retained route first performs one Acquire load:

- zero: return zero deliveries without acquiring the broker mutex; the load is
  the route's linearization point before a concurrent subscribe publication or
  after the final unsubscribe publication;
- nonzero: acquire the same mutex and run the original planner/commit unchanged;
- retained: always acquire the same mutex regardless of the hint.

A conservative stale nonzero only causes an unnecessary lock acquisition. The
code never publishes zero before removing the last trie entry and never publishes
nonzero before inserting the first trie entry. Recovery and clean-session tests
cover rebuilding and removing the hint. The hint owns no session, target, QoS,
retained, Will, or recovery state.

## Source accounting after

| Operation | Before | After, zero subscriptions | After, subscriptions exist |
|---|---:|---:|---:|
| Atomic subscription-count load | 0 | 1 outside lock | 1 outside lock |
| BrokerState mutex acquisitions | 1 | 0 | 1 |
| Trie matching calls | 1 | 0 | 1 |
| Topic-level `Vec` materializations | 1 | 0 | 1 |
| Temporary match/tenant maps | 3 | 0 | 3 |
| Route-plan `Vec` | 1 | 0 | 1 |
| Per-target planning/commit | unchanged | none | unchanged |

The lock type, `BrokerState` ownership, route planner, target metadata, packet-ID
allocation, live-send ordering, retained ordering, and QoS state machines did not
change. Temporary route-plan memory remains the same compact approximately
102 bytes/target when subscriptions exist and is zero on the bypass.

## QoS1 primary comparison

Three 20-second repetitions were run at 20k offered/s. Values are medians of each
reported metric.

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Accepted/PUBACK per second | 19,978.55 | 19,991.05 | +0.063% |
| PUBACK P50 | 0.36 ms | 0.34 ms | -5.6% |
| PUBACK P95 | 1.07 ms | 0.98 ms | -8.4% |
| PUBACK P99 | 1.50 ms | 1.36 ms | -9.3% |
| Server peak CPU | 140.9% | 134.3% | -4.7% |
| Peak RSS | 8,192 KiB | 8,208 KiB | +0.20% |
| Broker acquisitions | about 19,979/s | 0/s | -100% |
| Broker wait mean | 6.25 us | N/A (no acquisitions) | wait sum -100% |
| Broker wait P99 | <=250 us | N/A (no acquisitions) | removed |
| Broker hold mean | 1.01 us | N/A (no acquisitions) | hold sum -100% |
| Broker hold P99 | <=10 us | N/A (no acquisitions) | removed |

Throughput variation across the after runs was 0.225%, within the prior baseline
noise. The latency and CPU directions are favorable but are secondary evidence;
the acceptance result is the exact acquisition/wait-sum removal.

### Knee

| Offered QoS1 | Before completed/s | After completed/s | Delta |
|---:|---:|---:|---:|
| 25,000 | 24,381.2 | 24,324.4 | -0.23% |
| 30,000 | 27,070.8 | 27,106.1 | +0.13% |

The shared-host knee remains approximately 25--30k/s. The experiment did not meet
the alternative +10% knee-throughput threshold. EventBus contention and shared
host I/O/scheduling now determine the plateau in this workload.

## QoS0, QoS2, fanout, and memory

| Scenario | Before | After | Delta/result |
|---|---:|---:|---|
| QoS0 25k offered | 24,194.2/s | 24,202.5/s | +0.034% |
| QoS2 20k offered | 19,998.2/s | 19,997.1/s | -0.006% |
| QoS2 PUBCOMP P50/P95/P99 | 0.56/1.30/1.83 ms | 0.50/1.28/1.81 ms | no regression |
| 100-target plan | 49 us | 27 us median | noisy small fixture; structure unchanged |
| 1,000-target plan | 163 us | 166 us median | +1.8% |
| 2,000-target plan | 349 us | 345 us median | -1.1% |
| Plan bytes, 100/1k/2k | 10,090/101,890/204,890 | same | unchanged |

The fresh before fanout command was one process run; after values are the median
of three process runs. The 100-target result is dominated by cold-start noise and
is not claimed as a gain. The 1k/2k results and exact byte counts show no material
fanout or temporary-memory regression.

## Publisher scaling and fairness

The fixed 20k/s aggregate check used 10-second measurement intervals:

| Publishers | Before completed/s, P99 | After completed/s, P99 | Result |
|---:|---|---|---|
| 1 | 6,228.6, 0.09 ms | 7,178.7, 0.09 ms | client-window-bound; favorable |
| 100 | 19,997.3, 1.65 ms | 19,924.5, 1.67 ms | -0.36%, within short-run noise |
| 1,000 | 19,998.2, 1.49 ms | 19,997.1, 1.31 ms | equivalent throughput, lower tail |

The dedicated fairness scenario ran one saturated QoS1 publisher concurrently
with 100 QoS1 publishers at 10 messages/s each. Before and after both completed
all 15,000 low-rate messages with zero errors and identical 0.13 ms low-rate P99.
The hot stream P99 was 0.09 ms in both runs. No starvation or fairness regression
was observed.

## Overload and profile

At 50k offered QoS1/s for 10 seconds, the after build completed 28.59k/s, recorded
no client/server/admission/queue errors, held required work to at most 37 events,
and kept RSS within 7,632--8,320 KiB. The corresponding single before run completed
29.68k/s with pending work at most 74. This open-loop/shared-host point is noisy;
it is bounded-overload evidence, not a speedup or regression conclusion. There was
no deadlock, livelock, or monotonic queue/RSS growth.

The original representative profile put `__psynch_mutexwait` at 34.9% of non-park
top-of-stack samples. The after 30k profile put aggregate mutex wait at 36.2%
(1,440/3,974); this symbol covers all process mutexes. Dedicated metrics prove the
broker route contributed zero acquisitions in the after run, so the remaining
mutex profile is principally EventBus and other runtime synchronization. `sendto`
was 9.9%, `recvfrom` 6.9%, `mach_absolute_time` 6.0%, and `getentropy` 3.9%.
Removing one serialized boundary exposed the already-known EventBus/I/O limits; no
second optimization was attempted.

## Correctness evidence

All focused broker tests passed, including persistent QoS1/QoS2 overload atomicity,
QoS2 clean-session incarnation fencing, authorization reset, codec provenance,
Will subscriber pressure and restart, retained replacement reservation, persistent
unsubscribe, recovery, and compact-plan memory. Two new tests cover the zero-count
bypass, first/last subscription transitions, recovery restore, and clean-session
removal.

The following gates passed:

- Rust 1.88 format, locked workspace/all-target/all-feature Clippy, and locked
  workspace/all-feature tests;
- stable format, locked workspace/all-target/all-feature Clippy, and locked
  workspace/all-feature tests (one timing-sensitive SIGKILL test timed out once,
  then passed in isolation and in the clean full rerun);
- NetbaIoT-only MQTT conformance: 31/31;
- full MQTT release gate: 76/76, including Mosquitto 2.1.0 interoperability;
- 60-second, 12-generation graceful-restart soak: passed in 62.00 seconds, with
  all required EventAccepted IDs observed by the test contract;
- 50k offered bounded overload check and the dedicated publisher fairness check.

## Updated bottleneck order and next step

1. EventBus serialized state contention and wake/scheduling pressure.
2. Kernel send/receive, timekeeping, and Tokio scheduling overhead on the shared
   loopback host.
3. Per-event EventId entropy, followed closely by allocation/copy/JSON work.

The next experiment should isolate EventBus contention only. This report does not
authorize that work, and this change does not claim a higher production capacity.

