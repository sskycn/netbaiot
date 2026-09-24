# MQTT Broker local hot paths: measurement and changes

## Scope and method

The requested starting commit was `d4f7612a350d8c66e670ec897f3e7263ed2f00ff`. At the start of this run its successor, `65f6ba6a8dbc0796a1d14d8f67d33815372c9a8f`, committed the previously present protocol-fix working tree. The **before** build is an unmodified archive of `65f6ba6`; the **after** build is this working tree. No MQTT recovery wire record or payload representation changed in this run.

Measurements ran on a Mac mini, Apple M4, 16 GiB RAM, macOS arm64, Rust 1.88.0, release profile. The hotspot test uses three runs per scenario, with 32–2,000 samples per run; rows below normally show run 2. Fanout used one run with 30 routed messages per case. The concurrent test used three independent runs with 1,600 route plus ACK operations per worker-count setting; its numbers below are medians across those runs. Foundation used 10,000 iterations. These are local in-process measurements, not server capacity claims. Sample percentiles and operations per second are in the raw logs.

The default limits cap one Session at 128 offline messages and 32 QoS1/32 QoS2 inflight messages, and the Broker at 4,096 retained messages. Thus 1,000 offline/inflight messages and 10,000–50,000 retained messages would require raising actual safety limits; this run did not do so. The 4,000 pending and 10,000 delayed-Will fixtures are synthetic scaling tests with expanded Broker limits or direct bounded state construction. The 4,000 retained fixture stresses the global default ceiling while bypassing per-tenant admission. The 1,000-subscriber fanout fixture uses expanded limits. None is a production throughput or memory result.

Raw evidence: [baseline core](audit-evidence/mqtt-hotspots-baseline-core-65f.log), [baseline single Session](audit-evidence/mqtt-hotspots-baseline-single-65f.log), [baseline pending](audit-evidence/mqtt-hotspots-baseline-pending-65f.log), [baseline owned Will](audit-evidence/mqtt-hotspots-baseline-will-owned-65f.log), [baseline 50k Will](audit-evidence/mqtt-hotspots-baseline-will-50k-65f.log), [baseline retained](audit-evidence/mqtt-hotspots-baseline-retained-65f.log), [baseline fanout and exact subscribe](audit-evidence/mqtt-hotspots-baseline-extra-65f.log), [baseline foundation](audit-evidence/mqtt-hotspots-baseline-foundation-65f.log), [baseline concurrent](audit-evidence/mqtt-hotspots-baseline-concurrent-65f.log); corresponding [final core](audit-evidence/mqtt-hotspots-final-core.log), [final single Session](audit-evidence/mqtt-hotspots-final-single.log), [final 50k Will](audit-evidence/mqtt-hotspots-final-will-50k.log), [final retained](audit-evidence/mqtt-hotspots-final-retained.log), [final fanout and exact subscribe](audit-evidence/mqtt-hotspots-final-extra.log), [final foundation](audit-evidence/mqtt-hotspots-final-foundation.log), and [final concurrent](audit-evidence/mqtt-hotspots-final-concurrent.log). Concurrent repeats have `-r2` and `-r3` suffixes alongside the first-run logs. Final logs were rebuilt from the current source after clearing the shared release target, because a prior baseline archive run reused its test artifact.

## A. One Session

The recomputation microbenchmark confirms a small remaining expiry scan: at 0/100/128 offline messages its baseline p50 was approximately 0/83/167 ns. A 32-entry outbound scan was below 100 ns. The operating benchmark includes the Broker mutex and normal state updates:

| Operation, p50 ns | Offline 0 before → after | Offline 100 before → after | Offline 128 before → after |
| --- | ---: | ---: | ---: |
| Route + PUBACK | 1,958 → 1,750 | 2,667 → 2,125 | 3,000 → 2,166 |
| Route + PUBREC + PUBCOMP | 2,333 → 2,208 | 3,417 → 2,542 | 3,583 → 2,625 |
| SUBSCRIBE + UNSUBSCRIBE, no replay | 2,792 → 1,833 | 16,500 → 2,292 | 22,875 → 2,292 |
| `next_offline` promotion | 250 → 208 | 1,167 → 792 | 1,333 → 917 |

The large subscription slope came from two unconditional full Session clones: retained replay preflight and rollback backup. They now clone only when there is retained replay work. Empty replay still checks the count/byte admission limits. The retained replay failure rollback test remains active. The smaller route and promotion slope remains under the configured 128-message ceiling. Replacing all `sync_session_usage` mutations with incremental state would touch QoS and expiry transitions throughout the Broker for a measured sub-microsecond gain at the real limit. This run keeps its recomputation and accounting oracle.

## B. Tenant pending fairness

| 4,000 pending Session scenario | Before p50 / p95 / p99 | After p50 / p95 / p99 |
| --- | ---: | ---: |
| QoS1 capacity already full, ACK wake (ns) | 1,090,958 / 1,260,583 / 1,302,166 | 42 / 42 / 84 |
| All waiting QoS2, QoS1 wake (ns) | 1,045,000 / 1,099,917 / 1,112,833 | 42 / 84 / 458 |
| Remove and re-add queue-tail Session (ns) | 26,208 / 26,334 / 26,750 | 417 / 1,042 / 5,542 |

Pending queues are now keyed by tenant and QoS. A full tenant capacity returns before queue traversal. A release processes the matching class and stops when capacity fills. Ordered tokens preserve FIFO rotation; a membership map removes a reconnecting Session in O(log pending), without stale queue entries. The test oracle compares every membership and token to the queue, active Session, and offline-front QoS. QoS1 and QoS2 capacity-release regressions passed. A queue of individually ineligible sessions can still require inspection; the bounded session count remains the hard ceiling.

## C. Delayed Will ownership

| 10,000 future Wills, unrelated owner operation | Before p50 / p95 / p99 | After p50 / p95 / p99 |
| --- | ---: | ---: |
| Unrelated Clean Start delay release (ns) | 1,316,458 / 1,706,250 / 1,822,792 | 42 / 84 / 3,000 |
| Unrelated resume cancellation lookup (ns) | 1,982,625 / 2,587,833 / 2,634,959 | 42 / 84 / 166 |
| Owned Will resume cancellation (ns) | 1,943,667 / 2,484,375 / 2,619,208 | 250 / 333 / 458 |
| Owned Will Clean Start release (ns) | 1,320,708 / 2,190,583 / 2,357,000 | 167 / 250 / 292 |

At 50,000 future Wills, unrelated Clean Start was 9,759,625 → 42 ns and unrelated resume was 13,307,833 → 42 ns (before → after p50). Owned resume was 12,713,750 → 333 ns; owned Clean Start was 9,913,000 → 250 ns. This is a synthetic algorithm fixture under expanded limits, not a claim that the default configuration admits 50,000 simultaneous Wills.

Future Wills now have a derived SessionKey-to-(deadline, token) index and stable per-deadline token maps. Clean Start, resume, and due promotion touch owned entries, and restore rebuilds the index from authoritative PendingWill records. The index is neither serialized nor allowed to grow beyond bounded pending Will responsibilities. A same-deadline 64-Will test verifies unrelated lookup, one owner release, another owner cancellation, due promotion, and accounting consistency. Existing takeover, due-before-resume, pressure, and recovery tests passed. These numbers time the indexed helper rather than a full socket reconnect; the full reconnect includes other constant work.

## D. Retained lookup and replay

| Exact SUBSCRIBE with replay disabled | Before p50 / p95 / p99 | After p50 / p95 / p99 |
| --- | ---: | ---: |
| 0 retained (ns) | 1,375 / 1,708 / 2,625 | 1,083 / 1,125 / 1,167 |
| 1,000 retained (ns) | 217,875 / 231,833 / 236,250 | 917 / 959 / 1,125 |
| 4,000 retained (ns) | 857,250 / 887,083 / 923,292 | 1,042 / 1,084 / 1,167 |

Exact filters now use `HashMap::get`, then the same expiry, no-local, retain-handling and QoS checks. The before/after exact comparison above uses the real SUBSCRIBE path; the retained lookup microbenchmark was updated to mirror the new exact branch and is used only for wildcard comparisons. Selective wildcard lookup remains a scan: at 4,000 entries the selective `+` and selective `#` filter p50s were about 0.87/0.83 ms after, while broad `#` was about 0.51 ms and must produce thousands of matches. A retained trie would add bounded but nontrivial mutation/recovery indexing and `$`-topic cases; it is deferred until wildcard subscription latency is shown to matter in a realistic workload. Retained replay of large matched payload sets was not isolated from admission/clone costs in this run.

## E and H. Fanout and shared payload

At 1,000 matching subscribers, QoS0 route p50 went from 224 µs (64 B) to 436 µs (16 KiB) before, and from 213 µs to 482 µs after. QoS1 went from 712 µs to 1,295 µs before, and from 715 µs to 1,287 µs after. This shows that payload size matters at high fanout, while per-subscriber QoS state and routing also remain substantial. `BrokerMessage` still owns a `Vec<u8>`; no shared payload type was introduced. An isolated allocation/lock-hold A/B with `Arc<[u8]>` or `Bytes` is worth a separate compatibility review across serde, recovery, retained, offline, outbound, Will, and codec APIs. Current evidence does not isolate payload copying as the sole dominant cost. No RSS claim is made.

## F and I. Broker mutex contention and sharding decision

With 10,000 fixed Sessions and independent Session ownership per worker, median aggregate route-plus-ACK throughput was about 376k ops/s at one worker and 172k ops/s at eight workers before. The verified final runs were noisier: medians were about 227k and 101k ops/s, respectively; within each build, eight workers delivered less than half of the one-worker throughput. Eight-worker p99 ranged from 259–272 µs before and 482–665 µs after. Mean recorded BrokerLockWait rose from effectively 0 µs at one worker to roughly 31 µs before and 33–55 µs after at eight workers. Mean BrokerLockHold also rose in the noisy final runs, from about 2–3 µs before to 4–6 µs. The legacy metric has whole-microsecond resolution. Host load and CPU utilization were not controlled, so the absolute before/after regression cannot be attributed to this patch. The within-run decline establishes contention in this in-process fixture. There is no sharding implementation in this run.

If a later workload confirms sharding is needed, hash `SessionKey` for session ownership so one large tenant does not monopolize one shard. A versioned global subscription matcher would return candidate SessionKeys; route would preflight all affected shards and global quotas, then commit under ascending shard locks. Retained topics need a separately owned topic index with a defined lock order; wildcard subscriptions must still produce a complete candidate set. Auth registration and Session takeover must lock the owning shard under the existing auth-registration gate. A Will and its Session should share ownership; global count/byte limits need an atomic reservation ledger. Cross-tenant matching must be resolved by ACL before routing or included in the same multi-shard admission. Snapshot/recovery needs one consistent epoch or quiescent barrier across matcher, shards, retained state, and global quotas. A proposed lock order is auth-registration gate → matcher/global admission → numbered Session shards → retained ownership, with no reverse acquisition. These design obligations must be proven before adding mutexes.

## G. Small-scale foundation paths

The prior report recorded QoS1 route + ACK moving from 1,167 to 1,750 ns and inbound QoS2 accept + route from 750 to 1,167 ns. On this machine and source baseline, the foundation benchmark is **1,708 → 1,833 ns** for QoS1 and **1,250 → 1,291 ns** for inbound QoS2 (before → after). These single runs are susceptible to host noise. This run does not claim recovery of the earlier fixed cost; correctness and the bounded index improvements take precedence.

## Verification

Rust 1.88 and stable format, warning-free workspace Clippy, and workspace tests; the 125/125 MQTT release gate; MQTT 5 raw smoke and Mosquitto interop; and `mqtt_state`/`mqtt_recovery` fuzz at 5,000 iterations each passed. The first workspace test attempt was sandbox-blocked on a CLI smoke loopback bind and passed when rerun with local socket access. MQTT message representation did not change, so the optional v5 packet/publish fuzz expansion was not triggered. No long soak, RSS/peak allocation profile, or external CI check was run for this working tree.
