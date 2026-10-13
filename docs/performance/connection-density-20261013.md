# Connection density audit — 2026-10-13

This is an implementation and measurement checkpoint, **not a one-million-connection acceptance result**. Source baseline: `1e36f1b` on the existing `codex/performance-optimization-20261013` branch. It already contains earlier performance changes awaiting review; this is an incremental audit of that working state, **not a comparison with original main**. Changes stay on `codex/connection-density-20261013`; no merge or push to main is authorized.

## Measurement definitions

- RSS is the resident process footprint. It includes allocator effects and mappings; it is not the sum of requested Rust allocations.
- `allocation-profile` builds use `stats_alloc` to count requested live allocator bytes. Allocator metadata/size classes, resident page state, virtual address reservations, and kernel sockets are excluded. Counters are process-wide and include runtime/cache/index growth.
- `BytesMut::capacity`, HashMap spare buckets, and `ss` `rb`/`tb` are capacities/ceilings. Their units do not make them resident memory measurements.
- `connection_memory_reservation` is an admission allowance, not an allocation or an RSS estimate. The new default and validated minimum are 16 KiB, with actual reader capacities charged additionally. Independent MQTT, auth, command, ingress and event bounds remain. Their existing state-byte estimates and collection count bounds are not replaced by a malloc allocator admission layer; the logical global connection pool is not an exact bound on whole-process RSS/cgroup memory.
- Linux `/proc`, cgroup and inet_diag measurements are separate. A shared cgroup/namespace includes the local load process; its kernel memory delta must not be attributed solely to the server.
- Struct/Future sizes are measured on macOS ARM64 in release builds. They describe actual Rust layouts for that build, not portable size guarantees. The socket campaign uses Linux x86_64 independently. Linux v1 `memory.kmem`/`rss`/swap fields are recorded; v2 `memory.current`, `anon`, `sock`, `slab` fields are unavailable on this host. See [kernel cgroup v2 accounting](https://docs.kernel.org/admin-guide/cgroup-v2.html).

## Round 1: Reader and dynamic read admission

| Quantity | Baseline | After |
|---|---:|---:|
| Idle Reader buffer requested heap/capacity | 4,096 B | 0 B |
| Reader struct | 72 B | 96 B |
| Metered reader account heap | absent | 56 B |
| `read_more` Future | 4,296 B | 256 B |
| MQTT connection Future | 13,072 B | 4,992 B |
| TCP connection Future | 5,760 B | 1,760 B |
| Listener connection task Future | 13,624 B | 5,560 B |

The old 4,096-byte local read array was stored across an await, in addition to the heap buffer. The idle path now retains only an eight-byte scratch value. After the protocol validates its header, the Reader requests the exact bounded target and reads directly into the destination. No 4 KiB intermediate chunk is copied into it. An empty consumed buffer releases all retained capacity.

Small two-byte input requests two heap bytes; the 65,540-byte TCP frame probe requests 65,548 bytes over two allocations (eight-byte prefix plus complete frame), with zero reallocations. Both return to zero net requested heap after consumption and final frame drop. Maximum frames remain admitted with small fixed allowances: the four-byte TCP envelope is included in the read limit.

Global and authenticated-tenant admission use the same connection account and fail immediately under pressure. Concurrent reservation/rollback is protected by the connection-count mutex and a shared connection reader account. Buffer aliases carry RAII ownership through `Bytes::from_owner`; an accepted TCP frame can outlive its reader/connection without prematurely releasing its byte charge. No new semaphore waiter/task is created by buffer admission. The fixed allowance retains a measured 16 KiB minimum; configuring one byte cannot bypass connection task ownership admission. The aggregate byte ceilings can exceed 4 GiB, while individual requests retain checked bounds and JSON-exact/usize-safe configuration ceilings.

MQTT CONNECT credentials and temporary client identity copies are dropped before the established idle loop. Small MQTT 5 binary metadata is copied into its own bounded field, so it cannot retain an entire large CONNECT allocation.

## Round 2: Compact sessions and notifications

| Measured state | Baseline | After |
|---|---:|---:|
| StoredSession | 776 B | 280 B |
| ActiveSession | 48 B | 48 B |
| SessionKey | 72 B | 64 B |
| Empty HashMap / HashSet / VecDeque headers | 48 / 48 / 32 B | lazy pointer, 8 B each |
| Newly-created command channel requested heap | 4,128 B | 800 B |
| Newly-created broker channel requested heap | 1,056 B | 800 B |
| 10K modeled Sessions live heap/connection | 4,942.552 B | 1,614.552 B |
| 10K modeled broker live heap/connection | 3,193.652 B | 2,015.681 B |

These session probes include empty channel blocks and collection/table spare capacity, and exclude sockets, tasks, Reader, and identity provisioning. They are not capacity tests. Combined measured state saving is 4,505.972 B/connection. Boxed channel elements are allocated only when work is enqueued; existing count/byte reservations and rollback remain in front of ownership transfer. MQTT lazy extensions retain all QoS, subscription, offline, Will and NBMQ v6 recovery semantics; empty state is not materialized during ordinary reads/reconnect cleanup. `DeviceKey` identifiers already share `Arc<str>`; internal ClientId keys now do the same across broker indexes. Serialized recovery bytes remain unchanged in golden tests.

Compact client measurements: legacy read Future 4,152 B, legacy MQTT/TCP session Future 5,256 B and retained read buffer 4,096 B; compact packet read Future 264 B, hold Future 552 B, and device task Future 688 B. The 1,368 B establishment Future is boxed only during the bounded handshake.

One connection task remains. The isolated bare task/JoinSet probe measures about 184 requested heap bytes per pending task with JoinSet, and 312 with an owned sleep Future; the sleep Future itself is 112 B. Child cancellation tokens cost about 128.256 B/connection including parent child-list capacity at 1K; cloned shutdown observer tokens allocate zero. The listener now clones its shutdown observer; session takeover/invalidation retains its independent cancellation ownership. The listener does not expose per-connection mutation of this token. This measured control overhead did not justify replacing Tokio with Mio/epoll.

TLS handshake admission now uses a separate fail-fast limit (default 64). No tasks wait for a handshake slot. The slot is released before authentication and established operation. The measured rustls server struct is 1,168 B and Accept Future 1,208 B; their heap buffers are additional. The vendor implementation shrinks an empty encrypted deframer toward its 4 KiB read size. No TLS protocol or cipher support was removed. TCP handshake frames and copied credentials are also explicitly dropped before normal reads; a real TCP test sends the maximum legal frame after authentication to guard that budget boundary.

Runtime source compatibility review: `SessionKey.client_id` is now `Arc<str>` and runtime session registration returns a receiver of boxed commands; internal callers and serialization tests were updated. `netbaiot-protocol`, client/SDK wire contracts, NBMQ v6 and NBSP v3 versions are unchanged. Embedders using runtime internals must adapt typed receiver/ClientId construction.

## Real socket results

The macOS 10K MQTT 3.1.1 sample established and held all 10,000 sessions, with one server task and one server socket FD per device. Initial RSS delta: 30,295.7 B/connection; first modified sample: 11,957.0 B/connection (60.5% lower). Its first instrumentation baseline could precede complete initialization; requested-heap results from that early sample are excluded. The later synchronized Linux 10K sample is authoritative for heap delta. These preliminary CPU/P99 observations overlap other local work and do not establish a throughput/latency improvement.

Separate 1K macOS preliminary pairs, using allocator instrumentation for both versions (initial heap ticks were not synchronized to ready-server initialization; these heap deltas are provisional and excluded from acceptance):

| Protocol | Before RSS B/conn | After RSS B/conn | Before requested heap B/conn | After requested heap B/conn |
|---|---:|---:|---:|---:|
| MQTT 5.0 plaintext | 32,538.6 | 13,172.7 | 29,350.230 | 11,853.526 |
| MQTT TLS | 40,206.3 | 21,102.6 | 36,420.694 | 19,432.392 |
| Generic TCP | 26,820.6 | 10,584.1 | 25,238.960 | 9,887.954 |

Linux matched 2K pair (both held 2,000 real authenticated sessions, 500/s ramp):

| Quantity | Original | Modified |
|---|---:|---:|
| RSS delta B/connection | 30,341.1 | 11,853.8 |
| Server CPU, ramp sample (% of one core) | 4.690% | 3.463% |
| Server CPU, stable sample | 0.638% | 0.636% |
| Client connect P99 | 2.06 ms | 2.15 ms |
| Unplanned disconnect/error count | 0 | 0 |
| Shared cgroup kmem delta | 12,828,672 B | 12,767,232 B |

RSS reduced by 18,487.3 B/connection (60.93%). CPU/P99 are observations from one pair, not a statistically established capacity/latency result. No kernel socket memory improvement is claimed: shared kmem differs by only 61,440 B across 2K connections, includes both endpoints/page tables/other kernel allocations, and buffer settings were unchanged. Idle `rb`/`tb` ceilings were not treated as allocations, so socket buffer limits were not reduced without a measured active/slow-reader benefit; see [kernel TCP sysctl documentation](https://docs.kernel.org/networking/ip-sysctl.html). TCP-specific accounting is unavailable. The first instrumented heap baseline tick was not synchronized with ready-server initialization; that early heap delta is excluded and the collector now waits for a post-ready tick before measuring baseline.

Linux original 2K: 2,000 actual connected/held sessions; RSS delta 30,341.1 B/connection. Socket receive/send queues and `r`/`t`/`w` were zero at the sample. The summed `rb`/`tb` limits were 2,122,976,000 / 7,879,680,000 bytes: these are TCP buffer ceilings, not allocated payload memory. Shared-cgroup kmem grew from 1,712,128 to 14,540,800 bytes; it includes both server and clients and other kernel allocations. TCP-specific cgroup usage returned zero with accounting unavailable. It is not evidence that idle sockets have no kernel cost.

Linux completely idle 10K (compact client, KeepAlive=0, 500/s ramp, 40-second hold) established all 10,000, observed 10,000 gateway connections and 10,000 established listener sockets in three stability samples, and completed all 10,000 clean disconnects with zero errors. Server requested heap delta after a ready-server baseline: 11,586.988 B/connection. Server RSS was 99,999,744 B (delta 7,853.7 B/connection), **with 35,483,648 B of process swap**. The lower RSS is partly paging and must not be reported as an additional memory optimization. RSS delta plus added process swap is approximately 11,402 B/connection. Client RSS was 15,708,160 B with 4,055,040 B swap. Shared cgroup kmem delta was 60,825,600 B and shared swap 59,764,736 B. This is a short held-population result on a memory-constrained machine, not soak acceptance or a production capacity ceiling. Idle server CPU sampled at 0%; connect P99 was 0.81 ms at the configured 500/s ramp.

Linux TLS matched 1K (200/s ramp, 16-second hold, zero unplanned disconnects, no process swap): RSS delta 35,483.6 → 21,966.8 B/connection, saving 13,516.8 B (38.09%). After requested heap delta: 19,503.080 B/connection. Connect P99 observed 11.70 → 3.69 ms; server ramp CPU 18.252% → 18.468% of one core. These are single-pair observations; established connection memory is the optimization target, and no TLS throughput/CPU improvement is claimed. Separate `max_tls_handshakes=64` admission is retained.

## Hardware and acceptance limits

The supplied Linux host has one CPU, 474,640,384 bytes of RAM (452.65 MiB), 2 GiB swap, FD soft/hard limits 1,024/262,144, and system `fs.file-max=38,088`. Only the owned test child raises its soft FD limit; no global sysctl or kernel tuning was changed. A million socket FDs cannot fit these current ceilings, and measured user memory alone exceeds this machine's physical capacity well before a million. Swap is not a substitute for a successful density/latency acceptance test.

10K → 50K → 100K → 250K → 500K → 1M remains the required staged campaign. Only the explicitly recorded populations below are tested; the remaining stages, million-client TLS, long soak and mixed business load at million scale are **not run**. No million-connection production capacity is claimed. Many-tenant broker attach currently prunes a tenant budget table with a scan; this is a candidate establishment-CPU bottleneck requiring an appropriate many-tenant measured campaign.

## Final quiet local repeat

After all other local builds/loads stopped, both binaries were sampled with the ready-server heap baseline synchronized. At 10K real MQTT 3.1.1 connections, RSS delta was 30,333.3 → 11,783.4 B/connection; requested heap delta was 27,590.244 → 10,791.840 B/connection. Both established/held/cleanly disconnected all 10,000, with zero errors. Connect P99 observed 4.36 → 3.81 ms. This is one quiet pair, not throughput/soak certification.

The Linux 1K compact heartbeat sample held all clients, sent/received 9,795 matching PINGREQ/PINGRESP packets, had zero errors/unplanned disconnects, and observed PING P99 0.22 ms. Linux MQTT 5 persistent/subscribed 1K held all sessions and completed all clean disconnects. Actual high-scale downlink/mixed load, 50K..1M and long soak remain untested.

## Reproduction

`cargo build --release --locked -p netbaiot-server -p netbaiot-loadgen --features netbaiot-server/allocation-profile`

`python3 scripts/perf/connection_memory.py --transport mqtt --connections 10000 --hold-seconds 60 --allocation-profile`

Use `--mqtt-version 5`, `--tls`, `--persistent`, `--subscribe` independently. `--density` chooses the compact holder and `--ping-seconds N` chooses heartbeat traffic; zero selects genuinely idle MQTT with KeepAlive=0. The self-contained static-credential probe is capped at 25K by the server's hostile configuration size bound. For 50K..1M use the compact holder against an externally provisioned gateway/auth provider:

`target/release/netbaiot-loadgen --density density.json`

Example (a configured count, not a capacity result):

```json
{"address":"127.0.0.1:8080","connections":1000000,"offset":0,"tenant_width":1000001,"ramp_per_sec":1000,"handshake_concurrency":64,"hold_secs":3600,"ping_secs":30,"source_ips":["127.0.0.2","127.0.0.3"],"mqtt_version":4,"persistent":false,"subscribe":false}
```

The two-IP example is insufficient for one million tuples. With this host’s 32768..60999 ephemeral range, one million to a single endpoint needs at least 36 distinct source IPs or equivalent distributed client tuple space; the actual usable range also needs headroom. Source IPs must exist/be routable for the test and match the target address family. Multiple client hosts use disjoint offsets; each process has an explicit count and bounded handshake concurrency. The root alone waits for handshake capacity before spawning, so ramped sockets do not create a million sleeping connection tasks. Per-client pending QoS2 state is bounded to 32 IDs, response bodies to 64 KiB, and reads acquire a ten-second packet deadline after the first byte. No automatic reconnect/retry hides disconnects. Subscription MQTT ACKs are distinct from device command execution; use the existing mixed/downlink protocol tests for execution results and business ACK semantics.

A holder final established count is not simultaneous-online proof. The collector validates independent gateway active counts over stable samples, final connected/disconnected totals, and all error counters. Distributed runs need equivalent gateway/cgroup sampling and enough distinct client TCP tuples; one source IP/target port cannot supply one million connections. Both client and server resources must be collected separately.

## Validation

Completed before the final checkpoint: workspace fmt, clippy all-targets/all-features with warnings denied, workspace all-feature tests, 42/42 raw MQTT 3.1.1 conformance/differential checks, MQTT 5 raw and Mosquitto QoS0/1/2/expiry/Will regression, MQTT TLS trusted/untrusted CA interop; 20-second ASan/libFuzzer MQTT 3.1.1 (213,891 runs) and MQTT 5 (222,697 runs). Workspace tests include required admission rollback, auth 10K-publish invariant, slow sinks/outages, session takeover, lifecycle fencing, spool failures/corruption and planned recovery. Manual probes are run in isolation with `--release --ignored --nocapture --test-threads=1`.

The compact heartbeat holder first exposed its own shutdown-window error and the population collector rejected that run. It is retained as invalid evidence; its fix and regression are verified separately before use. No invalid sample is used to calculate memory savings. The first Linux candidate attempt reused old Cargo library artifacts because transported source mtimes predated those artifacts. It is excluded from optimization evidence. The candidate source was touched on the Linux host, workspace crates were recompiled, and the executable default-limits fingerprint is checked before the valid repeat. A preliminary Linux TLS run overlapped the first build and failed a management-handshake deadline; it is excluded and repeated after the build has exited. Final Linux results and final validation status are recorded with the raw evidence manifest.

Raw snapshots, binary hashes and validation outputs: [evidence manifest](connection-density-20261013/manifest.json). Final workspace checks: 574 passed, zero failed, 55 ignored (manual benchmarks/probes are counted separately). TCP ASan fuzz: 221,860 runs over 21 seconds, no crash. No 50K/100K/250K/500K/1M run or long soak was performed.
