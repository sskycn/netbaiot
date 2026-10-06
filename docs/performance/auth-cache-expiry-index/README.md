# AuthCache 热路径优化：实现、资源边界与 A/B 证据

日期：2026-10-06。实际起点是干净的 main
`76b28a1f401687c455dbc464f509be5e3fe74510`，没有回退历史基线。
生产算法提交 `85f1e31`；最终代码与回归提交 `dcf2d79`。
原始环境、命令、二进制指纹见 [environment.json](environment.json)、
[baseline-build.json](baseline-build.json) 和 [UDP 环境](udp/auth-expiry-environment.json)。
主机为 Apple M4、10 logical CPUs、16 GiB、macOS 27.0.1 arm64。
stable rustc/cargo 1.99.0；MSRV rustc/cargo 1.88.0。使用 Cargo 默认 release
profile（opt-level=3，无自定义 profile）。没有固定 CPU affinity、频率或独占整机。

## 范围与实现

生产代码只修改 [auth.rs](../../../crates/netbaiot-runtime/src/auth.rs)。新增
[AuthCache 测试](../../../crates/netbaiot-runtime/src/auth_cache_expiry_tests.rs)，
扩展 [UDP 测试](../../../crates/netbaiot-transports/src/udp_tests.rs)，补充
[认证缓存说明](../../authentication-cache.md)及历史 benchmark 的后续报告链接，其余变更为本目录证据。
EventBus、MQTT Broker、NBSP v3、NBMQ v6、UDP/MQTT/Business RPC wire schema、公开 API 均未修改。

旧 `prune_expired` 在每次 authenticate/verifier lookup 下扫描 entries、retain 全表、
重新 sum bytes、clone live keys 构造 HashSet，再扫描 FIFO order。即使没有到期记录，
每次命中也是 O(N)，有 O(N) 临时分配，且这些操作全部在既有 mutex 内执行。

新实现保留 HashMap 和 insertion/eviction FIFO order，增加
`BTreeSet<(Instant, AuthCacheKey)>`。私有 key 的 Ord 与原 Eq/Hash 对同一组字段排序。
每个 entry 恰好对应一个准确到期记录；Arc key clone 共享 credential ID 存储。
健康命中只检查最早 deadline、HashMap lookup，然后执行原验证/返回逻辑：
平均 O(1) lookup + 至多 O(log N) 最小值访问，不扫描 entries/order，不重算 bytes，
没有与表大小相关的临时分配。每次请求构造 credential ID Arc 的固定分配仍存在。

到期 E 条时，按 deadline pop/remove，逐条扣减 entry.bytes，最后仅一次 retain FIFO。
复杂度 O(E log N + N)，其中 O(N) order 清理由实际到期触发；E=0 完全跳过。
没有每个 expired key 扫一次 order 的 O(E*N) 路径。控制面 invalidation 仍扫描 entries，
并精确移除匹配 expiry 记录，复杂度至多 O(N log N)。无后台 task、worker、channel 或 GC。

两条认证路径共用 `insert_cache_entry`。插入前如果 key 已存在，先删除旧 entry、expiry、
bytes 和 FIFO ownership；正常 miss 不执行 FIFO retain。count/byte eviction pop 最老 FIFO
owner，并同步删除 expiry。拒绝超预算插入不会留下半个索引记录。
普通 hit 不提升 FIFO 位置，不改变为 LRU。

替换防御取得实际 RED→GREEN：[精选失败摘录](replacement-red.excerpt.log)显示旧 helper
替换 100 B 为 200 B 后错误累计到 300 B；相同回归现已通过，且检查 expiry/order 唯一性。
全部删除路径都在同一原有 mutex 下更新，没有 lazy stale nodes。expiry 和 FIFO 长度始终
等于 entries 长度，受 auth_cache_max_entries 限制；重复过期/插入/失效不会增加 stale ownership。

## 认证、安全与 fence

| 语义 | 保持方式与实际验证 |
|---|---|
| Positive | 原 credential fingerprint key、identity/profile clone、positive TTL、outage 时 live hit 可继续；未到期 provider 不增，过期 miss fail closed，恢复后刷新 |
| Negative | Authentication/Forbidden 的原缓存规则和 negative TTL 不变；无可信身份的负条目仍在任何 invalidation 时删除；outage/timeout 不负缓存 |
| Verifier | 原独立 verifier key、positive TTL、每包 HMAC 校验不变；错误 tag 不得通过；到期释放后重新 resolve，刷新仍只创建一个索引记录 |
| Single-flight | 原 watch sender、RAII leader、全局 bounded waiter semaphore 和 timeout 原样保留；positive/negative/verifier 两条路径各 128 并发、127 已入等待 follower，provider=1，全部得到相同身份或认证失败 |
| 异常与压力 | 两条路径均覆盖 leader abort/panic 后重新选 leader（总 provider=2）、waiter 超限 Overloaded、provider timeout/unavailable、inflight/waiter 完全释放及成功重试 |
| Epoch | 全局 epoch 仍在每次任何范围 invalidation 时推进；没有弱化为单 key fence。六个范围均验证已返回 candidate、verified hit 和延迟 provider success 的旧结果不可继续；不相关 version invalidation 也 fence 已验证请求 |
| UDP ACK | VerifiedDatagram `{verifier, epoch}` 和 with_current_verifier 原实现未改；verify 后、ingest 等待期间做六种 invalidation，EventAccepted 和 replay ownership 保留，旧签名回调不执行、NBA1 不发出 |
| Session | MQTT/TCP 的绑定 AuthContext/registration gate 未改；真实 MQTT 10,000 publishes 和 TCP bound-auth frame 回归在 end_to_end 中通过，不引入每消息认证 |

六个失效范围为 Device、Product、Tenant、CredentialVersion、AuthGeneration、All。
范围匹配的 positive/verifier 和全部 negative 释放 bytes、FIFO、expiry；其他身份保留。
`with_current_verifier` 仍在原 mutex 下同步完成 ACK 签名/发送，不跨 await。
TTL 到期本身未被改造成 epoch invalidation；已验证请求仍遵循原 epoch fence。
新增生产代码没有 unwrap/expect/panic/unsafe、secret logging 或 metric labels。
测试 provider 的 panic 是刻意测试 RAII 清理，仅在 cfg(test) 模块编译。

test-only `assert_cache_consistent` 核对 bytes=sum、双向准确 expiry、FIFO 唯一且无 stale key、
count/bytes ceilings。覆盖 helper replacement、真实 public-path mixed admission、count/byte
FIFO eviction、distinct/equal expiry、512 条同时 expiry 的 32 次周期、每种 invalidation 的
64 次插入/替换周期、single-flight completion，以及 invalidation/expiry 后容量恢复。
新 TTL 用例核对实际设置的 deadline 区间，然后同步推进 entry/index 的测试 deadline；
原使用真实 sleep 的 TTL 回归也继续运行。昂贵检查没有进入生产 hot path。

## Release microbenchmark

开始修改前以 git archive 冻结起点，构建并复制 baseline server/loadgen。
archive SHA256 和 source SHA256 已保存；本机副本位于 `target/auth-cache-expiry-index/`。
修改前与修改后分别执行三次相同命令，没有同时运行本任务的其他编译、测试或网络负载：

```bash
cargo +stable test --release -p netbaiot-runtime verifier_cache_hit_scaling_audit -- --ignored --nocapture --test-threads=1
```

每次命令对 1/64/512/4096 entries 各执行三批，每批 1,000 hits、100 live locked prunes；
16-byte HMAC、同一 current-thread runtime、stats_alloc System allocator。
表中是九个 batch 平均值的中位数，不是单次 hit 延迟分布的 p50。
锁时钟在取得 mutex 后开始，释放后记录；这些 probe 只进入测试 binary。
expire-all 为每次命令一批，取三批中位数。所有结果包含首轮冷启动/频率变化的较慢样本。

| Entries | Hit before/after ns | Mutex before/after ns | Allocations before/after | Allocated B before/after | Live prune before/after ns | Expire-all before/after ns |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 618 / 533 | 559 / 473 | 2 / 1 | 260 / 24 | 82 / 13 | 291 / 2,708 |
| 64 | 3,407 / 522 | 3,351 / 471 | 2 / 1 | 7,328 / 24 | 2,817 / 13 | 1,292 / 6,125 |
| 512 | 24,952 / 553 | 24,884 / 500 | 2 / 1 | 58,400 / 24 | 24,439 / 13 | 8,542 / 49,917 |
| 4096 | 213,244 / 528 | 213,165 / 477 | 2 / 1 | 466,976 / 24 | 210,465 / 13 | 60,208 / 351,459 |

每次独立命令的 hit ns 中位数也列出，避免仅报告最好一次：

| Entries | Before runs 1 / 2 / 3 | After runs 1 / 2 / 3 |
|---:|---:|---:|
| 1 | 1,670 / 603 / 608 | 1,654 / 529 / 530 |
| 64 | 6,950 / 3,299 / 3,407 | 1,429 / 516 / 522 |
| 512 | 25,267 / 24,952 / 24,778 | 1,282 / 553 / 552 |
| 4096 | 212,068 / 215,126 / 213,244 | 979 / 528 / 516 |

4096-entry hit 改善 **403.87x**，每个独立命令对照也超过 5x。
优化后各规模 median 为 522–553 ns，没有继续线性增长；live prune 为零 allocation。
唯一 24 B/hit 分配来自固定的 credential-ID Arc，未宣称零分配。
两版各 fixture populate provider calls=entries 数，测量的 9,000 hits/provider 新增 calls=0；
由原 benchmark 内 provider count 断言验证。

完整逐批 hit/mutex/alloc/prune/expire-all 数据见 [measurements.json](measurements.json)。
完整 benchmark stdout/stderr 已移出 Git；原始路径、大小、SHA-256 与源提交见
[archive manifest](../archive-manifest.json)，日志指纹也保留在
[raw-log-fingerprints.json](raw-log-fingerprints.json)。本机原件位于 repo-root
`local-performance-archive/`，没有已知的 Actions Artifact。[before-runs](before-runs.json)
和 [after-runs](after-runs.json)保留命令、耗时、退出码与原始日志 provenance。
优化后的 fixture 批量改变 deadline 时同步重建 index，此准备不在计时区间，
真实 prune 仍通过同一个 production helper。

**代价：** 4096 全部到期由 60.208 µs 升至 351.459 µs（约 5.84x），因逐条树删除。
原始 after 三次为 659.042 / 343.417 / 351.459 µs。这是有限 E 个记录的清理，
只在确实到期时执行；不是实时最坏 deadline 保证。没有把 expire-all 回退掩盖为改善。

## 新增常驻内存

三次独立运行：

```bash
cargo +stable test --release -p netbaiot-runtime expiry_index_memory_audit -- --ignored --nocapture --test-threads=1
```

| Entries | Tree retained bytes | Approx bytes/entry | Tree allocations |
|---:|---:|---:|---:|
| 1 | 808 | 808.000 | 1 |
| 64 | 7,368 | 115.125 | 9 |
| 512 | 64,888 | 126.734 | 79 |
| 4096 | 521,528 | 127.326 | 635 |

三次值相同；[memory 汇总数据](measurements.json)及 [运行记录](index-memory-runs.json)。
本平台 tuple=72 B（key=56 B，Instant=16 B），BTreeSet 本体=24 B；节点的固定数组、
内部指针和空槽使实际 retained 超过 tuple 大小。keys 和 Arc payload 在测量前准备，
计数只包括树分配；tree drop 后分配/释放完全相等。测的是 allocator 请求字节，
不包括 allocator header/page rounding 或整机 RSS，不是全部 cache 的内存成本。
约 127 B/entry 是该填充顺序的观测值，非所有平台/所有 occupancy 的上限。

新增索引常驻 O(N)，替代旧健康命中时 O(N) 临时 live set；健康 hit 的临时内存变为 O(1)。
原有 `state.bytes` 的 logical entry charge 不变，避免改变产品 count/byte eviction 语义。
索引固定尺寸和共享 ID 不引入新的 variable payload ownership，索引记录的数量严格由
现有 entry count ceiling 约束。树页和 allocator 的额外内存必须另计，不能把 4 MiB
logical cache budget 说成精确 RSS 上限，也不能声称这项优化没有常驻成本。

初次 memory probe 在打印 floating-point 数据后才核对 allocator，std formatter 的一次
64 B 初始化污染了结果，见[失败摘录](index-memory-instrumentation-fail.excerpt.log)。修正为
tree drop、读取计数及断言都先于打印，没有放宽释放断言；随后三个原样测量通过。

## 真实 UDP 配对

复用原有真实服务端与 `device_protocol_benchmark.py`，单个 UDP receive-loop 实际经过
`verify_signed_with_verifier → replay → ingest → with_current_verifier → NBA1`。
同一 frozen loadgen、两个 release server，512 个 UDP workers/credentials、每 worker
window=4、256 B payload，3 s warmup、1 s ramp、15 s 测量。
调高 fixture source/tenant rate 和 tenant replay ceiling，既有资源仍有限。
三对分别 before→after / after→before / before→after 串行运行，无并行编译/测试。

```bash
python3 scripts/perf/device_protocol_benchmark.py --server target/auth-cache-expiry-index/before-server --candidate target/auth-cache-expiry-index/after-server --loadgen target/auth-cache-expiry-index/loadgen --plan docs/performance/auth-cache-expiry-index/udp-plan.json --label auth-expiry --baseline 76b28a1f401687c455dbc464f509be5e3fe74510 --output target/performance/auth-cache-expiry-index/udp
```

完整计划见 [udp-plan.json](udp-plan.json)，归纳见 [udp-summary.json](udp-summary.json)。
逐秒 RSS/queues/counters、binary hashes、sampled tasks 和每次停机结果已移出 Git；
原始路径与 SHA-256 在 [archive manifest](../archive-manifest.json) 中。本机原件由
`local-performance-archive/` 保留；当前没有对应的 hosted artifact。重复的公开 fixture
credential 内容仍由原 harness 按 worker offset 重建。

| Run | Accepted/s before / after | Acceptance p99 ms before / after | Server CPU% before / after |
|---:|---:|---:|---:|
| 1 | 24,391.7 / 39,996.2 | 64.43 / 5.13 | 105.643 / 70.436 |
| 2 | 24,182.0 / 40,002.7 | 64.76 / 1.62 | 105.696 / 70.291 |
| 3 | 24,149.9 / 39,997.9 | 65.67 / 1.29 | 105.552 / 70.577 |
| Median | 24,182.0 / 39,997.9 | 64.76 / 1.62 | 105.643 / 70.436 |

优化后该 offered workload 中全部 attempted 成功，udp_no_ack=0；旧版约 227k–231k
scheduled sends 被 client window shed、约 7.5k udp_no_ack/轮。没有混淆 attempted
success 与 offered success。微小 scheduled/accepted 差异来自既有 harness 的跨测量边界计数。
这是共享主机、固定 40k offered/s 的有限 loopback 对照，不是生产吞吐或最大容量认证。
没有 4096 台真实网络负载测量；4096 cache 的真实 socket 功能回归另行执行。

六轮 cache 保持 512 entries，warmup 后 auth misses=0；pending_required 采样峰值 before
24–26 / after 20–24，全部 cooldown event_count/bytes/pending_required=0。
测量期 server RSS before 9,216–9,680 KiB / after 9,504–9,888 KiB。
优化后有预期的常驻增加，没有在本次有限窗口观察到持续队列/RSS 增长；这不证明长期无泄漏。
sampled runtime_tasks after 6–7，既有 sink worker 仍存在；静态 diff 显示没有新增 packet
task、ACK queue 或 ACK task。每轮 exit=0、forced=false，未靠 kill 伪造 graceful exit。

17 项 UDP focused 回归覆盖合法接纳、错误 HMAC、codec/version/permission、timestamp 两端、
过旧 replay、duplicate receipt、source rate limit、full required sink、admission rejection、
send failure、draining、ReplayWindow 资源边界、新增 4096-entry 真实 socket duplicate
以及六范围 verify/ACK 之间 invalidation。NBA1 仍只表示 EventAccepted，未表示 sink ACK。

## 最终验证与未运行项

验证命令及逐项结果见 [validation/results.json](validation/results.json)。完整命令日志
已移出 Git；原始路径与 SHA-256 在 [archive manifest](../archive-manifest.json) 中。
记录 source SHA256，最终文档提交不改变已验证代码。
MSRV/stable fmt、严格 clippy、完整 workspace 均已通过，各 **449 passed / 0 failed /
17 ignored**。focused cache **6 PASS**、新增 expiry **11 PASS**、UDP **17 PASS**、
end_to_end **24 PASS**（包括真实 MQTT 10,000 publishes、TCP bound AuthContext、registration
invalidation/quiesce、required sink/slow sink 责任回归）。完整 workspace 也实际执行现有
subprocess recovery、spool failure/repair、outage、生命周期与公开 serialization 回归。

```bash
cargo +1.88.0 fmt --all -- --check
cargo +1.88.0 clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --workspace --all-features
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +stable test --locked --workspace --all-features
cargo +stable xtask check
cargo +stable xtask check release --part rust
python3 fuzz/seed_corpus.py
cargo +nightly fuzz run udp_envelope -- -runs=10000 -max_len=1201
```

两条 xtask 检查均 PASS，各再次执行 fmt、严格 clippy、完整 workspace（449/0/17）。
UDP fuzz 使用 ASan，10,000 runs，exit=0，PASS；seed corpus 生成也 exit=0。
fuzz 是现有 UDP envelope/verifier/replay
短 ASan smoke，不是长期 fuzz，也不覆盖 async AuthCache 调度的所有 interleavings。
一次早期 broad filter `auth::` 也选中了 management_auth 的 JWKS socket fixtures；
受 sandbox 禁止监听而失败。完整失败日志已移出 Git，路径和 SHA-256 收录在 archive manifest。
允许本地监听的后续完整 MSRV/stable suite 原样重跑通过，未修改或延长现有断言。

NOT RUN：新分支 Windows/Linux native CI、独立主机 UDP 性能、4096-device 网络性能、
长时间 auth/UDP soak、长期 fuzz、新 slow-sink/outage 性能 campaign、整机连接容量、
SIGKILL loss/断电/底层 fsync 故障实验，以及其余 ignored broker/replay/near-capacity/
restart-soak 手工测试。17 ignored 的两个 cache 手工测量已被单独显式运行，其他 ignored
不计 PASS。NBSP/NBMQ/parser wire 未改，不用历史 CI 或历史恢复性能充当本次证据。

## 判断与剩余风险

本候选满足 healthy-hit allocation/复杂度、4096 median ≥5x、provider 不增、严格索引
ownership 和现有认证/UDP fence 的验收。新的主要 healthy-hit 成本为既有 HMAC/key
构造/identity clone/metrics/同步，不在本次测量中继续拆解或外推 EventBus bottleneck。
没有证据支持在此提交重构 EventBus，下一轮若需要应独立做真实网络剖析和配对实验。

保留的取舍是常驻 index 内存与 expire-all 更慢，以及全局 mutex 和到期时的一次 FIFO
扫描。MSRV/stable 本机通过不替代 native CI、长 soak 或调度穷尽证明。
planned restart/crash durability 和所有 wire 支持边界保持原承诺。
任务按四个逻辑提交组织；遵照本次明确限制，没有推送 main、force push、改历史、tag 或 release。
