# MQTT PUBLISH 局部优化与 Mosquitto 对照 — 2026-10-15

本报告记录实际执行的源码审查、Release 基线、候选实验和回归。确定收益是减少 Trie 临时分配、缩短本轮 Broker 路由临界区；端到端吞吐、CPU 和尾延迟未证明稳定提升。原始证据保存在任务 worktree 的 `target/performance/mqtt-publish-20261015/`。

## 范围与版本

基线为 `13b7a413a967a9a9f43765bf0b653b1c5f95be16`，最终生产候选为 `e0dafd373b877d26ad775acb0e34beb268ead1fe`。冻结 loadgen 来自 `ecd6cbc`，全部 A/B 使用同一二进制；后续 helper 整理保持此 MQTT 模式语义，保留旧 TCP command 模式。开始时 `main` 干净；工作在独立 `codex/mqtt-publish-perf-20261015` 分支。遵照本次任务的明确要求，只提交本地分支，不合并、不推送、不修改远程。

已阅读 `AGENTS.md`、`CONTRIBUTING.md`、架构及 benchmark 文档、2026-10-13 优化/连接密度/严格审查报告。既有 ACL、Reader、Session、Channel/Future、identifier 序列化、最终 required ACK 和 RPC 借用编码优化保留。没有重新引入已回退的内联 FrameBudget permit 实验。

环境：Apple M4 / 10 核 / 16 GiB，macOS 27.0.1 ARM64，Rust 1.99.0 / LLVM 23.1.1。Mosquitto 2.1.2 和 NetbaIoT 均为原生 ARM64。测试开始时及全部性能测试结束后系统 swap 均为零。服务器、负载生成器及 webhook 同机 loopback；系统桌面/浏览器后台活动未关闭，频率、温度和亲和性未控制。这些短测不代表 Linux 或生产最大容量。

服务器统一传入 `TOKIO_WORKER_THREADS=10`，负载工具使用自身固定 4 worker runtime；没有修改生产默认值。A/B 采用相同 Release 参数、锁文件、配额、负载工具和交替顺序，所有性能测量与本任务的构建/其他压测串行。参数为请求的 worker 值，没有单独观测 OS worker 数。

## 优化前问题分析

| 文件/函数 | 实际发现 | 测量证据 | 影响范围 | 优先级/处置 |
|---|---|---|---|---|
| `mqtt/broker/subscription.rs::matching` | 先收集全部 topic levels，即使没有订阅匹配 | 8 层 topic、64/1024 个下行订阅：2 次 allocation+reallocation / 128 B；输入构造在计时区外 | 每次实际进入 Trie 的发布，包括无匹配上行 | P1，实施局部优化 |
| `mqtt/broker/routing.rs::route_with_origin` / `preflight_route` | 全局零订阅原子提示已经存在；仅有 `/down` 订阅时 `/up` 仍须在锁内查 Trie | 独立锁计时组记录 route wait/hold，详见结果 | 已有订阅的 Broker 路由 | P1，仅缩短现有锁内工作，不分片或新增缓存 |
| `netbaiot-runtime/src/event.rs::publish` / worker | 大小计数在锁前；required 容量预检和提交仍在一个锁内；worker 使用受并发上限约束的 JoinSet | 网络组记录 EventBus 锁及队列/ACK 延迟；未证明单 Sink 表示或 JoinSet 是应重写的热点 | 完整网关链路 | P2，保留实现 |
| `sessions.rs::touch` / `quota.rs::check_rate_weighted` | 存在共享状态访问；Presence、速率窗口有语义约束 | 本轮没有独立函数成本归因；不把源码推测写成 CPU 瓶颈 | 活跃设备、Presence、限流 | P2，未修改 |
| `mqtt/codec/v311.rs::ack` / `publish`，MQTT5 codec/outbound | 固定 ACK 使用 Vec；PUBLISH 有 body/完整包构造；Reader payload 已有防大缓冲滞留的复制规则 | 本轮未单独测量这些分配或其端到端占比 | 协议编解码和出站 | P2，未修改公开返回类型或 payload 所有权 |
| `netbaiot-server/src/delivery.rs::HttpSink` | reqwest Client/连接池已复用；WebhookEnvelope 已借用事件；成功仍按配置的 2xx ACK 判断 | required webhook、慢端及 outage 实测，详见结果 | 业务投递 | P2，未改变并发或业务 API |
| `mqtt/fair.rs::FairWork` | 命令、Broker frame、普通 packet 按轮转顺序轮询；取消与期限在外层 | 既有公平性回归及独立真实网络检查 | MQTT 调度 | 保留实现 |

表中 `mqtt/...` 路径位于 `crates/netbaiot-transports/src/`，runtime 路径位于 `crates/netbaiot-runtime/src/`，server 路径位于 `apps/`。

## 实现与安全边界

生产改动集中在 `SubscriptionTrie::matching` / `match_at`。原实现把 topic 分割成 `Vec<&str>` 后按下标递归。最终实现借用 `Option<&str>` 表示未消费的后缀，使用 `split_once('/')` 逐层推进，同一个后缀传给精确和 `+` 分支；`None` 与 `Some("")` 分别表示 topic 耗尽和仍有一个空层级。

`#` 仍在耗尽检查之前合并，因此 `a/#` 继续匹配 `a`。`$` 只屏蔽根层的通配符。每个递归分支都有独立的借用后缀，没有可变共享游标。原 `merge_subscribers` 完整保留：QoS 取最大、No Local 逻辑与、Retain As Published 逻辑或，同一 Session 的多次匹配仍会合并。

没有新增索引、缓存、原子变量、任务或依赖；没有改变公共 API、配置、NBMQ v6/NBSP v3 或任何 wire 格式。所有调用仍位于原 Broker 锁内，SUBSCRIBE/UNSUBSCRIBE/PUBLISH 的同步边界不变。required fanout 预检/原子接纳、retained/Will、QoS 状态、计数/字节预算、回滚、EventAccepted 和 spool 责任均使用原路径。恢复后的 Trie 也使用同一 matcher。

新增回归用多个 Session 和混合订阅，对独立线性 `topic_matches` oracle 比较合并结果；覆盖 Unicode、空层级、`$` 根、精确/`+`/`#`、重复匹配、订阅替换和逐批删除。已有 raw 状态机、互操作、配额/回滚和重启回归继续执行。

仅测量工具新增 `--subscribe-down` / `subscribe_down_only`，与 uplink 订阅模式互斥。下行模式只订阅自己的 `/down`，QoS1/2 接纳仍按协议 ACK 计数，不等待不存在的 uplink 应用 echo；既有负载模式保持原语义。另新增串行 release Trie 探针，普通测试中明确 ignored，已单独手动执行。

### 修改文件清单

| 文件 | 修改 |
|---|---|
| `crates/netbaiot-transports/src/mqtt/broker/subscription.rs` | 用借用后缀推进 Trie，移除 topic Vec |
| `crates/netbaiot-transports/src/mqtt/broker/tests/sessions.rs` | 独立线性 oracle、选项合并、替换和删除回归 |
| `crates/netbaiot-transports/src/mqtt/broker/second_round_bench.rs` | 7 个 Trie 场景、3 档会话规模的手动测量 |
| `tools/netbaiot-loadgen/src/main.rs` | down-only 订阅模式、配置校验及 ACK 跟踪回归 |
| `scripts/perf/event_load.py` | 互斥的 down/uplink 参数与结果记录 |
| `scripts/perf/README.md` | 说明新模式和接纳/业务 ACK 边界 |
| `docs/performance/mqtt-publish-20261015/` | 中文报告、逐轮结果、验证清单和原始证据归档 |

## 实验记录与未采用方案

首次网络基线使用旧 `subscribe` 模式，隐含等待应用 `up_ack`；64 个客户端各发送 4 条后窗口被占满，产生 ACK deadline。网关实际接纳并 ACK 256 条。这是负载设置无效，保留 `baseline-initial.json`，不纳入性能结论。增加并验证 down-only 模式后重测，修正后的健康基线全程 published / 网关接纳 / Sink ACK 一致。

第一候选 `090c042` 把 Vec 换成可克隆 `str::Split` 游标。它减少同样的分配，但三轮订阅转发 CPU/事件中位数为 91.252 → 99.553 µs，约 +9.1%，因此未采用。使用与基线完全相同的组合构建命令重建，SHA256 与原候选一致，排除了构建特性差异；没有把退化原因直接归结为游标大小。全部证据保存在 `iterator-experiment/`，最终实现改用更小的借用字符串后缀，并重新执行 A/B。

## 测量定义

- `rate` 为目标发送速率；scheduled slots、missed slots、attempted、published、MQTT PUBACK/PUBCOMP、网关 EventsAccepted、required Sink ACK 分开保存。错过时隙不是服务器失败，目标 30k/s 不是实测最大吞吐。
- 主表 EventAccepted 回执/s 使用测量窗口 MQTT PUBACK 计数。全程网关接纳与 sink ACK 校验独立进行；窗口边界可能有少量在发布排程与实际时间戳之间跨界的消息，不用两个计数的微小差额声称丢失。
- CPU 为负载工具生命周期内的服务器进程累计 CPU 差值（包含 ramp/warmup/cooldown）除以同一全程网关接纳数。Sink ACK/s 同样是全程 ACK 数除以负载工具 elapsed，不能与测量窗口回执/s 当作同一分母。
- `sink-mode=none` 使用本地 required development AuditSink；其 ACK 不代表外部业务持久化。webhook 组单独测配置的 2xx ACK。
- PUBACK P50/P95/P99 是客户端观察值；对各轮百分位取中位数，不合并样本。RSS 是离散采样峰值，不包括内核 socket 内存，也不代表每事件堆分配。
- 现有 `event_accepted_to_sink_ack_us` 由毫秒墙钟年龄乘以 1,000 得到，分辨率仍为毫秒；保留其观测值，但不把均值变化解释成精确的亚毫秒延迟改善。EventBus eligible-queue wait 与 sink task duration 使用各自单调计时。
- stats_alloc 仅用于独立测试二进制，统计 allocation+reallocation 调用和申请字节；网络服务器未启用 allocator instrumentation。微基准是批次均摊时间，不是网络 P99；全链路每事件分配未测量。
- 锁计时单独启用，记录各 site 的 ns 累计值/次数。它有测量开销，不把该组吞吐与关闭计时的主组直接比较。
- Mosquitto 对照仅比较共同 MQTT wire 场景。NetbaIoT 的合法上行仍执行认证绑定、ACL、Codec、EventBus 和本地 required Sink；没有增加绕过这些步骤的生产模式。因此没有隔离的“纯 NetbaIoT Broker 网络吞吐”数字，也不能把双方比值称为纯协议效率差距。

## 性能结果

以下均为本轮实测。主组 5 对 A/B，顺序 AB/BA 交替；64 个连接只订阅自己的 `/down`，QoS1、明文、目标 30,000/s，预热 3 s、测量 15 s、冷却 2 s，本地 required AuditSink，锁计时关闭。`payload-bytes=1024` 是生成器参数，生成的 JSON 含序号等元数据，实际平均 payload 约 1,041 B；双方使用同一冻结生成器。

| 指标 | 原始版本 A | 最终版本 B | 变化 | 结论 |
|---|---:|---:|---:|---|
| 实际 published/s | 29,598.47 | 29,592.20 | -0.02% | 未证明稳定提升 |
| EventAccepted 回执/s | 29,598.47 | 29,592.27 | -0.02% | 未证明稳定提升 |
| Sink ACK/s（全生命周期） | 25,257.02 | 25,253.39 | -0.01% | 未证明稳定提升 |
| CPU/成功接纳事件 | 57.042 µs | 56.091 µs | -1.67% | 未证明稳定提升 |
| 进程 CPU（单核=100%） | 140.90% | 138.49% | -1.71% | 未证明稳定提升 |
| PUBACK P50 | 0.33 ms | 0.34 ms | +3.03% | 未证明稳定提升 |
| PUBACK P95 | 0.97 ms | 0.89 ms | -8.25% | 未证明稳定提升 |
| PUBACK P99 | 1.65 ms | 1.54 ms | -6.67% | 未证明稳定提升 |
| RSS 采样峰值 | 9,616 KiB | 9,536 KiB | -0.83% | 未证明稳定提升 |
| EventBus 排队均值 | 116.44 µs | 120.07 µs | +3.12% | 未证明稳定提升 |
| 接纳至 ACK 均值（毫秒量化） | 138.38 µs | 176.82 µs | +27.78% | 分辨率及噪声不足以判定改善 |
| Sink task 均值 | 0.086 µs | 0.102 µs | +18.96% | 未证明稳定提升 |
| 全链路每事件堆分配 | 未测量 | 未测量 | — | 下面只报告 Trie 局部计数 |

测量窗口各列独立中位数（不能跨列相加还原某一轮）：A scheduled 449,984、missed 5,874、attempted 443,977、published 443,977、PUBACK 443,977；B scheduled 449,993、missed 6,084、attempted 443,892、published 443,883、PUBACK 443,884。每轮精确计数与完整网关 ACK 校验保存在 summary。

CPU/事件范围 A 为 55.550–59.137 µs，B 为 52.421–57.341 µs；P99 范围 A 为 1.34–1.69 ms，B 为 1.40–1.79 ms，存在重叠。主组全程 published = gateway accepted = required ACK，最终 pending=0；服务器接纳拒绝、丢弃、重试和协议超时为零，64 个连接全部正常退出。生成器 missed slots 和窗口满单独保留，不算成服务器拒绝。

### Trie 局部测量

5 次独立进程 × 每次 3 个内部 repeat；每个 repeat 为 256 个批次、每批 32 次匹配。下表为 64 个会话键、8 层 topic 的中位数；overlap 每键插入 2 个 filter，其余每键 1 个。完整 1/64/1024 会话键结果见 `summary.json`。计时区不含输入构造。

| 匹配场景 | A → B，ns/次 | 变化 | allocation+reallocation / 次，A → B | 申请字节/次，A → B |
|---|---:|---:|---:|---:|
| down_only | 285.1 → 198.9 | -30.2% | 2 → 0 | 128 → 0 |
| early_miss | 101.9 → 18.4 | -82.0% | 2 → 0 | 128 → 0 |
| exact | 350.4 → 262.3 | -25.1% | 3 → 1 | 428 → 300 |
| plus | 342.2 → 259.5 | -24.2% | 3 → 1 | 428 → 300 |
| hash | 352.3 → 266.9 | -24.3% | 3 → 1 | 428 → 300 |
| overlap | 554.2 → 464.6 | -16.2% | 3 → 1 | 428 → 300 |
| fanout | 4,708.5 → 4,607.1 | -2.2% | 8 → 6 | 18,572 → 18,444 |

无匹配路径不再申请临时 topic Vec，也没有结果 HashMap 的堆分配。匹配路径仍保留原有 HashMap 和 SessionKey 所有权；多目标结果分配未消除。fanout 的约 2% 计时差不足以证明稳定提升；确定收益是每次少 2 次分配调用、少申请 128 B。这些数字不等于完整 MQTT 事件的分配总量。

### 独立锁诊断

3 对 A/B、同样的下行订阅场景、测量 10 s，显式启用现有锁计时。每轮先算累计 ns/次数，再取跨轮中位数。

| 指标 | 原始版本 A | 最终版本 B | 变化 | 结论 |
|---|---:|---:|---:|---|
| Broker route 等待均值 | 5,567.9 ns | 2,913.7 ns | -47.67% | 本诊断负载下降 |
| Broker route 持有均值 | 1,899.1 ns | 1,239.8 ns | -34.72% | 本诊断负载下降 |
| EventBus publish 等待均值 | 6,416.1 ns | 6,379.8 ns | -0.57% | 未证明稳定提升 |
| EventBus publish 持有均值 | 1,652.6 ns | 1,544.3 ns | -6.55% | 未证明稳定提升 |

Broker route 等待范围 A 4,256.7–5,718.0 ns，B 2,668.9–2,966.0 ns；持有范围 A 1,698.3–1,957.1 ns，B 1,210.8–1,247.0 ns，三轮范围分离。计时开销、同机竞争和短时窗口仍然限制外推；没有据此拆分锁或声称吞吐同比提高。

### 转发与完整业务网关

| 场景 | 对数/测量窗口 | PUBACK/s A → B | CPU µs/事件 A → B | PUBACK P99 ms A → B |
|---|---:|---:|---:|---:|
| 自己的上行订阅，fanout=1 | 5 / 10 s | 29,719.00 → 29,616.20 | 90.906 → 93.358 | 1.24 → 1.35 |
| 同上，延长窗口复核 | 3 / 30 s | 29,693.90 → 29,698.77 | 93.056 → 93.495 | 1.16 → 1.32 |
| required HTTP 2xx ACK，目标 1,000/s | 3 / 10 s | 1,000.00 → 1,000.00 | 145.396 → 121.538 | 0.37 → 0.37 |

短窗口 uplink CPU 中位数约 +2.7%，因此另做 30 s 测量、5 s 预热复核；长窗口 CPU 变化约 +0.47%，范围 A 91.279–95.906、B 93.320–96.171 µs。长窗口 P99 的中位数上升，但逐对结果为 1.16→1.34、1.63→1.32、1.14→1.12 ms，方向不一致。未证明端到端收益，也未观察到第一候选那种一致的约 9% CPU 退化；不能据此保证任意生产负载没有回归。

Webhook 的全生命周期 Sink ACK/s 为 807.55→807.44；均值 queue wait 为 16.80→18.77 µs，sink task 为 427.83→378.18 µs。CPU 范围 A 116.163–158.462、B 94.615–140.000 µs，P99 范围 A 0.34–4.66、B 0.29–0.49 ms，有明显环境波动，不能把 CPU 中位数下降当作已验证的业务吞吐收益。

### 连接、QoS、TLS、故障与公平性

| 检查 | A → B 实测 | 结论/范围 |
|---|---|---|
| 1,000 MQTT 在线订阅连接 | RSS 增量 15,712.3 → 15,892.5 B/连接 | 单对，全部连接真实保持；未证明改善 |
| 10,000 MQTT 在线订阅连接 | RSS 增量 13,621.7 → 13,620.0 B/连接 | 单对，全部在线，1 task+1 FD/连接；未证明改善 |
| QoS0，目标 30k/s，128 B 参数 | accepted=ACK 383,248 → 382,499 | 单对，QoS0 无 PUBACK，最终 pending=0 |
| QoS2，目标 10k/s | accepted=ACK 129,990 → 129,994 | 单对，协议两阶段完成，最终 pending=0 |
| MQTT5 + TLS + metadata，目标 10k/s | accepted=ACK 129,989 → 129,639 | 单对；计数差来自实际发布量，非服务器拒绝 |
| required webhook 延迟 10 ms，200/s | accepted=ACK 2,600 → 2,600 | 单对，最终 pending=0 |
| route-fairness，1 热连接+100 低速连接 | 低速组均 14,999 次 ACK，P99 均 0.13 ms | 单对，无超时/未完成；热客户端两侧均 15 次窗口满 |

连接测试为明文 MQTT 3.1.1、非持久会话、自己的订阅、无 PING、稳定保持 8 s；10k loaded RSS 为 155,296→155,216 KiB，未测内核 socket 内存和精确堆归属。TLS 握手 P50/P95/P99 为 A 2.11/3.45/3.64 ms、B 2.09/3.88/3.89 ms，稳态 PUBACK P99 为 0.92→0.90 ms；仅单对，不作性能改善结论。该公平性负载不是完整命令/重连混合容量测量。

Outage 使用 1 s HTTP 500、目标 300/s。A/B 均接纳 3,900 条，采样时 A 为 ACK 981 + pending 2,919，B 为 ACK 978 + pending 2,922，required sink 都按既有失败策略 paused。通用健康排空断言在两侧都失败，明确保留该失败；这不是全部投递成功。责任计数相等，生成器无错误，但该 driver 清理临时目录，未独立验证这些记录的恢复。耐久提交、失败保持存活和重放由下述专门 subprocess 回归验证，不能用 server exit=0 代替恢复证据。

### Mosquitto 共同 wire 场景对照

[Eclipse Mosquitto](https://github.com/eclipse-mosquitto/mosquitto) 2.1.2 参考实现为 loopback `allow_anonymous true`、persistence=false，QoS1、64 连接、同一 payload 生成器、明文、同一目标 30k/s；每场景 3 对交替 M/B，down 15 s、uplink 10 s，预热 3 s、冷却 2 s。NetbaIoT 仍经过认证绑定、ACL、JSON Codec、EventBus 和 required AuditSink，Mosquitto 不执行这些业务步骤。具体处理工作不等价，以下仅给共同 MQTT 场景观测，不能计算“纯 Broker 优劣倍数”。

| 场景/实现 | 实际 PUBACK/s | CPU µs/发布（全周期） | 进程 CPU % | PUBACK P50/P95/P99 ms | RSS 峰值 KiB |
|---|---:|---:|---:|---:|---:|
| down / Mosquitto | 29,680.93 | 14.432 | 36.52 | 0.33 / 0.82 / 1.32 | 5,376 |
| down / NetbaIoT 最终版 | 29,689.33 | 58.728 | 145.79 | 0.33 / 0.90 / 1.57 | 9,280 |
| uplink / Mosquitto | 29,905.10 | 17.291 | 41.59 | 0.61 / 1.00 / 1.18 | 5,600 |
| uplink / NetbaIoT 最终版 | 29,734.20 | 95.066 | 216.27 | 0.42 / 0.85 / 1.14 | 9,744 |

全部参考组 published=PUBACK，全部 NetbaIoT 对照组 published=accepted=Sink ACK，64/64 连接成功并正常退出。Mosquitto 不存在 NetbaIoT EventAccepted/业务 Sink ACK 指标；生成器字段虽名为 event_accepted_receipts，在 Mosquitto 上只能解释为 MQTT ACK。目标速率受负载调度影响，未做饱和容量搜索。完整业务网关的 HTTP ACK 结果独立列于上节。


## 正确性验证

所有下列命令实际执行，完整命令、耗时、退出码与日志索引在 `validation.json` 和原始归档。计数会在 workspace、release gate 和专项重跑之间重叠，不能相加作为独立覆盖数。

| 检查 | 实际结果 | 失败/忽略说明 |
|---|---|---|
| `cargo fmt --all -- --check` | exit 0 | 无格式错误 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | exit 0 | 无 warning |
| `cargo test --workspace --all-features` | 605 passed | 0 failed，57 ignored |
| `python3 -m unittest discover -s scripts/perf -p 'test_*.py'` | 27 passed | 0 failed |
| MQTT conformance 工具 unittest | 12 passed | 0 failed |
| `tests/mqtt_protocol_regressions.py` | 7 passed | 0 failed，使用冻结 Release 候选 |
| `tests/mqtt_conformance/run.py --release-gate --no-build` | 77/77 passed；125/125 normative requirements 有当前 PASS | 0 failed、0 reference-only observation；包含 raw、Mosquitto 差分/CLI/TLS、Rust 不变量 |
| `v5_smoke.py` / `v5_mosquitto.py` | 两脚本 exit 0 | QoS0/1/2、expiry、订阅选项、retained、Will Delay 等 |
| SDK `device_mqtt` example 构建 | exit 0 | 未跳过互操作构建 |
| `tests/run_device_profile_mosquitto.py` | exit 0，9 个 TCP/TLS/证书拒绝/持久重连场景完成 | 无失败；独立脚本内的小速率探针不算主性能结果 |
| `tests/measure_device_profile.py` | exit 0 | idle/saturated 探针完成，不用于服务器连接容量结论 |
| 显式 `subprocess_graceful_restart_sixty_second_soak` | 1 passed，64.30 s | 0 failed，0 ignored |
| 显式 `engineering_tls_qos1_http_sixty_second_soak` | 1 passed，60.21 s | 0 failed，0 ignored |
| 显式 `mixed_codec_reconnect_and_control_soak` | 1 passed，60.02 s；241 cycles / 3,856 events | 0 failed，0 ignored |
| 显式 release `second_round_trie_matching` | 最终 A/B 各 5 进程，每次 1 passed | 设置 `NETBAIOT_SECOND_ROUND=1`、`--ignored --test-threads=1`，未将默认跳过视为通过 |
| `fuzz-mqtt-state` | 157,062 次，31 s | 0 failure / exit 0 |
| `fuzz-mqtt-recovery` | 290,656 次，31 s | 0 failure / exit 0 |

fuzz 使用 `cargo +nightly fuzz run` 默认 AddressSanitizer，运行前执行项目 seed 脚本；每目标 `-max_total_time=30 -rss_limit_mb=2048 -timeout=5`，state 的 max_len=1024，recovery 的 max_len=1048576。编译时间另计。这只是有界 smoke，不能代替长期 fuzz；未修改 packet/业务 framing decoder，也未重跑全部其他 fuzz target。

默认 57 个 ignored 中，显式执行了 3 个相关 soak 和 1 个新增 Trie 探针；其余 53 个手动微基准/布局/近容量项目本轮未运行。标准 workspace 已通过 auth 一次+10,000 真实 MQTT 发布一次 provider 调用、required 多 Sink 原子回滚、count/byte 清理、慢端隔离、QoS2 操作 token/会话接管、Will/retained、管理 gate race、spool 损坏/容量/写入失败和重复重放等回归。

`subprocess_sigkill_exposes_the_documented_three_event_loss_window` 已实际通过，验证的是刻意保留的内存丢失窗口；并未获得 crash durability。`subprocess_spool_failure_stays_alive_until_repaired_then_replays_same_event_id` 与正常 restart replay 也已通过。Outage 性能脚本两次健康排空断言失败及旧负载设置超时分别列于上文，未改宽测试条件或资源限额来掩盖。

未执行：独立 Linux 服务端/外部负载机容量搜索、CPU flamegraph、worker 数量新一轮扫描、全链路每事件堆分配、内核 socket 内存、10k 以上连接、多 required Sink 的网络吞吐矩阵、24 h soak 和长期 fuzz。多 Sink 正确性有回归证据，不能写成多 Sink 性能已测。所有性能数字均为本机短时观测，未宣称生产容量。


## 复现与证据

原始命令、JSON、日志、冻结二进制 SHA256 和完整源版本索引见 `manifest.json`；逐轮统计见 `summary.json`，验证记录见 `validation.json`。`raw-evidence.tar.gz` 保存脱敏后的原始 JSON、日志和驱动脚本（包括未采用候选和无效负载实验），不包含可执行文件。冻结二进制和完整本地原始数据保留在 `target/performance/mqtt-publish-20261015/`。同一负载工具必须用于 A/B，先构建全部二进制，再串行执行；不要在压测时运行编译。

```bash
cargo build --release --locked -p netbaiot-server -p netbaiot-loadgen
NETBAIOT_SECOND_ROUND=1 cargo test --release --locked -p netbaiot-transports second_round_trie_matching -- --ignored --nocapture --test-threads=1
python3 scripts/perf/event_load.py --server-bin target/performance/mqtt-publish-20261015/bin/server-a --loadgen-bin target/performance/mqtt-publish-20261015/bin/loadgen --rate 30000 --duration 15 --warmup 3 --cooldown 2 --connections 64 --qos 1 --payload-bytes 1024 --sink-mode none --subscribe-down --audit-open-loop --server-workers 10 --no-lock-metrics
```

将 `server-a` 替换为冻结的 `server-b` 得到配对命令；上行订阅使用 `--subscribe-uplink`，锁诊断使用 `--lock-metrics`，webhook 使用 `--sink-mode webhook --rate 1000`。确切轮次顺序和其他参数以 evidence 的 campaign driver/命令记录为准。

## 后续研究

保留现有单 BrokerState、EventBus、Tokio 和受限 JoinSet。下一阶段应在独立 Linux 服务端与外部负载机上做 CPU flamegraph、锁 wait/hold 和更长窗口的容量搜索，分清锁竞争、任务唤醒、系统调用及负载端调度成本。再以测量决定是否优化小 fanout 临时 HashMap、单 required Sink 状态或固定小 ACK 编码。没有证据支持直接引入 Broker/EventBus 分片、Mio/DPDK、批量业务协议或放宽资源限制。

提交的 `summary.json` 在后续证据预算检查中压缩了重复延迟字段和 Trie 内部重复行；逐轮计数、CPU、回执百分位和 Trie 汇总保留。原始完整重复数据仍在本目录原有 `raw-evidence.tar.gz` 中，其归档和哈希未改变。
