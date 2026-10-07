# NetbaIoT 第二轮性能优化报告

## 结论与测量边界

**A. 当前测量没有证明单个 Broker Mutex 是主导瓶颈，保留现有单锁，不进行并发架构重构。** 本轮减少锁内分配、metadata 复制和无 expiry 消息的 ownership 查询，并限制同时到期重试的单次搬移量。固定 offered rate、局部 benchmark、配置上限均不代表生产容量。

这份报告记录独立优化分支的结果。每个候选先测量，再决定实现；每个保留项有独立 commit、完整 Rust 回归和串行 A/B。未实施项和被替换原型均保留在下表。详细矩阵、每轮数据与 SHA 见同目录 JSON；原始日志与冻结二进制位于本地 `target/second-round/`。

## Baseline

| 项目 | 值 |
|---|---|
| Commit | `c359a868a38bfd83816e855795456bbdf097893a` |
| Machine | Mac mini / Apple M4 / 10 cores / 16 GiB RAM |
| OS | macOS 27.0.1, build 26A434, arm64 |
| Rust | rustc 1.99.0 `b940084d7`, LLVM 23.1.1 |
| Cargo | 1.99.0 `5f94df478` |
| 原始 server | 12,169,200 bytes, SHA256 `497a6166c771c23da1ad59dba5c2775f188fef74afe7fe7197ed14090d175046` |
| 同一 MQTT generator | SHA256 `7aa0bc55e099131a752491d315dc03211bf748abe89f5b25897c527d89097c1e` |

基线修改前四个 Actions 均成功，Native recovery 和 Native DX 的 Ubuntu/macOS/Windows 全绿。链接和 job 证据见 [environment.json](environment.json)。六项基线命令（fmt、clippy、locked workspace tests、xtask check、release Rust、release MQTT）全部 PASS 后才开始性能修改。

所有计时在同一机器、工具链上串行执行，计时期间没有构建或测试。每个 A/B 使用相同限额、warmup、时长、冻结 generator。网络通常为 3 秒 warmup + 15 秒稳态 + 2 秒 cooldown；故障测试 20 秒含 5 秒 HTTP 500。局部测试每侧多轮、每轮三次，setup/cleanup 与样本存储不计时。分配次数包含 realloc 请求，累计 allocated bytes 不是峰值 live memory。

公开 ingress 只允许设备所属主题，且一个 DeviceKey 只有一个 live 连接。256 个真实客户端使用各自合法 uplink 订阅。10/100/1000 fanout 与 broad retained 是 broker 子系统测量；额外的真实高 fanout 使用同一设备的多个离线 persistent ClientId，再逐个 reconnect。没有放宽跨设备授权，没有把无关 sentinel 订阅称为真实消费。

## 候选、复杂度与决策

| 候选 | 决策 | 复杂度 / 原因 |
|---|---|---|
| B iterator `topic_matches` | **KEPT** `1d4ec8b` | 保持 O(levels)，移除两个临时 Vec，所有测量 case 0 allocation；保留空 level、`+`/`#`、`$` 规则 |
| C lazy tenant preflight + 一次 message charge | **KEPT** `2f7d9ce` | 删除中间 tenant HashSet 和 Will 全租户扫描；投影按 required target lazy 初始化，首次按 bounded hint reserve；仍先完整 preflight 再 commit |
| C growing HashMap 原型 | **REVERTED** | 1000 tenants 累计 bytes +12.7%，改为 bounded reservation，原始数据留存 |
| D incremental QoS counters | **NOT IMPLEMENTED** | usage/capacity 仍 O(outbound)；32 个为约 25/22ns，256 个约 142/136ns；未证明真实高 inflight critical path 占比足以承担 counters drift authority |
| E 无 expiry 先跳过 started lookup | **KEPT** `b6c9c25` | 仍 O(offline+outbound)，常见无 expiry 元素不做 hash 查询；有 expiry 的 started ownership 规则未改变 |
| E expiry multiset | **NOT IMPLEMENTED** | 未证明新增 ownership/deadline authority 有必要；保留扫描和最早 deadline 删除后的重新计算 |
| F demand-bounded retry promotion | **KEPT** `c7f0fd6` | 每次至多搬移与可用 worker/ready demand 对应的记录，至少 1 条 due 工作获得进展；whole bucket 或 bounded prefix，保持 deadline/FIFO |
| F 逐条 first-entry pop 原型 | **REVERTED** | 16k distinct deadline 的实际 worker throughput 约 -7%，改为 bounded bulk；原始日志留存 |
| G topic/properties sharing | **KEPT** `4ce34a5` | topic `Arc<str>`，非空 properties `Option<Arc<_>>`，空默认不分配，修改使用 COW；逻辑每份 responsibility quota 不变 |
| H retained trie/index | **NOT IMPLEMENTED** | exact 已 O(1)，wildcard 仍 O(retained + matched deliveries)；完整 replay 已测，但公开 ingress 的频率/主导锁热点证据不足 |
| I outbound order index | **NOT IMPLEMENTED** | 删除仍 O(outbound_order)；32/256 个 remove 约 83/292ns，未证明 replay-order 索引复杂度值得引入 |
| J operation lock histogram | **KEPT** `4f16c1c` | 固定 enum 分类，默认关闭，无时钟读取和 histogram 分配；不改变 std Mutex/原子性模型 |
| JSON 单次安全扫描 | **KEPT** `3392b3c` | 两次 O(bytes) 改为一次 O(bytes)，保留深度、成员数、数组拒绝、大小和 serde 完整验证 |
| Release production profile | **NOT IMPLEMENTED** | 实验 ThinLTO 体积 -24.2%、冷构建 +51.4%；固定率 MQTT/RPC 未证明容量收益，不加入 Cargo profile，也不改变默认 release |

D/E/I 的完整 ownership 矩阵见 [ownership-summary.json](ownership-summary.json) 与 [ownership-decision.json](ownership-decision.json)。Session/outbound/started 状态按合法状态机实际建立；不把独立小函数耗时相减后声称精确 lock fraction。未加 counters/index，因此未增加任何可能 drift 的派生 authority。

## 局部收益与 allocation

| 场景 | Before → After | Allocation / bytes |
|---|---|---|
| topic exact 32 levels | 约 650→307ns | 8 requests / 1024B → 0 / 0 |
| topic early mismatch 32 levels | 约 650→9ns | 0 allocation after |
| C 1000 targets / 1 tenant | 约 198→177µs | 528610→493786B |
| C 1000 targets / 1000 tenants | 约 240→208µs | 2015→2014 requests；645118→610294B |
| E started/no-expiry/outbound256 | 1467→179ns | 未增加分配 |
| E 完整 Q1 / Q2 ownership cycle, 256 | 7333→3250ns / 9250→3875ns | 使用实际 begin-transfer 与 ACK/PUBREL lifecycle |
| G profile A / fanout1000 | 约 624→572µs | 8014→6014 requests；811416→627416B |
| G profile C / fanout1000 | 约 1.06→0.569ms | 48014→6014 requests；2177416→627416B |
| G profile D / fanout1000 | 约 1.416→0.575ms | 80014→6014 requests；5985416→627416B |
| JSON flat 16KiB full codec | 25875→20463ns | codec allocation 未采集 |
| JSON flat 64KiB guards / full codec | 81083→58724ns / 103341→81052ns | 安全扫描本身无 allocation；serde 分配仍保留 |

G fresh input 包含新 payload 的第一次 Bytes 共享提升，未用已共享实例掩盖初次成本。所有保留的 metadata/profile A/B/C/D、fanout、tenant 和 QoS 矩阵见 [phase-g-summary.json](phase-g-summary.json) 与 [phase-c-summary.json](phase-c-summary.json)。

E 全部带 expiry 的 256 项扫描约 1458→1531ns（+73ns/+5.01%），已作为 tradeoff 复核；真实 Q2 P99 不变。JSON 256B flat micro 642.5→716ns（+73.5ns），未改动的 depth/member/serde controls 也有 +39..56% 变化，无法隔离极小操作的编译布局/测量影响；额外 256B MQTT ABBA 的 P99 +1.62%、RSS +0.27%，错误/过载为 0，未添加任意 hybrid 阈值。64B 是截断 envelope，deep 是不合法 telemetry scalar schema，valid 标记按真实 codec 结果；较大输入以白空格填充至 legal ceiling，不能代表所有真实 JSON 形状。

## E2E

Producer PUBACK/PUBCOMP 只表示 EventAccepted 与对应 MQTT responsibility 被接受，不表示业务持久化或执行。CPU 是 process 采样平均值（100%=1 core，含启动/收尾样本），不是整机稳定窗口利用率。

下表为各阶段 immediate-before / candidate 的每侧轮次中位数，不能把它们串乘为整体收益。所有列出的有效轮次均为 0 error、0 overload、0 unexpected disconnect、0 producer pending-at-disconnect。P99.9 有充足事件样本，但只有少数独立 host runs，调度暂停对其影响大；不据此承诺尾延迟或容量。

| 阶段 / workload | ACK/s A→B | P50 ms A→B | P95 ms A→B | P99 ms A→B | P99.9 ms A→B | RSS KiB A→B | CPU % A→B |
|---|---:|---:|---:|---:|---:|---:|---:|
| B topic: Q1 | 20000→20000 | 0.34→0.34 | 0.7→0.705 | 0.94→0.985 | 1.155→3.66 | 17872→17920 | 220.645→217.267 |
| C: q1 | 20000.667→20000.667 | 0.35→0.35 | 0.785→0.775 | 1.02→1.03 | 1.43→2.295 | 17944→18072 | 205.847→208.093 |
| E: q2 | 19999.333→20000.033 | 0.535→0.53 | 0.945→0.945 | 1.09→1.09 | 1.215→1.22 | 10664→10808 | 256.988→258.812 |
| F: outage | 1000→1000.025 | 0.12→0.12 | 0.215→0.22 | 0.29→0.3 | 0.64→0.925 | 28264→28400 | 19.862→19.6 |
| G: metadata | 20000.667→20000 | 0.38→0.355 | 0.845→0.77 | 1.02→0.975 | 1.19→1.225 | 12376→12304 | 219.303→240.775 |
| G: large | 19999.567→20000.133 | 0.68→0.62 | 1.4→1.31 | 1.75→1.585 | 2.52→2.25 | 18224→16416 | 384.39→362.28 |
| G: q1 | 19998.833→20000.667 | 0.355→0.35 | 0.79→0.785 | 1.095→1.06 | 10.535→1.59 | 19112→18208 | 207.705→194.74 |
| G: plain | 19999.233→19999.133 | 0.32→0.315 | 0.92→0.935 | 1.315→1.34 | 1.805→2.59 | 16928→16840 | 124.282→126.713 |
| JSON: large | 19905.467→19962.9 | 0.69→0.615 | 1.53→1.425 | 2.5→2.075 | 16.25→5.245 | 27792→21776 | 392.665→350.877 |
| JSON: q1 | 19995.333→19999.3 | 0.32→0.33 | 0.675→0.66 | 1.035→0.94 | 8.19→6.19 | 18808→18616 | 220.245→216.321 |
| JSON: small | 20000→19999.2 | 0.315→0.32 | 0.665→0.675 | 0.925→0.94 | 1.29→1.41 | 17568→17616 | 199.588→198.912 |


G metadata CPU +9.8% 是实际观察到的成本，不能隐藏；同阶段普通/大载荷 CPU 降低，plain CPU +1.96%。B P99 +4.8% 进行了额外 8 次交替复核（每侧累计 6 轮，P99 permutation p≈0.416）；这不是等价性证明。JSON 大载荷 realized rate 约 19.9k/s，generator 会丢弃错过的 scheduled slots，未将 offered 20k/s 写成已实现最大容量。

启动 fixture 失败记录：早期网络 fixture 默认 tenant subscription 上限 128 被合法拒绝，随后显式调整测试上限，未改生产默认。G 有一次候选启动失败，旧 harness 未保留 stderr，原因未知；随后增加 distinct port reservation 和 bounded startup diagnostics，后续轮次通过。不能把未知失败归因于已证实的端口冲突。

## Lock 与 retry

下表是旧 route-only broker 计时口径，仅用于每个 J 之前的同阶段 A/B；单位均为 mean µs。`—` 表示未采集，不能作为 0。J 之后的 all-site 总口径与这些数字不可直接比较。

| 阶段 / workload | Broker wait µs A→B | Broker hold µs A→B | EventBus wait µs A→B | EventBus hold µs A→B |
|---|---:|---:|---:|---:|
| B topic: Q1 | 27.543→27.627 | 5.815→5.879 | 0.593→0.585 | 0.21→0.203 |
| C: q1 | 27.184→26.056 | 5.779→5.466 | 0.655→0.698 | 0.165→0.182 |
| E: q2 | —→— | —→— | 0.326→0.337 | 0.107→0.119 |
| F: outage | 0.219→0.266 | 6.726→6.712 | 0.003→0.004 | 0.117→0.112 |
| G: metadata | 20.38→17.046 | 6.518→4.802 | 0.34→0.577 | 0.127→0.184 |
| G: large | 15.867→14.994 | 3.207→3.116 | 0.741→0.778 | 0.069→0.076 |
| G: q1 | 25.479→25.038 | 5.368→5.3 | 0.734→0.743 | 0.195→0.169 |
| G: plain | —→— | —→— | 2.555→2.381 | 0.329→0.301 |
| JSON: large | —→— | —→— | —→— | —→— |
| JSON: q1 | —→— | —→— | —→— | —→— |
| JSON: small | —→— | —→— | —→— | —→— |


F 对 4096 个 distinct deadline 同时到期的直接 take_ready hold，P99 降至约 647ns，最大约 1041ns；此前一次性搬移约 150µs。直接 drain 只证明单次 work 的界限；另用真实 owned async workers、并发 8，验证全部 ACK 和 count/byte 清空：4096 distinct throughput 285638→276893/s（-3.1%），16384 distinct 141439→138228/s（-2.3%），普通 ready +2.9%。收益为抑制锁突发，不能宣称 worker 容量提升。

真实 HTTP 500 故障产生约 5k backlog，P99 +3.45%、RSS +0.48%。Broker wait +21% / EventBus wait +58% 的绝对值很小但确实上升，已复核；hold 分别约 -0.21%/-4.6%。测量结束仍有约 3.8k required pending，不能声称它们全部已在窗口内 ACK；进程 planned exit=0，完整 spool/replay 和后述重启测试承担 durable commit/replay 验证。

J 启用固定 17 类 site histogram：route、subscribe、unsubscribe、puback、pubrec、pubrel、pubcomp、attach、detach、retained_replay、maintenance、recovery、outbound、will、invalidation、read、inbound。RAII early-return 仍释放锁；计时在解锁后写 histogram。默认关闭的同二进制行为保持无时钟/额外 histogram 分配，J 默认关闭 ABBA 对 G P99 -6.55%、RSS -2.31%。启用模式存在诊断开销，不当作默认生产容量。

| workload / site | wait mean µs | wait P99 上界 µs | hold mean µs | hold P99 上界 µs |
|---|---:|---:|---:|---:|
| q1 / route | 27.018 | 250 | 8.876 | 50 |
| q1 / puback | 25.018 | 250 | 4.951 | 25 |
| q1 / subscribe | 0.055 | 0.25 | 7.785 | 50 |
| q1 / attach | 0.058 | 0.25 | 13.55 | 100 |
| q1 / detach | 95.941 | 1000 | 13.035 | 50 |
| q1 / maintenance | 6.576 | 100 | 2.608 | 10 |
| q2 / pubrec | 15.178 | 250 | 2.472 | 10 |
| q2 / pubrel | 16.582 | 250 | 3.871 | 25 |
| q2 / pubcomp | 12.814 | 250 | 3.405 | 25 |
| metadata / route | 18.403 | 250 | 8.339 | 25 |


每种真实 workload 连续测 3 轮。累计 all-site wait / accepted event 为 Q1≈127.7µs、Q2≈145.6µs、metadata≈135.7µs，而 producer ACK mean≈0.416/0.569/0.418ms；**前者包括 producer ACK 之外的 subscriber ACK/outbound/maintenance，不能相除后声称 request critical-path 的 X%**。Q1/Q2/metadata 进程 CPU 中位数约 206/262/245%（100%=1 core）。尚有多核余量本身不构成 sharding 依据。Q2 route 工作归在 PUBREL phases，非 Route 计数为 0 不代表未路由。

完整 retained subscribe/replay、10..10k messages、exact/plus/selective/v1/#/#/no-match 均已测。10k exact P99≈1.83µs，selective≈523µs，no-match≈247µs，broad≈2.15ms；broad 本身必须建立 10k deliveries，索引无法删除这一责任。该 direct-broker 宽订阅不是公开 ingress 跨设备 workload。所有 histogram 为 bucket 上界，未触发的 site 在 JSON 为 null。

## Profile、离线回放与 memory

Release profile 在相同最终 source 上使用独立空 target 目录，默认 release 26.66s / 12,167,312B，临时 production 40.36s / 9,226,848B；registry/OS filesystem cache 未清空。10 次交替启动中首次为 A≈0.947s/B≈0.436s，之后大多 5–9ms；微小 cached startup 与 1ms polling 不支持“启动变快”的结论。MQTT ABBA throughput 无变化、P99 -5.34%、RSS -9.10%、CPU +5.23%。

RPC 稳态 500 offered/s，四轮 7488..7497 个 ACK / 15s workload（setup/recovery 不计入这个分母），event ACK P99 中位数 A≈1.87ms/B≈1.875ms，RSS A≈8728/B≈8080KiB；publish errors/auth timeouts 为 0。一个 release 轮次最后一条已 enqueue 的 event 尚未在采集窗口观察 ACK，不写成全部 ACK。初始 2k/s 使用默认 16/device/s 导致限流；提高相同测试 rate ceilings 后仍积累到有界队列而断开，两个无效压力 cohort 保留并排除 profile gate。未放大稳态 count/byte queue。详细 build commands、二进制 hashes、启动和三种 RPC cohort 见 [profile-summary.json](profile-summary.json)。没有新增 production profile / panic=abort。

原始 baseline 与最终优化版的真实 persistent offline ABBA：128 个同设备持久 ClientId ×32 条消息，每轮 4096/4096 replay，session-present 与 SourceMessageId/内容验证通过，planned exit=0。Reconnect+32 条 replay 的 P99 中位数约 1.233→1.220ms，backlog RSS 9328→8120KiB。Producer 每轮只有 32 个样本，此场景不能当作稳定 P99.9/最大容量；验证的是 MQTT replay，而不是 DeviceEvent.event_id。初始非法 fixture 字段为 900B，超过 codec 256B，被拒绝；改成四个 225B 字段，两侧相同，未修改 codec limits。

Idle memory ABBA：128 个明文、非 persistent、无订阅 MQTT 连接，hold 6s。Loaded RSS 11008→10880KiB，aggregate delta/connection 36992→35776B；每连接 1 FD / 1 owned task，active=128。RSS delta 包含 auth cache/runtime/allocator，不是 connection struct 大小。配置的 524288B logical reservation 不是实际 allocation。见 [final-replay-memory-summary.json](final-replay-memory-summary.json)。


## Correctness 与验证

优化未改变 MQTT QoS0/1/2、Receive Maximum、Packet Identifier、DUP、retained、Will、session/message expiry、persistent sessions、generation/incarnation/auth fencing 或 all-or-nothing admission。逻辑 tenant/global count+byte quota 按每个 delivery responsibility 收费，不因 Arc 共享减少。命令仍仅发送给 live local session；没有 offline DeviceCommand queue。

G 是 server-side Rust 表示变化：`BrokerMessage.topic` 和 `properties` 构造方需 `.into()`。Public protocol/client/device SDK wire types未改；historical JSON、NBMQ v1–v6 的现有 fixture 全部通过，新增 compact-message digest/JSON golden 和 COW/isolation/quota 测试。Spool/EventId、ACK 后回收及 duplicate replay 语义保留。QoS2 仍不是业务 exactly-once；planned spool 不提供未落盘流量的 abrupt-crash durability。

每个保留阶段运行 `cargo xtask check`（fmt check、locked clippy all-targets/all-features `-D warnings`、locked workspace all-features tests、evidence gate）；MQTT 修改额外运行 release MQTT/raw-state-machine/Mosquitto gate。G 额外运行请求的 spool 与 mqtt_recovery filters。最终完整 xtask、release Rust、release MQTT、release preflight、spool（16 tests）、mqtt_recovery filter、60 秒 ignored restart soak 全部 **PASS**，命令/日志见 [validation-results.json](validation-results.json)。Soak 为 12 代 planned restart，实际 68.45s，验证 accepted EventId 在 restart/spool/replay 后被观察到；默认 workspace suite 的 SIGKILL bounded-loss regression 也 PASS。

相关 8 个 fuzz target 各 **10,000 runs PASS**：mqtt_packet、mqtt_state、mqtt_recovery、mqtt_v5_packet、mqtt_v5_publish、mqtt_v5_subscribe、json_codec、restart_spool。使用 cargo-fuzz 默认 ASan/nightly，见 [fuzz-results.json](fuzz-results.json)；smoke 不等于安全审计或任意输入的完整证明。

未运行小时级/跨机器生产 soak、目标公网/TLS容量 campaign、真实跨设备 live fanout（授权模型不允许），或持续 maximum practical network inflight 的容量测试。高 inflight 1..256/1024 是实际 broker state-machine 子系统矩阵；网络 generator 默认 window 为 4。未测 codec allocation，未以未知指标写成 0。性能测试是短期本机实验；部署容量仍需目标环境测量。

基线三平台 Actions PASS 见 environment.json；优化合并 commit 的最终 Actions/checks 是该 source 的 CI 证据，随后清理任务从独立、全绿的 main 开始。

## Remaining hotspots 与第三轮门槛

保留 SessionUsage/capacity/outbound-order 和带 expiry 的扫描；selective retained 的 O(N) 成本在 10k 场景可见，但应先证明真实订阅频率和锁占比。`sync_session_usage` 仍更新多个 deadline/accounting index，metadata bytes 计算和 matched-delivery 建立也有成本。JSON serde/字段构造、RPC/网络调度与 ACK path 仍存在。

下一轮应使用目标部署的合法 subscriber/高 inflight/offline replay/真实 sink workload，记录 causal request stages 与 CPU。当前数据不支持先做 sharding，也不支持对配置值、固定 20k/s、单次 burst 或局部 throughput 作生产容量承诺。

## Reproduce

详见 [README](README.md)、同目录 binaries/probes manifests 和各阶段 summary。Raw logs 路径在本地 `target/second-round/`；相同 frozen generator 必须用于 A/B，计时不得与编译/测试并行。
