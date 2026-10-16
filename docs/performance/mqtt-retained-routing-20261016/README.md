# MQTT 上行路由与 Retained 补充优化 — 2026-10-15–16

本轮保留三项生产优化：没有可能匹配订阅时跳过 `/up` Broker 路由锁；Retain Handling 禁止重放时跳过 Retained 查找；Retained 替换复用已有键并避免不变过期索引的键分配。局部成本下降已实测，完整网关的吞吐、CPU、RSS 改善尚未证明。Retained 有序索引实验已撤回，普通通配符重放仍扫描原 HashMap。

## 范围、基线与复现

本轮基于 `2def138186711d5d9360e9afac601f432c9dbba2`，其生产源码与上一轮 `e0dafd3` 相同。上一轮已完成 Trie 借用后缀优化及 Mosquitto 网络对照，见 [上一轮报告](../mqtt-publish-20261015/README.md)，本轮不把那次收益重复计入。原始 `main` 仍为 `13b7a41`，未修改。最终生产候选为 `5bd93b4`，所有改动仅在本地 `codex/mqtt-publish-perf-20261015` 分支；没有合并、推送或修改远程。

提交：`048e889` 添加 Retained 测量；`7307add` 提前处理禁止重放策略；`3144e54` 复用 Retained 键及补充批量/路由探针；`5bd93b4` 添加上行订阅保守提示及回归。完整 SHA、二进制 SHA256、构建和执行命令、原始日志索引见 [manifest.json](manifest.json)。

环境为 Apple M4 / 10 核 / 16 GiB、macOS 27.0.1 ARM64、Rust/Cargo 1.99、LLVM 23.1.1、Python 3.9.6。服务器与负载机位于同一台机器的 loopback，桌面后台活动未关闭，频率、温度、CPU 亲和性未控制。服务器统一请求 10 个 Tokio worker，冻结负载工具来自 `ecd6cbc`；Release、Cargo.lock、配额和负载参数在 A/B 间相同。性能测试与本任务的构建及其他压测串行；本轮全部性能测试结束后系统 swap 为零。

基线服务器复用上一轮最终二进制。探针使用相同测量代码叠加到对应源码：Retain Handling A 是 `048e889`，替换写入和路由 A 是 `7307add` 加测量 overlay（该策略分支与其测量路径无关）；B 的生产代码均与最终候选一致。无运行时依赖、配置或 wire 格式变化。探针中扩大 fixture 容量仅用于容纳指定测试规模，不修改生产默认限制，不能作为生产容量声明。

原始证据目录：`target/performance/retained-index-20261015/`。按照仓库证据策略，完整文本、脚本及失败实验归档留在忽略目录 `target/performance/retained-index-20261015/raw-evidence.tar.gz`，不包含可执行文件；Git 只保留报告、摘要及哈希清单。本机路径在验证完成后脱敏，哈希对应脱敏后的文本。网络和微基准明细分别见 [summary.json](summary.json)、[micro-summary.json](micro-summary.json)。

## 六个方向的审查和取舍

| 方向 | 源码发现与本轮处置 |
|---|---|
| `/up` 路由 | 原有全局零订阅提示不能排除仅 `/down` 的订阅。增加固定大小的保守后缀计数，消除该场景 Broker route 锁与 Trie 匹配。 |
| Retained | 精确 Topic 已有 HashMap 查找；通配符仍全扫描。先跳过明确禁止重放的查询；替换写入减少分配。有序索引因写入代价撤回。 |
| QoS / ACK / 过期 | 租户用量、待唤醒集合和 DeadlineIndex 已有增量索引。`SessionUsage::from_session`、`has_outbound_capacity` 和 `next_message_expiry` 仍遍历单会话状态。本轮不新增每会话计数及其恢复/接管不变量。 |
| Trie / 精确匹配 | 上一轮已移除临时 topic Vec；本轮只增加随插入/删除维护的标量计数，不重做 matcher。 |
| 发送 / 唤醒 | 有界 Tokio channel 存指针，容量释放按租户/QoS 索引唤醒；没有每消息派生任务。保留现有内存布局和公平轮询。 |
| EventBus | 计费 JSON 遍历在锁前，required Sink 预检与提交在同一临界区，队列和 JoinSet 有界。记录实际锁/排队/业务 ACK，对其余热点没有独立 CPU 归因，不据源码猜测重写可靠性边界。 |

Mosquitto 的 [`retain.c`（v2.0.22）](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/src/retain.c) 使用 Topic 层级及子节点索引，按精确层级剪枝、按 `+`/`#` 遍历，并在交付前处理过期与授权；[`subs.c`](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/src/subs.c) 提供订阅层级设计参考。本轮借鉴“排除不可能的分支”，没有复制其网络架构或引入外部 broker。源码参考版本与本机互操作测试的 Mosquitto 2.1.2 不同。上一轮 Mosquitto 对照数字未作为当前候选的新结果；本轮重新执行成熟客户端互操作。

## 实现与语义保护

上行提示只统计末层为 `up`、`+` 或 `#` 的有效过滤器。其他末层字面量不可能匹配以 `/up` 结尾的 Topic；通配符即使前缀不匹配也计入，允许保守误报，禁止漏报。非零提示仍走原 Trie。计数在 Trie 插入、替换、删除时增量维护，由现有订阅发布点在 Broker Mutex 内以 Release 发布；路由以 Acquire 读取。并发订阅尚未完成时零值可将路由线性化到订阅之前，SUBACK 前已发布新提示。接管、清会话、过期、认证失效、回滚和恢复共享同一维护路径。只增加 Broker/Trie 各一个机器字，无按连接缓存。

仅非 Retained 消息可使用快速路径，原消息合法性和过期检查仍先执行。Retained、实际订阅匹配、QoS2 接纳事务、Will 及容量唤醒仍保留原路径；独立 1 s tick 和 ACK/容量释放继续负责有界维护；快速路径不再顺带执行该次 route 的过期维护，交付前的过期检查仍保留。认证、ACL、Codec、EventAccepted 及 required Sink 责任未绕过。

Retain Handling 2，以及 Retain Handling 1 的已有订阅，在查表/扫描之前直接产生空重放列表。订阅验证、代际检查、配额、订阅更新仍照常执行。Retained 写入仍使用原 HashMap 和原预检；`remove_entry` 保留已有 String 键，过期索引在借用键查询确认 deadline 变化后才构造拥有的键。删除、deadline 改变及过期回收仍执行原索引更新。

新增回归覆盖持锁时无匹配 `/up` 仍完成、Retained 不被该提示跳过、精确和通配符路由、重复订阅/删除、接管、恢复、CleanSession、认证失效，及 Retained deadline 的无→有、相同、修改、有→无、删除、到期回收。独立线性 matcher oracle 和 Broker 全量用量 oracle 均检查新增派生状态。现有 Retain Handling、No Local、RAP、原子接纳回滚测试继续运行。

## 局部性能实测

采用交替 AB/BA：路由和替换各 5 对进程，策略各 3 对进程；每进程 3 次内部重复，先取进程中位数，再对独立进程取中位数。完整内部数据与范围保留，未将 15 次内部重复冒充 15 个独立进程。stats_alloc 仅用于测试二进制，记录 allocation+reallocation 及申请字节；服务器网络组没有该 instrumentation。表中时间是批量均摊耗时，不是网络 P99。

| 场景 | A | B | 变化 |
|---|---:|---:|---:|
| `/up` 无匹配，64 个精确 `/down` 订阅 | 346.283 ns | 78.029 ns | -77.47% |
| `/up` 无匹配，1,024 个精确 `/down` 订阅 | 346.357 ns | 78.177 ns | -77.43% |
| 64 个不匹配的通配符订阅，保守回退 | 267.835 ns | 270.278 ns | +0.91%，范围重叠 |
| 1,024 个不匹配的通配符订阅，保守回退 | 269.394 ns | 271.097 ns | +0.63%，范围重叠 |
| Retain Handling 2，10,000 条 Retained | 40.752 µs | 0.666 µs | -98.36% |
| Retain Handling 1，重复订阅，10,000 条 Retained | 41.214 µs | 0.506 µs | -98.77% |
| 替换 Retained，100 条存量 | 379.793 ns | 343.843 ns | -9.47% |
| 替换 Retained，1,000 条存量 | 379.785 ns | 334.414 ns | -11.95% |
| 替换 Retained，10,000 条存量 | 383.325 ns | 338.957 ns | -11.57% |

路由每次输入含 1 KiB payload，2048 个批次×256 次；对象克隆与合法性检查在计时内，连接和订阅建立在外。64 个下行订阅的进程范围为 A 342.998–351.026 ns、B 76.696–79.709 ns。零订阅对照为 77.115→77.622 ns，已有零订阅路径没有额外可确认收益。路由两侧都为零次堆分配。

Retained 替换为 1024 批次×256 次，固定已存在 Topic、1 KiB payload、无过期值；每次 2 次/78 B 申请降为 0 次/0 B。10,000 条存量的进程范围为 A 375.867–393.134 ns、B 333.444–346.276 ns。稳态存量申请字节相同，不能将减少临时申请解释成相同比例的 RSS 下降。

策略测量为 128 批次×1 次，真实调用 subscribe/preflight/enqueue，含锁计时开销；RH2 每次 5 次/490 B，RH1 重订阅 2 次/2 B，分配未改变。其收益来自不扫描 Retained，而非减少订阅对象。普通允许重放的通配符仍是 O(N)，不声称获得上述倍数改善。

## 撤回实验和无效样本

尝试过以 BTreeMap 作为唯一 Retained 存储，利用固定前缀范围缩小匹配候选，保留 `a/#` 父层与原 topic matcher。它没有额外复制 payload 索引，选择性重放更快，但 1,000 条存量的替换写入出现每次 1 次/1,864 B 节点分配；五对短测的进程中位数为 370.117→482.430 ns。该短测范围有噪声，不将其当作精确稳态退化比例；确定的写入分配代价已足以撤回。最终仍为 HashMap。两版候选 patch、二进制哈希和原始结果均保留于 `ordered-first/`、`ordered-refined/`，其网络 pilot 不算最终性能。

初始 HashMap 键复用探针仅 512 次单操作计时，出现明显调度/频率波动（部分进程两倍以上），没有用它判断最终耗时；改为双方相同的较长批次并完整重跑五对，短样本仍保留。Retained 网络 pilot 初次使用 4,000 个认证设备超过既有默认配额；随后修正到 1,000 个/分租户，另修复 driver 的 RawClient 方法名错误。路由探针初版 `down/#` 超出既有 Topic 深度，改为合法且不匹配的 8 层 `other/.../#`。这些无效样本均不进入性能表，也没有放宽生产限制。

## 完整网关测量

主组 5 对 AB/BA：64 连接仅订阅各自 `/down`，MQTT 3.1.1 / QoS1、明文、目标 30,000/s、payload 参数 1024 B（实际 JSON 含序号，约 1041 B），warmup 3 s、测量 15 s、cooldown 2 s，required 本地 AuditSink，锁计时关闭。

| 主组指标（各轮中位数） | A | B |
|---|---:|---:|
| EventAccepted PUBACK/s | 29,334.800 | 29,245.200 |
| 服务器 CPU µs/接纳事件 | 59.023 | 58.737 |
| PUBACK P50 ms | 0.350 | 0.350 |
| PUBACK P95 ms | 1.010 | 1.010 |
| PUBACK P99 ms | 1.630 | 1.600 |
| RSS 采样峰值 KiB | 9,360.000 | 9,264.000 |
| EventBus 排队均值 µs | 138.441 | 146.342 |
| Sink task 均值 µs | 0.095 | 0.099 |

这些端到端差异没有证明稳定改善。CPU/事件进程范围： A 58.850–59.538 µs； B 57.911–59.097 µs；PUBACK P99 范围： A 1.59–1.71 ms； B 1.56–1.65 ms；范围重叠。目标速率和生成器错过时隙分别记录，不把目标 30k/s 当实测容量。

独立锁计时组为 3 对、10 s。A 的 route 锁获取次数逐轮为 385,382 / 386,572 / 384,486，与全程发布数一致；B 三轮均为 **0**。没有锁样本时平均等待/持有时间没有定义，不写成“0 ns 锁延迟”。A 的 route wait / hold 均值中位数分别为 2796.9 / 1265.6 ns。锁计时本身增加开销，B 也跳过这部分 instrumentation，因此不能把这一组 CPU 降幅直接当作生产收益。

| EventBus 锁计时（均值的进程中位数） | A | B |
|---|---:|---:|
| Publish wait ns | 6348.2 | 6480.0 |
| Publish hold ns | 1652.3 | 1571.3 |

EventBus 仍持有其原 required admission 临界区，未作独立 CPU 占比归因，也未修改队列、ACK、retry、worker 或 spool 语义。

| 控制场景 | 对数 / 测量窗口 | CPU µs/接纳事件 A → B | CPU 范围 A / B | 实发/s A → B |
|---|---|---:|---|---:|
| 实际 `/up` 订阅转发 | 3 / 10 s | 91.969 → 89.818 | 91.257–94.507 / 88.885–93.069 | 29719.10 → 29769.40 |
| required HTTP 2xx，1000/s | 3 / 10 s | 126.164 → 112.308 | 91.546–138.472 / 96.161–114.615 | 999.90 → 1000.10 |
| QoS0，128 B 参数，30k/s | 3 / 20 s | 47.788 → 43.170 | 44.501–49.905 / 42.668–48.499 | 29377.25 → 29432.35 |
| QoS2，10k/s | 3 / 20 s | 144.454 → 143.961 | 140.658–144.658 / 143.106–148.183 | 9999.75 → 9999.55 |
| MQTT5 + TLS + metadata，10k/s | 3 / 20 s | 100.218 → 97.871 | 90.743–103.043 / 97.003–98.922 | 10000.50 → 9999.80 |
| HTTP Sink 延迟 10 ms，200/s | 1 / 10 s | 211.538 → 211.538 | 211.538–211.538 / 211.538–211.538 | 200.00 → 200.00 |

QoS0/2/TLS 首对 10 s 控制组 CPU 分别为 35.605→37.560、107.701→116.717、81.537→90.541 µs/事件。因该偏高结果，追加上表各 3 对 20 s AB/BA；首对升高未在长测中稳定重现。短测与长测分开保存，未丢弃差样本，也未把不同测量窗口合并。控制组用于检查退化，不据少量、范围重叠的样本声称稳定 CPU 改善。

所有健康网络轮次均校验 published = 网关 EventsAccepted = required Sink ACK，QoS1 的 PUBACK、QoS2 的 PUBCOMP 也等于全程 published；上行转发的 subscriber_publishes 同样一致。最终 pending=0，64/64 连接正常退出，无 client error、协议/接纳拒绝、丢弃、retry 或 timeout。`network-count-audit.json` 保存逐轮核对。生成器的 missed slots / window full 保留，不能当作服务器拒绝；带 uplink echo 的 `event_accepted_receipts` 会合计不同回执，本报告用独立 PUBACK/PUBCOMP 和网关接纳指标。

CPU 采样范围是整个负载工具生命周期（含 ramp/warmup/cooldown），分母为同范围网关接纳数。P50/P95/P99 是客户端测量窗口的观测值，对各轮百分位取中位数，不合并样本。AuditSink ACK 不代表外部业务持久化；Webhook 以测试 handler 的配置 2xx 确认。`event_accepted_to_sink_ack_us` 内部按毫秒年龄换算，原值保留，不能解释为精确亚毫秒计时。所有实验均为同机短测，没有搜索饱和容量。

连接内存为两对 AB / BA，10,000 活跃 MQTT 3.1.1 非持久连接，各自订阅、无 PING、按 1000/s ramp、稳定保持 8 s：

| 轮次 | A loaded RSS KiB | B loaded RSS KiB | A / B 每连接 RSS 增量 B |
|---|---:|---:|---:|
| 1 | 152,480 | 155,616 | 13339.9 / 13664.3 |
| 2 | 155,648 | 155,376 | 13669.2 / 13629.8 |

四次均确认 10,000 个活跃连接，10,000/10,000 正常连接/退出，10,006 个运行时任务。RSS 为离散采样，未测内核 socket 内存或精确堆归属，不声明每连接内存下降，也不把配置逻辑预算当成实测内存。

## 验证结果

最终源码 `5bd93b4` 重新构建后的服务器 SHA256 与全部候选网络测量使用的冻结二进制完全一致：`455210f7480a6f8a2bc909b5bd784d42193b6e159739beb0a8f2f6bf670ac6f9`。完整命令和退出码见 [validation.json](validation.json)，所有 18 项验证均 exit 0。

| 检查 | 实际结果 |
|---|---|
| `cargo fmt --all -- --check` | 通过 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 通过 |
| `cargo test --workspace --all-features` | 608 passed、0 failed、59 ignored |
| 性能工具 Python 测试 | 27 passed |
| 原始协议回归 | 7 passed |
| 协议门禁工具测试 | 12 passed |
| `tests/mqtt_conformance/run.py --release-gate --no-build` | 77/77 PASS，125/125 已映射规范要求有当前 PASS 证据 |
| MQTT5 raw smoke / Mosquitto 2.1.2 互操作 | QoS0/1/2、Retained、离线会话、Expiry、Will Delay 等现有用例通过 |
| SDK example 构建 / Mosquitto profile / device profile 测量 | 全部通过 |
| planned restart、TLS + HTTP outage、mixed codec reconnect soak | 各显式执行 60 s，用例均通过 |
| `mqtt_state` fuzz | 30 s 预算，实际 31 s、89,270 runs，无崩溃 |
| `mqtt_recovery` fuzz | 30 s 预算，实际 31 s、291,746 runs，无崩溃 |
| 文档证据预算与补丁空白 | `cargo xtask check evidence`、`git diff --check` 通过；超限摘要已精简，原始数据未改变 |
| 本轮三个 Release 性能探针 | 路由 5 对、Retained 替换 5 对、Retain Handling 3 对，设置环境开关并显式执行 ignored 用例 |
| 健康网络 / 连接内存 | 55 轮网络计数审计、4 次 10k 活跃连接测量通过 |

全测试还覆盖了一次认证后 10,000 次真实 MQTT 发布只调用一次 provider、原子 required fanout 回滚、计数/字节清理、slow-sink 隔离、session generation/operation token、spool 损坏与失败保持存活、修复后的稳定 event_id 重放，以及既有 SIGKILL 三条事件丢失窗口回归。仍不承诺突然断电/崩溃时的内存流量持久性。

59 个 ignored 用例未被当作通过；上表列出的手动性能/soak 用例已单独运行，其余 ignored benchmark 没有逐一执行。本轮没有运行独立 Linux 服务器/外部压测机、饱和容量搜索、6/24 小时 soak、全链路每事件分配归因或新的生产规模 SIGKILL 丢失曲线。

复现当前候选的手动探针：

```bash
NETBAIOT_SECOND_ROUND=1 cargo test --release --locked -p netbaiot-transports --lib uplink_no_match_route_measurement -- --ignored --nocapture --test-threads=1
NETBAIOT_SECOND_ROUND=1 cargo test --release --locked -p netbaiot-transports --lib retained_store_update_measurement -- --ignored --nocapture --test-threads=1
NETBAIOT_SECOND_ROUND=1 cargo test --release --locked -p netbaiot-transports --lib second_round_retained_subscribe -- --ignored --nocapture --test-threads=1
```

A/B 需要分别构建并冻结 manifest 指定的基线及测量 overlay；不能用最终源码重复运行两次冒充修改前后。网络执行参数和交替顺序在本地归档的 `run_campaign.py`、`continue_campaign.py`、`control_repeats.py`、`commands.jsonl` 中，验证由 `validate.py`、`validate_extra.py` 记录。

## 修改文件

源码路径前缀为 `crates/netbaiot-transports/src/mqtt/broker/`：

| 文件 | 内容 |
|---|---|
| `mod.rs`、`subscription.rs`、`maintenance.rs`、`routing.rs` | 保守上行订阅计数、发布提示、路由快速路径；Retain Handling 提前排除查询 |
| `retained.rs` | 复用替换键、避免相同 deadline 的临时键分配 |
| `tests/mod.rs`、`tests/sessions.rs`、`tests/routing.rs`、`tests/expiry.rs` | 独立计数 oracle、匹配/生命周期/无锁和 deadline 回归 |
| `second_round_bench.rs` | 禁止重放策略、替换写入和无匹配路由的串行探针 |
| `docs/performance/README.md`、本目录 | 报告索引、实测摘要、来源和验证清单 |
| `docs/performance/mqtt-publish-20261015/README.md`、`summary.json` | 修正上一轮摘要超限，核对归档确保原计数和汇总数值未改变 |

## 剩余成本与后续测量

普通 Retained 通配符重放仍扫描全表；本轮未保留通用索引。下一步若继续做层级索引，需要同时量化新增、替换、删除、过期、恢复及内存成本，不能只展示选择性查询的读收益。

QoS 的单会话 inflight/expiry 扫描、实际 fanout 的临时状态，以及 EventBus admission/worker 调度仍值得独立归因。此次只确认其源码路径和现有锁指标，没有证明它们各自占多少 CPU。应先在独立 Linux 服务端与负载机上做较长窗口、CPU profile 和饱和搜索，再决定是否引入会话增量计数或改变单 Sink 表示；当前不拆 Broker Mutex、不更换 Tokio、不新增缓存或任务队列。
