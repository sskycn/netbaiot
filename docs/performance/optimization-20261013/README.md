本轮保留四项局部优化：MQTT 主题鉴权、标识符序列化、EventBus 最后一次成功 ACK 和 Business RPC 事件编码。分配次数减少有可复现的直接证据；相同 Tokio worker 环境下，尚未证明持续网络吞吐或 CPU/事件获得超出噪声的改善。另一个配置实验在本机降低了 CPU 成本，同时增加尾延迟，不能替代代码收益或作为通用默认值。

代码在 `codex/performance-optimization-20261013` 分支，基线为 `cae81a9ffc35e5f19b068014018ed3af27632c82`，被测候选生产代码为 `1e36f1b3498761f2569f5aca65529d4cdd8ba8b1`。后续提交修正测量工具与证据，没有改变生产优化。首次交付提交 `a6a8ca0` 按当时要求仅保留本地分支。用户随后明确授权“提交代码，合并分支并清理”，本次按该授权合并并推送 main；分支名称保留作历史记录。各轮均有独立提交作为回退点；内联 permit 实验已回退。生产改动、资源回滚及测量工具修正均经过独立 Agent 审查，代理没有同时修改同一文件。

受控主测在 Apple M4、10 核、16 GiB、macOS 27.0.1 ARM64、Rust 1.99.0 / LLVM 23.1.1 上执行，使用 release 固定二进制；没有添加 allocator、unsafe、平台专用编译标志或生产 runtime 默认值。服务器与负载工具使用 loopback，同机短测不代表 Linux 或生产容量。发现另一聊天使用相同 checkout 后，本任务迁至独立 worktree，并在用户授权的协调窗口内串行测量，暂停其他本地构建和负载；远端工作继续。未控制 CPU 频率、线程亲和性及系统后台活动。

主测 A/B 各五次，交替顺序，服务器环境值固定 `TOKIO_WORKER_THREADS=10`，负载工具保持固定二进制和自己的四 worker runtime。64 条连接、MQTT 3.1.1 QoS1、1 KiB 载荷、每连接订阅自己的 uplink，目标 30,000/s，预热 5 秒、测量 20 秒、冷却 2 秒，启用 open-loop 审计，关闭 opt-in 锁计时。下表取每轮指标的中位数，不合并百分位样本。

| 指标 | 优化前 A | 优化后 B | 变化 |
|---|---|---|---|
| EventAccepted 回执/s | 29723.500 | 29720.250 | -0.01% |
| Sink ACK/s（全程） | 26461.872 | 26455.322 | -0.02% |
| PUBACK P50 | 0.410 ms | 0.410 ms | +0.00% |
| PUBACK P95 | 0.770 ms | 0.780 ms | +1.30% |
| PUBACK P99 | 1.050 ms | 1.030 ms | -1.90% |
| CPU/事件（全程） | 94.501 µs | 94.296 µs | -0.22% |
| RSS 采样峰值 | 12176.000 KiB | 12096.000 KiB | -0.66% |
| 全链路每事件分配 | 未测量 | 未测量 | 局部路径见下表 |
| 主测 Broker 锁等待/持有 | 未测量 | 未测量 | 主测关闭锁计时 |
| 代码 A/B 公平性 | 诊断结果见说明 | 诊断结果见说明 | 无受控提升结论 |

EventAccepted/s 使用测量窗口的 MQTT PUBACK 回执计数。订阅产生的 `event_accepted_receipts` 在此场景包含发布与订阅两份回执，不能拿它当网关事件数。`sink-mode=none` 实际使用本地 required development-audit Sink；它的 ACK 不代表外部业务持久化。表中的 Sink ACK/s 是全程网关 ACK 计数除以负载生成器实际 elapsed（含建连、预热、冷却），并非同一个 20 秒窗口。CPU 是 `ps` 的累计进程 CPU 时间除以全程网关 EventAccepted，含 setup/预热、不含退出；不能除以测量窗口的事件数。RSS 是离散采样峰值，未包含内核 socket 内存。

五轮 A/B 主测及五轮 B/2-worker 配置测量都保留 scheduled/missed/attempted/published、真实网关接纳、Sink ACK 和错误计数。完整数据见 [summary.json](summary.json)；未提交给网关的 missed slots 不能算成功，固定 offered rate 也不证明最大可持续容量。所有主测的 published、PUBACK、网关接纳、Sink ACK 全程计数一致，退出码正常，最终指标 pending 为零；分配下降没有被转换成虚构的端到端提升百分比。

外部确认另外使用 required webhook：64 连接、1,000/s、5 秒预热、15 秒测量，A/B 各三次交替、同一 worker 环境。所有全程 EventAccepted 均获得配置的 2xx ACK。

| 指标 | 优化前 A | 优化后 B | 变化 |
|---|---|---|---|
| EventAccepted 回执/s | 1000.000 | 1000.000 | +0.00% |
| 外部 2xx Sink ACK/s（全程） | 865.676 | 865.852 | +0.02% |
| PUBACK P99 | 0.250 ms | 0.240 ms | -4.00% |
| CPU/事件 | 115.506 µs | 118.006 µs | +2.16% |

Webhook handler 的 `rejected` 聚合连接级异常也保存在摘要中，不宣称所有 handler 异常为零。其墙钟毫秒直方图止于读完请求体，包含预热且早于写出 2xx；它不能当作精确的设备到业务 ACK P99。真实外部 ACK 计数以网关指标为准。这个固定低速场景同样没有证明网络吞吐或 CPU 改善。

ACL 行以身份三个分段各 1 B 的探针为例，统计 allocation 加 reallocation 调用。身份每段 16 B 时，原实现的 up/down_ack/三次 up 检查为 3/6/9 次，64 B 时为 4/8/12 次，优化后均为零；不将短 ID 的数字泛化到所有设备。

分配探针在隔离、串行 release 运行中使用 `stats_alloc`，将输入构造移出测量区。下表是相应函数/表达式的成本，不是全链路每事件 allocation；不能把各行简单相加。基线探针由 `1a7753f` 的纯测量改动提供，生产行为仍对应基线；原始 CSV 行和重复轮次收录于 summary，详细日志留在忽略目录。

| 优化路径 | 优化前 | 优化后 | 可验证收益 |
|---|---|---|---|
| publish_acl，uplink 一次检查 | 1 allocation | 0 | 删除格式化临时 String |
| publish_acl，down_ack 一次检查 | 2 allocations | 0 | 删除两次 namespace 格式化 |
| 普通 uplink 的三次原有检查 | 3 allocations | 0 | 保留所有原有检查位置 |
| DeviceEvent JSON 长度计数（小包） | 4 allocations / 32 B | 0 / 0 B | 六类 identifier 直接 serialize_str |
| 两 required Sink 的 spool JSON 长度计数 | 8 allocations | 0 | ID 和 map key 均借用序列化 |
| EventBus 冷状态、单 required Sink 最后 ACK | 1 allocation / 232 B | 0 / 0 B | 不构造即将删除的 attempts 节点 |
| RPC 64×128 字符 telemetry 编码，原基线→最终 | 140 allocations + 7 reallocations / 28,950 B | 1 + 7 / 16,384 B | 移除 identifier 临时量及事件深拷贝 |
| RPC 同一最终版本 owned→borrowed 表达式 | 136 allocations + 7 reallocations / 28,918 B | 1 + 7 / 16,384 B | 借用 DTO 单独省 135 allocations / 12,534 B |

RPC 五轮交替 owned/borrowed 探针对 64-field 大事件编码得到约 6.4 µs→3.8 µs 的均值，受 allocator instrumentation 影响，仅代表编码表达式。ACL 的 batch 均摊 ns 百分位不是单事件 P99。EventBus 冷状态探针的 P50 中位数 292→250 ns 同样不代表网络尾延迟。网络完整 allocation 和 allocator 碎片没有测量。

实际实现如下，复杂度主要留在回归测试和测量工具中。

1. `mqtt/topics.rs::publish_acl` 从绑定的认证身份逐段匹配完整 namespace 与分隔符，只接受精确 up/down_ack 后缀。没有从 topic 信任身份，没有删除权限或跨设备检查。与旧 format! 实现对照覆盖前缀、截断、额外层级、Unicode、最大 ID 和权限组合。提交 `9052f85`。
2. `netbaiot-protocol/src/lib.rs` 的 identifier 宏实现 `Serialize::serialize_str(self.as_str())`，保留 checked Deserialize、Schema、公开转换和长度约束。JSON 字节、map key、验证及 public API 一致；没有 wire/version 改动。提交 `6d95f2c`。
3. `event.rs::complete_detailed` 在原互斥锁与完整 operation-token 校验之后，仅在失败或仍有其他 delivery 责任时保存 attempt history。唯一最后成功的 event 随即在同一锁中删除，省去瞬时 BTreeMap 节点。失败重试、peer ACK 历史、stale operation、spool 和 count/byte 清理测试保留。没有改锁顺序、原子接纳、ACK 边界或稳定 event_id。提交 `4ef4aae`。
4. `business_rpc/v3.rs` 用私有 `EventDeliveryRef` 在同步 JSON 编码期间借用原 DeviceEvent；输出仍是拥有数据的 Vec，借用不会跨网络 await。delivery/subscription ID、attempt、字段顺序、流控与 ACK 语义不变。四种事件、可选字段、转义和边界值与 public owned DTO 的精确 JSON/roundtrip 回归通过。提交 `4003fad`。

内联 FrameBudget permit 数组实验虽减少一次分配，却使 BrokerDelivery 从 112 增至 136 B、route target 从 104 增至 136 B，实际分配字节增加约 8 B/target，fanout 微基准约 1%，真实网络无稳定改善。已在 `641b00e` 回退 production 表示，只保留资源回滚/生命周期回归和测量探针。没有为了去除 Mutex 而拆分 Broker；没有在缺乏 CPU 证据时替换 EventBus JoinSet：直接 future 的析构 panic、任务故障隔离和公平性会增加复杂度。没有盲目添加 TCP_NODELAY，既有延迟并不能证明 Nagle 是瓶颈。

worker 配置比较只使用相同候选二进制，分别传入环境值 10 和 2，各五轮，与代码 A/B 分开。1/2/4/10 的短校准先确定 2 值值得重复比较，1 值尾延迟更差。所有环境值只传给服务器，不改变负载与 Sink；工具记录的是传入值，实际 OS 线程数未独立观测。

| 同一候选版本指标 | 环境值 10 | 环境值 2 | 变化 |
|---|---|---|---|
| EventAccepted 回执/s | 29720.250 | 29830.650 | +0.37% |
| CPU/事件 | 94.296 µs | 36.078 µs | -61.74% |
| PUBACK P50 | 0.410 ms | 0.430 ms | +4.88% |
| PUBACK P95 | 0.780 ms | 0.950 ms | +21.79% |
| PUBACK P99 | 1.030 ms | 1.280 ms | +24.27% |
| RSS 采样峰值 | 12096.000 KiB | 11680.000 KiB | -3.44% |

公平性配置复测在一条高频发布连接旁运行 100 条低速连接、各 10/s，三轮交替 worker 环境：环境值 10：低速 P99 中位数 0.14 ms，各轮 ACK [15000, 14999, 14999]；环境值 2：低速 P99 中位数 0.15 ms，各轮 ACK [14999, 14999, 14998]。高频端采用 legacy pacing，实际发送速率在摘要中，不能把名义 20k/s 当作真实 offered。此实验检查配置权衡，不能推导代码 A/B 公平性改善；原先共享环境的代码 A/B 公平性仅作诊断，中位低速 P99 0.14→0.14 ms，保留一次 B=0.36 ms 的异常值。生产默认 worker 数保持原样；本机 CPU 下降不证明 Linux、高连接数或更大吞吐下应使用同一值。

连接内存的三轮诊断测量验证真实 1,000 条连接均建立、在线且退出，无错误，FD 与 owned tasks 各增加 1,000。普通 MQTT RSS 增量/连接中位数 30.704→31.408 KiB，TLS 39.552→39.424 KiB。它们来自共享环境系列，未证明本轮降低连接内存，也不代表内核或离线 persistent-session 总成本。原工具把部分 admission 数除以请求的 1,000，产生错误的低估值；这些结果已明确排除，工具修正为必须满足完整 population 并观察自然退出。仅保持 5 秒，不作为长时 RSS 稳定性证据。

可靠性证据补充了 RPC 大包退出验证。round4→round5、16 KiB、目标 100/s、15 秒负载，各三次交替：六例 consumer ACK 数按文件名顺序为 [1500, 1490, 1500, 1500, 1499, 1500]，收尾均仍有 1 个 required event / 17,236 B。少于目标的生成数没有算作成功。旧工具只等 3 秒便 SIGKILL，无法证明计划退出；现在等待 shutdown deadline+5 秒，六次均约 20 秒后正常退出，EventBus spool 17,452 B、MQTT snapshot 6,943 B，以 0600 文件/0700 目录保留原件和不透明副本，记录 SHA256。成功退出加 snapshot 留存是耐久提交证据，不是独立的 checksum 解码或 replay 测试。计划恢复另由 subprocess soak 验证。

RPC 的 `event_ack_ms` 是 consumer 收到已解码 event 到主动 ack 返回，包含 sleep(0)，不是设备到业务 ACK；`end_to_end_ms` 在该工具测的是 auth。其 rate 字段分母还包括 recovery 窗口。此前 small/large 网络数据受共享环境及旧退出逻辑限制，只作诊断，未据此宣称借用 DTO 提高端到端吞吐。

四 Codec 的网络矩阵基线和最终各 324 项，覆盖 MQTT 3.1.1/5.0 QoS0/1/2、分帧 TCP、签名 UDP、小/大 telemetry、heartbeat 和在线命令，均无操作错误，末次队列为零。100/1,000 设备 MQTT5 TLS 短测通过。附加最终单次 QoS2、MQTT5 metadata+TLS、慢 Sink 10 ms 均完成并清空；高 offered 50k 案例存在大量生成器 missed slots，实际接纳 740,951，不能称为 50k 持续容量或网关过载拒绝验证。outage 案例接纳 10,000，观察到 Sink ACK 2,689、pending 7,311、retry 3；旧 event_load 工具未保存 spool，因此该例不作为恢复通过证据。计数和例外保留于摘要。

mTLS Command 回归原 frozen loadgen 会将合法 dedup progress 误计错误：每第 10 个命令的一次去重重试要求 retry receipt 完全等于原 Queued。静态审查确认与既有 transport-state 契约不符。工具修正为相同 command_id 加 Queued/Dispatching/Sent/Received，仍拒绝 Expired/Failed/错误 ID/RPC 错误，保留交付去重和应用执行 ACK 检查；OutcomeUnknown retry 同样收紧。旧失败保留，使用修正工具的 A/B 功能复测结果见 validation manifest，不用它比较性能。

修正后的 60 秒 mTLS 功能回归，A/B 均实际接纳、交付并确认 60 条命令（MQTT 56、TCP 4），CommandAck 业务事件 ACK 60，无重复或错误；另有 1 次明确 unavailable，未发生其后的交付。双方所有检查均通过，退出及 snapshot 留存正常。该功能验证与 fuzz 编译并行，只用于正确性，不采纳其延迟或 CPU 数字。

必须检查与实际结果见 [validation.json](validation.json)，详细日志均位于 `target/performance/optimization-20261013/validation/`。生产变更的 fmt/clippy/all-features workspace 测试通过；隔离首次全量测试有一项 V3 readiness 10 秒启动超时，完整原失败日志保留，随后全量重跑通过，不隐藏该暂态失败。MQTT 3.1.1 本机 raw 31/31、MQTT5 raw smoke、Mosquitto 2.1.2 客户端 QoS0/1/2/持久会话/retained/Will、TLS 可信 CA+hostname 与错误 CA 拒绝均通过。Mosquitto broker 未安装，差分 reference release-gate 未运行。真实 subprocess restart soak 65.18 秒通过，含 forced spool/recovery pair 和 12 个健康 generation；没有 6/24 小时 soak、生产 outage SLA、多机/WAN、Linux x86_64/ARM64 容量或 SIGKILL 丢失量的新实测。

最终完整验收为 fmt check、全目标/全功能 Clippy `-D warnings`、workspace 全功能测试 558 passed / 0 failed / 48 ignored，Python 测量工具测试 23 passed。忽略的探针和 soak 不因此算作通过，另行执行的项目按上文列出。Fuzz 的独立 lockfile 仅同步过期的 path package 版本和已有 core crypto 依赖，未新增生产 runtime 依赖。

使用 nightly 1.101.0（2026-10-04）、cargo-fuzz 默认 ASan、现有 seed corpus，五个目标分别串行运行 15 秒 engine 预算（实际各 16 秒），全部 exit 0：

| Fuzz 目标 | 实际执行次数 | 最大输入参数 | 结果 |
|---|---:|---:|---|
| mqtt_packet | 152,141 | 65,540 B | 通过 |
| mqtt_v5_packet | 166,644 | 65,540 B | 通过 |
| restart_spool | 135,380 | 1,048,576 B | 通过 |
| business_rpc_v3 | 688,431 | 65,540 B | 通过 |
| json_codec | 1,622,951 | 65,537 B | 通过 |

每个目标使用 `cargo +nightly fuzz run <target> -- -max_total_time=15 -max_len=<上表> -rss_limit_mb=2048 -timeout=5`，设置 `CARGO_NET_OFFLINE=true`、`CARGO_BUILD_JOBS=2`。构建时间不算 engine 时间；目标内部的输入限制仍生效。这是短 smoke，不能替代长期 fuzz、安全审计或内存有界性的完整证明。

主测中 worker 数对 CPU 的影响是已测事实；把它精确归因于 broker Mutex、Tokio 唤醒、syscall 或 macOS 核调度仍需 profiling。原 macOS sample 以 parked threads 为主，不能按所有采样占比给 CPU 热点排名。下一阶段应在独立 Linux 服务端、外部生成器上测实际 CPU flamegraph、锁 wait/hold、持续最大 ACK 速率和更长稳定窗口，再决定是否缩短 route 临界区或做有界调度优化。现有数据不支持大规模锁拆分、codec 重写、allocator 替换或改变可靠性边界；本轮在可重复收益不足时停止继续增加复杂度。

[manifest.json](manifest.json) 记录 source revision、release binary SHA256、工具源码 SHA256、命令与原始证据文件 SHA256；[summary.json](summary.json) 保存各轮数字和微基准原始 CSV 行。清理 worktree 时，全部原始证据及冻结二进制迁移至主工作区忽略的 `target/performance/optimization-20261013-isolated-a6a8ca0/`，迁移后逐项复核字节数与 SHA256。新物理位置见 manifest 的 raw_evidence_root；历史执行命令保留当时的路径。日志、二进制及敏感/不透明 recovery 数据不进入 Git。不要对含 spool 的目录递归执行文本路径替换。所有发布 manifest 使用 <repo> 占位路径，重放需替换为自己的路径，并使用同一 compiler/profile/limits 与冻结 loadgen。

复现主测的单次命令如下。先在两个独立 checkout 分别构建基线与候选，把二进制复制到不可覆盖的比较目录；保持同一 loadgen 和 worker 环境。按 manifest 的顺序串行重复五次，避免构建或其他压测同时运行。

```bash
cargo build --release --locked -p netbaiot-server -p netbaiot-loadgen --bin netbaiot-server --bin netbaiot-loadgen
python3 scripts/perf/event_load.py --server-bin /absolute/path/to/frozen/server --loadgen-bin /absolute/path/to/frozen/loadgen --server-workers 10 --rate 30000 --duration 20 --warmup 5 --connections 64 --sink-mode none --qos 1 --payload-bytes 1024 --subscribe-uplink --audit-open-loop --no-lock-metrics
python3 scripts/perf/event_load.py --server-bin /absolute/path/to/frozen/server --loadgen-bin /absolute/path/to/frozen/loadgen --server-workers 10 --rate 1000 --duration 15 --warmup 5 --connections 64 --sink-mode webhook --qos 1 --audit-open-loop --no-lock-metrics
python3 scripts/perf/mixed_load.py --server-bin /absolute/path/to/frozen/server --loadgen-bin /absolute/path/to/frozen/loadgen --server-workers 2 --scenario route-fairness --duration 15 --warmup 5 --cooldown 2
cargo test --release -p netbaiot-runtime identifier_serialization_hotspot -- --ignored --nocapture
cargo test --release -p netbaiot-runtime engineering_eventbus_accounting_benchmark -- --ignored --nocapture
NETBAIOT_SECOND_ROUND=1 cargo test --release -p netbaiot-transports second_round_publish_acl -- --ignored --nocapture
cargo bench -p netbaiot-transports --bench business_event
```

严格验收与恢复命令：

```bash
cargo fmt --all
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
python3 -m unittest discover -s scripts/perf -p 'test_*.py' -v
NETBAIOT_TEST_SERVER=/absolute/path/to/frozen/server python3 tests/mqtt_conformance/run.py --netbaiot-only --no-build
NETBAIOT_TEST_SERVER=/absolute/path/to/frozen/server python3 tests/mqtt_conformance/v5_smoke.py
python3 tests/run_mosquitto_cli_interop.py
python3 tests/run_mosquitto_tls_interop.py
cargo test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```
