# 多 Codec 加固与可靠性审核报告

日期：2026-10-11。基线为实际 `main` HEAD `6e16b31`，开始时工作区干净。
实现提交：`adb9f09`（联合提交、测试和自动化）、`fb37a3c`（有界写入优化）。
补充提交：`2ead168`（报告/实测/CI）、`6a21ee2`（等价 MSRV lint 修正）、
`55ab419`（Linux CPU 精度与交替补测工具）。
本报告覆盖 JSON/CBOR/MessagePack/Protobuf V1，未增加 Codec 或修改 wire 版本。

## 风险结论

| 检查项 | 分类与证据 | 处理 |
| --- | --- | --- |
| 二进制 fuzz 未进入自动化 | 已确认缺陷：原 MQTT Device Profile 工作流没有执行三个新目标，也没有匹配 Codec 修改路径 | 独立工作流按源码、测试、Schema、配置和依赖路径触发，三个 ASan 目标各限时 30 秒 |
| GatewayControl / EventBus 一致性 | 已复现：旧 `Ingress::apply_control_snapshot` 更新控制 revision 2，后续接纳记录仍是 revision 1；HTTP 的预检和第二次路由提交间也允许库级路由写入交错 | 联合提交，失败保留两侧旧状态；真实 HTTP、确定性竞争和故障注入回归 |
| JSON 下行先分配再检查 | 可以证明的资源问题：合法字段的总输出能超过 decoded_bytes，旧实现仍先分配完整 Vec | 有界 `to_writer`，提前停止输出；不是 wire 漏洞修复或崩溃持久性变化 |
| Protobuf 中间复制 | 已测量的性能开销，未确认内存安全缺陷：64 数字参数 71 次分配/5,009 B，64 文本参数 135 次/37,521 B | 保留复制前预算、encoded_len 和 prost 实现；本次不引入手写 Protobuf 编码 |
| 无产品配置的动态 Codec 混用 | 有意保留的 V1 行为；旧英文“一产品一个 Codec”表述不够准确 | 明确“已配置产品”与“未配置产品”的区别；增加不同设备绑定不同 Codec、后续产品冲突及缓存结果重查测试 |
| 二进制 preflight 与解码差异 | 未发现已确认缺陷。结构扫描是语义解析的超集，不应单独作为接纳判定；validate_payload 与 decode 调用同一完整解析函数 | 增加 168 组限额/语义对照、75 组分配矩阵、深度/长度/预算边界和 fuzz 不变量；不作不必要的解析器重构 |
| Codec、缓存失效、重连、恢复时序 | 本次范围内未发现新缺陷；注册重查、现有失效 epoch/generation、MQTT provenance 和生命周期 fence 继续生效 | 新增 profile 添加、admitted 更新与 quiesce 测试；执行完整 MQTT、会话和恢复回归 |

另确认两个维护问题：QoS2 预验证解析错误没有计入 `codec_failures`，已补计；
基线提交的 `docs/performance/multi-codec/evidence/codecs.csv` 被证据门禁判为原始文件，
已转为聚合 `summary.json`。原 CSV 的提交、路径、大小及 SHA-256 保留，能从原提交恢复；
本机副本位于 `target/evidence/multi-codec-baseline/codecs.csv`。未放宽证据门禁。
该问题也出现在基线的 [GitHub Rust checks](https://github.com/sskycn/netbaiot/actions/runs/38105213071)：
测试通过后证据检查拒绝该 CSV，与本机复现一致。
首次本次 CI 的 Rust 1.88 Clippy 又确认基线已有的 `auth.rs` 非最简布尔表达式警告。
将 `!is_some_and(expired)` 改成等价的 `is_none_or(not_expired)`，不改变缓存过期行为、
预算或锁；未禁用 lint。额外执行 1.88 Clippy，并重新执行全部本地门禁。

## 联合提交的正确性边界

Ingress 在生命周期 admission guard 内获取认证注册同步边界，检查在线会话的产品
Codec 冲突，构建完整有界候选控制索引。EventBus 使用原有状态锁排除事件接纳及路由
写入，在锁内编译一次候选路由，再发布 GatewayControl。控制索引发布是线性化点；
其后只有不会失败的路由和 revision 赋值，没有第二次预检、分配或可恢复错误。

控制读取可能先看到新索引，此时 EventBus 仍被锁定，事件只能等到新路由就绪后进入。
不要求两块内存在同一 CPU 指令中赋值，但不存在接纳新状态时使用旧路由的可观察窗口。
已有接受记录继续保留其原 routing revision 和 sink 责任。控制发布失败、未知 sink、
过期 revision 和并发路由写入均不会造成半提交。仅替换路由的候选还携带来源 revision，
避免并发底层控制写入后覆盖新产品状态。

没有给正常 MQTT/TCP 上报增加锁。锁顺序为注册同步边界 → EventBus → 控制索引；
会话冲突检查先完成并释放 Sessions 锁，既有 AuthCache → Sessions → MqttBroker
顺序未反转。没有在这些同步锁内 await。管理 HTTP 的两个更新入口均改用 Ingress。
底层组件独立修改接口继续供初始化/独立库使用；运行中的网关组合必须走联合 API。

## 资源与编码兼容性

JSON 固定向量、字段顺序、Unicode、引号/反斜杠、负零、极小/极大有限浮点及精确
输出边界均通过。公共领域类型、device_v1.proto 字段编号/枚举、TCP 长度前缀及认证
JSON、NBI1/NBA1、MQTT QoS0/1/2、NBSP v3 和 NBMQ v6 未修改。

有界写入器按整段写入或失败处理 `write_all`，避免通用 partial-write 循环；长度使用
checked_add，容量增长保留 try_reserve_exact 和硬上限，失败不改动已有字节。
这是有性能证据的局部优化，没有 unsafe。CBOR/MessagePack 使用同一写入器受益。

75 组分配矩阵分别覆盖 1/2/16/64/65 字段和 0/16/128/256/257 字节文本。
成功的二进制样本累计分配没有超过该矩阵的 decoded_bytes；恶意长度声明在堆分配
前失败。有限深度、累计结构预算、重复键、未知字段、非有限数值及整数边界另有独立
向量和回归。该证据不把输入长度等同于进程内存保证，也不构成任意自定义限额、
任意分配器下的峰值证明。累计分配、返回对象大小、栈、峰值活跃堆和 RSS 是不同量。

新指标只有固定名称：`codec_profile_unsupported`、`codec_profile_mismatch`、
`command_codec_failures`，没有动态 Codec/设备/消息标签。公开单元 CodecError 保持
兼容，语法解析与结构预算拒绝仍归入 CodecFailures；本次没有假称能从该错误区分它们。

## 自动化和可靠性覆盖

新的 Codec CI 包含三个独立 ASan fuzz job，以及真实网关四 Codec 网络 smoke。
Fuzz 输入包含独立合法向量、全部消息类别、64 字段及文本边界、溢出长度、重复信封/
遥测键、未知 Protobuf 字段、深层结构、截断及尾随错误。默认与紧限额下均检查：
单事件、可信身份/接收时间、字段/文本限额、有限 Scalar、合法 execution、完整语义
一致性及两次解码的 source/occurred_at 保持一致、event_id 新生成。原 JSON/MQTT/TCP/
UDP/spool fuzz 目标全部保留。固定种子与复现输入由 CI 保存 14 天，仅使用公开测试数据。

确定性测试覆盖提交失败、旧预检被另一写入超越、发布期间接纳互斥、已接纳 revision
不被重写、路由更新不丢产品、真实管理 HTTP 错误回滚、quiesce 等待已批准操作和拒绝
新操作。多编码慢 Sink 测试填满 16 条必需责任，验证拒绝回滚、控制更新保留旧记录、
释放后 count/bytes 均为零。既有测试覆盖独立慢 Sink、超时/不可用、离线命令、失效
竞态、重放及损坏恢复，均随完整工作区测试运行。

新增 60 秒真实混合 Codec 重连测试经过 241 轮、3,856 条事件，交替 MQTT 3.1.1/5.0
QoS1/2、分片 TCP、认证 UDP及产品/路由 revision 更新；每轮验证队列归零，结束确认
没有活会话残留。发生器限制每秒四轮，证据容器每轮清空。现有计划重启测试运行
64.8 秒，完成强制 spool/recovery 后的多轮健康子进程 drain；均真实执行而非配置证明。

测试开发中的失败均调查并保留本机日志：网络 sandbox 被拒后改用获准回环执行；
Mosquitto 在已安装的 /usr/local/sbin，修正 PATH 后完整门禁通过；连续重连最初触发
默认每 IP 32 次/秒限流，随后过快连接耗尽发生器主机临时端口。明确负载限额并限制
重连速率后通过，未删除/削弱断言。端口耗尽期间同时运行的 CLI demo 超时，在停止
该负载后完整工作区测试通过。开发过程的编译/格式问题亦修复后重跑。
延长命令测量还触发默认 4,096 条去重容量，指标确认有界拒绝；补测显式使用 1 秒
去重保留，前后版本配置一致，容量上限及失败断言保留。一次补测启动发生重叠后立即
终止，原结果留存为 discarded，重新串行测量，不纳入性能结论。

## 实际验证

| 命令 | 最终执行结果 |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo test --workspace --all-features` | PASS：540 个通过，46 个默认忽略，41 个结果套件 |
| `cargo +1.88.0 check --workspace --all-targets --all-features --locked` | PASS |
| `cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings` | PASS：修正 CI 发现的既有等价布尔表达式 |
| `PATH=/usr/local/sbin:$PATH cargo xtask check mqtt` | PASS：原始状态机/协议回归、125/125 normative coverage、MQTT5、Mosquitto、SDK 及 profile measurement |
| `python3 tests/multi_codec_interop.py` | PASS：40 条确认 Webhook，四种 Codec、两个 MQTT 版本、QoS0/1/2和四类载荷 |
| `cargo test --locked -p netbaiot-transports --test multi_codec` | PASS：11 个，另一个 soak 默认忽略并单独执行 |
| `cargo test --locked -p netbaiot-transports --test multi_codec mixed_codec_reconnect_and_control_soak -- --ignored --nocapture` | PASS：60.02 秒 |
| `cargo test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | PASS：64.80 秒 |
| `cargo test --release --locked -p netbaiot-codecs --test allocations -- --nocapture` | PASS：75 组分配矩阵及独立提前拒绝检查 |
| 三个 binary `cargo +nightly fuzz run ... -- -max_total_time=30 -timeout=5 -max_len=65536 -seed=42` | PASS：CBOR 2,216,058、MessagePack 2,665,772、Protobuf 2,328,278 次，各实际 31 秒，无崩溃 |
| 扩展 corpus 后三个 binary 各 `-runs=10000` | PASS |
| JSON、MQTT packet、MQTT5 publish、TCP frame、UDP envelope、restart spool 各 `-runs=10000` | PASS，共 60,000 次 |
| `cargo xtask check evidence` | PASS，既有 CSV 门禁问题已按规则修复 |
| Python py_compile（workspace 内 pycache）与 Git diff 检查 | PASS |
| GitHub Actions `55ab419`：Rust checks、Native recovery、Native developer experience | PASS：包括 stable/MSRV、MQTT 与 Linux x86_64/ARM64 原生质量回归；ARM64 非性能对照 |
| GitHub Actions `38110812037`：三个 Codec fuzz、网络 smoke、Linux 成对性能 | PASS：binary ASan 共 4,170,257 次；完整网络/微基准 artifact 已下载验证 |
| GitHub Actions `38111901825`：交替微基准、正确 CPU 补测及 fuzz/network | PASS：性能回归与测量精度问题已如实记录 |

原始命令输出在 `target/performance/codec-audit/`，聚合与校验值在
[性能证据](performance/codec-audit/README.md)。ASan 在 macOS 有 atos 符号化警告，
未发生 sanitizer 错误；不要将 fuzz 进程的 quarantine RSS 当作单载荷峰值内存。

## 性能与局限

最终实测表和方法见 [性能对照](performance/codec-audit/README.md)。所有数字使用真实
同机旧/新二进制、相同工具及资源配置，原始文件留在 target，聚合提交到 docs。
协议回执只表示 EventAccepted；QoS0 没有回执，QoS2 测完整 PUBCOMP。
Webhook 延迟终点是接收端 flush 2xx 响应，区别于网关观察 ACK及业务最终完成；
命令延迟终点是设备收到字节，区别于执行。

主网络矩阵前后各 324 轮，错误均为 0，结束队列计数/字节及连接清零；场景速率
变化中位数 −0.146%。短矩阵的命令 P99 恶化经 2 秒×3 轮补测未复现相同方向：
旧/新确认 20,552/20,700 条命令，P99 场景变化中位数 −6.35%，仍有个别场景增加。
全部差异公开，不以该短测试声称排除统计回归。
32 客户端、10ms 慢 Webhook 的八路径补测前后各 32 轮，确认 2,972/2,980 条事件，
无客户端错误，各场景结束队列/连接清零；发送器为有界闭环，强制过载另有测试。
JSON 超限累计分配 32,792→400 B（−98.78%），正常 64 数字参数编码耗时 +8.13%，
2 参数为 +25.72%（绝对增加约 64ns）；CBOR/MessagePack 64 参数编码耗时分别
−34.51%/−35.75%。这是下行局部收益与资源安全取舍，不是生产上报容量结论。

Linux x86_64 成对测量已实际执行并成功：4 CPU Azure runner，相同工具和资源配置，
每场景 1 秒×3 次，前后各 324 轮，确认 605,790/608,916 条事件和 13,438/13,435 条
命令，错误均为 0；场景速率变化中位数 +0.473%。初次 Linux 大文本 JSON 编码
存在 +77.53% 时间差，交替顺序重复微基准确认为 +77.12%；首次 ps CPU 精度问题已修正为
procfs tick，旧 CPU 数字明确不作精细结论。原始 artifact 已下载并验证 digest。
本机 macOS ARM64 与 Linux 各自的完整差异在性能报告，不跨平台混比。
Linux 交替补测还观察到 CBOR/MessagePack 文本 +17.28%/+16.98%、未改源码的
Protobuf 文本 +22.33%，数字编码收益不能代表所有负载。已考虑回退，按资源安全
优先级保留有界输出并承担成本；尚未完成无统计分配器的 CPU profiling，不能唯一
定位全部代码生成/运行差异。正确 procfs CPU 补测的全部 108 场景也实际通过，旧/新
确认 38,279/38,488 条事件、899/887 条命令，无错误且队列清零；不作短窗口容量结论。
未实际执行 Linux ARM64 成对性能、跨主机容量、TLS 全矩阵、小时/天级 soak、SIGKILL 损失
campaign、峰值活跃堆 profiling或最大全控制快照高频更新下的吞吐认证。短矩阵、累计
分配及 60 秒测试不能替代这些证据。

JSON 正常编码的有界检查有明确微基准耗时成本，应结合在线命令比例评估；保留
该资源边界符合项目优先级，不能为吞吐退回超限后才检查的实现。Protobuf 复制优化
仍可作为独立测量任务；本次没有以理论零复制替代已验证 prost。必需投递、业务 ACK、
离线命令限制和 at-least-once 均未降级，计划重启 spool 不提供意外断电持久性。

## 修改文件与 Git 交付

- `runtime/control.rs`、`event.rs`、`ingress.rs`、`transports/management_http.rs`：联合提交及并发/错误边界。
- `codecs/json/v1.rs`、`common.rs`：有界 JSON 与已测量的整段写入优化；`binary.rs` 仅增加边界测试。
- `codecs/tests`、`benches/codecs.rs`、`transports/tests/multi_codec.rs`：兼容、资源、控制、慢 Sink、实际重连和性能测试。
- `runtime/metrics.rs`、`commands.rs`：固定错误类别计数，补计 QoS2 预验证错误；`auth.rs` 等价表达式兼容 Rust 1.88 lint。
- `.github/workflows/codec-audit.yml`、`fuzz`：有界 fuzz、共享不变量、可重现 corpus和实际网络 smoke。
- `scripts/perf/codec_campaign.py`：四编码八路径及在线命令的真实网络测量；`scripts/perf/README.md` 记录方法/限制。
- `docs/codecs*.md`、历史多 Codec 报告/聚合、本报告及性能目录：协议行为澄清、审核和实际证据。

Git 按仓库约定将任务分支合并到 main、推送 origin/main 后删除本地任务分支；
最终提交哈希和同步状态见本次交付消息。没有创建或删除其它用户工作树/分支。
