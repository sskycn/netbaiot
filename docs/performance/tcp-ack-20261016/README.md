# 原生 TCP 二进制 EventAccepted ACK v1：实测记录

本次仅替换原生 TCP 成功接纳回执。正文 42 字节、完整帧 46 字节，Version=1、Type=1，UUID 使用原始 16 字节，其余多字节整数均为大端，时间戳为有符号 Unix 毫秒。协议说明见 [设备协议](../../device-protocol.zh-CN.md)。这是不向后兼容的变更，没有协商、JSON 回退或双格式路径，TCP 客户端须同步升级。

## 实现与边界

- `netbaiot-protocol::tcp_ack` 提供独立的固定数组编码和严格解码；不新增依赖、线程、锁或 unsafe。
- `tcp::connection` 在原有 `ingest().await` 成功后，从原始 receipt 编码 `[u8; 46]` 并调用原有带超时的 `write_all`。保留帧大小检查、串行顺序、EventAccepted、背压和连接退出语义。
- JSON 认证请求/结果、业务 Codec、命令下行、MQTT、UDP 的实现不变。`EventAccepted` 公共数据类型仍可用于其他 JSON API；TCP 网络路径已移除 JSON 回执。
- Rust 负载器、Python 示例和四 Codec 实测客户端以及相关测试都使用二进制 ACK。业务 RPC 客户端属于独立协议，不改变其消息格式。

## 测量方法

环境仅为当前 Apple M4、macOS ARM64，同机回环；服务端 10 个 Tokio worker、负载器 4 个 worker。256 字节上行，单租户、每连接独立设备；使用真实必需 AuditSink，经过正常 EventBus 接纳及 sink ACK，不代表真实外部业务系统的容量。生产默认配额没有改动。两版使用同一压测配置：请求/IP/设备/租户速率上限均为 2,000,000/s，以免速率限流遮蔽协议成本；完整覆盖项见 manifest。并发接纳仍为全局 16、租户 4、设备 1，等待槽 16、等待超时 25 ms。下文吞吐不是出厂默认速率限制下的容量。数值来自 Release 可执行文件；[manifest](manifest.json) 包含编译器、源基线及四个冻结二进制的 SHA256。

基线为提交 `0454c76`，其生产代码与 `a2b46a2` 相同。早期原始记录的 `production_revision` 字段对两版均沿用基线默认值（最终 110k 复测已明确标注 working-tree），candidate 的实际未提交代码由 manifest 中的源文件哈希及二进制 SHA256 标识；原始记录未被重写。新旧版本分别配对旧 JSON 客户端和新二进制客户端；协议变化要求匹配客户端。客户端解析开销变化也可能影响同机测量，不能把全部端到端变化归因于服务端编码。

逐级加压每档 20 秒，预热 3 秒、爬升 1 秒；每组串行，无并行编译/测试，无 profiler。通过 `caffeinate -i` 防止休眠。吞吐=测量期收到的成功 ACK 数/指定测量秒数。CPU/事件使用同一对采样点的服务端进程 CPU 增量/服务端 EventAccepted 增量，不用不同窗口的客户端 ACK 数作分母。P99 只覆盖成功 ACK，不包含未发送和未确认消息；RSS 是采样峰值。

稳定判断同时考虑 ACK/attempted、ACK/offered、超时/协议错误、P99 和退出后的资源归零；客户端遗漏计划发送同样记录，不能只看 ACK/attempted。原始配置、每秒采样、日志位于忽略目录 `target/performance/tcp-ack-20261016/` 与 `target/mixed-audit/`；本目录只保存无本机路径/凭据的摘要和原始文件哈希。

复现实测：

```bash
caffeinate -i python3 scripts/perf/device_protocol_benchmark.py \
  --plan target/performance/tcp-ack-20261016/repeated-plan.json \
  --server target/performance/tcp-ack-20261016/bin/server-before \
  --candidate target/performance/tcp-ack-20261016/bin/server-after \
  --loadgen target/performance/tcp-ack-20261016/bin/loadgen-before \
  --candidate-loadgen target/performance/tcp-ack-20261016/bin/loadgen-after \
  --baseline 0454c76 --label tcp-ack-repeat \
  --output target/performance/tcp-ack-20261016/repeated --resume
```

完整 plan 内容包含在 manifest。新的重复执行需更换 plan 的 name，避免覆盖已有运行；`--resume` 仅复用哈希和配置一致的已完成记录。

## 编码分配

独立串行 Release 微基准，每种实现重复 3 轮、每轮 100,000 次，使用现有 `stats_alloc` 测试分配器。输入构造不计入；旧路径精确复现 `serde_json::to_vec` 后加 TCP 长度前缀，仅存在于忽略的测量测试中，不是运行时兼容路径。

| 指标 | 旧 JSON ACK | 二进制 ACK |
|---|---:|---:|
| allocation 调用/ACK（不含 realloc 调用） | 2 | 0 |
| stats_alloc bytes_allocated/ACK | 390 B | 0 B |
| 此输入的完整帧长度 | 134 B | 46 B |
| 编码计时 P99，3 轮相同 | 167 ns | 42 ns |

计时结果呈现约 42 ns 的离散步长，新编码中位计时落入 0 ns 桶，因此不据此宣称纳秒级精确延迟或端到端倍数。分配量属于 ACK 编码，不是整个摄取链路的每事件分配；网络服务器未启用测试分配器。旧 JSON 帧长度随数值位数变化，134 B 只是本次固定输入。

## 32 连接过载证据

[完整逐级摘要](steps-32-connections.json) 保留每秒 10k/30k/60k/90k 的所有新旧结果。默认 `max_ingress_waiters=16`，32 个单租户并发连接在高压时触及原有接纳限制，导致连接关闭、未确认消息和重连。每秒 30k 及以上两版都不能作为稳定容量，未调大生产配额来掩盖这个结果。另用 8 个连接测量 ACK 变更，保持服务端配置相同。

## 8 连接逐级结果

完整结果见 [20k–110k](steps-8-connections.json) 与 [140k/180k](steps-upper.json)。180k/s 档中，旧版实际收到 145,303.8 ACK/s，新版 148,790.1 ACK/s；两版已发送消息均获 ACK，但分别只达到目标发送量的约 80.7% 与 82.7%。这是客户端窗口/调度与服务端共同作用下的饱和观测，不是稳定容量。

## 140k/s 交替重复：每版 3 × 60 秒

按旧/新、新/旧、旧/新顺序执行。下表为三轮中位数；[逐轮摘要](repeated-140k.json) 保留所有计数与采样窗口。

| 指标 | 旧 JSON ACK | 二进制 ACK | 变化 |
|---|---:|---:|---:|
| 成功 ACK/s | 139,880.5 | 139,825.6 | −0.04%，基本持平 |
| 服务端 CPU/事件 | 35.14 µs | 34.30 µs | −2.37% |
| ACK P99 | 2.11 ms | 2.01 ms | −4.74% |
| 采样峰值 RSS 的中位数 | 9,920 KiB | 9,824 KiB | −0.97% |
| ACK/计划发送量，中位数 | 99.915% | 99.875% | 仍有少量客户端未发送 |
| 三轮发送尝试未观察到 ACK 的总数 | 754 | 0 | 旧版第三轮有摄取失败与连接关闭 |

CPU/事件范围：旧版 34.92–35.56 µs，新版 34.24–34.48 µs。P99 范围：旧版 2.05–2.17 ms，新版 1.97–2.12 ms，区间有重叠；不宣称统计显著性。

旧版第三轮测量期有 6 次 remote close/reconnect，ACK/attempted 为 99.991%；其余旧版轮次及全部新版轮次均为 100%。754 是 attempted−accepted 的窗口差，不等同于 754 个事件被网关丢失；部分事件可能已接纳而未观察到 ACK。负载器原始 `unconfirmed` 计数为 381，其按故障发生时刻统计，窗口与 attempted/accepted 归属不同，保留原值，不强行对齐。

所有运行冷却后 event_count/event_bytes/pending_required 与连接数归零，runtime_tasks 回到空闲值，正常退出。不能把旧版这一轮关闭归因于 ACK 编码本身；该轮 `ingress_rejected=6`、`ingress_admission_rejects=0`，现有计数器及 error 级日志不足以定位具体失败分支，不能把它归因为接纳等待超时。桌面调度噪声和既有资源边界仍在。更低负载另做交替复测，避免只展示有利的高压样本。

## 110k/s 较低压力复测：每版 3 × 30 秒

按新/旧、旧/新、新/旧顺序运行，见 [逐轮摘要](repeated-110k.json)。所有轮次 ACK/attempted 均为 100%，没有测量期 disconnect、unconfirmed 或 protocol_error，退出后资源归零。

| 中位数 | 旧 JSON ACK | 二进制 ACK |
|---|---:|---:|
| 成功 ACK/s | 110,000.3 | 109,954.0 |
| 服务端 CPU/事件 | 33.77 µs | 27.44 µs |
| ACK P99 | 1.36 ms | 1.23 ms |
| 采样峰值 RSS | 9,712 KiB | 10,256 KiB |
| ACK/计划发送量 | 100.0002% | 99.9582% |

吞吐基本持平，CPU/事件在这组下降 18.73%，P99 下降 9.56%；但 RSS 中位数**上升 544 KiB（5.60%）**，不能声称整体 RSS 一定降低。CPU/事件范围旧版 33.73–33.96 µs、新版 27.36–27.91 µs；该收益显著不同于前一组 140k 的约 2.4%，说明负载、客户端解析和同机调度对结果影响很大，不能统一归因为 ACK 编码的指令数。

目标发送量是 rate × 秒数；定时和边界调度可能让实际 scheduled 稍高，故 100.0002% 不被截断为 100%。新版三轮仍分别有 2、440、1,543 个 schedule_missed，以及 0、1,025、2,923 个 client_window_full；这些是未发出的计划消息，不是已发送消息被确认。两版在本次短时复测均满足预设 99.5% ACK/offered 门槛。

本机最高通过三轮完整已发送消息确认的测试档位为新版 140k/s；旧版 140k 第三轮有摄取失败，110k 三轮没有。档位之间未做细分、未做长时间异机测试，不能把这些档位当作精确最大可持续容量，也不能把单次高压故障消失宣称为可靠性修复。

## 正确性与限制

- `cargo fmt --all -- --check` 和全 workspace/all-targets/all-features Clippy（`-D warnings`）通过。
- 全 workspace/all-features：613 通过、0 失败、61 忽略。忽略项不是已执行测试；编码微基准另行显式执行。
- 协议覆盖黄金字节向量、signed i64 边界、计数范围、截断/超长/未知版本与类型/旧 JSON 拒绝；Python 有相同向量。
- 真实 TCP 流水线覆盖分片、顺序及 EventId 对应、解析失败和配额失败无成功 ACK。有限双向缓冲区覆盖 ACK 部分写入、慢读端写超时、停止后续接纳、保留已接纳事件和释放连接/读缓冲资源。
- 全套测试包含 TLS 共享监听、四 Codec、认证缓存、required sink 回滚、慢 sink、业务客户端及 subprocess 正常重启/spool 故障/稳定 EventId 回放/SIGKILL 有界丢失语义。
- Python 单元测试 41 通过；更新的 Python 客户端连接真实 Release 服务端，JSON/CBOR/MessagePack/Protobuf 各运行心跳和命令下行，共 8 个用例，均无错误；心跳另验证 required webhook ACK。命令用例验证下行字节，设备执行 ACK 由 Rust 集成测试覆盖。
- TCP 分帧和新增 ACK 解码 fuzz：31 秒，318,422 次，无失败，最大输入 65,540 字节，RSS 上限 2 GiB，单次超时 5 秒。
- macOS `netstat -s -p tcp` 在本次执行中连发送/接收计数都为 0，标记为不可用，不据此声称零重传或零内核丢包。应用层的未确认和拒绝计数独立记录。
- 不宣称生产最大容量、异机网络表现或无限时稳定。未测 Linux、TLS 性能、真实远端 sink 容量或全链路分配；未修改 EventBus、认证、配额和系统调用实现。
