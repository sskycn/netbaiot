# NetbaIoT 生产验证（进行中）

基线 `1458400`。本阶段不增加 Codec/传输，不修改 V1 字段编号、MQTT 状态机、TCP framing、NBI1/NBA1 或 EventAccepted。
生产资格必须由已完成的实际负载、故障和长期运行证据决定。配置允许 10,000 台不等于已经证明可长期服务 10,000 台。

## 可重复的独立测试系统

`netbaiot-loadgen` 是独立进程；生产模式使用四种固定 Codec 的独立编码器、真实 MQTT 3.1.1/5.0、TCP 和签名 UDP socket。
不使用服务器内部函数代替网络接入。每台设备一个有所有权的任务，最多 10,000 台、每台最多 32 个 inflight、总业务观察窗口最多 320,000 条且过期回收。
业务 ACK 探针最多 64 条连接，HTTP header 8 KiB、body 64 KiB、读写 deadline 5 秒；全部任务在结束时取消并 join。
统计使用共享、有界直方图，不保存每条消息的原始延迟数组，不记录设备 payload。

```bash
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bin netbaiot-server --bin netbaiot-loadgen
python3 scripts/perf/production_readiness.py --devices 100 1000 5000 10000 \
  --scenario sensor telemetry16 telemetry64 --duration 30 --sample-interval 5 --tls \
  --output target/performance/production-readiness/capacity
```

默认扫描四个 Codec 和四个传输。`--codec json cbor msgpack protobuf`、`--transport mqtt311 mqtt5 tcp udp` 可筛选。
`sensor` 每台每 10 秒发送 temperature/humidity/battery；高频模型每台每秒发送 16/64 字段。
高频字段 f0–f63 使用相同的 0–63 数值。Protobuf 使用已有 unsigned_integer 字段，传感器小数使用已有 double 字段；四种格式解码后的语义经过对照测试。
64 字段合法负载连同最长测试 source ID、NBI1 envelope 必须保持在原有 UDP 1200 字节上限以内。
`--rate` 指定总发送率；丢失的计划时间槽单独计数，不能当作完整提供了目标负载。
测试 TLS 使用仓库 localhost fixture，仅供测试。UDP 使用原有 HMAC，与 TLS 分开记录。

每个 case 保存 `summary.json`、环境/二进制 SHA256、服务器/负载配置、前后 metrics、最终 status、每分钟 `resources.jsonl` 和有界统计 `load.jsonl`。
原始证据保存在忽略的 `target/performance/production-readiness/` 或 Actions artifact，Git 只保留摘要和 manifest。
不同机器的 CPU 型号/核数、线程数和负载进程共享情况必须一起报告，不能把主机差异归因于架构本身。

## ACK 与耗时口径

- `submitted`：实际写出的上报；`transport_received`：服务端 transport 接收计数；`event_accepted`：服务端 EventBus 已接纳计数。
- `event_accepted_receipts`：客户端有效 PUBACK/PUBCOMP、结构化 TCP receipt 或 HMAC/版本/boot/sequence 均有效的 NBA1。
- `business_acks_emitted`：测试业务应用发送配置的 204；`sink_confirmed`：网关实际观察到的 Sink ACK。
- `device_send_to_business_ack`：同一负载进程单调时钟，从设备 write 开始到业务应用发送 ACK；该应用不提供业务持久化或执行证明。
- 连接阶段分别记录 TCP connect、TLS handshake、认证 round trip、重连次数；MQTT/TCP 正常上报不再次调用认证服务。
- 服务端分别记录 transport validation、admission、codec decode/normalize、Ingress 总耗时、EventBus eligible queue wait、Sink attempt 至业务 ACK、EventAccepted 至 Sink ACK。
- 每个服务端直方图给 P50/P95/P99 的 bucket **上界**，溢出返回 null；旧 EventAccepted 至 Sink ACK 指标使用毫秒时间戳，不能宣称微秒精度。
- 端到端分位数不能通过相加各阶段的分位数得到；Codec microbenchmark 与整个系统 E2E 数据分别报告。

PASS 要求：目标连接全部建立、发送/接纳/Sink ACK 对账、无客户端错误/计划缺失/窗口丢弃/强制任务终止、结束时 session 和 required queue 归零。
同机 loopback、短期 PASS 只证明对应窗口；不代表隔离服务器的稳定生产吞吐或网络容量。

## 异常与长期运行

```bash
for fault in wrong_hmac wrong_version oversize repeat_sequence reconnect half_connection; do
  python3 scripts/perf/production_readiness.py --devices 100 --scenario telemetry16 \
    --duration 60 --sample-interval 1 --fault "$fault" \
    --output "target/performance/production-readiness/fault-$fault"
done
python3 scripts/perf/production_readiness.py --devices 1000 --codec json --transport mqtt311 \
  --scenario telemetry16 --duration 21600 --sample-interval 60 --tls \
  --output target/performance/production-readiness/soak6h
python3 scripts/perf/production_readiness.py --devices 1000 --codec json --transport mqtt311 \
  --scenario sensor --duration 86400 --sample-interval 60 --tls \
  --output target/performance/production-readiness/soak24h
```

错误 `schema_version` 在已认证数据路径验证；`wrong_codec_version` 配置 unsupported 静态认证 profile V2，预期在监听前由完整启动校验拒绝，不宣称进行了网络压力测试。
UDP 重复 sequence 的已接纳副本可以再次获得合法 NBA1，但不得再次提交新的业务事件。
半连接/重连/无效负载的成功标准是拒绝语义与最终资源清理，短期测试不能证明没有长期泄漏。

每分钟记录 RSS、CPU、FD、连接、session、Tokio task、EventBus count/bytes、Sink count/bytes。
增长分析排除前 60 秒，比较稳定期首尾窗口；超过阈值自动保存 `reproducer.json` 候选。RSS 的分配器保留容量不能直接称为泄漏。
确认增长后必须缩减设备数/输入/持续时间并复现，添加针对真实原因的回归测试，修复后重新执行完整长期运行；候选不冒称已证实的最小复现。

## 运维指标与 profiling

新增固定、无身份 label 的指标：`netbaiot_connections_total`、`netbaiot_active_sessions`、`netbaiot_ingress_total`、`netbaiot_ingress_error_total`、
`netbaiot_codec_decode_total`、`netbaiot_codec_error_total`、`netbaiot_codec_latency_us_*`、`netbaiot_eventbus_pending`、`netbaiot_eventbus_pending_bytes`、
`netbaiot_eventbus_rejected_total`、`netbaiot_sink_pending`、`netbaiot_sink_failed_total`、`netbaiot_runtime_tasks`。
保留旧指标兼容现有监控。active_sessions/eventbus/sink/task 是 scrape 时诊断，不是新热路径锁。
Codec 错误包括现有 validate/decode 失败；sink_failed 保留 required delivery 失败口径，所有 attempt 错误按已有固定 reason 指标查看。
设备、事件、命令、client ID 与 payload 均不成为 label。

原生 Linux CI 分别真实执行 x86_64/ARM64 TLS 负载、微基准、异常输入、perf 栈/flamegraph 和 heaptrack 分配画像。
Profiling 使用独立 case，避免把 profiler 开销混进普通容量数据。仅对测试凭据与合成设备进行分析。
`--profiling` 允许记录与 ingress 拒绝完全对账、所有已接纳工作已 ACK 的 `profiled_with_rejections`，该状态永远不是容量 PASS；遗漏 required ACK、未归零队列、未知丢失仍失败。
CPU/heap 热点得到证据后再决定减少复制/分配或锁竞争；没有证据不进行猜测性热路径修改。禁止 unsafe、安全检查删减、协议变化。
工具方法见 [Inferno](https://github.com/jonhoo/inferno) 和 [heaptrack](https://github.com/KDE/heaptrack)。
tokio-console 需要专门 runtime instrumentation，当前使用稳定 runtime task 计数；未执行 console 检查时明确标记。

## 生产结论与部署规格

最终报告必须填充以下实测项目，尚未完成的运行保持“进行中/未验证”，不能以配置值或历史数据库时代报告补齐：

| 项目 | 当前结论 |
| --- | --- |
| 最大长期稳定设备数量 | 等待容量与完整 soak 证据 |
| 最大稳定吞吐、P95/P99、CPU/RSS | 等待完整容量 case，不使用短 smoke 推算 |
| Linux x86_64 与 ARM64 | 等待本阶段原生真实负载 artifact |
| 6 小时/24 小时 | 等待真实完整运行，不能加速时钟代替 |
| 1000/5000/10000 部署 CPU/内存/网络规格 | 等待对应平台及负载的实测，暂不提供估算 |
| 未覆盖风险 | 同机测量、真实 WAN/NIC、外部认证/业务 ACK 服务、真实设备差异、长期开机与断电恢复 |

已完成的本阶段预检：[本机摘要](performance/production-readiness/local-preflight.json)、[来源 manifest](performance/production-readiness/local-manifest.json)。
macOS ARM64 同机 TLS MQTT 3.1.1、每例 1000 台传感器、12 秒：四 Codec 均 1200 条发送/接纳/业务确认，连接成功率 100%，最终 session/required queue 为零。
设备发送至业务 ACK P95 0.33–0.35 ms，P99 0.53–0.71 ms；这些是短期 loopback 预检，不能推导 Linux 部署规格。
本阶段完整 workspace 542 项通过、46 项既有 ignored；另外实际执行 TLS outage 60.67 秒、混合 Codec 重连 60.02 秒、子进程重启 64.36 秒，均通过。
6/24 小时正在本机用冻结二进制执行：四 Codec/传输配对，每台独立网关 1000 设备；逐分钟采样。八台同机测试网关不是单网关 8000 台容量证据。
Linux x86_64/ARM64 的完整真实矩阵见 [Actions](https://github.com/sskycn/netbaiot/actions/runs/38113863601)，运行尚未完成时不写成 PASS。

部署时按 [运维指南](operations-guide.md) 配置独立管理授权、验证服务器 TLS 证书、限制暴露端口与管理网，设置有界 count/byte、ACK/retry/timeout 与恢复 spool。
FD 的实际峰值和服务启动 `RLIMIT_NOFILE` 必须一起验证；工具只尝试调整自有进程的 soft limit，不修改系统参数。
不提供未经实际 NIC/专用服务器测量的 sysctl 调优数字。应用以 event_id 幂等处理可能重复的投递与恢复。
计划重启 drain/spool 成功才可退出；突然崩溃可能丢失尚未 spooled 的有界内存流量。生产容量建议必须同时满足可靠性约束。
