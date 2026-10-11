# 容量与稳定性测试

`netbaiot-loadgen` 是独立 Rust 进程，通过真实 MQTT 3.1.1/5.0、TCP 和签名 UDP
发送四种现有 V1 Codec，不用服务器内部调用代替网络。测试不增加 Codec/传输，
也不改变 MQTT、TCP framing、NBI1/NBA1 或 EventAccepted 语义。

## 运行负载

```bash
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bin netbaiot-server --bin netbaiot-loadgen
python3 scripts/perf/production_readiness.py --devices 100 1000 5000 10000 \
  --scenario sensor telemetry16 telemetry64 --duration 30 --sample-interval 5 --tls \
  --output target/performance/production-readiness/capacity
```

默认扫描四 Codec、四传输；用 `--codec json cbor msgpack protobuf` 和
`--transport mqtt311 mqtt5 tcp udp` 筛选。`sensor` 每台每 10 秒发送
 temperature/humidity/battery；`telemetry16`/`telemetry64` 每台每秒发送 16/64 字段。
`--rate` 设置总发送率，错过的时间槽单独计数。TLS 使用 localhost 测试证书；
UDP 保留 HMAC 和 1200 字节硬限制。

`--fault` 支持 `wrong_hmac`、`wrong_version`、`wrong_codec_version`、`oversize`、
`repeat_sequence`、`reconnect`、`half_connection`。不支持的静态 Codec profile
会在启动前拒绝；这不能作为已经执行异常设备网络负载的证据。

长期运行可使用新的、未占用的输出目录：

```bash
python3 scripts/perf/production_soak.py --devices 1000 \
  --output target/performance/production-readiness/soak
```

控制器冻结二进制并拥有八组有限运行：四 Codec/传输配对各运行 6/24 小时，
每分钟采样。每组有独立网关；八组 1000 台不是单网关 8000 台证据。
Ctrl-C 会停止所属 gateway/load/sink 进程。长期运行使用真实时间。

## 报告与边界

每例保存 `summary.json`、配置、环境与二进制 SHA256、前后 metrics、最终 status，
以及 `resources.jsonl`、`load.jsonl`。原始日志与 profiling 输出放在忽略的
`target/performance/`；Git 只保留精简结果和来源。

分别统计建连、TLS、认证、重连、发送、transport receive、EventAccepted、有效设备
receipt、业务应用发出 ACK、网关观察到 Sink ACK、拒绝/丢弃。设备发送至测试业务 ACK
使用负载进程的单调时钟，不表示业务持久化或执行完成。阶段分位数是直方图 bucket
上界，不能相加得到端到端分位数。

每台设备一个有所有权的任务，最多 10,000 台；业务观察窗口最多 320,000 条并过期
回收。业务 ACK 探针最多 64 条连接，header 8 KiB、body 64 KiB、deadline 5 秒。
任务取消后 join；指标不使用设备 ID 标签，也不记录 payload。

增长检查排除预热并生成候选 `reproducer.json`。候选仍需缩小、确认原因、添加回归、
修复并实际重跑，不能直接称为已证明的最小泄漏复现。`--profiling` 会区分带接纳拒绝的
诊断运行与正常容量 PASS；Linux perf/heaptrack 尚未完成成功验证。

## 当前验证范围

[传感器短测](performance/production-readiness/local-preflight.json)、
[来源](performance/production-readiness/local-manifest.json)和
[停止时的结果](performance/production-readiness/stopped-summary.json)保留实际数据。
macOS ARM64 同机四 Codec、TLS MQTT 3.1.1、1000 台、12 秒预检各完成 1200 条
发送/接纳/Sink 确认。JSON MQTT 5.0/TLS、1000 台、64 字段、35 秒对照完成
34,999 条确认，端到端 P95 0.41 ms、P99 3.23 ms，RSS 峰值 55,040 KiB。
这些短测不代表最大稳定吞吐或生产容量。

6/24 小时运行约 14 分钟后停止；完整 Linux 容量/异常矩阵、专用 NIC/WAN 和长期
泄漏验证未完成。目前不给出 1000/5000/10000 台生产 CPU/内存/网络数值。
部署安全与恢复要求见[运维指南](operations-guide.md)，测量要求见[基准说明](benchmarks.zh-CN.md)。
