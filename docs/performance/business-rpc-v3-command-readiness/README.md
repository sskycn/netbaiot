# Business RPC V3 与 Command 组合验证

本目录保存真实网关、真实 V3 socket 与真实 MQTT 设备的本机 mTLS 测量。正式结论以各次 `summary.json`、`loadgen.json` 和 `final-metrics.txt` 为准。脚本使用测试 CA 和测试证书，不代表公网、跨节点或生产证书环境。

## 复现

```sh
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bins
python3 tools/netbaiot-loadgen/run_business_rpc_v3_command_readiness.py configs/business-rpc-command-readiness/smoke-60s.json --output docs/performance/business-rpc-v3-command-readiness/smoke-60s-mtls-final
python3 tools/netbaiot-loadgen/run_business_rpc_v3_command_readiness.py configs/business-rpc-command-readiness/readiness-30m.json --output docs/performance/business-rpc-v3-command-readiness/readiness-30m-mtls
```

脚本启动一次性本地网关，Provider/Event 身份与 Command 身份使用不同的 mTLS 客户端证书。`command_device_deliveries` 在模拟 MQTT/TCP 设备收到应用 Command 时增加，并分别统计两种 southbound transport；`command_device_acks` 在设备提交应用 ACK 时增加，`command_ack_event_unique` 在业务端按 `event_id` 去重后增加。`loadgen.json` 的 `command_traces` 逐个记录 CommandId、RPC 尝试和结果、设备投递及 ACK。当前客户端 API 不公开内部 RPC request_id，因此结果不声称逐个记录 request_id。RPC 的 Queued 回执不计入设备执行。每十个新 Command 对同一 `command_id` 和同一内容进行一次显式重试。

该负载包含远程设备认证、UDP verifier 查询、telemetry/EventDelivery/EventAck、真实 MQTT/TCP Command/CommandAck、`auth.invalidate` 和 Provider/Event 连接重建。正式 `v3_dual` 配置分别重建 Provider 与 EventSubscription，以观察父流故障隔离；早期 `r4/r5` 合并连接样本在重连时同时重建两者。response-loss RST、跨 HTTP/V2/V3 去重、离线与 takeover 仍需结合单独集成测试判定。

Command 去重只在当前进程的配置保留窗口内生效。默认 `command_ttl_ms=300000` 控制在线会话接受 Command 的有效期，`command_dedup_ttl_ms=300000` 控制成功 Accepted 回执的去重保留期。`CommandService` 在下一次 `send()` 时清理到期项；停止流量后 `Accepted` 项不会因定时后台任务自行归零。重启后 registry 为空，业务消费者须按稳定 `event_id` 幂等处理至少一次 EventDelivery。

结果不得解读为 exactly-once Command 执行，也不得用于宣称当前产品默认协议或 V3 成熟度发生变化。

## 初次运行记录

| 样本 | 结果 | 原因与处理 |
| --- | --- | --- |
| `smoke-60s-mtls` | 未进入负载 | Python 管理指标探针使用了系统代理，loopback 就绪探测超时；探针改为禁用代理。 |
| `smoke-60s-mtls-r2` | 未进入负载 | mTLS principal 仅授权 `demo`，EventSubscription 未指定 tenant filter，被网关拒绝；客户端明确使用 `demo` filter。 |
| `smoke-60s-mtls-r3` | 负载工具收尾失败 | TCP 模拟设备收到连接 EOF 时直接退出，导致原始计数未输出；改为在测试截止前有界重连，并保留网关指标。 |
| `smoke-60s-mtls-r4` | PASS | 59 次真实设备投递，59 次设备应用 ACK，59 个唯一 CommandAck Event；MQTT 55、TCP 4，重复设备投递 0，最终 required pending 0。 |
| `smoke-60s-mtls-r5` | PASS | 加入逐 Command trace 后重跑，59 次设备投递与 59 个唯一 CommandAck Event，重复投递 0。 |
| `smoke-60s-mtls-v3-dual` | PASS | Provider 与 EventSubscription 独立重连，60 次设备投递与 60 个唯一 CommandAck Event，重复投递 0。 |
| `smoke-60s-mtls-final` | PASS | 延长设备观察窗口后，60 条 Command 已接纳且全部到达设备，60 个唯一 CommandAck Event，最终 required pending 0。 |
| `readiness-30m-mtls-startup-failure` | 未进入负载 | 50 台 MQTT 设备共用 loopback IP，超过测试网关默认 `max_connections_per_ip=32`；认证缓存记录了 32 个设备，随后设备连接超时。脚本现在按设备数设置 IP 与租户连接上限。 |
| `readiness-30m-mtls-50-device-preflight` | 15 秒连接预检 | 50 台 MQTT 加一台 TCP 设备成功初始化；16 条 Command 全部到达 MQTT 并产生唯一 CommandAck Event。时长不足以轮询到 TCP 命令，也不是 30 分钟通过结果。 |
| `readiness-30m-mtls-unpaced` | 正确性门禁 PASS，稳态就绪无效 | 1,733 条 Command 各投递一次，5,706 个 EventAccepted 全部 sink ACK，dedup 峰值 297；但未限速认证流量触发 19,048 次入口拒绝、139 次认证超时和 244 次 telemetry publish 失败。保留原始曲线，不用它证明正常稳态。 |
| `readiness-30m-paced-60s-preflight-failure` | 正确性门禁 PASS，入口门禁 FAIL | 每秒 2 次认证后，仍因 50 台设备集中建连触发 5 次入口拒绝；调整一次性测试网关的每 IP 连接与建连速率预算。 |
| `readiness-30m-paced-60s-preflight` | PASS | 每秒 2 次认证，入口拒绝、认证超时与 publish 失败均为 0；60 条 Command 各投递一次（MQTT 59、TCP 1），241 个 EventAccepted 全部 sink ACK。 |

前三次失败是测试脚本/配置问题，没有证据表明网关静默丢失已接受的 required Event。尤其 r3 的网关指标显示 200 个 EventAccepted、200 个 sink ACK，但负载工具因 EOF 未写出完整 Command 明细；不可将其当作通过样本。
