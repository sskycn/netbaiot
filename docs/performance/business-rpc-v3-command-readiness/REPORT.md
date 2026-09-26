# Business RPC V3 + Command 组合验证报告

## 基线、范围与判定

起始 `main` 为 `343df9b3bcbff0e29db1e822812b5062b41a9f17`，高于任务文本中提到的 `58d49aa` 一个 v0.2.2 发布提交。正式 30 分钟样本在干净的 `codex/v3-command-readiness` 提交 `270979dea68dd7759099bc2542687e9dda1de8e6` 上运行。原始 [正式汇总](readiness-30m-mtls-final/summary.json)、[逐命令和时间序列](readiness-30m-mtls-final/loadgen.json)、[网关指标](readiness-30m-mtls-final/final-metrics.txt) 与 [配置](readiness-30m-mtls-final/gateway-config.json) 均保留。随后新增的短 TTL 与计划重启代码只在测试文件中，不改变运行时或该长测所用产品代码。

环境为 macOS 26.6.2 / arm64 / Apple M4 / 16 GiB RAM、Rust 1.97.1、release 构建、本机 loopback、测试 CA 的双向 TLS。V3 配置是原默认的 8 KiB DATA payload、256 KiB 流窗口、4 MiB 连接窗口、256 并发流，`v3_send_ahead=null`。一次性测试网关把共用 loopback IP 的连接及建连速率预算设为 96，正常认证新连接按 2/s 限速；默认 Command 有效期和去重保留期各 300,000 ms，去重容量 4,096。该负载是稳定性验证，不是容量上限测量。

| 项目 | 判定 | 证据边界 |
| --- | --- | --- |
| 已覆盖的功能正确性 | PASS | 真实 MQTT/TCP Command、CommandAck、跨入口去重、短 TTL、V3 response loss、计划重启与 Event 回放测试通过。 |
| 60 秒本机 mTLS mixed smoke | PASS | [正式 60 秒样本](smoke-60s-mtls-final/summary.json)；60 条接纳 Command 各投递一次，required Event 全部 ACK。 |
| 30 分钟本机 mTLS mixed soak | PASS | [正式分类样本](readiness-30m-mtls-final/summary.json) 的所有门禁通过；16 次 telemetry 本地入队 `Offline` 均在主动重连后 0–8 ms 内发生，均未跨越 EventAccepted。 |
| 真实公网 / Linux 丢包 | NOT RUN | 无相应隔离网络环境证据。 |
| 跨部署生产证据 | PARTIAL | 本机 30 分钟结果不能替代公网、长时、故障网络、多节点验证。 |

## 正确性覆盖

| 场景 | 结果与证据 |
| --- | --- |
| 真实 MQTT/TCP Command、设备应用 ACK、CommandAck Event | 60 秒与 30 分钟 mTLS 样本均逐 CommandId 对账；TCP 和 MQTT 分别计数。 |
| MQTT 未订阅 `/down`、离线后同 ID 重试、旧会话淘汰 | `v3_command_real_mqtt_dedup_http_ack_and_lost_response`；旧 socket 关闭，只有当前可投递会话收件。 |
| V3 接纳后丢 RPC 响应 | 同一测试用真实 V3 socket 发送并等待设备收件，然后 RESET/关闭原连接，另一 V3 客户端以同 CommandId 重试；设备无第二次收件。SDK 内部 request_id 未向测试 API 暴露，不能声称逐个记录。 |
| HTTP↔V3、V2↔V3 去重 | V3 MQTT 与 V3 TCP 子进程测试；V2 和 HTTP 都使用同一 `Arc<CommandService>`。V3 TCP 测试覆盖 V2→V3 与 V3→V2。 |
| 相同 ID 不同内容冲突、并发重复、容量满后命中 | V3 MQTT 子进程测试、V3 TCP 容量测试、`command_service_*` 回归。常规长测没有注入冲突。 |
| RESET 前、短 deadline 未收完 body、接纳后 RESET | V3 原始 socket 回归；接纳后责任仍在且同 ID 重试不再次下发。 |
| Drain 后新命令拒绝、已有 Accepted receipt 可查 | `command_service_retries_conflicts_and_drain_share_one_dispatch`。 |
| 短 TTL 真实设备语义 | 新增 `v3_command_real_tcp_short_dedup_ttl_and_shorter_command_ttl`：测试专用 Command TTL 100 ms、dedup TTL 500 ms；200 ms 后同 ID 返回原回执，窗口结束后可重新下发。默认 300 秒的回收由长测时间线间接验证。 |
| 惰性过期顺序和容量恢复 | `command_dedup_expiry_is_ordered_and_lazily_reclaimed` 与容量回归；`Accepted` 只在后续 `send()` 中被物理清理，无后台定时清零承诺。 |
| Event NACK 重试身份 | `real_socket_v3_provider_event_and_invalidation`：同一 `event_id`，不同 `delivery_id`。 |
| 计划重启、Event spool、Command dedup 清空 | 新增 `v3_planned_restart_replays_event_but_resets_command_dedup`：未 ACK Event 以同 `event_id` 回放；新进程 dedup 为 0，同 CommandId 可再次投递。另运行忽略的 60 秒多代子进程重启 soak。 |
| mTLS 证书映射、主机名和租户限制 | 正式混合样本使用 Provider/Event 与 Commands 两个测试证书；V3 TCP mTLS 测试验证其他租户 `Forbidden`、V3 mTLS 测试验证错误主机名拒绝。 |
| auth.invalidate、Provider/Event 重连 | 长测 89 次成功 invalidation、29 次 Provider 与 39 次 EventSubscription 主连接重建；30 次 Provider sync 成功、0 次失败。invalidation 针对 UDP verifier 身份，未在同一活跃 Command 设备上证明授权切换竞态。 |

## 60 秒与 30 分钟结果

| 计数 | 60 秒正式 mTLS smoke | 30 分钟正式 mTLS soak |
| --- | ---: | ---: |
| Command RPC 请求 / 新 CommandId | 67 / 61 | 1,981 / 1,801 |
| 新 Command 接纳 / 显式 Unavailable | 60 / 1 | 1,795 / 6 |
| MQTT / TCP 设备收件 | 56 / 4 | 1,760 / 35 |
| 设备应用 ACK / 唯一 CommandAck Event / 业务 ACK | 60 / 60 / 60 | 1,795 / 1,795 / 1,795 |
| 设备重复收件 / Unavailable 后隐式投递 | 0 / 0 | 0 / 0 |
| telemetry 成功本地入队 / 显式入队失败 | 119 / 1 | 3,584 / 16 |
| EventAccepted / sink ACK / sink retry | 208 / 208 / 0 | 7,176 / 7,176 / 37 |
| 认证请求成功 / 超时 | 587 / 1 | 3,581 / 0 |
| 入口拒绝 | 60 秒样本未作为零拒绝门禁 | 0 |
| loadgen Provider+Event 重连 / invalidation | 2 / 2 | 68 / 89 |

30 分钟中 16 次 telemetry 失败均为设备 SDK 明确返回 `Offline`；原始 `publish_failures` 和 `fault_markers` 表明每次距最近一次 Provider 或 EventSubscription 重连 0–8 ms。`publisher.publish()` 只做设备 SDK 本地有界入队，失败时未形成 EventAccepted。网关 `events_rejected_total=0`，已接纳的 7,176 个 Event 与 7,176 次 sink ACK 完全对齐；37 次 sink retry 是连接故障下的显式重试。UDP verifier 发送 1,797 次，收到 1,793 次 NBA1 receipt；相应 EventAccepted 仍包含全部 1,797 个，不能把 UDP receipt 接收率说成 100%。

## Dedup 与资源时间线

新接纳 Command 速率为 1,795/1,800≈0.997/s，按 300 秒 retention 估算稳态约 299 条。每 5 秒采样的稳态中位数 299、峰值 301，网关累计淘汰 1,496、命中 180、冲突 0、去重容量拒绝 0，最终 `InFlight=0`、`Accepted=299≤4096`。停止流量后不会定时清理这 299 条；下一次 `CommandService::send()` 才清理已过期项。Command TTL 限制新下发的有效时间，dedup TTL 保留已接纳回执，两个配置语义独立。

| 资源 | 基线 / warm | 负载峰值 | 稳态中位数 | 收尾 |
| --- | ---: | ---: | ---: | ---: |
| 网关 RSS | 7,472 / 10,144 KiB | 11,584 KiB | 11,488 KiB | 11,200 KiB |
| 网关采样 CPU | 0.6% / 0.0% | 0.7% | 0.2% | 0.0% |
| V3 活跃连接 | 3 / 3 | 3 | 3 | 0 |
| V3 活跃流 | 2 / 2 | 网关内部峰值 8 | 2 | 0 |
| V3 queued bytes | 0 / 0 | 网关内部峰值 1,746 | 0 | 0 |
| V3 reassembly bytes | 0 / 0 | 网关内部峰值 875 | 0 | 0 |
| RPC pending items / bytes | 0 / 0 | 1 / 197 | 0 / 0 | 0 / 0 |
| dedup entries / InFlight | 0 / 0 | 301 / 采样 0 | 299 / 0 | 299 / 0 |

RSS 在 360–900 秒、900–1,440 秒、1,440–1,800 秒三段的中位数分别为 11,360、11,504、11,552 KiB；该范围内增量 192 KiB，未观察到随 68 次重连或 1,795 条命令线性增长。CPU 是 macOS `ps` 采样值，不代表跨平台 CPU 成本或容量。内部峰值计数能捕获 5 秒时间线漏掉的短暂占用。最终 V3 连接/流、控制/Event/Command 队列、重组保留、RPC pending、EventBus pending required 均为 0。

## 失败样本与生产边界

原始失败结果没有删除：[未限速 30 分钟](readiness-30m-mtls-unpaced/summary.json) 的正确性门禁通过，但认证流量触发 19,048 次入口拒绝、139 次认证超时、244 次 telemetry 入队失败，不能用作正常稳态证据。[限速但零 publish 错误门禁失败的 30 分钟样本](readiness-30m-mtls-paced-publish-failure/summary.json) 已无入口拒绝或认证超时，但 15 次显式入队失败当时未分类。随后 [20 秒故障风暴](fault-storm-20s-mtls/summary.json) 和正式分类长测表明观测到的此类失败为主动重连瞬间的本地 `Offline`，已接纳 Event 的责任保持完整。更早的启动/60 秒失败和预检也在 [README](README.md) 中逐项保留。

本轮未运行公网、Linux `tc netem` 真实丢包、1 小时、6 小时、多节点、生产证书、长时故障风暴或 V3 principal 过期后的混合恢复。没有在正式 30 分钟场景中执行整机计划重启、MQTT session takeover、同设备授权版本变更、revision gap、Command response-loss 注入或随机 RESET；这些由定向测试覆盖其中一部分，不能表述为长测已覆盖。客户端 API 不暴露逐次 RPC request_id/stream_id，因此逐 Command trace 保留 CommandId、尝试结果、设备收件和 ACK，但不能宣称逐个验证内部 request_id/stream_id 更换。没有测 Command 各阶段完整延迟拆解、dedup 命中延迟或 lock wait，也没有可靠的公网性能对照。

结论限于进程内、配置保留窗口内、同租户同 CommandId 同语义内容的重复下发保护。重启、TTL 到期或不同节点均可能再次执行同 ID；设备和业务消费者仍须幂等处理。Gateway 不保存离线命令、Command 去重历史或业务状态；V3 默认协议与实验成熟度均未改变。

## 验证命令

以下均已通过，长测原始输出保存在本目录。完整 workspace 测试在本机需要允许绑定 loopback TCP/UDP listener。

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
python3 tests/mqtt_conformance/run.py --netbaiot-only  # 31/31
cargo +nightly fuzz run business_rpc_v3 -- -runs=10000
cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored  # 64.11 s, PASS
```

构建时使用 `CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0` 限制本机磁盘占用。nightly fuzz 的 10,000 次运行完成；nightly 编译器对现有 `AtomicUsize::fetch_update` 发出弃用警告，并有 macOS 符号化警告，均非本轮失败或产品代码变更。没有运行 1 小时配置、6 小时 soak、公网或 Linux 网络损伤注入。
