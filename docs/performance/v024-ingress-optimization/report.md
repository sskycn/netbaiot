# v0.2.4 接入与恢复优化：测量记录

## 范围与环境

- 原始生产代码：`f30d7c06a97d5cb851e3e30530378c86082efcf9`；优化代码：`6dcadf2`。对比使用分别由这两个版本构建的 release server 和同一份原始版本 release loadgen。
- Apple M4、10 核、16 GiB，macOS 27.0.1 arm64；编译使用 Rust/Cargo 1.99.0，另以 Rust 1.88.0 执行全特性编译检查。网关与负载生成器同机，经 loopback；TLS 关闭；无外部业务服务，采用即时确认的本地必需 Sink。
- [成对计划](plan.json)：每协议两个 15 秒测量窗口，3 秒预热、1 秒 ramp；固定 4 个 loadgen Tokio worker。MQTT/TCP 分别提供 20,000 次/秒、32 个设备；UDP 提供 40,000 次/秒、32 个设备。按 AB/BA 顺序串行执行，无编译或其他压测并行。下表取两次中位数。CPU 单位为单核用量，RSS 为采样峰值 KiB。成功率为成功接纳回执/实际尝试，P99 只统计成功回执。
- 记录的是这台主机上的短时负载表现，不是生产容量或独立主机压测。指标来自 `device_protocol_benchmark.py` 的原始 JSON，位于本机被忽略的 `target/performance/v024-ingress-optimization/`；没有把原始日志和绝对路径提交到仓库。

| 场景 | 接纳/秒：原始 → 优化 | 成功率：原始 → 优化 | 回执 P99 ms：原始 → 优化 | 网关 CPU 核：原始 → 优化 | 峰值 RSS KiB：原始 → 优化 |
| --- | ---: | ---: | ---: | ---: | ---: |
| MQTT QoS1 | 19,977.7 → 19,982.6 | 99.894% → 99.914% | 1.040 → 0.995 | 1.137 → 1.123 | 9,712 → 9,568 |
| TCP | 19,961.2 → 19,951.1 | 99.806% → 99.749% | 1.000 → 0.865 | 1.027 → 0.977 | 9,392 → 9,376 |
| UDP，最终调度 | 39,997.6 → 39,996.8 | 100% → 99.998% | 1.050 → 1.040 | 0.717 → 0.948 | 7,896 → 8,216 |

UDP 第一版让每个报文都进入有界任务集，测得 39,719.9 次接纳/秒、99.298% 成功率、P99 1.275 ms、1.413 核；因此没有保留。最终版在接收任务中先轮询一次，只将真正挂起的鉴权或接纳交给有界任务集。最终 UDP 两次测量窗口分别有 26 和 0 次未收到回执；包含预热与冷却的全程新增过载丢弃计数分别为 26 和 32。CPU 中位数仍较原始版高约 32%，是慢鉴权并发隔离的实际代价，不能据此宣称 UDP 最高吞吐提高。慢鉴权设备与其他设备并行、同设备重复包只接纳一次、失效凭据与重放序列的行为由 UDP 回归测试验证。

## MQTT QoS 与 EventBus

另用 `event_load.py` 在同机、无 TLS、32 连接、5,000 发布/秒、12 秒测量窗口、单次成对运行 QoS1/QoS2；两个版本都使用同一原始 loadgen，并启用现有 EventBus 锁指标。

| 场景 | 原始完成/发布、回执 P99 | 优化完成/发布、回执 P99 |
| --- | ---: | ---: |
| MQTT QoS1 | 59,979/59,979，1.04 ms | 59,995/59,995，0.43 ms |
| MQTT QoS2 | 60,000/60,000，0.73 ms | 59,995/59,995，0.85 ms |

四组均无断连时待确认报文，loadgen/server 正常退出。每个 QoS 只有一个短窗口，不能从尾延迟差异推断稳定收益。通用 MQTT 计划使用负载生成器默认的 QoS1。

QoS0 另以相同的 32 连接、5,000 次/秒、12 秒窗口各测一次：原始版发送 59,995 条且网关 `ingress_accepted` 为 59,995；优化版发送 59,991 条且网关接纳 59,991。采样 CPU 中位数为 29.4% → 33.9%，峰值 RSS 为 8,848 → 8,864 KiB。QoS0 没有协议接纳回执，所以设备到 EventAccepted 的 P50/P95/P99 **未测量**；不能把 socket 写入延迟充当接纳延迟。

`eventbus_probe` 对原始版与优化版均运行了 1/4/8 Sink、1/10 ms 慢 Sink、重试和过载隔离场景。即时单 Sink 10,000 事件的完成速率为 234,589 → 163,478 次/秒，4 Sink 为 75,846 → 77,786，8 Sink 为 29,877 → 36,491；1/10 ms 慢 Sink 分别为 3,505 → 3,552 和 673 → 672。过载场景两版均只接纳 1,024、拒绝 1,024，并将慢 Sink 积压限制在 1,024。短探针结果随场景变化，且本轮未改变 EventBus 热路径；现有证据不支持进一步更换锁或调度数据结构。EventBus 的共享快照改动只在关闭恢复路径执行。

## 隔离的 release 微测

以下都是同机单线程测试夹具；基线 `Sessions` 的相同测试体临时加入原始版本测试模块，测完后恢复，基线生产源码和工作树仍与参考提交一致。P95/P99 是整个调用时间；锁等待/持有只记录了优化版的均值，原始版与两版锁时间分位数均**未测量**。

| 场景 | 原始 P95/P99 | 优化 P95/P99 | 分配 |
| --- | ---: | ---: | ---: |
| 4,096 在线设备，首页 32 条 | 550/594 µs | 1.17/1.21 µs | 524,288 → 2,048 B/查询 |
| 4,096 在线设备，尾页 32 条 | 624/920 µs | 10.7/12.2 µs | 524,288 → 2,048 B/查询 |
| 4,096 历史 Presence，重复连接 | 4.25/4.29 µs | 0.375/0.375 µs | 两版均 4,560 B/次 |

优化版查询锁等待均值约 21 ns（首页）/16 ns（尾页），持有均值约 1.0/10.0 µs；这些不是 P95/P99。列表仍为 offset/limit API，最大 256 条，尾页仍须跳过前面的有序索引项。满容量时 Presence 仍执行清理并可能有 O(n) 锁占用；当前连接热路径不再每次全表扫描。

`AuthCache` 4,096 项命中测试中，整个 HMAC 校验调用中位数 988 → 559 ns，共享锁持有中位数 889 → 77 ns，仍为 24 B/次分配；这只证明命中路径锁缩短。缓存未命中、外部提供方延迟的 P95/P99 未测量。新 `max_auth_provider_requests` 仅约束 HTTP 提供方并发，不改变默认 ingress 容量。

256 个 4 KiB 事件、每事件两个必需 Sink 的恢复快照构建中，深拷贝视图分配 1,148,928 B/515 次，共享引用视图分配 45,056 B/258 次。该数字仅是快照视图的分配量，**不是进程峰值 RSS**。512 条 spool 记录的串行同步保存中位数为 10.89 → 12.72 ms，恢复读取中位数 1.19 → 1.20 ms；流式完整校验的收益是旧记录不再集中驻留，未证明保存更快。NBSP v3 记录格式、generation、整流摘要与原子提交不变。

## 复现与边界

```bash
cargo build --release --locked --workspace
python3 scripts/perf/device_protocol_benchmark.py \
  --plan docs/performance/v024-ingress-optimization/plan.json \
  --server <tmp>/baseline-target/release/netbaiot-server \
  --candidate target/release/netbaiot-server \
  --loadgen <tmp>/baseline-target/release/netbaiot-loadgen \
  --baseline f30d7c06a97d5cb851e3e30530378c86082efcf9 \
  --output target/performance/v024-ingress-optimization/replay
cargo run --release --locked -p netbaiot-runtime --example eventbus_probe
```

恢复与故障的正确性由工作区测试、真实子进程重启及单独的 60 秒重启 soak 检查。未做跨机压测、长时生产 soak、断电试验、真实外部鉴权提供方高延迟分布、最大 metadata 下的 RSS 压力测试或 Linux 本机执行；这些不能从短时 loopback 数据推算。`restart_spool` fuzz 目标保留，但本轮 15 秒尝试因本机缺少 `rand 0.10.3` 缓存且配置代理不可连接，样本未执行。

## 验证记录

- **通过**：最终 `cargo xtask check`（实际执行 `cargo fmt --all -- --check`、全工作区所有目标/特性的 Clippy `-D warnings`、全工作区全特性测试和性能证据预算检查）；`cargo build --release --locked --workspace`；`cargo +1.88.0 check --locked --workspace --all-features`；`cargo xtask schema --check` 与 `cargo xtask config-reference --check`；`git diff --check`。
- **通过**：UDP 并发/重放回归、NBSP 流式与缓冲解码器的完整文件/逐位置截断/逐字节位翻转一致性、EventBus 多必需 Sink、损坏和旧版本恢复、真实子进程落盘失败留存与重放；另行运行 60 秒、12 代进程的优雅重启 soak（64.78 秒）。
- **通过**：31 项 MQTT 3.1.1 NetbaIoT 原始报文状态机检查、Mosquitto CLI 持久会话和退订互操作、Mosquitto CLI TLS 证书校验互操作、MQTT 5.0 QoS/过期/订阅选项/Will Delay 烟测。
- **已尝试但未执行样本**：`cargo +nightly fuzz run restart_spool -- -max_total_time=15` 在编译时因缺失缓存依赖与不可连接的本机代理失败；现有 fuzz 目标没有被删改。
- **未执行**：需要本机 `mosquitto` 服务端程序的完整差异套件、Linux 目标上的实际编译/运行、长时生产 soak 与断电实验。已安装的 `mosquitto_pub`/`mosquitto_sub` 客户端互操作已单独通过。
