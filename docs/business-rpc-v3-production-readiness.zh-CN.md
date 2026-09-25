# Business RPC V3 实测与生产就绪边界

**当前判定：V3 wire、流状态、流控和功能回归已实现，但 16 KiB Event 的 HOL 性能门禁尚未通过。** 本页记录实际观测和仍需解决的问题；它不构成公网生产容量声明。V3 在 TCP/TLS 上切分应用消息并调度不同流，不能消除 TCP 丢包引起的队头阻塞。

## 环境与复现

数据来自 macOS/aarch64、Rust 1.97.1、release 网关和 loadgen、本机 loopback。业务身份使用开发令牌；生产 mTLS 由真实 socket 集成测试覆盖，本组负载没有使用 mTLS。每种模式都启动全新网关和独立 restart spool。测试持续 15 秒，另有 2 秒 warmup 与 2 秒 recovery；4 个并发设备认证、每秒 1 个 16,384 字节 telemetry 事件、应用 ACK 延迟 0。用户态 TCP 代理每方向对每次最多 16 KiB 的读取增加 25 ms 与 `bytes / 32768` 秒等待。它没有注入 TCP 丢包，也没有证明真实公网 RTT。原始 JSON 的 `git_commit=be078f12a9de94570c0dbdedf2aaf8f3d54ab715`、`git_dirty=true` 表示由本任务未提交工作树构建，不能视为该基线提交的性能。

最终 15 秒样本的 release 二进制在两项 `Debug` 输出脱敏改动前构建；这两项只改变调试格式，不改变 wire 编码、调度或正常负载路径。完整测试门禁在脱敏后重新运行。指标和延迟仍只代表测量时的工作树，不能当作提交后生产性能。

复现命令：

```sh
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bins
python3 tools/netbaiot-loadgen/run_business_rpc_v3_hol.py --duration-secs 15 --output docs/performance/business-rpc-v3/final-15s
```

结果的 auth 延迟是设备端到端连接时间，包含 MQTT 建连、网关缓存及 RPC；网关 `business_rpc_auth_latency_us` 是单独的 RPC 指标。`p95/p99` 为本次短样本统计，不是服务等级保证。CPU 为 250 ms 采样的网关工作期平均；RSS 是采样峰值。`auth/s` 由 loadgen 的实际运行秒数计算，并非可持续吞吐能力。

| 模式与原始结果 | Auth 成功/请求 | Event ACK | Auth p50/p95/p99 ms | Auth/s | 网关 CPU % | RSS 峰值 KiB | V3 发送帧 |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| [V2 单连接](performance/business-rpc-v3/final-15s/multiplexed.json) | 157/157 | 15 | 85.67/1988/5047 | 7.84 | 0.91 | 7904 | — |
| [V2 双连接](performance/business-rpc-v3/final-15s/dual.json) | 159/160 | 15 | 82.50/1952/4647 | 8.20 | 0.82 | 8032 | — |
| [V3 单连接，4 KiB DATA](performance/business-rpc-v3/final-15s/v3-4096.json) | 155/155 | 15 | 71.35/2019/6052 | 7.12 | 0.86 | 8064 | 223 |
| [V3 单连接，8 KiB DATA](performance/business-rpc-v3/final-15s/v3-8192.json) | 153/154 | 15 | 81.12/1644/6150 | 7.16 | 0.86 | 8192 | 191 |
| [V3 单连接，16 KiB DATA](performance/business-rpc-v3/final-15s/v3-16384.json) | 151/151 | 15 | 81.79/1844/7429 | 7.64 | 0.77 | 7984 | 175 |

4/8/16 KiB 三组使用相同的 256 KiB 初始流窗口和 4 MiB 连接窗口。8 KiB 本次 p95 比 V2 单连接低约 17%，但 p99 高约 22%；4 KiB p95 更高，16 KiB p95 仅略低且 p99 更高。[前一轮独立原始样本](performance/business-rpc-v3/v3-8192.json) 的 8 KiB p95 低约 36%，p99 却高约 62%。两轮都不能说明优势来自跨流交错：下面的 wire 抓取显示默认窗口下 Event 内没有 Auth DATA 插入。因此仍保留 8 KiB 的保守默认帧上限，**不能以这些短样本宣布 V3 已稳定、明显地降低既有 16 KiB workload 的完整消息 HOL**。更少的 16 KiB 帧并不自动代表更低延迟。本次未测单独的持续网络吞吐峰值；表中的 auth/s 与 V3 帧数仅能说明该负载下的工作量和帧开销。

三组 V3 的网关 RPC p95 都落在 `≤1000 ms` 的固定桶；认证队列等待 p95 分别为 `≤10/25/10 μs`，scheduler 首 DATA 等待 p95 均为 `≤100 μs`。这表明尾延迟主要发生在本地接纳之后的流传输/远端往返，而非网关队列或 scheduler。应用 handler p95 约 `0.01 ms`。Event ACK 15/15，重连 0，协议错误 0。网关 EventAck 与 scheduler 等待直方图均使用固定微秒桶；后者从 writer 接纳 Body 到选中首个 DATA，不包括 socket 写入与下游处理。细节见各组 `*-metrics.txt`。

## HOL 原因调查

以 `NETBAIOT_HOL_CAPTURE=1` 重跑 10 秒 V3 8 KiB 模式。代理仅记录 gateway→client 的 V3 帧头顺序，不保存 Hello、令牌或 Event payload；[默认窗口帧头追踪](performance/business-rpc-v3/trace-default/v3-8192-1-down.jsonl) 可直接复核。256 KiB 流窗口时，11 个 Event 子流各分成 3 个 DATA 帧，但 **0/11** 在自身首末 DATA 之间出现 Auth DATA。流窗口大于实际约 17 KiB 的 Event body，网关可以在 Auth 请求到达 writer 前把全部 Event 分片送入 TCP 字节流；之后的调度无法抢占这些已经排队的字节。这是本次负载没有呈现明显收益的直接 wire 证据。该轮设备认证 p95/p99 为 1758/4396 ms；[原始 JSON](performance/business-rpc-v3/trace-default/v3-8192.json) 和 metrics 同目录保留。

只将 V3 客户端协商的初始流窗口改为 8 KiB，保持同一 16 KiB Event、代理、10 秒时长与 8 KiB 帧上限，[对照帧头追踪](performance/business-rpc-v3/trace-window-8192/v3-8192-1-down.jsonl) 中 **3/11** 个 Event 有 Auth DATA 穿插（共 16 个 Auth DATA 帧），网关流窗口停顿计数 36；设备认证 p95/p99 为 1693/4589 ms，见 [原始 JSON](performance/business-rpc-v3/trace-window-8192/v3-8192.json)。流控给调度器实际抢占机会，但也可能让大消息逐窗口等待往返。两次短测的 p95 差仅 65 ms，p99 反而高 193 ms，不能据此把默认流窗口降至 8 KiB。可通过 `--stream-window-bytes 8192 --modes v3-8192` 复现该诊断。

## 资源和故障边界

最终三组 15 秒 V3 测量的峰值活动流均为 7，outbound queued bytes 峰值均为 17,562，reassembly reservation 峰值为 860/860/858 bytes；连接与流 window stall 均为 0。负载结束后的活动 V3 连接、活动流、queued bytes、reassembly reservation 全部为 0；每组 Event ACK 15/15。三组 RSS 峰值为 8064/8192/7984 KiB。上述仅覆盖这个低并发短负载，不等于配置的连接、字节或设备容量。

另运行了同配置的 [60 秒 V3 短 soak](performance/business-rpc-v3/soak-60s/v3-8192.json)：设备认证 553/556（3 次超时、0 次凭据拒绝）、Event ACK 60/60、Event retry 0、sync failure 0；网关 RSS 峰值 8016 KiB，工作期平均 CPU 0.79%，峰值活动流 7、排队 17,562 bytes、重组预留 860 bytes。结束后活动连接/流、排队/重组字节均为 0。网关记录了 1 次异常连接关闭，但未有 V3 protocol error；不能把 60 秒视为长时稳定性依据。

[每连接 5 秒代理断线的 20 秒测试](performance/business-rpc-v3/disconnect-5s/v3-8192.json) 完成 220/220 次认证和 18/18 次已收到事件的应用 ACK；21 次设备发布尝试中 18 次入队、3 次因断线失败，EventBus 发生 1 次事件重试。网关指标记录 5 次 V3 建连与 5 次 reset sync 成功，0 次 sync failure、0 次协议错误；测量结束后活动连接/流及排队/重组字节为 0。这是强制断线恢复测试，不是 TCP packet loss 测试，也不能将未入队的 3 次发布计作网关已接受事件。

自动测试覆盖二进制畸形帧、奇偶 ID 与单调性、父子流清理、响应状态、重组预留释放、两个窗口的算术与续传、阻塞流跳过、控制帧 burst 上限、64 KiB Event 与 100 B RPC 在真实 duplex 字节流上的 DATA 穿插、取消读帧时不丢失半帧，以及 V2/V3 同端口、Provider sync、并发认证、Event ACK/重试、失效、mTLS 和代际栅栏。真实 socket 测试仍是短时本机回归，不是公网性能或长时可靠性测试。

| 门禁 | 状态 |
| --- | --- |
| `cargo fmt --all -- --check`、完整 clippy、workspace tests | PASS；workspace 的手动/长时 ignored 用例仍按其原有状态，不计作已执行 |
| MQTT `--netbaiot-only` 回归 | PASS，31/31 |
| `business_rpc_v3` fuzz | PASS，10,000 次，无崩溃 |
| V2/V3 短时同环境功能、60 秒短 soak、代理强制断线与资源回收 | PASS，本页原始 JSON 与 metrics；断线轮有 3 次设备发布未入队 |
| 16 KiB Event 的稳定、明显 V3 HOL 改善 | **未通过**；默认窗口无 wire 交错，短样本尾延迟波动 |
| Linux `tc netem` 50 ms+0.1% 与 100 ms+1% 真实 packet loss | **NOT RUN**；本机 macOS 无 Linux `tc` 环境 |
| 跨机器公网 RTT、长期 soak、持续大事件吞吐 | **NOT RUN** |

V3 仍不包含 QUIC、transport 层 HOL 消除、`device.command.send`、command 幂等、离线命令、多节点路由、event inflight 大于 1 或新的二进制应用 DTO。业务消费者仍须按稳定 `event_id` 幂等处理；计划重启 spool 与突然故障的有界内存丢失语义保持原状。
