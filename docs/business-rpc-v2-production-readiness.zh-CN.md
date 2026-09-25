# Business RPC Stream V2 生产就绪门禁

起始代码为 `76c0d658d410e82eecdbfb8c1d31f09d73af42bc`。本页只记录本轮实际运行的负载和验证。**当前结论：尚不能宣称已具备完整公网生产部署依据**。mTLS、本机限速流代理、过载和 60 秒短 soak 已有观测；跨机器公网、Linux `tc netem` 真实丢包、1 小时及 6 小时 soak 尚未运行。长时稳定性和丢包恢复门禁保持 `NOT RUN`。

## 环境和数据来源

原始结果见 [`performance/business-rpc-v2/`](performance/business-rpc-v2/)。机器为 Mac16,10、10 逻辑 CPU、macOS Darwin 25.6.0、arm64、Rust 1.97.1。网关及 loadgen 为 release profile，单机回环；设备侧为 MQTT 3.1.1，**Business TCP 使用 mTLS**，采用仓库公开的测试证书。业务地址、设备地址、管理地址为不同回环端口；两种业务拓扑均使用同一个 `business_tcp` 地址。管理指标使用 250 ms 采样，CPU 用 `ps` 进程百分比采样；`peak_sampled` 是采样最大值。原始 JSON 的 `git_commit` 为起始 SHA 且 `git_dirty=true`，表示二进制由本任务工作树构建，不能把该 SHA 当作这些测量的干净提交版本。

普通测量网关配置：`business_rpc.max_connections=8`、`auth_max_inflight=16`、正/负 auth cache TTL 为 100/10 ms、每 IP 请求上限 512/s、sink ACK timeout 为默认 5 s、事件窗口 1。过载测量单独使用 `auth_max_inflight=2`、正/负 TTL 为 2/1 ms、handler 延迟 100 ms。负载参数、时长、拓扑、mTLS 模式和申报的网络 profile 均在每个 JSON 内；业务证书和私钥路径、token 内容均不进入结果。`network_injection=external_unverified` 表明 loadgen 记录了操作者申报的代理参数，不能自行证明系统层 RTT。事件大小是 telemetry 文本值字节数，序列化帧还包含字段和事件元数据；当前默认 codec 每字段 256 字节、最多 64 字段，因此本轮选择 1,024 和 16,384 文本字节，没有以非法 64 KiB 事件凑数据。

下表是上述工作树在本机的**观测值**，不是生产容量。网关直方图用固定微秒桶，JSON 报告的 p50/p95/p99 是桶上界；没有精确最大值时 `max=null`。客户端 TLS、Ready、handler 和设备端到端直方图有实测最大值。设备端到端认证包含 MQTT 连接、SDK 重试、缓存、RPC 和 session 注册，不能视为纯 RPC RTT。

| 运行（原始 JSON） | 数量 | 客户端 TLS p50/p95/p99/max ms | 客户端 Ready p50/p95/p99/max ms | 网关 CPU baseline/平均/采样峰值 % | 网关 RSS baseline/warm/peak/recovery KiB |
| --- | ---: | --- | --- | --- | --- |
| [mTLS handshake](performance/business-rpc-v2/readiness-mtls-handshake.json) | 3985/3985 成功，5 s | 0.82/0.99/1.19/2.01 | 1.21/1.37/1.60/3.74 | 0.2/43.2/44.1 | 6704/7952/8096/8096 |

| 运行（原始 JSON） | 设备端到端 p50/p95/p99/max ms | 网关 `rpc_total` p50/p95/p99/max ms | 客户端 handler p50/p95/p99/max ms | 网关 CPU baseline/平均/采样峰值 % | RSS baseline/warm/peak/recovery KiB |
| --- | --- | --- | --- | --- | --- |
| [mTLS steady auth](performance/business-rpc-v2/readiness-mtls-steady-auth.json)，308/308 成功，10 s | 46.64/685/1753/4887 | ≤0.25/≤0.25/≤0.5/null | 0.01/0.01/0.01/0.04 | 0/0.72/1.4 | 7536/7952/8064/8064 |

`rpc_total` 是网关 `business_rpc_auth_latency_us`；新增 `business_rpc_admission_us`、`business_rpc_queue_wait_us`、`business_rpc_remote_wait_us` 分别覆盖 RPC 本地接纳、排队至 transport writer 取出、入队后至 Response/timeout。`remote_wait` 包含排队、写帧、网络和 handler，不能简单相加得出互斥阶段。客户端 `tls_handshake` 仅覆盖 rustls 握手，`sync` 覆盖 V2 Hello、reset sync、订阅至 Ready；handler 由 loadgen 应用观测。`auth_cache_lookup` 与 session registration 目前没有单独精确计时；JSON 对不可测项使用 `null`，不填零。所有指标名称和 `method`/`code` 标签都是固定枚举，没有设备、请求或修订号标签。

## 错误语义与设备可见结果

`BusinessRpcFrame::Response` 保留 wire `RpcErrorCode`。网关的 `netbaiot_business_rpc_remote_errors_total{method,code}` 保留每一种业务端错误；`business_rpc_method_results_total` 记录本地最终结果；`auth_stale_responses_total` 单列失效 epoch 丢弃的旧授权结果。只有明确 `DeviceRejected` 能使 AuthCache 建立设备凭据负缓存。实测过载时网关计数与设备 SDK 终态不相同，不能再靠设备错误文字统计网关过载。

| Business RPC code / 情况 | runtime `Error` | 设备负缓存 | MQTT 3.1.1 设备侧 |
| --- | --- | --- | --- |
| `DeviceRejected` | `Authentication` | 是 | CONNACK 4，SDK `Unauthenticated` |
| `Unavailable`, provider `Unauthenticated` | `Unavailable` | 否 | 连接关闭/重试后的粗粒度失败 |
| `Overloaded` | `Overloaded` | 否 | MQTT 3.1.1 无容量码；SDK 可能超时或报 unavailable |
| `Timeout` | `Timeout` | 否 | 连接关闭/重试后的粗粒度失败 |
| principal `Forbidden` | `Invalid` | 否 | 业务连接可能收到 GoAway；设备连接不能据此判定凭据拒绝 |
| `Internal` | `Internal` | 否 | 粗粒度失败 |
| `StaleRevision`, `Conflict` | `Conflict` | 否 | 同步/重试后的粗粒度失败 |
| `InvalidRequest`, `UnknownMethod` | `Invalid` | 否 | 粗粒度失败 |
| 本地 AuthCache epoch 或最终注册栅栏失效 | `Unavailable`，`auth_stale_responses_total` 增加 | 否 | 连接重试/粗粒度失败 |

在 [过载原始结果](performance/business-rpc-v2/readiness-overload.json) 中，配置的 12 秒负载有 516 次**网关** Business RPC overload、pending 峰值正好为配置上限 2，最终 pending 项/字节及业务连接均为 0；155 次外层 SDK 连接中 134 成功、21 以 SDK Timeout 结束，`counts.overloaded=0`。这不是网关没有过载。MQTT 3.1.1 认证阶段在非明确凭据拒绝时不伪造非标准 CONNACK；SDK 有界重试后的终态也未必等于单次网关内部原因。诊断应看网关指标、日志/trace 与同次负载结果的计数差值。TCP/UDP 保持现有公开协议。

## 同端口拓扑与网络条件

轻负载顺序对比在同一 `business_tcp` 监听器上使用 `1 × Multiplexed` 或 `1 × AuthControl + 1 × Events`，相同事件率、ACK 延迟、认证并发和 TLS。下表设备认证列是客户端端到端 p95，RPC 列是网关桶上界 p95；ACK 列为客户端从收到投递至调用 ACK 成功，主要包含设定的 100 ms 应用等待，**不代表端到端网络 ACK**。新增网关 EventAck 直方图用于后续报告。网络代理对每个方向每次读取的 stream chunk 延迟 10/25/50 ms，分别为 *WAN-like 用户态流延迟*，不是经测量确认的 20/50/100 ms 系统 RTT。代理没有删除字节，不能称为 TCP 丢包。

| 代理配置及原始结果 | 拓扑 | auth 成功 | 事件 ACK | 认证端到端 p95/p99 ms | 网关 RPC p95 桶上界 ms | 客户端 ACK p95 ms |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| [10 ms/方向](performance/business-rpc-v2/readiness-stream-delay-20ms.json) | multiplexed / dual | 180 / 182 | 10 / 10 | 358/657 · 282/651 | ≤50 / ≤50 | 103 / 103 |
| [25 ms/方向](performance/business-rpc-v2/readiness-stream-delay-50ms.json) | multiplexed / dual | 142 / 149 | 10 / 10 | 202/299 · 196/203 | ≤500 / ≤500 | 103 / 103 |
| [50 ms/方向](performance/business-rpc-v2/readiness-stream-delay-100ms.json) | multiplexed / dual | 89 / 92 | 10 / 10 | 423/509 · 290/301 | ≤500 / ≤500 | 103 / 103 |
| [50 ms + 0–20 ms jitter/方向](performance/business-rpc-v2/readiness-stream-delay-100ms-jitter.json) | multiplexed / dual | 79 / 78 | 10 / 10 | 352/544 · 329/545 | ≤500 / ≤500 | 103 / 103 |

[本机 1 KiB](performance/business-rpc-v2/readiness-topology-1k-local.json) 与 [本机 16 KiB](performance/business-rpc-v2/readiness-topology-16k-local.json) 轻负载对比每种拓扑均完成 15 次事件 ACK，未出现网关 overload；认证尾延迟没有稳定的单方向优势。`topology_compare` 顺序运行适合功能等价检查；若前一轮有未完成 required 事件，后轮可能先消费旧事件，不能把它当作公平的吞吐基准。以下 HOL 比较因此改用两个全新网关。

为验证真实 stream HOL，另用**各自全新网关、独立 spool、相同 server limits 与同一个代理参数**运行 16,384 字节 telemetry、1 event/s、0 ms 应用 ACK、25 ms/方向及 32 KiB/s 每方向限速。两轮各发起 11 次 SDK 发布，收到并应用 ACK 10 次，gateway overload 和 sync failure 都为 0。结果：[multiplexed](performance/business-rpc-v2/readiness-hol-isolated-multiplexed.json) 认证 120 次、设备端到端 p95/p99 729/802 ms、网关 RPC p95/p99 桶上界 1000/1000 ms；[dual](performance/business-rpc-v2/readiness-hol-isolated-dual.json) 认证 223 次、p95/p99 242/268 ms、网关 RPC 上界 500/500 ms。两者网关认证队列等待 p99 桶上界分别仅 0.025/0.05 ms，网关 CPU 工作平均为 0.27%/1.56%，RSS baseline/peak 为 6112/8448 与 6080/8656 KiB；最终 pending 和 active business connection 均为 0。此单机短测说明大帧和受限带宽下，**已写入同一 TCP stream 的事件字节**可抬高认证尾延迟；应用队列优先级只决定尚未写出的帧顺序，不能抢占已进入 stream 的字节。双连接隔离该字节流，但多一个 TLS 连接与资源成本。对低负载或小帧可使用 multiplexed；类似此测的受限链路和大事件可优先考虑同端口双角色连接，并在真实链路上复测。它不是协议硬要求，也不能从一次短测推导通用容量。

## 故障、生命周期与 soak

使用有界 [用户态代理](../tools/netbaiot-loadgen/business_stream_proxy.py) 的 [临时黑洞 + 断连](performance/business-rpc-v2/readiness-blackhole-disconnect.json) 与 [强制 RST](performance/business-rpc-v2/readiness-forced-reset.json) 分别完成 162/162、292/292 次设备认证；恢复后的 pending 项/字节与活动业务连接均为 0，reset sync 成功计数分别增加 2、5。修复后这两轮的 SDK `rejected=0`；旧 AuthCache epoch 失效不再返回设备凭据拒绝。代理还支持限速和单向 FIN；这里只把实际执行的流故障标为已测。真实 Linux packet loss 未运行；仓库提供 [netem 脚本](../tools/netbaiot-loadgen/netem_business_rpc.sh)，应在隔离网络命名空间里运行 50 ms+0.1% 和 100 ms+1% loss，不能用随机删 stream 字节替代。

[60 秒 mTLS 双角色短 soak](performance/business-rpc-v2/readiness-short-soak.json) 包含并发认证、2 event/s、100 ms ACK、1 UDP verifier/s、每 10 秒设备失效、每 15 秒 event 连接重连、每 20 秒 provider 重连。结果为 1838/1838 设备认证、119/119 事件应用 ACK、57 次 verifier 探测中 52 次收到 NBA1 ACK、5 次重连、6 次失效、0 次 sync failure、4 次迟到 Response；最终 pending 项/字节、业务连接与队列采样值均为 0。15/30/45/60 秒快照在 JSON `peaks.timeline`；网关 RSS baseline/warm/peak/recovery 为 7712/8368/8672/8224 KiB，CPU baseline/工作平均/采样峰值/恢复为 0/1.94/4.4/0%。5 次 UDP 探测未获 ACK 处于定期失效/重连负载中，本轮未将其解释成凭据拒绝或声称所有 verifier 请求无损。

正式 [30 分钟](../configs/business-rpc-production-readiness/smoke-30m.json)、[1 小时](../configs/business-rpc-production-readiness/readiness-1h.json)、[可选 6 小时](../configs/business-rpc-production-readiness/optional-6h.json) 配置已交付，默认 CI 不运行。将证书路径、地址、网关 PID 和管理 URL 改为实际值后，可运行：

```sh
cargo run --locked --release -p netbaiot-loadgen --bin business_rpc -- configs/business-rpc-production-readiness/readiness-1h.json
```

1h: **NOT RUN**；6h: **NOT RUN**。自动测试已验证旧 business 连接退出后，服务端替换 principal 映射并重启，新证书建立新连接并完成设备认证，旧证书不能再进入 Ready；同时覆盖有效 principal 在空闲 Serving 时到期退出。公网跨机、真实 RTT、真实 TCP packet loss、长时间负载中的自动证书轮换，以及 Business 服务与网关同时重启的完整负载链仍未运行。既有单元/子进程测试覆盖相关代际和计划重启语义，但不能替代上述环境测量。

## 门禁判定

| 门禁 | 状态与依据 |
| --- | --- |
| 错误映射、负缓存、旧 epoch、mTLS hostname/客户端证书、无 TLS downgrade、角色/租户、principal 到期、证书与映射替换重连 | **PASS（自动测试）**：错误矩阵、真实证书矩阵、idle Serving 到期及旧/新证书交接；只由 `DeviceRejected` 负缓存 |
| Required EventAck、稳定 event_id、慢 ACK 与认证并行、pending/队列回收 | **PASS（自动测试 + 短测）**：既有 EventBus/Business RPC 测试与本轮短测最终资源 0 |
| reset sync、用户态延迟、临时黑洞、RST、双角色重连 | **PASS（最终代码本机短测）**；故障后 pending/连接归零，黑洞与 RST 测量无设备凭据拒绝终态 |
| mTLS handshake、steady auth、两拓扑、10/25/50 ms 每方向流延迟、CPU、RSS、短 soak | **MEASURED**，仅对本机、本配置、本工作树有效 |
| `tc netem` packet loss 与公网跨机真实 RTT | **NOT RUN**（本机 macOS 无 Linux `tc` 环境） |
| 1 小时 soak / 6 小时 soak | **NOT RUN / NOT RUN** |
| 总体公网生产依据 | **未通过完整门禁**；缺失真实公网/丢包及长时数据，不得声称支持某设备数或通用 p99 |

## 代码与回归验证

在本轮最终代码上通过：`cargo fmt --all -- --check`；stable 和 Rust 1.88.0 各自的 `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` 与 `cargo test --locked --workspace --all-features`；`python3 tests/mqtt_conformance/run.py --netbaiot-only`（31/31）；`cargo +nightly fuzz run business_rpc_v2 -- -runs=10000`（10,000 次，无崩溃）；`subprocess_graceful_restart_sixty_second_soak`（65.23 秒，PASS）。Python 代理语法检查和 netem shell 语法检查也通过。长时负载及 Linux netem 的 `NOT RUN` 状态不受这些自动回归结果改变。

## 复现与边界

用户态延迟：启动 `python3 tools/netbaiot-loadgen/business_stream_proxy.py --listen 127.0.0.1:19112 --target 127.0.0.1:19102 --delay-ms 25`，将 loadgen `business_address` 设为代理地址，`business_transport.server_name` 仍为证书中的真实名称。加 `--jitter-ms`、`--bytes-per-second`、`--blackhole-after-secs`、`--blackhole-for-secs`、`--disconnect-after-secs`、`--reset-after-secs` 或 `--half-open-after-secs` 可按需注入其他流故障；该工具不进入生产 runtime。Linux 真丢包脚本须在隔离网络命名空间有 `CAP_NET_ADMIN` 时运行，例：`netem_business_rpc.sh 50 0.1 -- <benchmark command>`；执行后自动删除自己添加的 qdisc。不要在共享网络接口上运行。

所有性能数字仅为上述机器和工作树在对应配置下的观测。没有进行热点 profile，因此没有调整协议编码、锁、队列或 timeout。没有测量通用设备容量、跨节点路由、证书自动轮换或 crash durability；业务消费者仍须按稳定 `event_id` 幂等处理，突然断电仍可能丢失未入 spool 的有界内存事件。
