# Business RPC V3 HOL 第二阶段：发送提前量实验

## Baseline

| 项目 | 值 |
| --- | --- |
| starting HEAD | `4d3583b1e6e4d57e5cfa8f8a94e3ee7b0aa43ea2` |
| ending HEAD | 本报告所在任务分支提交；准确 SHA 以 `git rev-parse HEAD` 为准 |
| branch | `codex/business-rpc-v3` |
| worktree | `/Users/sam/Dev/work/sskycn/netbaiot` |
| 平台 | macOS 26.6.2、arm64、本机 loopback、release 网关和 loadgen |

实验 JSON 的 `git_sha` 是构建时的 **starting HEAD**，且当时工作区有未提交代码；不能把数据归于原始提交。各模式重新启动网关并使用独立 spool。正式测量使用开发令牌和明文 loopback，mTLS 仅由真实 socket 集成测试覆盖。`auth/s` 是此工作负载的完成速率，Event payload B/s 是已 ACK payload 字节数除以标称时长，均不是最大容量。

## Root Cause

原始 16 KiB Event 在 V3 wire 中约 17.4 KiB，使用 8 KiB DATA 帧时分成 3 帧。第一阶段的帧头抓取显示默认 256 KiB stream window 下 **0/11** 个 Event 有 Auth DATA 插入。第二阶段在 gateway 记录 body 入调度器、DATA 被选中及 writer 完成时刻，并在 proxy 记录帧顺序：同类基线 discovery 的 **11 个 Event 全部在 competing Auth body 进入 runnable state 前选完并写完自身 DATA**，所以机会数为 0/11，实际穿插也为 0/11。先把 receive stream window 缩到 8 KiB 可制造等待 peer credit 的机会（第一阶段 3/11）；本轮恢复 256 KiB receive window，仅用 16 KiB 本地 send-ahead，10 秒 discovery 中机会与穿插都变为 4/11。由此可确定当前 0/11 的直接原因是 **Event 提交速度超过后到 Auth 的到达速度**，而非两个 runnable stream 同时存在时 DRR 拒绝抢占。

V3 在 T0 body 入队、T1 选流、T2 构造帧时仍能重排未提交 DATA；T3 开始写入后，单 writer 按帧顺序调用 `write_all`。T4 `AsyncWrite` 完成仅说明下层接受了字节。查阅本地 `tokio-rustls 0.26.4` 的 `TlsStream::poll_write` 和 `common::Stream::poll_write`：明文可先交给 rustls writer，函数会推进密文 I/O，但 Ready 不保证密文最终写出。故 TLS 下 **不能将 T4 等同 T6 socket 接纳，更不能等同 T7 出口/ACK**；本轮没有测定 rustls 可缓存密文的固定字节上限。明文 `ObservedSocket` 可记录底层 `TcpStream::poll_write` 接纳字节，仍不等于 T7。无论在哪一层，一旦已进入有序 TLS/TCP 字节流，后到 RPC 无法越过已提交 DATA。

代理每方向对一次最多 16 KiB 的 `read` 增加 25 ms，再按本次读取字节数限速；它在等待前已从内核读取数据。记录了 `read_chunk_bytes`，因此帧大小可能改变 read 分块及延迟次数。这里的 25 ms 是**每次用户态读取的延迟**，不是测得的网络 RTT。默认 socket send buffer 实测 146,988 B，足以容纳约 17.4 KiB 的 Event；但因 proxy 预读和 TLS 缓存，不能把 0/11 单独归咎于 `SO_SNDBUF`。

## Instrumentation

生产指标新增固定基数的 V3 DATA selected bytes、send-ahead stalls、writer wait 直方图；既有 `data_bytes_sent` 记录 writer 成功后的字节。实验 debug 日志记录连接内 stream、帧类型、帧长度、body 入队、选帧及完成时刻；`NETBAIOT_V3_SOCKET_TRACE=1` 才额外输出 socket 接纳长度。日志不输出 payload 或凭据，生产指标没有 stream、device、event、request ID 标签。日志追踪本身会扰动亚毫秒 LAN 测量，所以 LAN 对照关闭它。

分析器将 competing 定义为 Event 尚有未选 DATA 时另一 RPC body 进入 runnable 状态；success 是该 RPC DATA 出现在 Event 首末 DATA 之间。其 `preemption_ms` 是该 RPC runnable 到首 DATA writer 完成，不含网络往返。正式 60 秒结果的代理帧头顺序也验证了 8/16 KiB 候选的 60/60、59–60/60 实际穿插。低机会数的基线比率没有统计意义。`scheduler_wait_us` 是 body 入队至首选帧，`business_rpc_auth_latency_us` 是网关侧 transport RPC，设备 Auth p50/p95/p99 另含 MQTT 建连、网关认证路径及业务往返；三者不能互换。正式候选中网关 scheduler wait p95 位于 `≤250 μs` 桶、writer wait p95 位于 `≤100 μs` 桶，网关 transport Auth p95 位于 `≤1 s` 桶。Auth handler delay 配为 0，fresh device 使每次认证触发 provider 调用（另有一个发布设备调用）；未发现毫秒级 handler 人为睡眠。

## Design Changes

增加**可选、仅 gateway 本地**的 `business_rpc.v3_send_ahead = {"stream_bytes":16384,"connection_bytes":131072}`。默认 `null`，维持原有 V3 行为、8 KiB frame、256 KiB receive stream window、4 MiB receive connection window及 RPC:Event 4:1 调度权重。不改 Hello、wire、V2、SDK 默认或应用 ACK 语义。

调度器保留 `Bytes + cursor`；未提交 body 继续占原有 queue byte budget，不复制 DATA、不新建等待队列。另用计数跟踪每 stream 和整条连接已经选出、但尚未观察到相应 peer `WINDOW_UPDATE` 的 DATA：每次选帧同时受协议 flow window 与本地两个 send-ahead 额度约束。stream update 只释放 stream counter；connection update 只释放 connection counter，不能重复释放。`AsyncWrite` 完成不释放。因而在没有 peer progress 时，单 bulk stream 最多领先本地限额，所有 streams 合计最多领先 connection 限额；这只是**尚未提交数据的调度界限**，不保证 RPC 在已提交 TCP 字节前抢占。config 要求 frame ≤ stream ≤ connection ≤ 16 MiB；非法或 1 byte 配置返回配置错误，不静默截断。

最小 receive window 没有改变。当前 receiver 在每个非最终 DATA 处理后返回相应 stream 和 connection `WINDOW_UPDATE`，不等 256 KiB 阈值；8 KiB send-ahead 与 8 KiB frame 的进度测试覆盖这一点。控制帧、窗口及 body 仍由既有有界路径处理；没有 sleep、`yield_now` 或自适应拥塞控制。

## Correctness

`window_update` 先验证非零增量及协议 window 上限，再释放本地计数；算术使用 checked add，释放使用不低于零的 saturating subtract。stream RESET 清掉未提交 body 和该 stream 的 send-ahead counter；已经提交的连接字节仍计在 connection counter，直到 connection credit 返回或连接销毁，避免 RESET 后超额发送。连接销毁 drop 全部本地计数。一个 stream 的额度耗尽不会阻挡仍有额度的其他 stream；全部阻塞时 writer 等待新 stream、`WINDOW_UPDATE`、控制帧或关闭消息，不空转。DRR 4:1 权重未改，Event 仍能继续推进。

新增 mux 测试覆盖 1 MiB bulk 后到 100 B RPC、三个 bulk 的 128 KiB 连接上限、RESET 计数、非法 tiny 配置和 8 KiB send-ahead 配 256 KiB receive window 的 credit 进度；paused-time writer 测试覆盖额度耗尽后的等待。完整 V2/V3 socket、mTLS、flow-control、provider reset/replacement、Event ACK、GOAWAY、资源释放与 shutdown 回归均通过。

## Benchmark Matrix

Discovery 固定 16 KiB Event、1 Event/s、4 Auth 并发、25 ms/方向/次 read、32 KiB/s/方向、15 秒，frame 与 stream send-ahead 如下；connection send-ahead 一般 128 KiB，256 KiB 行为 256 KiB。**每格仅一次、且部分复用身份造成缓存效应，只用于筛选，不能据此选默认值。** Event throughput 各成功行均为 offered 16,384 payload B/s；Event 完成列为 p95 ms。帧数是全部 V3 发送帧，不只是 DATA。CPU 为工作期采样均值，RSS 为峰值。

| frame KiB | stream KiB | Auth p50/p95/p99 ms | Auth/s | Event B/s | Event p95 ms | CPU % | RSS KiB | 帧数 | 机会/穿插 |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 8 | 77.93/2491/5378 | 7.9 | 16384 | 1019.8 | 0.57 | 8016 | 222 | 3/3 |
| 4 | 16 | 89.83/1751/5685 | 7.4 | 16384 | 840.0 | 0.70 | 8000 | 223 | 3/3 |
| 8 | 8 | 84.15/1877/4580 | 7.3 | 16384 | 1028.2 | 0.65 | 8208 | 191 | 3/3 |
| 8 | 16 | 83.14/1943/5175 | 7.9 | 16384 | 770.2 | 0.55 | 8096 | 190 | 2/2 |
| 8 | 32 | 84.49/1173/10003 | 8.3 | 16384 | 753.7 | 0.55 | 8160 | 190 | 0/0 |
| 8 | 64 | 94.03/1149/4646 | 7.2 | 16384 | 882.7 | 0.40 | 8064 | 190 | 0/0 |
| 8 | 128 | 86.02/1860/4392 | 7.0 | 16384 | 989.9 | 0.49 | 8080 | 190 | 1/1 |
| 8 | 256 | 82.35/1844/6800 | 6.6 | 16384 | 898.0 | 0.32 | 8064 | 192 | 1/1 |
| 16 | 16 | 75.77/1115/10003 | 6.8 | 16384 | 994.7 | 0.51 | 8144 | 176 | 4/4 |
| 16 | 32 | 73.58/1721/8515 | 7.0 | 16384 | 859.3 | 0.29 | 7984 | 175 | 0/0 |
| 16 | 64 | 86.82/1172/9093 | 7.1 | 16384 | 763.0 | 0.26 | 8064 | 175 | 0/0 |

在 256 KiB/s 中等链路的相同 discovery 条件下，8 KiB frame 配 8/16/32 KiB send-ahead 的 Event p95 分别是 649.8/295.7/201.9 ms，机会/穿插为 2/2、1/1、0/0，Auth p95 为 1227/1852/1914 ms；单轮噪声很大。微基准以 1 MiB generic Event 先运行四帧，再插入 100 B RPC，证实 4/8/16 KiB frame 组合下 late RPC 之前最多选出相应 8/16/32/64 KiB send-ahead，见 `scheduler-microbench.csv`；其 synthetic credit 与 frames/s 不是网络吞吐。多个 bulk 的连接总额由单元测试验证，生产 Event inflight 仍为 1。

## Repeated Runs

正式场景采用**每轮全新 gateway/spool、fresh 设备身份、3 轮 × 60 秒**；Event 每秒 1 个、Auth 并发 4、原始代理参数同上。每轮 V3 Event ACK 60/60，Auth 请求数见表。下表 p50/p95/p99、Auth/s、Event completion、CPU、RSS 为三轮中位数；括号是 p95/p99 或 Event p95 的三轮范围。V2 dual 使用两个独立 TCP 连接，不能与 V3 single 假定有同样的底层隔离。

| 模式 | Auth 样本/轮 | Auth p50/p95/p99 ms | Auth p95 范围 | Auth p99 范围 | Auth/s | Event payload B/s | Event completion p50/p95 ms | Event p95 范围 | CPU % | RSS KiB | 机会/穿插/轮 |
| --- | --- | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | --- |
| V2 单连接 | 913/911/891 | 147/693/720 | 684–695 | 719–721 | 14.57 | 16384 | 未测 | 未测 | 0.90 | 14176 | — |
| V2 双连接 | 1626/1425/1268 | 169/171/171 | 150–194 | 151–195 | 22.80 | 16384 | 未测 | 未测 | 1.77 | 15072 | — |
| V3 原默认 | 695/671/721 | 193/721/734 | 707–733 | 724–740 | 11.03 | 16384 | 763.4/803.9 | 797.9–806.1 | 0.70 | 11776 | 1/1、0/0、0/0 |
| V3 8/128 KiB | 745/779/812 | 355/449/450 | 448–453 | 449–454 | 12.46 | 16384 | 850.0/1013.4 | 961.8–1014.8 | 0.88 | 11920 | 60/60、60/60、60/60 |
| V3 16/128 KiB | 721/725/741 | 193/643/669 | 640–643 | 648–674 | 11.55 | 16384 | 705.4/737.7 | 731.8–784.9 | 0.56 | 11504 | 60/60、59/59、59/59 |

16 KiB 对原默认的 Auth p95 中位数约 **−10.8%**，p99 约 **−8.9%**，Event p95 约 **−8.2%**，本 offered rate 下 Event ACK 吞吐持平。8 KiB 的 Auth p95 约 −37.7%，但 Event p95 约 +26.1%、Auth p50 约 +84%。16 KiB 候选在有竞争机会时的 preemption p95 三轮中位数 **0.202 ms**；8 KiB 为 **0.237 ms**。它证明本地额度稳定地保留调度机会，却不能推出端到端 p99 由调度器独自决定。V3 帧数三轮范围：原默认 2350–2500、8 KiB 2570–2772、16 KiB 2499–2560；send-ahead stall 分别 0、459–519、249–262。提高控制帧率和 peer credit 往返是成本的一部分。

另在 **256 KiB/s/方向、2 Event/s、16 Auth 并发、15 秒、fresh identity** 重复三轮，Event ACK 均为 30/30：原默认 Auth p95 中位数 2043 ms（2013–2129），p99 5315 ms（5088–5951），Auth/s 22.98，Event completion p50/p95 为 182.9/212.8 ms；16 KiB 候选相应为 1927 ms（1912–2109）、4810 ms（4460–6731）、21.19/s、262.5/291.2 ms。即 Auth p95 仅约 −5.7%，Auth/s 约 −7.8%，Event p95 约 **+36.9%**；机会/穿插从基线 1/1、4/4、1/0，变成候选 14/14、16/16、18/18。候选某轮 preemption p95 达 228.6 ms，不能声称所有轮都是亚毫秒。一次更早的相同单轮候选只 ACK 28/30，结果保留；rate=10/s 的过载试验仅 10 次成功入队、9 次 ACK，不作为成功的 sustained throughput。

无代理、无 debug frame trace 的 loopback 场景重复三轮：原默认 Auth p50/p95 中位数 0.43/1.01 ms，16 KiB 为 0.41/1.16 ms，Event 都 ACK 15/15，Auth/s 22.00/22.01，CPU 0.60/0.62%，RSS 9888/9792 KiB；p95 范围分别 0.91–1.28 与 0.94–1.57 ms，没有明确 LAN 优势。两组 p99 中位数达 6.724/6.996 s，分别有 2/2/3 与 2/3/0 次 timeout；极少数超时主导 p99，不能归因于 mux。含 debug trace 的 LAN 初次对照受到记录开销明显干扰，原始输出仍保留但不用于普通 LAN 延迟结论。

## Socket Experiments

gateway V3 单独试了默认与请求 4096 B 的 `SO_SNDBUF`，实测返回 146,988 与 4096 B。在 32 KiB/s、15 秒单轮、无 send-ahead 时，竞争/穿插都为 0/15；Auth p95 为 695/757 ms，Event p95 为 768/805 ms，Event ACK 吞吐均 16 KiB/s。16 KiB send-ahead 加上两种 socket buffer 后，两轮竞争/穿插都为 14/14；Auth p95 为 642/620 ms，p99 为 646/734 ms，Event p95 为 814/805 ms。结果混合且仅单轮，**没有独立、稳定的 `SO_SNDBUF` 收益**。proxy 先读入最多 16 KiB 再限速，小 send buffer 仍允许整个 Event 快速提交；该选项仅为实验配置，不推荐生产默认。

当前 macOS 的独立连接 socket capability probe 支持 `TCP_NOTSENT_LOWAT`，8/16/32 KiB 均能 set/get；在 receiver 应用首次读之前 sender 已被接受 540,672/557,056/573,440 B，默认约 949,804 B，单独 `SO_SNDBUF=4096` 约 135,168 B。该选项约束 **unsent** 字节，不限制已经到 peer kernel 或未 ACK 的全部数据。它**没有接入 gateway V3，也没有 V3 Auth/Event 测量**，所以不能声称有 HOL 收益或提供 production opt-in；其它平台若不支持则应标记 NOT SUPPORTED。现有安全 `socket2` API 在此目标未提供该选项，本轮没有为可选平台调优引入 `unsafe`。

## Recommendation

**No, keep current default**：8 KiB frame、256 KiB receive stream window、无 send-ahead 限制。16/128 KiB 是可解释的**显式 opt-in 低速链路实验参数**，不是普适 Pareto 最优：低 offered rate 三轮的 Auth 与 Event latency 改善稳定，持续积压时 Auth 收益缩小、完成速率与 Event latency 付出代价；LAN 无明确收益，也没有 mTLS 性能、真实 WAN 长 soak 或高 RTT 大 body 容量数据。强隔离仍建议业务使用独立 Auth/Event 连接。不要通过降低 receive window 或改权重来掩盖本地发送模型的取舍。

## Architectural Boundary

V3 能调度尚未提交到有序 TCP/TLS 流的 application DATA。后到的 stream 不能抢占已经进入该有序字节流的数据；本地 send-ahead 只控制继续提交的界限，不能消除 TCP 丢包 HOL，也不改变 Event 应用 ACK 与传输写入的区别。

## Tests

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all -- --check` | PASS，最终提交前复核 |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | PASS，使用 `CARGO_INCREMENTAL=0` |
| `cargo test --locked --workspace --all-features` | PASS，使用 `CARGO_INCREMENTAL=0`；包含 V2/V3 socket 与 mTLS 回归 |
| `python3 tests/mqtt_conformance/run.py --netbaiot-only` | PASS，31/31 |
| `cargo +nightly fuzz run business_rpc_v3 -- -runs=10000` | PASS，10,000 次，无 crash |
| `cargo bench --locked -p netbaiot-v3-mux --bench send_ahead` | PASS；仅调度器合成 microbenchmark |
| 3×60 秒正式代理负载、3×15 秒持续积压、3×15 秒无代理 LAN | PASS，所述样本；不是公网 soak |
| `TCP_NOTSENT_LOWAT` gateway V3 性能、带 packet loss 的 Linux `tc netem`、公网长 soak、1 MiB 真实业务 Event、mTLS 性能 | NOT RUN |

先前 sandbox 下 UDP socket 测试因系统 `EPERM` 失败；允许 socket 的完整 workspace test 重跑通过。上述 PASS 不包含仓库标记 ignored 的手工/长时用例。

## Remaining Limits

当前业务 telemetry payload 上限为 16 KiB；1 MiB generic mux body 仅在单元测试和微基准使用，未为了本实验改变业务/协议上限。1 MiB 真实 Event sustained workload **NOT RUN**。仍未实现 QUIC、`device.command.send`、离线命令或多节点路由；Event inflight 仍为 1。16 KiB 候选的 15 秒持续积压虽改善 Auth p95，但不构成长期可靠性或生产容量证明。TCP packet-loss HOL、跨机器 RTT 和长期公网 soak 未测。原始成功和失败输出、配置及重现实验说明见 [HOL2 证据目录](performance/business-rpc-v3-hol2/README.md)。
