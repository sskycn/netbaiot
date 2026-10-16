# UDP 优化实测与撤回记录（2026-10-16）

**本次未达到 UDP 整链路优化目标；所有运行时优化候选均已撤回。**
最后的通知合并方案能减少排队 publish 的一次分配，但未证明整链路收益；
5 分钟对照 CPU 基本持平、P99 略升，未按局部微基准收益交付生产改动。
认证、重放、UDP 收发和 EventBus 运行时代码恢复为基线；没有提升配额、减少校验或改变 ACK/EventAccepted 语义。
保留的是四线程并发重复报文回归、可重跑的分配基准、CPU profiling/容量汇总工具、测试与审查证据。
**原全局 replay Mutex 跨 await 仍然存在；不能称它已解决。**

共完成 281 次 UDP Release 运行，覆盖原版和 15 个优化候选、逐级加压、配对复测、诊断与故障场景；它们不是 281 次同条件重复。
另保留两次因 Idle Sleep 未完成的运行。最后 P 的 60k/s、300 秒对照为：CPU/事件 28.554→28.552us，
回执吞吐 59,934.6→59,939.8/s，P99 1.48→1.52ms，回执率 99.8914%→99.8996%。
这不能支持整链路提速或稳定容量上移。排队 publish 的分配 4→3、584→552 字节是局部事实，不能代替系统结果。
慢 sink 对照的 P 也出现 CPU/事件增加、吞吐减少，因此按用户要求保守撤回。

本次没有得到可复现的最大稳定吞吐量。共享桌面出现非单调抖动，连部分低于拐点的短测试也有缺失回执；
不把某轮通过、固定发送目标、过载吞吐或历史结果当作生产容量，也不为了得到好看数字修改预先声明的门槛。

## 范围与结论口径

基线是 a2b46a2ad49a43ee538a6de74afddac350763c23。工作树开始时无未提交改动。
回退后重新构建的服务端与负载器 SHA256 均与冻结基线完全一致，记录在 manifest 的 final_rebuild。
本次不推送、不合并；不增加依赖、unsafe、线程或锁分片，不修改生产 Limits 默认值。
按用户要求仅使用当前 macOS ARM64；未运行 Linux recvmmsg/sendmmsg，也未引入未经实测的 Linux 特殊实现。

环境：Apple M4，10 个逻辑 CPU，16 GiB RAM，macOS 27.0.1，Rust 1.99.0。
Release 默认优化级别，服务端 10 个 Tokio worker，负载器 4 个 worker。
64 个 UDP 设备，256 字节应用负载，NBI1/HMAC-SHA256 与 NBA1 完整验证；
每设备窗口 128，ACK 超时 1 秒。本机 loopback 的服务端和负载器竞争同一 CPU。
正常容量场景使用必需 AuditSink 的显式 ACK，不代表外部业务系统已持久化。

测试 fixture 在两端完全相同：设备和认证缓存上限 4096，缓存 8 MiB，速率限制 2,000,000/s，
连接 IP/租户上限 256；其余为配置默认值，包括 UDP 在途上限 64。
这些是既有压测夹具的配置，不是生产默认配置变更；无法外推生产默认配置或跨机网络容量。

## 测量方法

先阅读已有协议、测试和性能报告，再进行 macOS sample 与 Instruments Time Profiler CPU 采样。
Time Profiler 单独跑 10 秒，关闭 waiting-thread 记录；容量对照不启用 profiler 或锁计时。
后期复测关闭会干扰进程调度的 midpoint top，但保留相同的每秒管理采样、进程 CPU 和 RSS。

CPU/事件 = 同一对管理采样区间的服务端 user+system CPU 增量 / EventAccepted 增量。
回执吞吐、接纳率和 P99 来自负载器测量窗口；接纳率分母为实际 attempted，不是 scheduled。
另报 client_window_full，避免用窗口限流隐藏发送不足。CPU 采样区间与回执窗口不完全相同。
RSS 为 1Hz 采样峰值，不是内核记录的绝对峰值。P99 只统计成功验证的 ACK，不能脱离缺失 ACK 阅读。
`udp_no_ack` 的超时/窗口记账边界不同于 attempted-accepted，两者保留原值，不强行配平。
内核 UDP full-socket drops 是整个主机 warmup/measure/cooldown 的差值，不能归因到单个进程。

稳定筛选条件：每次重复回执率至少 99.9%，P99 <5ms，队列没有持续增长，冷却后必需队列归零且正常退出。
另外报告零缺失 ACK 档位。逐级加压、重复拐点、较长运行均为有限时长观测，非无限期生产稳定性承诺。

## CPU profile

基线有效 Running 样本权重 16,397ms，另有 2ms 无栈。多线程权重之和可超过 10 秒墙钟时间。
叶帧：sendto 15.61%，mach_absolute_time 10.87%，psynch_cvwait 8.00%，
psynch_mutexwait 6.66%，recvfrom 6.22%，kevent 5.90%，SHA256 压缩 5.82%，
cvsignal 5.15%，mutexdrop 4.11%，getentropy 3.79%。主要 malloc 叶帧约 1.08%，memmove 0.91%，JSON parse 0.81%。
包含子栈的 with_current_verifier 为 21.79%，ingest_with_identity 17.17%；
EventBus complete 2.99%，publish 2.69%，take_ready 1.60%。
这些是采样栈占比，不能解释成精确内核执行时间或等待时间；sample 的 parked 栈未当作 CPU 热度。
原始表的安全摘要见 [基线 CPU profile](cpu-profile-baseline.json)。


旧压测脚本的环境采集硬编码了 `rustc +1.88.0`，不能代表编译器。
已修正为当前工具链。所有冻结服务端和负载器二进制都包含
`b940084d7eb6a299eb4bfeb8e34901bc051e7ac4` 的标准库构建路径，与实际 Rust 1.99.0 一致；
原始记录未篡改，manifest 明确记录这项元数据更正。
两个缺少测量期结果的运行与系统 Idle Sleep 对应，列为 incomplete，未计入性能统计。
随后用随压测命令退出的 `caffeinate -i` 临时防空闲休眠；有效历史运行没有超过 10 秒的采样间隔缺口。

## 最后一个候选 P 的 Release 对照（已撤回）

每档交替 A/B、B/A，2 轮；每轮 15 秒测量、3 秒预热、1 秒 ramp。
下表是各轮中位数；RSS 是各轮采样峰值的中位数，缺失 ACK 是各轮原始计数之和。
A 是原始提交，P 只有通知合并，现已撤回；最终运行时代码与 A 相同。CPU/事件和 ACK 的窗口不同，见上面的定义。

| 发送目标/s | 回执/s A→P | CPU us/EventAccepted A→P | P99 ms A→P | RSS KiB A→P | 回执率 % A→P | 缺失 ACK 合计 A→P |
|---:|---:|---:|---:|---:|---:|---:|
| 40,000 | 40,002.7 → 39,965.8 | 24.395 → 24.527 | 1.300 → 1.315 | 8,160 → 8,224 | 100.0000 → 99.9150 | 0 → 1,020 |
| 50,000 | 49,933.7 → 49,998.7 | 27.532 → 27.423 | 1.520 → 1.490 | 8,264 → 8,160 | 99.8618 → 99.9915 | 2,073 → 127 |
| 60,000 | 59,918.7 → 59,778.0 | 29.052 → 28.951 | 1.515 → 1.640 | 8,224 → 8,464 | 99.8641 → 99.6325 | 2,464 → 6,610 |
| 65,000 | 64,804.4 → 64,865.8 | 29.722 → 29.557 | 1.680 → 1.770 | 8,232 → 8,184 | 99.7005 → 99.7866 | 5,840 → 4,197 |
| 70,000 | 68,888.0 → 69,746.1 | 31.112 → 30.572 | 4.870 → 2.395 | 8,568 → 8,320 | 98.9429 → 99.6422 | 25,865 → 7,645 |
| 80,000 | 78,338.2 → 78,905.1 | 33.207 → 32.919 | 6.020 → 4.065 | 8,448 → 8,328 | 98.0524 → 98.6312 | 50,007 → 35,215 |
| 100,000 | 74,803.6 → 74,735.8 | 33.892 → 33.746 | 17.070 → 18.415 | 8,184 → 8,272 | 90.4008 → 90.3783 | 253,625 → 253,974 |


完整逐轮数据、拒绝/丢包计数、窗口耗尽、内核计数、队列和退出状态见 [candidate-p-capacity.json](candidate-p-capacity.json)。
不把 `scheduled` 当作实际发送，不把服务端 EventAccepted 与成功回执混为一个计数。

## 稳定边界与持续运行

300 秒测量、10 秒预热、2 秒 ramp，64 个设备，目标 60,000/s；A/P 各一次：

| 发送目标/s | 回执/s A→P | CPU us/EventAccepted A→P | P99 ms A→P | RSS KiB A→P | 回执率 % A→P | 缺失 ACK 合计 A→P |
|---:|---:|---:|---:|---:|---:|---:|
| 60,000 | 59,934.6 → 59,939.8 | 28.554 → 28.552 | 1.480 → 1.520 | 9,040 → 8,496 | 99.8914 → 99.8996 | 19,552 → 18,066 |


两端均 **未达到 99.9%**：P 的精确回执率是 99.8996278%，不能因显示为 99.90% 就判通过。
两端冷却后 pending_required 均为 0，正常退出；采样必需积压峰值 19→20。
RSS 首/尾为 A 8,960/9,040KiB、P 8,080/8,496KiB；这是有限时长的进程采样，不是堆分配总量或泄漏证明。
完整生命周期 UDP 在途上限拒收 22,041→18,064，ACK 发送失败均为 0；主机内核满 socket 丢包 240→27。
这些计数包含预热/冷却，不能与测量窗口的缺失回执逐项相减。

在最后七档、每档两轮短测中，P 的 50k/s 两轮都达到门槛，但 40k/s 有一轮失败、60k/s 也有一轮失败；
A 的短测同样出现非单调结果。不能据此认证“50k 以下稳定”或声称找到了严格稳定上限。
70k/s 以上的显著损失和 100k/s 的客户端窗口耗尽进一步说明，发送目标并非接纳能力。
所有档位都保留原始失败计数；P 的 100k/s P99 中位数还从 17.070 增至 18.415ms。

见 [300 秒逐轮数据](candidate-p-soak.json)。

## 通知合并的局部收益（不足以支持保留）

### EventBus

必需 sink 的有序预留、原子入队和 ACK 责任不变。只对 ready 队列从空到非空的 sink 发送通知；
已有队列由现有 permit、完成事件和 deadline 检查继续推进。
两 sink 的空/非空交替回归验证无遗漏唤醒、无遗漏交付。该修改及回归现保存在
[通知实验补丁](notify-experiment.patch)，未应用于最终代码。

冻结前后测试二进制，先各预热一次，再按 ABBA BAAB 运行，
每个深度每轮测量 100,000 次 publish；setup/cleanup 在计时和分配区域外。
每个表格单元取 4 个进程 × 3 个内层重复的中位数。测试分配器只存在于测试二进制，计时包含其统计开销；不能外推为未插桩运行时的同等比例收益。

| 既有积压 | publish ops/s 前→后 | publish P99 ns 前→后 | 分配次数 前→后 | 分配字节 前→后 |
|---:|---:|---:|---:|---:|
| 1 | 2,581,903.5 → 2,735,758.5 | 459 → 437.5 | 4 → 3 | 584 → 552 |
| 16 | 2,560,910 → 2,678,533.5 | 459 → 458 | 4 → 3 | 584 → 552 |
| 64 | 2,530,600 → 2,618,256.5 | 479.5 → 459 | 4 → 3 | 584 → 552 |

这只证明排队 publish 少 1 次分配、32 字节，局部吞吐提高 3.46%–5.96%，**不是 UDP 整链路分配次数或容量**。
初次 2,000 次短微测量的时间结果不稳定，未用来宣称吞吐收益；该 pilot 也保留。
见 [正式分配数据](eventbus-allocations.json) 和 [pilot](eventbus-allocation-pilot.json)。

### 整链路逐项消融

HMAC 的 B→D、通知合并的 D→E 各只改变一项；都在相同的实验 replay B 上比较，
不能把其绝对容量当成 P 或最终交付版本的容量。P 只有通知合并，已另外与原版 A 完成完整配对复测，最终也未保留。
全部数值来自真实 Release 运行；过载档、异常波动及缺失 ACK 均保留。

| 对照 / 发送目标 | 回执/s 前→后 | CPU us/事件 前→后 | P99 ms 前→后 | RSS KiB 前→后 | 回执率 % 前→后 | 缺失 ACK 合计 前→后 |
|---|---:|---:|---:|---:|---:|---:|
| B→D HMAC / 60,000 | 59,863 → 59,996 | 16.977 → 16.438 | 10.26 → 1.40 | 8,544 → 8,160 | 99.771 → 100.000 | 4,896 → 349 |
| B→D HMAC / 80,000 | 76,406 → 79,885 | 17.744 → 17.273 | 27.57 → 19.59 | 9,072 → 8,032 | 96.291 → 99.855 | 116,851 → 17,033 |
| B→D HMAC / 100,000 | 77,564 → 79,378 | 17.762 → 17.512 | 20.99 → 22.14 | 8,560 → 8,112 | 92.187 → 92.350 | 256,246 → 255,310 |
| D→E 通知 / 60,000 | 59,996 → 60,000 | 16.305 → 16.242 | 3.07 → 3.04 | 8,112 → 8,144 | 100.000 → 100.000 | 1,067 → 625 |
| D→E 通知 / 80,000 | 78,382 → 79,805 | 17.441 → 16.876 | 25.91 → 19.48 | 8,656 → 8,528 | 98.290 → 99.755 | 70,281 → 30,572 |


逐轮数据见 [experiments-hmac-eventbus.json](experiments-hmac-eventbus.json)。

## 撤回的方案

HMAC 方案预计算 keyed inner/outer SHA-256 状态，以 Arc 共享；仍逐包验证完整 MAC。
64 个 verifier 的逻辑缓存占用从 11,116 增至 20,332 字节，每条增加 144 字节，已计入原缓存配额。
K（HMAC+通知）在 65k/s、300 秒对照中 CPU/事件 29.978→29.020us，P99 1.63→1.56ms，
但回执率 99.9011%→99.7789%，回执吞吐 64,935.7→64,854.1/s，
采样 RSS 峰值 8,368→8,928KiB，缺失 ACK 19,469→43,107；冷却后均无未处理必需交付。
共享桌面抖动也影响原版，不能把每次损失都归因到代码；但该组合没有通过门槛，未交付。
随后将两项拆开，在原版 replay 上各做三轮 65k/s、20 秒消融：

| 对照 / 发送目标 | 回执/s 前→后 | CPU us/事件 前→后 | P99 ms 前→后 | RSS KiB 前→后 | 回执率 % 前→后 | 缺失 ACK 合计 前→后 |
|---|---:|---:|---:|---:|---:|---:|
| HMAC only (O) / 65,000 | 64,694.2 → 64,570.7 | 29.856 → 29.146 | 1.790 → 1.900 | 8,384 → 8,160 | 99.5292 → 99.3397 | 21,767 → 30,993 |
| 通知 only (P) / 65,000 | 64,804.9 → 64,969.7 | 29.758 → 29.441 | 1.650 → 1.510 | 8,400 → 8,208 | 99.7009 → 99.9535 | 13,155 → 11,444 |


最终撤回 HMAC，缓存内存增加也随之撤回。通知优化随后也因整链路收益不足而撤回。
原型 HMAC 通过 3 种 key × 12 种长度与 fresh MAC 对比、逐字节破坏完整 tag、短/长 tag 拒绝与 clone 隔离验证。
这些测试和修改保存在 [HMAC 实验补丁](hmac-experiment.patch)，没有应用到最终代码。
全部 K 数据见 [7 档容量](candidate-k-capacity.json)、[5 分钟运行](candidate-k-soak.json)，
原版 replay 上的独立对照见 [isolated-final-options.json](isolated-final-options.json)。


- B/E：移除异步入口的同步 replay 锁+RAII 预留显著降低 CPU，但 80k/s 尾延迟恶化。E 的最终三轮对照中 P99 中位数为 4.54→19.00ms，故撤回。
- I/L：补回异步公平入口，并补上“等待入口期间更新 codec profile 必须重新校验”的回归。正确性通过，但 L 的 70k/s 两轮回执率为 99.680%/99.674%，原版 A 为 99.949%/99.914%。按预先声明的稳定门槛撤回。
- C：接收缓冲区和 Box future 回收没有 CPU 收益，80k/s P99 从 21.67→25.83ms，撤回。
- F/G/H/J：每包 spawn、最多四包 join 批处理、周期 spawn、仅补协作预算，没有解决吞吐/尾延迟取舍，撤回。
- M/N：满额时 `try_join_next` 回收完成任务，逻辑上不放宽在途上限，但独立 K→N 实测出现小幅 CPU/吞吐退化，也撤回。

Replay 原型曾通过跨设备接纳、32 个并发重复包、取消/失败回滚、配额与 expiry 恢复的测试。
校验时机回归先在旧候选上失败（错误 ACK），修正后通过；这些都不等于性能通过。
可审查 [Replay 实验补丁](rejected-replay.patch) 和 [JoinSet 实验补丁](rejected-join-reap.patch)；**它们没有应用在当前交付代码中**。

数据分别保存在 [replay 实验](experiments-replay.json)、[最终未通过的 L](candidate-l-capacity.json)、
[replay 独立对照](replay-isolated.json)、[I 的部分容量测量](candidate-i-capacity.json)、
[收发/调度实验](experiments-rejected-io.json)、[JoinSet 实验](reap-experiments.json)。

## 正确性与故障验证

回退后的最终代码重新执行：

- `cargo fmt --all -- --check`：通过。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`：通过。
- `cargo test --workspace --all-features`：608 通过，0 失败，60 ignored；未把 ignored 当作通过。
- Python 测量/工具单测：32 通过，包括 CPU/事件窗口、缺失内核计数、计数器回退和 CPU profile 权重解析。
- UDP Release 回归：22 通过、1 ignored。
- 显式执行三项 ignored 持续测试并通过：`subprocess_graceful_restart_sixty_second_soak`、`mixed_codec_reconnect_and_control_soak`、`engineering_tls_qos1_http_sixty_second_soak`，各约 60 秒。
- `cargo +nightly fuzz run udp_envelope -- -max_total_time=30 -max_len=1201 -rss_limit_mb=2048 -timeout=5`：实际 31 秒、247,541 次运行，无失败。

工作区测试覆盖 required fanout 失败回滚、计数/字节清理、慢 sink 隔离、外部认证和 HTTP sink 故障恢复、
失效栅栏、quiesce、spool 失败保持存活、损坏拒绝、重复重放、SIGKILL 丢失窗口及 planned restart。
所有候选 P 上的验证另存 `validation-p`；最终验证命令、退出码、日志 SHA256 见 manifest。
没有执行全部 ignored 工程基准、全部 fuzz target 或长期生产 soak。

最终保留四线程、32 个并发重复报文回归；同一消息只能产生一个 EventAccepted。
现有测试继续覆盖 10,000 次重复 ACK、失效/轮换栅栏、时钟/旧序号、ACK WouldBlock、准入失败重试、满 sink、资源清理、quiesce 和恢复。

真实慢 HTTP required sink 对照：200/s 目标、8 个设备、窗口 16，15 秒测量；sink 延迟 200ms，测量第 5 秒恢复。
该故障 fixture 在两端同设 sink/global count 上限 128、drain 1 秒、shutdown 10 秒。

| 指标 | A → P（已撤回） |
|---|---:|
| 回执吞吐/s | 144.2 → 140.9 |
| CPU us/EventAccepted | 218.385 → 229.286 |
| P99 ms | 0.22 → 0.24 |
| RSS 采样峰值 KiB | 9,088 → 9,056 |
| 回执率 | 77.8618% → 77.3993% |
| udp_no_ack | 722 → 730 |
| client_window_full | 222 → 270 |

两端积压均到达 128 上限；恢复后冷却积压归零、正常退出，内核满 socket 丢包均为 0。
ACK 仍只表示 EventAccepted，不能把此低 ACK 延迟当作慢 sink 已完成业务处理。
这组结果既验证了背压与恢复，也没有提供保留 P 的性能依据。

独立锁计时诊断在 60k/s 下观测 EventBus 锁平均等待约 0.443→0.401us、平均持锁约 0.020→0.017us；
这些是带计时插桩且有量化误差的均值，不混入容量数据，也不证明长尾锁等待消失。
完整数据见 [诊断与故障](candidate-p-diagnostics.json)。

## 剩余瓶颈与未验证范围

最后候选 P 的 Time Profiler 有效 Running 权重 15,589ms：叶帧 sendto 17.14%、recvfrom 7.13%、
mach_absolute_time 10.01%、psynch_mutexwait 6.66%、SHA256 压缩 6.40%；
with_current_verifier 包含子栈 23.66%，ingest_with_identity 17.15%。
热点仍集中在逐包系统调用、时钟、同步及认证处理；通知优化没有改变这一结构。
这是一份独立采样，不将不同采样窗口占比之差当作吞吐收益。见 [P 采样摘要](cpu-profile-candidate-p.json)。
最终运行时已恢复 A，前述基线 profile 对应其代码。

- 原全局 replay 锁跨 await 尚未消除；本次已实现、验证并撤回不达标替代方案，不能称这一问题已解决。
- UDP 仍为单接收任务、每个 datagram 一次 recv_from、每个 ACK 一次 try_send_to。ACK send 位于凭据失效同步边界内；不能直接移出锁来宣称优化。
- NBI1 已借用 payload/tag；采样中的解析/复制叶帧占比较低，没有为此引入不受验证的新 parser 或 unsafe。
- 没有测 Linux、recvmmsg/sendmmsg、跨机网络、其他硬件或长期生产运行；没有用微基准/短压测宣称生产容量。
- 没有测完整 UDP 路径的分配次数。已测的是 EventBus 的受控分配区域、进程 RSS、以及带签名真实 UDP 回执的整链路表现。

## 重现与证据

[manifest.json](manifest.json) 记录源码/二进制/负载器指纹、环境、验证命令和证据范围。
所有原始日志、配置和 trace 留在忽略目录 `target/performance/udp-20261016` 与 `target/mixed-audit`，
不提交 Instruments TOC/环境、活配置或原始 trace。紧凑 JSON 含每个原始记录的 SHA256；每个数据文件不超过 256 KiB。

```sh
cargo build --release --locked -p netbaiot-server -p netbaiot-loadgen
# Choose fresh per-run names without deleting the original evidence.
python3 - <<'PLAN'
import json, time
from pathlib import Path
out = Path('target/performance/udp-reproduction')
out.mkdir(parents=True, exist_ok=True)
plan = json.loads(Path('docs/performance/udp-20261016/candidate-p-plan.json').read_text())
suffix = str(time.time_ns())
for run in plan:
    run['name'] += '-repro-' + suffix
(out / 'plan.json').write_text(json.dumps(plan))
PLAN
caffeinate -i python3 scripts/perf/device_protocol_benchmark.py \
  --server target/performance/udp-20261016/bin/server-a \
  --candidate target/performance/udp-20261016/bin/server-p \
  --loadgen target/performance/udp-20261016/bin/loadgen \
  --plan target/performance/udp-reproduction/plan.json \
  --baseline a2b46a2ad49a43ee538a6de74afddac350763c23 \
  --label reproduction --output target/performance/udp-reproduction
python3 scripts/perf/udp_capacity_summary.py target/performance/udp-reproduction target/performance/udp-reproduction/summary.json
```

命令重现的是已撤回的 P 原型，使用本机已冻结的二进制。当前源码的运行时是 A。
新 checkout 需要分别构建基线和应用通知实验补丁后的 P，并核对 SHA256，不能用同一二进制冒充 A/B。
本次没有提交、推送或合并代码。
