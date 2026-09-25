# Business RPC V2 可靠性与负载门禁

本轮基于 `583bb0d` 开始；附件所列的 `ef8ee53` 是前一个 Business RPC 提交，二者之间已有独立的 MQTT Device Profile 提交。本门禁不把开发机的瞬时吞吐当成生产容量。正确性测试判定 PASS/FAIL，性能数据只记录观测值。

## 授权边界

- `connection_epoch` 由 BusinessRpcRegistry 的 provider 租约生成，只用于阻止旧连接的 Response、Cancel 和 Drop 操作触及新 provider。注册后先 Syncing，完整 `auth.sync(reset)` 清理本地缓存、在线 session 与 MQTT 持久会话；收到业务端的确认 Ping 后才 Serving。
- `AuthCache epoch` 是本地失效栅栏。认证或 verifier miss 开始时记录 epoch；即使旧请求带着数值更高的 `auth_revision` 返回，只要其 future 跨过本地失效边界，就不能写缓存、返回成功或注册 session。Ingress 在同一 `auth_registration` 门下检查 candidate 并注册 session。
- `auth_revision` 属于业务权威。顺序修订执行后才推进，重复修订返回零变更，较低修订不回退。缺口或 incarnation 变化关闭 Serving、清理在途 RPC、全域失效并要求 reset sync。新权威 incarnation 的修订可从较小正数重新开始；`mark_syncing` 将旧租约的当前修订清零。
- 离线宽限从 provider 退出 Serving 的单调时钟时刻开始。状态通知携带转变时刻与递增的状态序号；即使 Serving 和断线两次通知在 watcher 读取前合并，或在同一时钟 tick 内发生，也不会沿用旧 deadline。连续的非 Serving 更新不重启宽限。零表示立即全域失效；有限宽限在精确 deadline 到期，不靠周期轮询。`invalidate_if_offline` 在 registry 状态锁下验证完整状态快照，并执行完整失效；旧定时器与新 Serving 或同代际 reset sync 同时 ready 时不会清理新的授权状态。断线期间新 miss 一律 Unavailable；未过期的旧正缓存仅在尚未到期的非零宽限内可继续使用。

Business RPC、AuthCache、Ingress 的锁顺序为 registry state → auth-registration gate → AuthCache → Sessions → MqttBroker。现有控制请求不会在持有后四个锁时回取 registry state。失效、sync 与新 Serving 的顺序由同一状态边界检查。

## 生命周期与过载

Registry 的 pending 先于 outbound enqueue 登记，count/byte permit 由 PendingGuard 和队列帧 RAII 持有。取消、超时、断线、sync 和修订推进会删除 pending；迟到响应仅计入固定名称的指标。错误方法响应忽略并允许后续正确响应使用同一 request ID，直到总 deadline 到期。SDK 退出连接时先终止 handler worker，再等待 worker、reader 和 writer task 实际结束；旧 `BusinessDelivery` 仅保存旧 writer，无法向新连接发 ACK。

认证、控制和事件各自有界。事件窗口仍为 1；慢 ACK 期间认证可独立进展。超过 pending count、byte 或队列预算立即返回 Overloaded，不建隐蔽等待 task；只有明确的 DeviceRejected 才能负缓存。帧长度先检查后分配，解析 fuzz target 保持原有版本。

## 负载工具

工具位于 `tools/netbaiot-loadgen/src/bin/business_rpc.rs`，运行方式：

```sh
cargo run --locked --release -p netbaiot-loadgen --bin business_rpc -- configs/business-rpc-load.example.json
```

在 JSON 中选择 `auth`、`multiplexed`、`reconnect`、`verifier` 或 `consumer_outage`。开发模式只连回环地址，业务 token 由 `NETBAIOT_BUSINESS_RPC_TOKEN` 提供；管理指标采样另需 `NETBAIOT_ADMIN_SECRET`。配置文件不包含密钥。服务端须为 Business RPC V2 开启 `business_tcp`、`device_auth: business_rpc`、`event_delivery: business_rpc`，并使用可识别 `demo/sensor` 的 JSON codec。`gateway_pid` 可选；Linux 读取 `/proc/<pid>/status`，macOS 使用 `ps`，不可用时 RSS 为 null。`management_url` 可选，采样固定名称的 pending/queue/同步/迟到指标。

- `auth`：有界并发真实 MQTT CONNECT，触发远程 `device.authenticate`；用固定 32 个设备和 ClientId 避免压测器自身填满持久会话表。若要测纯远端认证率，临时网关配置须采用短正缓存 TTL，并同时查看 `auth_provider_calls`。`auth_handler_delay_ms` 可为饱和测试延迟业务认证 handler，最大 5 秒。
- `multiplexed`：同一业务 socket 上 ACK 延时、事件发布与认证压力；`event_reconnect_every_secs` 可周期性关闭并 reset sync，报告 EventAck 和认证延迟。
- `reconnect`：每轮 reset sync、一次设备认证和正常关闭；`reconnect_cycles` 与 `duration_secs` 同时为下限，`reconnect_pause_ms` 控制节奏。设置 300 秒、300 次、1000 ms 即可做五分钟以上手工 soak。
- `verifier`：同一 UDP boot ID、递增序号、真实 HMAC 与 NBA1 ACK；中途 `auth.invalidate(Device)`，分别记录失效前和总 provider lookup。事件由唯一 ACK 循环消费，避免事件出口压力掩盖 verifier 命中。
- `consumer_outage`：前半段不 ACK，后半段恢复 ACK，按本次运行的 source ID 前缀统计重投与稳定 `event_id`。若 ACK timeout 大于故障时段，应延长 `duration_secs`，使测试跨过实际重试时刻。

每个场景输出 JSON，内嵌无密钥的运行配置；直方图以 10 μs 桶覆盖 100 ms 内延迟，之后用 1 ms 桶覆盖至 60 s。`publish_enqueued` 只表示设备 SDK 的有界出站队列已接纳发布请求，不代表 MQTT PUBACK 或网关 EventAccepted；事件出口以 `event_acks` 计数。`requests_per_second` 使用包含恢复等待的完整进程时长作分母，是真实运行平均值。RSS 包含 baseline、warm、peak、workload 结束、恢复等待后的数值。采样可能漏掉极短峰值；峰值是采样峰值。累计网关计数带 `_total`，跨场景运行时应取差值。工具没有 TLS/mTLS 压测选项，生产链路性能仍需单独测。

## 正确性证据

| 不变量 | 自动测试 |
| --- | --- |
| 本地 epoch 与较高远端修订独立，Device/All 失效后旧 auth 不能复活 | `local_epoch_rejects_high_revision_response_after_device_and_all_invalidation`、`delayed_business_rpc_auth_cannot_register_after_device_invalidation` |
| verifier 旧响应不得接纳 datagram 或污染缓存 | `stale_verifier_cannot_validate_datagram_or_poison_cache` |
| 旧租约 cleanup/Response 不影响新租约 | `stale_lease_cleanup_and_response_cannot_touch_reconnected_provider` |
| gap 关闭 Serving 并 reset；重复修订幂等 | `one_socket_authentication_progresses_while_event_ack_waits` 子进程用例 |
| 零宽限、29,999/30,000 ms、合并通知与重连边界 | `business_auth_zero_grace_invalidates_without_clock_advance`、`business_auth_grace_deadline_and_reconnect_are_generation_fenced`、`collapsed_serving_disconnect_starts_grace_at_actual_disconnect`、`offline_invalidation_and_new_serving_share_one_generation_boundary`、`old_offline_timer_cannot_revoke_a_new_sync_on_same_provider_epoch`、`zero_offline_grace_revokes_live_session_and_requires_reset_sync` |
| count/byte permit 及连接代际回收 | `queue_and_byte_overload_release_all_admission_permits`、`syncing_and_revision_advance_reclaim_pending_and_fence_late_responses`、`one_hundred_reconnect_generations_and_shutdown_release_driver`、`old_delivery_ack_cannot_use_replacement_writer`、`old_auth_handler_is_cancelled_before_replacement_writer_is_ready` |

## 实测与边界

原始 JSON 位于 [`docs/performance/business-rpc-v2/`](performance/business-rpc-v2/)。环境为 macOS Darwin 25.6.0、arm64、Rust stable 1.97.1；网关和工具均为 **dev profile**，同机回环明文，单个网关进程，管理指标每 250 ms 采样。普通场景的网关 `auth_max_inflight=16`、正缓存 TTL 30 秒、sink ACK 超时 8 秒；认证饱和场景单独重启网关，`auth_max_inflight=2`、正缓存 TTL 2 ms、负缓存 TTL 1 ms。业务 handler 延迟和场景并发在 JSON 的 `config` 中记录。五分钟 soak 开始时的工具版本尚未在 JSON 中内嵌配置；其实际参数为 `duration_secs=300`、`reconnect_cycles=300`、`reconnect_pause_ms=1000`、`auth_concurrency=4`、`warmup_secs=10`、`recovery_secs=5`、`sample_period_ms=250`。

下表 `requests_per_second` 是包括恢复等待及未完成 SDK 连接收尾时间的进程平均值。认证延迟是 **设备 SDK 发起 MQTT CONNECT 至连接结果** 的端到端时间，可能包含 SDK 重试；verifier 延迟是 UDP 发送至 NBA1 ACK。它们不是纯 Business RPC 帧往返延迟，也不是生产容量。

| 场景及原始结果 | 配置时长 / 实际时长 | 并发 | 请求成功 / 总数 | 请求/秒 | p50 / p95 / p99 / max (ms) |
| --- | --- | ---: | ---: | ---: | --- |
| [认证校准](performance/business-rpc-v2/auth.json) | 5 / 9.01 s | 4 | 73 / 73 | 8.10 | 47.50 / 1255 / 7877 / 7877 |
| [近上限](performance/business-rpc-v2/near_capacity.json) | 20 / 23.26 s | 2 | 169 / 169 | 7.27 | 252 / 352 / 407 / 414 |
| [超上限](performance/business-rpc-v2/saturation.json) | 30 / 41.76 s | 32 | 160 / 230 | 5.51 | 2346 / 10003 / 10004 / 10004 |
| [重连 soak](performance/business-rpc-v2/reconnect_soak.json) | 300 / 307.27 s | 每轮 1 | 300 / 300 | 0.98 | 2.08 / 2.68 / 2.91 / 3.04 |
| [UDP verifier](performance/business-rpc-v2/verifier.json) | 5 / 6.26 s | 顺序 10/s | 51 / 51 | 8.14 | 1.05 / 1.22 / 1.82 / 1.82 |
| [多路争用](performance/business-rpc-v2/multiplexed_soak.json) | 60 / 66.76 s | 认证 4；事件 5/s | 664 / 670 | 10.04 | 46.24 / 1758 / 7810 / 10004 |
| [消费端故障](performance/business-rpc-v2/consumer_outage.json) | 20 / 26.01 s | 认证 4；事件 2/s | 252 / 252 | 9.69 | 38.39 / 2092 / 6537 / 7880 |

| 场景 | pending 峰值（项/字节） | 队列峰值（项/字节） | RSS baseline / warm / peak / post-soak / recovery (KiB) |
| --- | ---: | ---: | --- |
| 认证校准 | 0 / 0 | 0 / 0 | 15792 / 15824 / 15824 / 15824 / 15824 |
| 近上限 | 2 / 394 | 0 / 0 | 11440 / 13776 / 13888 / 13888 / 13888 |
| 超上限 | 2 / 394 | 0 / 0 | 15696 / 15760 / 15792 / 15792 / 15792 |
| 重连 soak | 0 / 0 | 0 / 0 | 11632 / 13904 / 14192 / 14192 / 14192 |
| UDP verifier | 0 / 0 | 0 / 0 | 14432 / 14432 / 14448 / 14448 / 14448 |
| 多路争用 | 0 / 0 | 0 / 0 | 14496 / 15120 / 15200 / 15200 / 15200 |
| 消费端故障 | 0 / 0 | 0 / 0 | 15216 / 15232 / 15248 / 15248 / 15248 |

近上限运行时 pending 峰值恰为配置上限 2；超上限运行时仍为 2，网关 `business_rpc_overloads_total` 从 0 增加到 444，运行后 pending 项/字节和 active business connection 均为 0。32 并发客户端有 70 次连接失败，设备 SDK 将部分网关拒绝报告为 `ServerUnavailable`，因此工具基于客户端错误文字分类的 `counts.overloaded=0` **不能代表网关未过载**。该场景的明确过载证据是网关计数器和有界 pending。认证校准在同一个网关稍后运行，累计过载值 446 不能归给该校准场景。250 ms 采样漏掉短暂队列占用时会记录 0，精确资源清理由确定性单元测试判定。

`auth_provider_calls` 计的是远端 handler 调用，`counts.requests` 计的是工具外层 SDK 连接尝试；SDK 内部重试可使前者大于后者。累计网关指标跨场景继续增长，只有同一网关进程内的前后差值能归给单场景。

五分钟 soak 进行了 300 次 reset sync、300 次远端认证且均成功，`provider_sync_failure_total=0`，结束时 pending 为 0、active business connection 为 0。普通场景在 30 秒离线宽限后观测到一次 grace expiration；下一次连接完成 reset sync 后 verifier 的冷/热/失效/重查序列为失效前 1 次远端 lookup、失效后累计 2 次，51/51 UDP ACK 成功。

多路争用在同一业务 socket 上使用 100 ms EventAck 延迟和每 10 秒重连，共完成 5 次重连、281 次事件应用 ACK；EventAck p50/p95/p99/max 为 103/104/105/105 ms。6 次认证连接失败和 19 次 SDK 发布入队失败发生在重连/压力期间；成功的 `publish_enqueued` 不证明网关接纳。消费端前 10 秒不 ACK，后半恢复，收到 39 次投递、37 次 ACK 和 2 次 `event_id` 不变的重试。两场景的队列与 pending 在采样结束时均归零。

本地观察不能证明更长时间、不同硬件、mTLS 或真实业务 handler 的容量；本轮未量化 CPU 利用率，也未运行跨机器负载。计划重启仍按 EventBus/spool 规则，突然 SIGKILL 对未入 spool 的内存事件不保证不丢。尚无 Command RPC、业务端离线命令队列或事件窗口扩展。`rustls-pemfile` 的既有维护告警仅记录，本轮不变更依赖。

## 验证记录

以下命令在最终代码上执行并通过：

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features --quiet
cargo +1.88.0 clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --workspace --all-features --quiet
python3 tests/mqtt_conformance/run.py --netbaiot-only
cargo +nightly fuzz run business_rpc_v2 -- -runs=10000
cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

MQTT conformance 为 31/31；Business RPC fuzz 处理 10,000 次输入无崩溃；推送版本上的重启 soak 用时 64.41 秒通过。普通全量测试保留原有按用途 `ignored` 的测量用例，已单独执行上述 60 秒用例。负载测量使用本节列出的真实 loopback 进程，未运行 mTLS、跨机器负载或更长的多小时 soak；没有以这些未运行项推断生产容量。
