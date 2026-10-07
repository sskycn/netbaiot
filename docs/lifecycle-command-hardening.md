# Lifecycle、命令状态与资源边界加固报告

> Historical implementation/measurement evidence from before the current-only cleanup (baseline `21a6945`, 2026-10-07). Commands, old protocol names and compatibility claims below describe their recorded revisions. Current support and upgrade instructions: [protocol matrix](protocol-support.md), [breaking change](migration/current-protocol-only.md).

日期：2026-10-05。审计基线：`ef8d54c`，工作区版本 `0.2.2`。
本报告记录本次修复和本机实测，不宣称生产容量、崩溃持久性或业务 exactly-once。
先完成第 1—5 项并通过完整 fmt/clippy/test，再执行第 6—8 项。

## 1. 控制面 mutation 与计划关机

[问题] HTTP 与 Business RPC 的权威状态修改可能越过 quiesce，导致成功撤销的 MQTT session 又从较早的快照恢复。

[根因] 原有 auth-registration 锁解决鉴权撤销与注册的竞争，但没有把 mutation 纳入 Lifecycle 的 active admission。另对 state/count 两个独立原子量仅使用 Acquire/Release，不足以证明关闭方和进入方不能同时读到对方旧值。

[修改] HTTP control/routes、共享 V2/V3 auth.sync/auth.invalidate worker、后台 provider 撤销、command send 使用 AdmissionGuard。已有 guard 可通过 admitted helper 完成操作，避免 quiesce 后嵌套申请误拒绝已准入的工作。guard 覆盖 revision/confirmation 发布；拒绝的 sync 不修改 serving/confirmation。入场计数、二次状态检查、关闭转移使用 SeqCst 总序。

[为什么安全] 保留生命周期和原有锁顺序：auth-registration → AuthCache → Sessions → MqttBroker。guard 不持有 mutex；没有同步锁跨 await。只读诊断继续服务，drain 不走 mutation admission，重复请求保持既有行为。快照前等待已准入工作；关闭后请求明确拒绝。

[测试] `management_mutations_close_but_diagnostics_and_drain_remain_available` 覆盖四个 HTTP mutation 的 Running 成功、Quiescing 503，以及诊断/drain；`quiesce_waits_for_invalidation_before_committing_mqtt_recovery` 用可控阻塞 finalizer 和真实 recovery 文件验证两种合法顺序；`shared_v2_v3_control_worker_rejects_sync_and_invalidation_after_quiesce` 验证 V2/V3 实际共用 worker、Unavailable 和 authority 状态不变。

[结果] 上述回归通过。未新建 V2/V3 各自的 wire race hook；共享 worker 单测与两套真实 RPC 集成测试共同覆盖。

## 2. MQTT/TCP 注册与 quiesce

[问题] 已完成认证的 candidate 可能在关闭 admission 后才注册成功。

[根因] authenticate 后的一次状态检查不能覆盖最终 freshness check、Sessions 注册与 broker attach。

[修改] `Ingress::register_session_with` 自行申请 guard；MQTT 3.1.1、MQTT 5、TCP 显式持有 guard，调用 admitted helper，并持续到有超时上限的成功握手写入及 Will/command readiness 设置结束。

[为什么安全] auth-registration 同步边界、incarnation/generation 检查和 RAII attachment/Will owner 保留。失败、取消和早退均释放 guard。关机可能等待一个已开始握手的 write timeout，这是有界等待。

[测试] `authenticated_candidates_cannot_register_after_quiesce` 先完成 candidate，再关闭 gate，分别尝试 MQTT 3.1.1 attach、MQTT 5 attach 和 TCP 注册；确认无 live/persistent session。原有撤销、接管、CONNACK 失败、Will 和外部客户端回归继续运行。

[结果] 回归通过；不会通过 gate 创建关闭后开始的新 session。已经准入的握手允许完成，不承诺设备在计划关闭期间继续保持在线。

## 3. Command expiry 与短期 dedup

[问题] 未发送的命令过期后，指标和重复 CommandId 的回答仍可能停在 Queued。

[根因] dedup 保存最初 dispatch；transport 跳过过期项而未发布状态。去重保留期可能远长于命令有效期。

[修改] 每次 dispatch 增加固定大小 `Arc<CommandProgress>`，共享于有界 dedup、connection queue 和 MQTT QoS 状态。Queued 原子转为 Dispatching 或 Expired；写完为 Sent，精确 MQTT ACK 为 Received，显式失败为 Failed。过期只发生在尚未开始传输的 Queued，计入现有低基数 CommandFailed，且只计一次。TCP/两版 MQTT、broker handoff 和 dequeue 均发布状态。每次新 dispatch 使用独立 Arc，隔离 dedup 淘汰后同 ID 的迟到更新。

[为什么安全] 不改公共 wire enum、不持久化 command history、不自动重发业务命令。已有 MQTT QoS exchange 仍可恢复：失败写入不证明设备未收到，后续成功写入/精确 ACK 可更新 Failed；Received/Expired 不被迟到失败覆盖。Sent、Received 均不表示设备执行成功。首次实际 dispatch 失败会移除 dedup 预留，保留原来的显式重试规则；已经接受后异步 Failed 的 duplicate 返回已知状态，不另发一次。

[测试] 短 expires_at/长 dedup TTL、过期指标一次、重复 ID 最新状态、不同 payload Conflict、失败 admission 再试、dedup 淘汰隔离、已发送不被 TTL 改为 Expired；两版 MQTT 精确 ACK/错误 ACK、QoS1/2 写失败后重连、broker 交接时到期与资源回收。

[结果] 回归通过。自审发现并修正了 Failed 后成功 QoS 恢复的状态更新，以及 QoS0 在 begin-transfer 后再次检查时钟可能误丢包的边界：所有 TTL 放弃均发生在传输开始之前。取消中的写入仍可能是 Dispatching/Failed，属于不确定结果，调用方不得据此盲目 retry。

## 4. SessionEndpoint accounting

[问题] `expires_at=None` 可在 queued 增加后提前返回，遗漏归还计数。

[根因] 可失败的纯 validation 放在资源 mutation 之后。

[修改] 在申请 slot、byte permit 和增加 queued 前验证 expiry。`try_send` 失败仍由 QueuedCommand Drop 归还全部所有权；Drop 同时终结未发送命令状态。

[为什么安全] 不改变现有 count/byte 上限或队列类型，没有增加等待任务。构造顺序保证每次失败要么尚无资源，要么有 RAII owner。

[测试] `enqueue_validation_and_every_reservation_failure_roll_back` 覆盖缺失 expiry、channel full、receiver closed、connection/tenant/process bytes 和 slots 耗尽，检查 queued、bytes 与 permits 恢复。

[结果] 通过；未发现本次路径残留计数或 permit。

## 5. HTTP Auth Provider 服务身份

[问题] HTTPS 保护传输，但原请求没有 gateway 独立服务身份。

[根因] authenticate/resolve_verifier 的 request builder 未添加服务认证。

[修改] 可选环境变量 `NETBAIOT_AUTH_PROVIDER_TOKEN`，统一 request helper 为两种请求添加 Bearer header。校验非空、最多 4096 ASCII 可见字节；HeaderValue 标记 sensitive。非 loopback 无 token 输出固定警告。

[为什么安全] 保持 HTTPS/loopback HTTP、no_proxy、禁重定向、并发/响应/超时限制。可选配置保留现有部署兼容性。token 不进入 JSON config、Debug、日志、指标或 recovery。未引入 mTLS 或新协议依赖。

[测试] `http_auth_provider_authenticates_both_authority_requests_and_redacts_token` 通过实际 HTTP 捕获两种请求，验证 header、有/无 token、request Debug/error 脱敏；`http_auth_provider_token_validation_preserves_transport_policy` 验证无效 token 和 URL 策略。

[结果] 通过。生产应配置 token；loopback 开发仍可不配。

## 6. UDP ReplayWindow

[问题] 每个合法 datagram 做 HashMap retain 和按设备/租户全表计数。

[根因] expiry 与 quota 没有独立索引，常见 replay check 为 O(N)。

[修改] 保持原 ReplayKey/map，增加 BTreeSet expiry 索引、device/tenant 计数。刷新替换旧 expiry 节点，每个 live entry 恰有一个节点；回收只遍历已过期项，并删除归零计数。没有任务、惰性 tombstone 或不断增长的 heap。

[为什么安全] 单 receive-loop 的 check → EventAccepted → commit 顺序不变；global/tenant/device admission 上限继续约束所有索引。identity 长度有界，额外内存是有界元数据。相同版本/boot/seq 的合法 HMAC 重传仍重新 ACK，不引入 payload hash。

[测试] 冻结 `ef8d54c` 旧实现仅作测试 oracle。10,000 次差分轨迹包括时间倒退、轮换、重复、过期和容量边界；20,000 次热刷新证明 expiry 节点不积累。所有 UDP ACK/压力/鉴权测试运行。

[结果] release 7 组交替顺序配对、每组 20,000 操作的中位数如下（ns/op）。这些是本机微基准，不是 UDP PPS。

| entries | duplicate 旧→新 | 新 key check 旧→新 | 已有 key check+commit 旧→新 |
|---:|---:|---:|---:|
| 0 | 18→18 | 16→15 | 81→205 |
| 64 | 80→39 | 564→53 | 112→165 |
| 256 | 212→38 | 2258→52 | 257→176 |
| 1024 | 746→39 | 9773→53 | 786→187 |

depth=0 的 duplicate 列实际为空窗口 check 基线。check+commit 每次推进时钟，强制 expiry 索引刷新；小窗口更慢是真实代价。check 的 HashMap 查询/计数均摊 O(1)，树最小项查询/刷新存在树高成本，批量回收 O(k log N)，不声称严格 O(1)。额外索引 RSS 未单独测量。原始数据已从 Git 清理；路径、大小和 SHA-256 见[性能证据归档清单](performance/archive-manifest.json)，环境信息见 [environment.json](performance/lifecycle-hardening/environment.json)。

## 7. EventBus retry queue：测量与保留决定

[问题] `take_ready`/`next_ready_delay` 在 mutex 内扫描 retry backlog。

[根因] 同一个 VecDeque 同时承担 pending ownership 和 ready 选择。

[修改] 运行已有 `eventbus_queue_depth_probe`，保存可重现数据；本次不修改 EventBus 生产代码。用户允许在无法充分证明重构安全时交付 benchmark/设计。

[为什么安全] required sink 的全目标预留/提交、stable event_id、inflight、drain/spool/replay 及 byte accounting 原样保留。

[测试] 现有 EventBus 全套测试（原子回滚、慢 sink 隔离、panic、timeout、retry、spool/restore、count/bytes）和 release probe。每个 depth 1,000 次采样，表中单位 ns，依次 p50/p95/p99。

| depth | take_ready | next_ready_delay | complete |
|---:|---:|---:|---:|
| 0 | 292/334/375 | 209/250/417 | 708/792/917 |
| 1000 | 1208/1459/1500 | 5292/6292/6375 | 750/958/1041 |
| 10000 | 4375/5125/5208 | 25000/29250/30584 | 375/458/500 |
| 16383 | 6292/6541/8292 | 36583/39584/45042 | 292/375/459 |

[结果] 确认扫描成本随深度增长，未证明新实现的收益或生产吞吐。清理前原始数据的路径、大小和 SHA-256 见[性能证据归档清单](performance/archive-manifest.json)。

后续设计应把唯一 DeliveryRecord owner 放在有界 slot arena，ready FIFO 与 deadline 有序索引仅引用 slot/generation。queued、delayed、inflight 必须互斥；deadline 索引每个 delayed owner 恰有一项，取消/ACK 删除它，不能无限积累 stale heap 节点。顺序应明确采用 deadline 加稳定 sequence，并证明与当前 retry fairness 的兼容性。完整迁移前必须验证：

1. admission 在确定性 SinkId 顺序下先预留所有 required 容量，再统一提交；index 内存计入资源上界。
2. attempt、jitter、max_age 使用既有规则，blocked deadline 不阻止其他 ready work。
3. worker 停止/取消/panic 后 owner 可被唯一回收；spool_records 遍历所有未 ACK owner，包括不确定 inflight。
4. spool/recovery 保留 event_id、pending sinks 与 attempt metadata；恢复 deadline 按既有语义重建，不增加双重 ownership。
5. 测试负载下比较锁等待、p99、慢 sink 隔离、RSS、drain 时长和故障回滚，证明收益后再替换。

## 8. MQTT recovery 锁：一致性与内存优先

[问题] 流式 recovery 写入期间持有 broker mutex，慢磁盘延长锁时间。

[根因] 同一 coherent view 覆盖 session、retained、Will、QoS、incarnation 和 accounting。简单解锁需要复制或转移全部 ownership。

[修改] lifecycle fence 完成后，核实服务 shutdown 顺序为 quiesce 等待 → 停 listener → join device/maintenance owner → MQTT commit。保留现有锁，补充 tradeoff 注释；commit 仍由 spawn_blocking 执行，单 record scratch 有界。

[为什么安全] 不复制整个 broker payload，不破坏 NBMQ v6 末尾 record count/byte count/digest，不改变 fsync、rename、目录 fsync、失败后保持 alive/unready 的策略。此结论依赖 server shutdown 调用顺序，不是允许任意调用者一边写 snapshot 一边变更 state。

[测试] 现有 corruption/semantic invalid、旧格式兼容、Will/QoS2/takeover、子进程计划重启、spool failure 和 SIGKILL；另运行 10/50/100 MiB streaming recovery benchmark。

[结果] 本机逻辑 payload 10/50/100 MiB 对应文件 10,501,150 / 52,505,702 / 105,012,162 bytes；commit 78/200/388 ms，decode 39/185/369 ms，最大 record 72,320 bytes。不是 mutex hold time 的独立测量，也未测此运行的 RSS 峰值或慢磁盘尾延迟。清理前原始数据的路径、大小和 SHA-256 见[性能证据归档清单](performance/archive-manifest.json)。未来 ownership-transfer 方案必须先证明冻结 writer、Will/QoS 完整性和峰值内存，当前不做复杂重构。

## 9. 文档与兼容性

[问题] AGENTS 中 MQTT5 out-of-scope 和部分 NBMQ v3/v4 描述落后于源码。

[根因] 当前项目已包含有明确边界的 MQTT5、NBMQ v6 和 Business RPC V3，而一些运行指南沿用早期文字。

[修改] 同步当前 AGENTS、restart/control/API/RPC/auth/delivery/resource 文档。明确 NBMQ v6 写入、v1–v5 只读兼容；MQTT5 按 `docs/mqtt.md` 定义的已实现范围；RPC V3 flow control/generation/GoAway/readiness 未修改。历史报告保留历史测量语境。

[为什么安全] 无 wire version/crate version/配置 schema 改动，无数据库、unsafe、无界 channel、新 runtime 服务或持久 command history。新增 tracker 只在有界 runtime state 中存在，不进入 NBMQ/业务 spool。

[测试] 对照源码版本常量、当前文档与序列化/协议回归；编译检查公共 crate 分层。

[结果] 本次修改同步完成。README、quick-start、demo 及其他新产品文档有并行工作进行，本次不提交或覆盖这些无关修改。

## 验证、失败记录与限制

第一阶段完整工作区测试：389 passed / 0 failed / 14 ignored。
第二阶段第一次完整检查：391 passed / 0 failed / 15 ignored；最后自审又补充两个 regression，stable 与 Rust 1.88 最终均为 **393 passed / 0 failed / 15 ignored**。MSRV fmt/clippy 同样通过。全部命令、耗时、阶段结果与中途失败计数见 [validation.json](performance/lifecycle-hardening/validation.json)，新增 13 个常规 regression 和 1 个 ignored benchmark 的完整名称见 [new-tests.json](performance/lifecycle-hardening/new-tests.json)。

已执行：

- `cargo fmt --all -- --check`；`cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`；`cargo test --locked --workspace --all-features`。
- 第一阶段和自审中的定向 command、enqueue、lifecycle、RPC、HTTP provider、UDP 检查；其功能用例均包含在最终工作区套件中。
- `cargo build --locked -p netbaiot-server`；`python3 tests/mqtt_protocol_regressions.py --repo . --output ...`：7/7。
- `python3 tests/mqtt_conformance/run.py --release-gate --no-build`：76/76，normative coverage 125/125；包括 raw state machine 与 Mosquitto 外部参考/客户端。
- `python3 tests/mqtt_conformance/v5_smoke.py` 与 `v5_mosquitto.py`：通过。
- 构建 SDK `device_mqtt` example，`tests/run_device_profile_mosquitto.py`：3.1.1/5 TCP/TLS 与各 5 次 persistent reconnect 通过。
- `tests/measure_device_profile.py`：通过，本次 SDK probe idle/saturated RSS 4656/8416 KiB、均 1 owned task、11 OS threads，仅是该测试条件的快照。
- `subprocess_graceful_restart_sixty_second_soak --ignored --nocapture`：1/1，64.10 秒；不是长期 soak。
- 三个 release 微基准：`replay_hot_path_probe`、`eventbus_queue_depth_probe`、`mqtt_recovery_streaming_benchmark_manual`，均通过。
- `fuzz/seed_corpus.py` 后，11 个现有目标各 `cargo +nightly fuzz run TARGET -- -runs=10000 -timeout=5 -max_len=131072`：mqtt_device_profile、mqtt_packet、mqtt_v5_packet、mqtt_state、mqtt_recovery、udp_envelope、tcp_frame、restart_spool、business_stream、business_rpc_v2、business_rpc_v3，全部退出 0，ASan 默认开启。
- `cargo audit --json`：198 个依赖、0 known vulnerabilities，RustSec database commit `ef6173cbc5c50ec8166f9a5b28f07834144373ee`；1 个维护状态提示 `RUSTSEC-2025-0134`，rustls-pemfile 2.2.0 停止维护。未把这个 warning 隐去，也未将其描述为已知漏洞。

过程中出现过的失败：

- sandbox 禁止 loopback socket bind，导致 JWKS/UDP 等网络测试 PermissionDenied；在授权的本机网络执行环境重跑通过。
- 环境代理 `127.0.0.1:1080` 不可用导致依赖下载失败；仅测试子进程清除代理环境后重试成功，没有修改用户代理配置。
- 新 fixture 缺少 JsonV1 codec，以及旧断言仍要求 duplicate 永远 Queued：分别修正 fixture 和更新为最新已知状态语义。没有跳过功能用例。
- `one_socket_authentication_progresses_while_event_ack_waits` 曾有一次 teardown 后状态断言失败；隔离重跑及随后完整套件通过。尚未证明根因，记录为时序敏感测试待观察，不能把重跑当作不存在问题的证明。
- stable Rust 1.99 对 `Atomic::fetch_update` 新增 deprecation warning；保留 Rust 1.88 可用 API，在对应方法加窄范围 allow 与 MSRV 注释。常规 clippy 问题已修正，不全局禁用警告。

未执行：数小时/数天 soak、生产规模连接/负载、UDP 新索引单独 RSS/allocator 测量、慢磁盘真实断电、跨 OS/架构 release build、完整 fuzz 长期 campaign。现有服务器子进程测试确实执行了 SIGKILL、spool 写失败后修复和 replay，但不能替代物理介质断电测试。其余 ignored broker hotspot/near-default-capacity probes 不属于本次改动路径，未全跑。未发布 tag 或 release。

## 故障视角自审与残余事项

已检查修改路径的关闭中途、鉴权撤销/失联、session takeover、迟到 ACK、重复 packet、写超时/取消、channel/bytes/slots 耗尽；结合现有 required fanout rollback、slow sink、sink panic、损坏 recovery/spool、子进程 disk failure 测试。无新的无界资源、锁顺序反转、任务每命令、静默 ACK 或命令执行成功承诺。

本次范围内没有尚未解决且已确认的 P0/P1。此结论不是对全仓库或所有调度交错的形式化证明。非阻塞后续项：EventBus 深 backlog 扫描、recovery 慢磁盘延迟测量、UDP 索引额外内存量化、上述时序敏感测试的确定性复现、rustls-pemfile 向 rustls-pki-types PEM API 的独立迁移。SeqCst fence 给出内存序约束，但本次没有另加 Loom 模型。

建议在现有 `0.2.2` 后发布 **0.2.3** patch；本次仅给建议，不改版本或发布。Changelog 草稿：

- Fix control/auth/command mutations and MQTT/TCP registration crossing planned shutdown admission.
- Track bounded, process-local command expiry and latest transport receipt state without automatic command retries.
- Fix command queue accounting on invalid expiry and failed reservations.
- Add optional `NETBAIOT_AUTH_PROVIDER_TOKEN` for both HTTP authority methods.
- Index UDP replay expiry and quotas while preserving authenticated duplicate ACK semantics.
- Preserve MQTT 3.1.1/5, NBMQ v6 recovery and Business RPC V3 behavior; add race, failure, accounting and interoperability regressions.
- Document measured retry/snapshot tradeoffs and remaining measurement limits.

## 修改文件

文件清单见 [changed-files.txt](performance/lifecycle-hardening/changed-files.txt)，仅列本次提交内容；并行 README/产品文档/demo 不在其中。原始详细测试日志保留于本机 `target/hardening-validation/`，摘要和测试命令随 [performance/lifecycle-hardening](performance/lifecycle-hardening/) 交付。
