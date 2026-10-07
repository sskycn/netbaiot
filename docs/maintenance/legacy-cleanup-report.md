# Legacy Protocol / Compatibility / Dead-Code Cleanup

日期：2026-10-07。先完成第二轮优化，再独立执行本次清理。清理不改变当前业务 wire、MQTT QoS 或当前恢复写入格式，不引入新的迁移器、外部 broker、数据库、锁模型或离线命令队列。

## Baseline 与最终协议矩阵

基线为 `21a694576a84436c1d27a69f59ea1d1a32e5006b`，对应[第二轮优化报告](../performance/second-round/report.md)。删除之前完成全仓依赖/入口/消费者审计、fmt/check/strict clippy/locked workspace tests/xtask 和 release 构建；冻结实际 release 二进制。基线四个 Actions 与 Native recovery/DX 三平台均 PASS，见 [baseline-ci.json](legacy-cleanup/baseline-ci.json) 和[入口分类清单](legacy-cleanup-inventory.md)。

| 独立命名空间 | Before | After |
|---|---|---|
| Business stream/RPC | Stream V1、RPC V2、RPC V3 | 当前 Business RPC V3 only |
| MQTT 恢复文件 NBMQ | JSON/v1、v2–v6 | 当前 NBMQ v6 only |
| EventBus restart spool NBSP | v1、v2、v3 | 当前 NBSP v3 only |
| MQTT 设备协议 | 3.1.1、5.0 | 3.1.1、5.0 保留 |
| 管理 HTTP / 设备 JSON / topic namespace | `/api/v1` / JSON 1 / `v1/t/...` | 保留 |

NBSP v3 和已删除的 NBMQ v3 是不同格式。版本 header 仍存在，用于明确拒绝旧版本、未来版本和破损输入；不部分恢复、不降级、不尝试另一个 reader。旧客户端必须升级，旧恢复数据必须在升级前由旧版本完成恢复/提交，具体步骤见 [Breaking Change](../migration/current-protocol-only.md)。

## Deleted code 与直接当前路径

| 类别 | 删除或简化内容 | 保留/迁移的责任 |
|---|---|---|
| V1 模块/函数 | 删除 `apps/netbaiot-server/src/business_stream_v1.rs`；`serve_business_mixed`、V1 hello/subscription/framing/writer、`ActiveStreamLease`；root client 的 V1 reader/writer/reconnect/ACK driver | `EventStream`/`Delivery` 有实际消费者，改用当前 V3 driver；手动 ACK、独立 event token、count/byte bounds、取消和 drop 停止任务仍有效 |
| V1 公共 types | `StreamClientFrame`、`StreamServerFrame`、未再使用的 `EventAck` 及旧 JSON goldens | 当前 `V3EventAck` 精确 identity/status 序列化测试；保留公共 `DeviceEvent`/`EventDelivery`/`EventFilter` |
| V2 公共 types/API | `BusinessRpcFrame`、`BusinessLimits`、V2 version/event-window constants、`BusinessRpcClientConfig`、`BusinessRpcClient`、`BusinessDelivery`、旧 driver/handshake/ACK/reconnect API；没有 deprecated wrapper | 当前 client/config/delivery 是实际唯一业务流；共享 auth DTO、`BusinessRole`、`RpcError` 是当前协议使用的 types，保留其 wire |
| V2 server 路径 | JSON V2 connection/event/command/writer loops、version dispatcher、`serve_accepted`、V2 reply bridge；私有通用 `RpcReply` 直接供当前 control worker 使用 | TLS principal、tenant/method/sink scopes、provider leases、auth registration/admission gate、command capacity 与 session fencing |
| 版本 wrappers | `AnyBusiness`/`AnyDeliveries`/`AnyDelivery`、V3→V2 config adapter、无消费者的 connection-timing tuple/统计、单实现 DeliveryView 和纯 forwarding connect wrapper | loadgen 直接用当前 client/delivery；实际 V3 provider/events 双连接拓扑保留 |
| NBMQ reader/migration | v1 JSON decoder、v1–v5 constants/version ceilings、跨版本字段 defaults、legacy subscription/payload deserializer、broker snapshot Deserialize、incarnation/packet-order 修复 fallback | 当前有界 streaming decode/checksum/whole-image trailer、完整 auth/profile provenance、QoS/Will/expiry/ACL/ownership 校验与当前 v6 writer |
| NBSP reader/migration | v1/v2 reader/trailer fallback、旧分片 aggregation/coalescing/migration、旧 ConfigAck 检测及专用错误 | 当前 v3 generation/count/digest/trailer；权威文件、fsync/rename、目录/临时文件边界、失败保留 ownership 和 ACK 后删除 |
| 错误 | 旧 ConfigAck 专用 incompatibility 错误 | 通用 `UnsupportedRecoveryVersion(u32)`，现有 Invalid/Storage/Overloaded 语义与结构化 RPC/client errors |
| Metrics/logs | 无生产调用的 Event/Command queue classes、零值旧 queue labels、V2 假 command-response queue pressure 测试；class enum/args/arrays/always-Some tracking wrapper | 原固定 control queue gauge 名称和 `class="control"` 保留；当前 V3 writer queue gauges 保留；验证 count/byte/permit 释放 |
| 配置/CLI | `version`、`allow_v1`、optional `v3` dispatcher、`v3_send_ahead`/旧 socket-buffer 字段名、旧 `NETBAIOT_BUSINESS_STREAM_TOKEN` 入口；loadgen 旧 protocol topology variants | 单一 `limits`/`send_ahead`/`experiment_socket_send_buffer_bytes` 当前 config；无 serde alias，旧字段按 deny_unknown_fields 拒绝；当前 event 地址/token 与所有管理/设备命令保留 |
| Features/dependencies | 没有仅服务历史协议的 Cargo feature，未制造新 feature；移除 8 条无用直接依赖边，见下表 | 当前 `schema` 与其它实际 feature 保留；没有声称所有传递外部 package 都删除 |
| Scripts/examples | 删除 V1 Python business TCP consumer、`capacity_consumer.py`、`capacity_audit.py` 和旧 framing tests；V2 Rust examples/集成测试迁移并改名；教程不再调用已删除 consumer | `tutorial_smoke.sh` 实跑当前 CLI 订阅 + MQTT/TCP/UDP + shutdown；历史 CSV summary integrity 测试仍有真实用途 |
| Fuzz/CI | 删除 V1 `business_stream` 与 V2 `business_rpc_v2` targets，替换为当前 bootstrap；Native recovery 先行 gate 由旧 test-name prefix 改为当前 recovery 模块；MQTT catalog 旧恢复 filters 替换为 current golden/roundtrip/MQTT5/unsupported tests | 当前 framing/state/recovery/corrupt/unsupported fuzz；Rust release evidence 必须实际有通过的测试，`0 tests`/ignored-only 不通过；全部 workspace/native Windows 测试保留，无 ignore/continue-on-error/放宽 timeout/sleep；维护验证报告也触发已有 fuzz smoke |

完整文件变化见 [changed-files.json](legacy-cleanup/changed-files.json)。[removed-declarations.json](legacy-cleanup/removed-declarations.json) 列出按文件比较的 157 个声明名称候选：它是词法清单，重复函数名和改名不代表全仓同名对象都不存在；上表记录语义审计结论。

### Tests / fixtures 的删除与迁移

旧协议兼容/downgrade/启用开关测试删除。实际 correctness 测试迁移到当前协议，包括稳定 EventId 的 required replay、stale ACK fencing、provider 旧 handler 取消、100 次 reconnect epoch 后 shutdown/drop、满 receive queue、auth grace/revision gap、mTLS/scoped auth、MQTT/TCP command/dedup、真实 command pressure、慢消费者手动 ACK 及连接释放。当前 ACK 必须精确匹配 stream/delivery/event/status；旧/future bootstrap 用最小 header 验证拒绝，不重新实现旧 framing。

删除 NBMQ v1–v5 的 9 个历史 binary fixtures、NBSP v1/v2 的 4 个 fixtures 及旧 spool generator/README。保留当前 v6 empty golden；新增 nonempty v6 golden 是由删除前的当前 writer 冻结，包含 session/QoS/retained/Will。当前 decoder→restore→writer 的逐字节测试通过；NBMQ `write_recovery` 和 NBSP `commit_sync` 写入 framing 函数与基线文本相同。仍保留当前 v6 显式 unknown-profile marker，恢复后必须 reauthenticate 才能恢复可投递状态；它不是旧版本默认 profile。当前 outbound started flag、显式 absent Will origin、超限/校验和/破损/未知 header 均有覆盖。

协议迁移暴露了已有当前 V3 revision-gap 问题：registry demote 后 client 仍停在 Ready。修复仅重置对应 Provider parent，返回结构化 StaleRevision 后重新同步，保留独立 EventSubscription。现有 gap/grace 测试通过。V3 socket tests 使用已有 TCP/UDP 成对预留 helper 修正 AddrInUse fixture；未增加重试预算或 timeout 来隐藏错误。

## LOC

按 git 跟踪的 `.rs` 实际物理行计数，包括 tests/examples 和此次新增当前格式 measurement probes；不是“纯生产代码行数”。新增 correctness tests 会抵消部分删除。原始值见 [loc.json](legacy-cleanup/loc.json)。

| Crate | Before | After | 净减少 |
|---|---:|---:|---:|
| netbaiot-server | 10,225 | 9,572 | 653 |
| netbaiot-cli | 2,457 | 2,463 | -6 |
| netbaiot-runtime | 15,154 | 14,983 | 171 |
| netbaiot-client | 4,507 | 3,206 | 1,301 |
| netbaiot-device-sdk | 2,355 | 2,355 | 0 |
| netbaiot-transports | 27,063 | 25,629 | 1,434 |
| netbaiot-protocol | 1,873 | 1,587 | 286 |

## Release binary size / dependency graph

同主机/toolchain/profile 的实际文件大小。CLI package 的 executable 名为 `netbaiot`。SHA-256 在 [before-binaries.json](legacy-cleanup/before-binaries.json) / [after-binaries.json](legacy-cleanup/after-binaries.json)；测量冻结 implementation `8213095`，之后只有教程、注释、文档和 CI 报告路径变化。

| Executable | Before bytes | After bytes | 差值 |
|---|---:|---:|---:|
| netbaiot-server | 12,168,384 | 11,211,040 | -957,344（-7.87%） |
| netbaiot（CLI） | 16,422,432 | 16,295,792 | -126,640（-0.77%） |
| netbaiot-loadgen | 5,273,920 | 5,274,160 | +240 |
| business_rpc loadgen | 7,928,608 | 7,267,376 | -661,232（-8.34%） |
| admission_auth probe | 1,076,576 | 1,077,552 | +976 |
| udp_ack probe | 595,968 | 595,968 | 0 |

清理前后均执行 `cargo tree`，结合 workspace metadata、全部 target/feature 调用点和严格编译审计，移除下列直接 normal dependency。外部 package 仍被其它 workspace crate/测试使用，整个 lock graph 外部 package 删除数为 0。`cargo-machete` 未安装，NOT RUN；未安装工具或新增开发依赖。

| Crate | 移除依赖 |
|---|---|
| client | tracing |
| protocol | serde_json normal dependency；dev dependency 保留 |
| server | subtle |
| core | uuid |
| transports | thiserror |
| v3-mux | serde、serde_json |
| CLI | tracing |

## Before / After 实际测量

Mac mini M4（10 cores、16 GiB），macOS 27.0.1 arm64，Rust 1.99.0。所有 timed measurements 串行，期间没有 cargo build、测试或 fuzz。A/B 使用同一冻结 load generator、相同负载和 count/byte limits；baseline 工具的旧 config 布局只存在于本机忽略的测量证据，未保留到 runtime。完整 compact 数据见 [measurement-summary.json](legacy-cleanup/measurement-summary.json)，本机原始日志路径/hash 见 [raw-manifest.json](legacy-cleanup/raw-manifest.json)。

### MQTT

256 connections、1 KiB、QoS1、订阅 uplink、20,000/s offered load、development audit sink。不是容量上限测试。三组对照都保留，各 2A+2B；值为每组 run statistic 的中位数。

| Cohort | 预热 / active / 顺序 | ACK/s A→B | p50 ms A→B | p99 ms A→B | p99.9 ms A→B | RSS peak KiB A→B |
|---|---|---|---|---|---|---|
| 初始 | 3s / 15s / ABBA | 20,000.67→20,000.00 | .330→.325 | .935→1.020 | 2.425→10.400 | 17,648→19,440 |
| 同负载复测 | 3s / 15s / ABBA | 20,001.33→20,000.00 | .325→.320 | .905→.945 | 1.205→7.520 | 17,816→18,360 |
| 延长预热、交换顺序 | 10s / 30s / BAAB | 19,998.27→19,999.78 | .330→.320 | 1.050→1.050 | 14.425→15.925 | 20,504→19,520 |

全部 12 runs 错误、异常断连、disconnect pending 和 overload 为 0。初始两组 B 的极端尾延迟较高，不能忽略；交换顺序后 A 也有 11.18/17.67 ms、B 有 11.96/19.89 ms 的 p99.9。这里证明目标负载吞吐和典型延迟保持，**没有证明 p99.9 等价或改善**；RSS/CPU 和极端尾部存在同机短测波动，未把它包装为性能收益。当前 cleanup 不新增锁/队列架构优化。

### 当前 Business RPC 与启动

mTLS 当前 V3，15s active，500 events/s、20 commands/s、10 auth/s，1 KiB event，ABBA；使用同一冻结 current-wire `business_rpc` generator。A Event ACK 7,482/7,500，B 7,496/7,496，即 active 区间约 499–500/s；event ACK p99 A 1.60/1.67 ms，B 1.65/1.68 ms。四轮各 auth 148/148；各 300 commands accepted、300 device delivered、300 device ACK、300 CommandAck event ACK，0 duplicates/unknown/accepted-without-delivery/publish-errors。

每轮总 330 command requests，另有 30 次在 device worker 的 15s 区间结束后产生的 tail errors；总 elapsed 约 22.264s（含收尾）。这些错误在 A/B 均存在，**不能声称所有请求成功、0 command errors，也不能用 15s 分母描述整个收尾阶段**。

真实进程启动→认证 `GET /api/v1/ready` 200，共 10A+10B、ABBA，fresh recovery directory、静态 development auth/audit sink、无 business listener；device 自动端口、预留 management 端口。中位数 A **3.943 ms**、B **3.862 ms**；每次 SIGTERM 后 exit 0。这是指定轻量配置下的本机启动测量，不代表带大恢复文件的生产启动。

### 当前 Recovery

实际当前 writer durable commit（含 fsync/rename）与 decoder/restore；1/128/512 records，每组 3 warmups+20 measured iterations，ABBA。恢复后 count 校验通过；不是内存 serialize microbenchmark。下表为各组 p50 的中位数；所有大小和 p95/p99 保留在 JSON。

| 512 records | A ms | B ms | 差值 |
|---|---:|---:|---:|
| NBMQ save | 28.028 | 28.016 | -0.04% |
| NBMQ load | 9.851 | 9.877 | +0.26% |
| NBSP save | 12.515 | 12.739 | +1.79% |
| NBSP load | 1.221 | 1.206 | -1.17% |

同一 workload 的文件大小相同：NBMQ 512 records 为 2,370,038 bytes；NBSP 为 179,780 bytes。没有改变当前 persistence framing。

## Validation

| 检查 | 状态 / 范围 |
|---|---|
| fmt/check/strict clippy | PASS：`cargo fmt --all -- --check`、`cargo check --workspace --all-targets --all-features`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`，0 warnings |
| Locked workspace / xtask | PASS：`cargo test --locked --workspace --all-features`、`cargo xtask check` |
| Release gates | PASS：`cargo xtask check release --part rust`、`--part mqtt`、`--part preflight`；schema/config reference/9 release-tool tests |
| MQTT external conformance | PASS：raw state tests、Mosquitto 3.1.1/5、QoS、persistent reconnect、TLS、untrusted/wrong/expired cert；使用已安装工具，Mosquitto 不成为运行依赖 |
| 当前 RPC 独立 gate | xtask 无单独 RPC part；PASS：实际 V3 socket/mTLS/auth/events/commands/official client/full workspace、高风险 repeats 和上述 current-wire load |
| Recovery/lifecycle 重复 | PASS：8 cohorts 每模式 30 次，2 个 certificate cohorts 每模式 10 次；default harness 与 `--test-threads=1`；520 cohort executions、4,300 test executions；manifest 记录 test names/binary hash |
| 最终 metrics 修改后复测 | PASS：当前 RPC 12 units + 真正 current command-pressure test，各模式 30 次；再 780 test executions，总计 **5,080**。最终完整 workspace/gates 随后通过 |
| Subprocess soak | PASS：`subprocess_graceful_restart_sixty_second_soak`，实际 67.90s；其它 restart/spool failure/replay/ownership paths 在上述 repeats 中覆盖 |
| ASAN fuzz | PASS：bootstrap、business_rpc_v3、mqtt_recovery、restart_spool、mqtt_packet、mqtt_v5_packet、mqtt_state、mqtt_device_profile 各 10,000 runs，合计 **80,000**，final dependency graph；[fuzz-summary.json](legacy-cleanup/fuzz-summary.json) |
| Public API docs / deps / Python / 教程 | PASS：`cargo doc --locked --workspace --all-features --no-deps`，当前 sidebar 无已删除 public types；cargo tree/manual audit；Python tools；实际 `bash scripts/tutorial_smoke.sh` 当前订阅和设备/管理路径 |
| 更长期/非本机容量 | NOT RUN：cleanup 没有重新执行小时级 soak、真实 WAN/multi-host 容量、全目标长时间 fuzz、Linux netem；已有历史数据不冒充当前结果 |

具体 cohort/test names、阶段边界与依赖审计见 [repeat-cohorts.json](legacy-cleanup/repeat-cohorts.json) / [verification-summary.json](legacy-cleanup/verification-summary.json)。故意忽略的手动 benchmark 在指定 `--ignored` 测量中执行；不把 0 tests 或未运行项目标成 PASS。早期开发失败、工具 PATH 阻塞和 startup harness 端口错误已修正并重跑，原始日志保留。最终核查发现旧 MQTT catalog 的 deleted filter 会因 `cargo test` 成功但 0 tests 而假通过，已替换为当前测试并修复 harness；新增无测试/仅 ignored/混合 workspace/部分成功但 command failed 回归覆盖（12 Python harness tests PASS）。旧 `final-release-mqtt.log` 不作为当前 catalog gate 的验收证据，修正后的 `report-final-mqtt.log` 才是最终结果。

## CI

跨平台结果将在合并后记录到 [ci.json](legacy-cleanup/ci.json)。最终验收要求 Rust checks、MQTT fuzz smoke、Native developer experience、Native recovery and lifecycle 全部成功，且两个 Native matrix 的 Ubuntu/macOS/Windows 全绿；不能用本机 PASS 替代 Windows。

## Remaining legacy / compatibility names

本次目标协议在 live implementation 中为 0；版本拒绝 header 不是兼容实现。全仓语义搜索逐文件结果和理由见 [remaining-protocol-references.json](legacy-cleanup/remaining-protocol-references.json)。以下名称有独立、实际用途：

| 名称/位置 | 保留原因 |
|---|---|
| `docs/business-rpc-v2-{reliability,production-readiness}.zh-CN.md`；旧 correctness/reliability/lifecycle/Windows/SDK/MQTT/device-HTTP 审计 | 带历史提示的既有测量、修复证据，引用当时 commit；不重写历史，也不当作当前操作指南 |
| `docs/performance/**`、`docs/audit-evidence/**`、`docs/eventbus-{batch,lock}-evidence/**`、release notes/raw evidence 中 V1/V2/old test names | 历史数值、场景名和测试结果保持原义；`matrix-32k-v2` 是实验 revision，不是当前 RPC V2 入口 |
| inventory、breaking guide、本报告中的旧版本与已删除声明清单 | 解释删除和基线；没有运行时 fallback/旧 reader/migration |
| `JsonV1`、`v1/t/...`、`/api/v1`、HTTP/1.1、MQTT 3.1.1/5、MQTT v5 property helpers | 当前独立设备/管理标准和命名空间，不属于 Business RPC 或 NBMQ 历史版本 |
| `management_auth.legacy_static_token_enabled`、management bootstrap/certificate 兼容名称和 HTTPS provider optional-token 提示 | 当前已使用的管理授权/TLS/provider 配置；与被删除的业务流协议不同，保留安全与 public HTTP 行为 |
| `netbaiot-core` compatibility reexport、AuthInvalidationResult legacy counters | 有当前消费者/公开 wire；不是旧 RPC envelope，不删除在用契约 |
| `LegacyReplayWindow` / UDP differential test | 仅 cfg(test) 的有界参照算法，比较现有 UDP replay 索引的正确性，不是旧协议 runtime |
| `#[allow(deprecated)]` 与 Rust 1.88 MSRV 注释 | 原子 API 的编译器版本兼容，不是 deprecated RPC wrapper |
| Metrics 中 legacy histogram 描述、xtask `legacy-summary.json` fixture | 当前 histogram 分辨率对比和历史 evidence budget 验证；无旧协议 dispatcher |
| `legacy_cleanup_recovery_measurement` / `NETBAIOT_LEGACY_BENCH` | 本任务命名的 cfg(test) 手动测量，只读写当前 NBMQ6/NBSP3；不包含旧实现 |
| `legacy_device_addresses...`、旧 device_configs 输入测试 | 当前配置拒绝测试，不做 ambiguous conversion 或配置持久化 |
| V3/v6/current 文件名和类型 | 真实 wire-format identity 或明确的 current test scope；为重命名而扩大 diff 无收益 |

模块删除、Recovery 删除和 generic cleanup 分别提交；主要 implementation commits 为 `00bbeca`、`ed7336f`、`792bcc2`、`d13630c`、`97ef890`、`8213095`。旧 branch/fixture/format 的 Git 历史仍可查，不是新运行时兼容层。
