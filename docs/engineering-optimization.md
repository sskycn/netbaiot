# Engineering optimization (0.2.3 baseline)

## 1. 修改摘要

基线为 `7df10ba`（Windows POSIX 路径修复已完成），本轮按六阶段提交。
增加原生归档验证、增量 required 责任计数、借用式 HTTP JSON、Sink 故障分类与暂停、
租户积压配额，以及真实 TLS/outage/restart 测试。没有修改 EventAccepted、MQTT wire、
DeviceEvent JSON、NBSP v3 / NBMQ v6、broker 并发模型或既有生产默认资源上限。

## 2. 修改文件列表

| 文件 | 内容 |
| --- | --- |
| `.github/workflows/dx-platform.yml` | Windows/Linux/macOS 原生构建、打包和归档验证 |
| `scripts/release_package.py`, `tests/test_release_tooling.py` | canonical POSIX member、安全路径和链接回归 |
| `crates/netbaiot-runtime/src/event.rs` | required/tenant 记账、完成操作 fencing、Sink 暂停和诊断、invariant 测试 |
| `crates/netbaiot-runtime/src/limits.rs` | 两个租户 backlog 配置及兼容/范围测试 |
| `crates/netbaiot-runtime/src/metrics.rs` | 固定失败原因标签和聚合指标 |
| `crates/netbaiot-runtime/src/event_accounting_bench.rs` | 相同 backlog 的 publish/usage/complete/retry/restore 测量 |
| `apps/netbaiot-server/src/delivery.rs` | 借用 envelope、HTTP 错误分类、bounded Retry-After |
| `apps/netbaiot-server/src/delivery_tests.rs`, `delivery_failure_tests.rs` | JSON 等价、分配测量、真实 HTTP 故障夹具 |
| `apps/netbaiot-server/Cargo.toml`, `Cargo.lock` | 复用 stats_alloc 的测试依赖 |
| `apps/netbaiot-server/tests/server.rs`, `engineering_soak/mod.rs` | 有界 TLS MQTT QoS1 → confirmed HTTP 故障恢复 soak |
| `crates/netbaiot-transports/src/management_http.rs` | additive Sink 状态与指标 |
| `crates/netbaiot-transports/src/mqtt/broker_hotspot_bench.rs`, `broker_engineering_bench.rs` | 20 种会话状态、ACK/重连/锁持有测量，仅测试代码 |
| `docs/configuration.md`, `configuration-fields.md`, `schema/netbaiot-config.schema.json` | 配额含义、生成参考和 schema |
| `docs/architecture.zh-CN.md`, `benchmarks.md`, `performance/engineering-hotspots/report.md` | level 4/5 分类与历史 promotion 说明 |
| `docs/performance/engineering-optimization/*.json` | 小型、可复核的测量汇总 |
| `docs/engineering-optimization.md` | 本报告及逐阶段结果 |

## 3. 修复的问题

Windows tar manifest 与宿主路径分隔符不一致的问题由 `7df10ba` 修复，本轮保留并加强原生验证。
归档 member 禁止反斜杠、绝对路径、`..`、非 canonical 别名、重复、symlink 和特殊文件；
链接解析覆盖嵌套、百分号编码、合法 `./` / `../` 与越界逃逸，未放松任何安全检查。
中文架构文档现在准确区分 MQTT level 4 / 5；历史 due promotion 结果保留，明确标注
`c7f0fd6` 后当前实现已有 demand budget。英文架构文档原已正确，不作无必要改动。

## 4. EventBus accounting 设计

`pending_required` 统计未 ACK 的 required Sink 责任份数，而非事件数。
publish / 整批 restore 在预检中 checked_add，全部容量验证通过后提交；ACK 仅在
当前 inflight operation token 匹配且 required 集合确实移除成员时扣减。
重复、错误、迟到完成及饱和 attempt 均不能重复扣账。Retry、永久错误、max_attempts / max_age
耗尽、暂停和 spool 均保留责任。生产 `usage()` 只读计数，测试才重新扫描 authoritative 状态。
invariant 同时验证 active bytes、每 Sink ready/delayed/inflight、年龄桶和每租户账目。

## 5. HTTP Sink 优化

私有 `WebhookEnvelope` 借用 IDs 和 event kind，复制时间戳等标量，直接交 reqwest 序列化。
不保留另一份 Value 树或事件生命周期 JSON cache。四种 event kind 的 exact JSON-value
测试覆盖九个字段、null、嵌套 payload、枚举、UUID、时间戳及 Unicode；object member
顺序不是既有 API 合约。HTTP 超时、禁止重定向和 4096-byte 响应上限保留。

## 6. Sink failure / pause 设计

保留 `SinkError::{Retryable,Permanent}` 和 `EventSink::deliver`，增加默认
`deliver_detailed`、无秘密的 `SinkFailure` 和封闭 reason 枚举。跨 server/runtime crate
传递分类和重试提示需要这组 additive public API；原 Sink 实现仍可使用默认适配。
429 / 503 delta-seconds 提示在 HTTP 和 EventBus 两层限制为本地 `retry_max_ms`。
HTTP-date evaluated but not implemented。永久失败或连续三次失败暂停 required Sink；
到期只允许一个 probe，ACK 恢复。既有 inflight 正常完成，全部责任和配额一直保留。

状态置于现有 SinkState，不增加共享 Mutex、网络期间持锁、后台 timer 或每事件 task。
管理状态遍历最多 max_sinks 个条目，年龄桶数受 Sink count 限制。
指标只使用固定失败原因标签，数量/字节/inflight/retry/failure/drop/pause/age/hint 均有诊断；
设备、事件、客户端、Sink ID 和 URL 不作 Prometheus label。

## 7. Tenant backlog 设计

原 tenant ingress slots 无法限制长期 accepted backlog，现增加每租户事件 count / serialized bytes。
同一事件 bytes 只计一次，直到最后一份 Sink 责任结束才释放；空租户条目删除。
map 条目数受全局事件数限制，已存在 tenant publish / complete 不克隆 ID。
全局、租户及 required Sink 容量一起 preflight，restore 整批投影，失败零提交。
best-effort 自身满时仍按原策略 shed，不改变 required acceptance。

新配置默认 16384 events / 67108864 bytes，原默认容量和配置解析不变。
自定义放大的旧 global limits 可能需要显式 tenant 值；降低 quota 后超过限额的已提交
recovery 必须等待配置修正，不能丢弃。详见[配置说明](configuration.md)。

## 8. 测试结果

| 检查 | 状态 / 证据 |
| --- | --- |
| 五阶段逐阶段 `cargo xtask check` | PASS：fmt、Clippy、workspace tests、evidence budget |
| 阶段六 `cargo xtask check` | PASS；最后的夹具/说明修正还由最终 rust gate 复核 |
| EventBus invariant 和配额专项 | PASS：34 tests，6 manual benchmarks ignored |
| HTTP JSON / 故障矩阵 | PASS：200、500、429/503 + hints、401/403/404、302、deadline、refused、invalid、declared/chunked oversize、恢复 |
| TLS QoS1 HTTP outage | PASS：短版 CI + 60 秒 600 事件；event_id 稳定，全部成功 Sink ACK，active/pending/queue/bytes/inflight=0 |
| planned restart soak | PASS：12 cycles × 5 秒驻留，spool/replay stable IDs；两个 soak 合计约 130 秒 |
| owned tasks / clean exit / spool cleanup | PASS：TLS soak 恢复任务基线、正常退出、recovery records=0 |
| stable / Rust 1.88 final rust gate | PASS：各 496 workspace tests；fmt / all-targets all-features Clippy `-D warnings` |
| MQTT external release gate | PASS：77 conformance cases，125/125 normative requirements；raw regressions、MQTT 5 smoke / Mosquitto、Device SDK TCP/TLS / certificate rejection / persistent reconnect / bounded-memory probes |
| release preflight | PASS：生成 schema/reference 无 drift，版本 / 112 package inputs / sanitized evidence，13 Python regressions |
| native archive jobs | PASS：Windows x86_64-pc-windows-msvc、Linux、macOS 原生 `package --no-build`，全部 native workspace / first-use checks；本机 macOS extracted smoke 亦 PASS |
| bounded restart_spool fuzz smoke | PASS：nightly，208238 runs / 21 秒，max_len=65536，RSS limit=512 MiB |

测试夹具早期曾暴露无效管理客户端和超出默认设备限流的发布频率，已修正；
没有删除测试、放宽生产限制或提前 ACK。失败记录不能作为通过证据，表中 PASS 仅对应修正后的运行。
本机 Mosquitto broker 已安装于工具的 `sbin` 目录，初次 MQTT gate 因 PATH 未覆盖该目录
报告 BLOCKED；重跑仅为该测试命令增加 PATH，不安装新运行时依赖。
Windows native workspace 暴露关闭端口夹具的不成立假设：50 ms 和 2000 ms 均返回
Timeout，原生 TCP 探测也在 2000 ms 内超时，不能把该环境的关闭端口固定等同立即拒绝。
新增原生 CI failure-tail annotation 定位后，在自动端口范围之外至多检查 32 个候选，
以独立原生 TCP 的实际拒绝/超时结果严格验证对应 HTTP 分类；成功连接或其他结果仍失败。
另以真实 TLS 证书拒绝在每个平台严格验证 Network / Retryable。50 ms body-timeout
和所有 HTTP 分类断言保留。只修正测试；生产分类与默认 timeout 未改变。
最终代码 `5a2c306` 的[三平台 native CI](https://github.com/sskycn/netbaiot/actions/runs/37730417541)
和[完整 release CI](https://github.com/sskycn/netbaiot/actions/runs/37730418188)均 PASS。
初次 Windows FAIL 与环境 PATH BLOCKED 已解决，当前无未解决的 FAIL / BLOCKED；
小型[验证记录](performance/engineering-optimization/validation.json)保存逐 job 结果。

## 9. Benchmark before / after

同一 macOS arm64 / Rust 1.99.0，serial release、三个 repeats，setup / cleanup 在测量外。
[EventBus 数据](performance/engineering-optimization/eventbus.json)包含 before、after-counter 和
after-final；HTTP 的[同 workload 数据](performance/engineering-optimization/webhook.json)保留全部 repeats。
这些是子系统微测量，不是容量、负载上限或 SLA。

| active events | 1 | 64 | 256 | 1024 | 4096 | 16384 |
| --- | --- | --- | --- | --- | --- | --- |
| usage before P50 ns | 42 | 125 | 250 | 875 | 4667 | 20334 |
| usage after-final P50 ns | 42 | 42 | 42 | 41 | 41 | 41 |

最终版本包含 token、租户配额和诊断记账，因此其他操作有成本增加，未隐瞒：

| 16384 active workload | before / final P50 ns | before / final allocations | before / final allocated bytes |
| --- | --- | --- | --- |
| publish | 333 / 417 | 8 / 8 | 616 / 616 |
| complete | 208 / 292 | 1 / 1 | 232 / 232 |
| retry | 84 / 167 | 2 / 2 | 424 / 456 |
| restore | 500 / 666 | 12 / 13 | 2176 / 2544 |

HTTP request construction 的 32/1024/16384/65536-byte payload，旧/新 allocations 为
37/13、38/15、38/15、38/15；P50 ns 为 2417/1125、2708/1542、8334/6416、20625/17584。
网络时间不在该测量内，不能直接推算端到端收益。

[MQTT 矩阵](performance/engineering-optimization/mqtt.json)覆盖 offline=0/1/10/100/128（默认 max），
outbound=0/1/10/32 的全部 20 组合，usage / expiry / ACK / route+ACK / reconnect 共 300 行。
outbound=32 的测量临时使用 33 个 QoS1 slots、配套 65 个 frame 上限与重算 recovery bound，
以容纳被测的一条新消息；fixture Limits 经过 validate，生产默认值不变。
ACK 隔离后台 offline dispatch，重连从相同已认证 snapshot 开始，包含正常 resume。
lock hold 只计算被测 action，metric render 在计时和分配区域外。

| outbound=32，offline | ACK / route+ACK / reconnect P50 ns | 对应 mean lock hold ns |
| --- | --- | --- |
| 0 | 917 / 2625 / 3708 | 832 / 1145 / 3580 |
| 128 | 1042 / 2958 / 4500 | 966 / 1311 / 4353 |

微小 usage / expiry 样本有 0 / 41 ns 的时钟量化，不能声称零成本。
未证明复杂 cache/index 或锁替换值得引入，因此 evaluated but not implemented。
[soak 数据](performance/engineering-optimization/soak.json)记录 10 events/s、20 秒 HTTP outage、
600 events、784 requests、故障末尾 200 责任 / 51200 bytes、最终清零和约 9 ms shutdown。
固定测试/subprocess suite 的 `time -l` maximum RSS 为 27394048 bytes（约 26.1 MiB），
不含编译；它不能换算每连接内存，也不是生产 gateway 容量。主机非专属测量环境。

## 10. 未执行测试

NOT RUN：数小时/多日 soak、生产网络/DNS/TLS故障注入、大规模混合入口容量、
专属主机每连接 heap/RSS、完整长期 fuzz campaign、真实断电。MQTT 并发模型和 decoder
未修改；保留既有 parser / state fuzz targets。没有创建或移动 release tag，没有发布新版本。

## 11. 已知风险

新 quota 是长期占用上限，不是公平调度或保留容量；未配置更小值时仍可能由一个租户
耗尽全局配额。暂停以本地 max retry interval 为周期，永久错误会持续保留和周期探测；
运营人员必须修复 Sink 或安全排空，不能靠丢 required 责任恢复 readiness。
诊断和操作 token 有额外 bounded 记账开销，benchmark 已记录。abrupt crash 仍可丢失
尚未 spool 的有界内存流量；只承诺 planned graceful restart，不声称 crash durability。

## 12. 下一阶段建议

在专属环境按真实 payload/fanout/backlog 做长 soak 和 per-site lock tail profiling，
验证运营所需租户 quota 与恢复容量。只有证据表明 session usage / expiry 是首要瓶颈时，
才评估 incremental QoS counters / deletable expiry index，并保留 authoritative invariant。
继续在 tag 之前运行原生 package gate；不要从微测量或短 soak 推导生产 SLA。

## 逐阶段实施记录

This staged change preserves EventAccepted, required delivery ownership, MQTT wire
behavior, NBSP v3 and NBMQ v6 recovery, and the database-free bounded runtime.

## Phase 1: portable, safe release archives

The POSIX manifest/link fix in `7df10ba` is retained. Native developer CI now builds
release binaries and executes `cargo xtask package --no-build --target <host>` on
Windows, Linux and macOS before a tag is created. Validation rejects backslashes
and noncanonical member names, so Windows extraction cannot reinterpret a member
as traversal and aliases cannot evade duplicate-member checks.
The native matrix runs on main, pull requests and `codex/**` branches so the task
branch can be verified before merging; it replaces the stale single feature-branch filter.

Tests cover nested/percent-encoded links, relative parents, POSIX and Windows
manifest paths, absolute members, traversal, symlinks and duplicate members.
PASS: 13 release-tool tests, `cargo xtask check` (format, Clippy, workspace tests,
evidence check), and native macOS `package --no-build`. Final phase-6 native
Windows/Linux/macOS package jobs all PASS; see the validation record above.
The initial Windows fixture failures were diagnosed and corrected.

## Phase 2: incremental required responsibilities

`State.pending_required` counts sink responsibilities, not events. Publish and
whole-batch restore preflight checked additions before commit. Only removal of a
present required sink on a terminal ACK decrements it. Retries, permanent errors,
timeouts, panic, cancellation and spool snapshots retain ownership. Duplicate or
unrelated completions cannot alter inflight or byte accounting.

The test-only `assert_eventbus_invariants` recomputes required responsibilities,
active bytes, per-sink count/bytes and ready/delayed/inflight accounting. Existing
admission, restore, duplicate, retry, panic, isolation and spool tests invoke it.
PASS: 30 EventBus tests and `cargo xtask check`. The serial release benchmark uses identical before/after
workloads at 1/64/256/1024/4096/16384 active events. Median usage latency (ns) was
42/125/250/875/4667/20334 before and 41/41/41/41/41/41 after. These are isolated
subsystem measurements on this macOS host, not capacity or SLA claims. Publish,
complete, retry and restore allocation/timing rows are retained in the final
measurement summary; their costs remain visible rather than being inferred from
the usage improvement.

## Phase 3: borrowed HTTP envelope

The private `WebhookEnvelope` borrows IDs and `DeviceEventKind`; timestamps and
the event type are copied scalars. The request serializes once into reqwest's
body. No event-lifetime JSON cache or additional payload tree is retained.
Exact JSON-value tests cover every event kind, null and populated occurrence
timestamps, UUIDs, numeric/boolean/text telemetry and escaped Unicode. Object
member ordering is not an API contract; all nine fields and nested values match
the old serializer.

The same reqwest request-construction benchmark compares the retained legacy
`json!` implementation and the borrowed envelope (512 samples, three repeats):

| Payload bytes | Old/new median ns | Old/new allocations | Old/new allocated bytes |
| --- | --- | --- | --- |
| 32 | 2417 / 1125 | 37 / 13 | 3451 / 1327 |
| 1024 | 2708 / 1542 | 38 / 15 | 6271 / 3405 |
| 16384 | 8334 / 6416 | 38 / 15 | 52351 / 34125 |
| 65536 | 20625 / 17584 | 38 / 15 | 199807 / 132429 |

This excludes network time and does not imply the same end-to-end improvement.
PASS: JSON compatibility tests, the release benchmark and `cargo xtask check`.

## Phase 4: failure diagnostics, bounded retry hints and pause

The original `SinkError::{Retryable,Permanent}` and `EventSink::deliver` remain
source-compatible. An additive, defaulted `deliver_detailed` hook and sanitized
`SinkFailure` metadata are necessary for the server crate to report retry hints
and a closed reason vocabulary to the runtime without another shared mutex.
Reasons distinguish network, timeout, 429, 5xx, auth, other 4xx, malformed/oversized
responses, panic and unspecified custom-sink errors. No secrets or remote error
bodies enter diagnostics.

429/503 delta-seconds Retry-After is parsed with a checked u64 conversion and
clamped to `retry_max_ms` in both HttpSink and EventBus. Invalid values are ignored;
HTTP-date support was evaluated but not implemented. Local backoff still provides
a nonzero minimum. Redirects and the 4096-byte response-body ceiling remain.

Each required sink pauses after a permanent failure or three consecutive failures,
using the existing local maximum retry interval. At expiry, one recovery probe is
allowed; ACK clears the pause. Previously started requests complete normally.
Nothing releases count, bytes or required ownership until ACK. No new task, lock,
background timer or per-event breaker is introduced. Shutdown still drains or
spools accepted required work, even while a sink is paused.

A current inflight operation token rejects duplicate/stale completions, including
saturated persisted attempt counts. The token is node-local and transient; no
spool format changes are needed. Inflight token entries are bounded by existing
delivery concurrency. A count-bounded timestamp bucket index per sink provides
oldest age without scanning active events on metrics/status reads.

Administrative status adds bounded per-sink diagnostics. Prometheus exposes
aggregate queue/count/bytes/inflight/pause/age/hint-use values and a fixed reason
vocabulary. Sink/device/event IDs and URLs are never metric labels.

Fixtures exercise 200, 500, 429/503 with hints, 401/403/404, deadline, refused
connection, invalid response, declared and chunked oversized bodies, and recovery
to 200. Tests verify spool ownership, stable event IDs, cleanup, one probe,
duplicate completion protection and nonzero drain reporting during an outage.
PASS: 32 EventBus tests, HTTP classification/outage tests and `cargo xtask check`.

## Phase 5: tenant backlog quota

Existing tenant ingress limits bound active admission, not accepted backlog.
EventBus now keeps one count/byte entry per tenant with outstanding events;
the map is bounded by global event count and empty entries are removed. Bytes
count the serialized event once, independent of fanout. Retry, pause, inflight and
spooling preserve ownership; the last sink completion releases the event quota.

Publish preflights global, tenant and required sink capacity before mutation.
Restore projects the entire batch in local bounded accounting before committing.
Checked additions prevent wraparound. Best-effort capacity shedding is unchanged.
The new Limits fields default to the existing global defaults, preserving default
capacity and configuration parsing; customized larger global limits may require
explicit tenant settings. Operators can set smaller tenant limits for isolation. Generated
schema and field references include the additive settings.

PASS: 34 EventBus tests, including tenant count/byte rejection, unrelated-tenant
progress, final-sink release, best-effort shedding, failed restore rollback and
duplicate restoration. The authoritative test invariant also recomputes every
tenant entry. `cargo xtask check` validates the workspace and generated artifacts.
