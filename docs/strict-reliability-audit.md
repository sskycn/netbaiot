# NetbaIoT 严格可靠性审计与修复

审计日期：2026-10-06。结论：**本次审查范围内没有剩余已知阻塞项。** Windows/Linux/macOS 原生恢复、退出和完整 workspace 已实际执行通过。经作者单独授权只推送审计分支验证 CI；未合并或推送 main，未创建 tag 或 Release，项目版本仍为 0.2.3。最终分支提交仍须通过同一 CI 门禁后方可合并。

## 审计基线

- 实际起点为干净、与 origin/main 同步的 main：`f68bfd4a8f563e265673ae76e70c7c795b27ee86`。历史 `6d16bf8` 仅用于定位，不作为当前行为依据。
- 工作分支：`codex/strict-reliability-audit`。最终生产行为修复提交：`f3a5d7c034354f45fbf3251a9903e781c5ae15ba`；随后仅调整测试模块位置与报告，不改生产行为。末轮本地验证包含这次测试位置调整，记录 tracked diff hash；后续分支提交继续经过 exact-commit CI。
- 原生环境：macOS，Darwin 27.0.0，arm64，`aarch64-apple-darwin`。
- stable：rustc 1.99.0 `b940084d7`，cargo 1.99.0 `5f94df478`；MSRV：rustc 1.88.0 `6b00bc388`，cargo 1.88.0 `873a06493`。
- 实际工具：Python 3.9.6、Mosquitto 2.1.2 broker/pub/sub、cargo-fuzz/nightly、actionlint、cargo-audit 0.22.2。开始时没有 Windows/Linux 原生执行主机，也没有 GitHub API 登录凭据；作者随后授权推送审计分支，使用 GitHub hosted 三平台原生 runner。
- 起点 writer：NBSP v2、NBMQ v6。起点相关基线：spool 9 PASS；MQTT recovery 单元测试 10 PASS、2 ignored，相关 lifecycle integration 1 PASS。通过的旧测试没有覆盖整记录边界截断。
- 已阅读根 AGENTS.md、架构、交付/生命周期、恢复格式、MQTT 支持边界及相关测试。没有提供 `netbaiot_strict_audit_tests.zip`，使用仓库 API 自行编写回归。
- 实际审查范围：两套恢复读写/清理、EventBus publish/restore/worker ownership、server composition/startup/drop、两个 MQTT connection loop/Reader、AuthCache UDP verifier hit。未改公开 wire schema、SDK API、业务配置所有权或离线命令行为。

## 问题清单

分类遵照任务要求：1=当前已修复；2=当前存在且动态复现；3=清晰静态风险，尚未完整动态复现；4=原判断不成立或需要缩小。

| 问题 | 起点结论与触发条件 | 动态证据、影响 | 最终代码定位 |
|---|---|---|---|
| NBSP 整体完整性 | **2**。v2 只有记录 checksum；截断/删除完整尾记录、改变 generation、重复/调换完整记录仍可被接受 | 真实 Rust writer 写三条，再由真实 recover 路径读取变体；7 种非法变体被接受，造成待恢复责任缺失/重复/错误代际 | `crates/netbaiot-runtime/src/spool.rs:165,244,328` |
| 恢复 I/O 分类及读取边界 | **3**。存在 `exists()`、忽略目录项错误及 metadata 后全量读取的路径；并非每个权限问题都被忽略 | 新增实际权限/超限/损坏、注入 Read/目录项错误测试 PASS。没有对所有旧路径逐项取得 red；不可将它们都称为已复现漏洞 | `crates/netbaiot-runtime/src/recovery_io.rs:10,32,64,108`；`crates/netbaiot-transports/src/mqtt/broker.rs:5295` |
| Windows directory flush/replace | **3**。原读模式打开目录并 `sync_all` 没有证明有效；正常停止可能被存储错误永久阻塞 | 核实 Rust 1.88/Win32/安全封装源码；初始没有 native red；修复后九项原生恢复/退出及完整 workspace **PASS**，保留旧 Windows 问题只做静态确认的边界 | `crates/netbaiot-runtime/src/recovery_io.rs:179`；两套 writer |
| active EventId 覆盖 | **2**。公共 EventBus/custom Codec 提交已有 active ID，无冲突检查 | 相同/不同 payload 的重复 publish 原先成功；可能让旧 ACK 结算被覆盖责任。并非默认远程设备可自选 ID | `crates/netbaiot-runtime/src/event.rs:363` |
| restore 语义/整批原子性 | **2**。空/重复 pending sink、累计容量及后半批错误未完整预检 | 原测试失败，出现非法接纳或前半批已修改状态；多 sink 队列/计数/required 责任受影响 | `crates/netbaiot-runtime/src/event.rs:476` |
| 启动失败 worker 所有权 | **2**。构造立即 spawn，后续 TLS/认证/绑定错误提前返回 | 隔离 runtime 中 task 数不回基线，实际 red 失败；泄漏任务/引用/句柄 | `apps/netbaiot-server/src/lib.rs:970,1219`；`crates/netbaiot-runtime/src/event.rs:195,248,300` |
| MQTT 控制包饥饿 | **2**。biased 普通工作固定优先级，持续 outbound/command ready | 实际 connection mock 持续补充真实 broker 帧，两个版本均超过 PINGREQ 的选择次数界限；ACK/QoS/KeepAlive 进度受影响 | `crates/netbaiot-transports/src/mqtt/fair.rs:16`；`mod.rs:363`；`v5_connection.rs:454` |
| 恢复目录独占 | **3**。起点没有跨进程目录锁，不同端口可共享文件 | 修复后双子进程拒绝竞争者、退出/启动失败后重新取得 PASS；没有把旧实现运行成完整双进程 red | `crates/netbaiot-runtime/src/recovery_io.rs:202`；`apps/netbaiot-server/src/lib.rs:837` |
| 重试临时文件数量 | **2（最终自审补充）**。已有 authority 时可绕过目录遍历容量检查，删除失败后可不断新建 temp | 两套 writer 在已有 authority + 17 个遗留 temp 场景都曾继续提交；新增相同回归 red→green。未真实制造 ACL 导致 temp 删除失败 | `crates/netbaiot-runtime/src/recovery_io.rs:119`；两套 writer |
| 原生 CI 新发现：流拒绝错误丢失 | **2**。HELLO 后客户端已 pipeline SUBSCRIBE，拒绝后立即关闭仍有未读数据 | Windows 原有 official-client 断言实际 FAIL：Unauthenticated 被 ConnectionLost 替代；修复后同一断言 PASS | `apps/netbaiot-server/src/business_stream_v1.rs:201,242,257` |
| AuthCache hit 全表 prune | 当前风险仍存在，**测量确认**；不是本次正确性修复前提 | 1/64/512/4096 entries 的命中、锁持有、分配和过期成本实测，生产算法保留 | `crates/netbaiot-runtime/src/auth.rs:647,847` |

需要缩小的历史判断：NBMQ **在起点已经有** whole-stream trailer/digest，不应声称本次为 MQTT 新增整体完整性。旧实现的 dangling authority symlink 测试也能被拒绝，不能拿该用例声称复现“所有符号链接都被当作首次启动”。这些反证不否定其余 I/O 路径需要直接分类和有界读取。

## 修复说明与红绿证据

### 恢复完整性与 I/O

NBSP v3 writer 对 header、record framing/content/order、trailer count/byte count 做整体 SHA-256；reader 先核对全图结束结构及 digest，再按硬限解码。以下是 `whole_snapshot_integrity` 的真实 Rust 结果；完整三记录图两次都能恢复。

| 修改三记录图 | 修复前 v2 | 修复后 v3 |
|---|---|---|
| 仅文件头 | ACCEPTED | REJECTED |
| 截到第 1 条末尾 | ACCEPTED | REJECTED |
| 截到第 2 条末尾 | ACCEPTED | REJECTED |
| 截到记录中间 | REJECTED | REJECTED |
| 修改 header generation | ACCEPTED | REJECTED |
| 删除完整记录 | ACCEPTED | REJECTED |
| 重复完整记录 | ACCEPTED | REJECTED |
| 调换完整记录顺序 | ACCEPTED | REJECTED |
| 追加垃圾 | REJECTED | REJECTED |

每次提交先确认旧 authority 可解码，再生成独占临时文件、逐记录有界序列化、同步、关闭、替换并完成平台步骤。损坏 authority 不回退旧图也不覆盖。`encode_record` 在追加前检查 record 上限；trailer 开销纳入 segment/total ceiling。server 交给 commit 的 pending payload 不再整批 clone。

公共 `commit([])` 是 no-op，不能清除旧责任。已 ACK 的 `remove_committed` 是另一操作：验证 identity/generation 和完整 cleanup 集合，未知/不可读 stale 文件使 authority 保留；旧 handle 不会删除新 generation。非规范文件名的合法 v2 generation 也保留，迁移后增长正确。

统一 I/O 层区分目录缺失、读不到、格式无效、超限。目录项错误不丢弃；Read 使用实际读取 ceiling；MQTT streaming reader 对读中错误保留 Storage 分类并探测精确 EOF。拒绝链接/特殊文件，Unix 使用 no-follow/nonblocking，Windows regular-file open 检查 reparse flag。父路径/目录必须受信任，运行中替换整个目录不受支持。

新增故障测试包括实际不可读文件和不可访问父目录（明确拒绝以 root 运行而假通过）、中途目录项/Read 错误注入、读取超限、authority 损坏、cleanup 失败、空 commit、旧 handle、bounded serialization、代际迁移和两个 writer 的遗留 temp 容量。两套 writer 创建 temp 前检查 `spool_max_records + 16` 总目录项预算；预算满时保留旧图并返回 Overloaded，修复目录后可继续提交。

### 责任与启动生命周期

`publish` 在同一状态 mutex 下、任何队列/计数修改前拒绝 active ID。没有永久 ID 历史库；原责任全部完成后仍可复用 ID。测试核对同/不同 payload、两 required sinks、第一次 ACK 后不提前 drained、剩余 spool responsibility 和并发一成功一 Conflict。

`restore` 只保留 payload 所有权一次，先算 bounded JSON bytes，再同锁校验全批 ID、sink 唯一/存在/required、global 和各 sink 累计 count/bytes、非负 accepted_at、fanout 和 attempts 数量，随后整体提交。允许历史 routing revision、已完成/已移除 sink 的历史 attempts 和饱和 u32 attempt。记录非法不静默丢弃，失败不留前半批状态。

保留公开 `EventBus::new` 的自动运行兼容行为；server 使用 `new_paused`，完整恢复/认证/证书/监听器/business 配置准备完成后才启动 owned workers 并 mark_running。正常结束异步 join；运行 future Drop 通过 owner cancel/abort 兜底。Drop 是强制取消，不是假装 drain 或承诺未 spooled 内存数据的安全退出。

隔离 current-thread runtime 对 TLS/recovery/admin/TCP/UDP/management/business listener/business token 八类错误各执行三次，并等待任务基线，另验证 running future Drop 后端口和目录锁可重新取得。paused restore 在 owned start 前不会向 sink 投递。

### MQTT 普通工作公平性

command、outbound frame、inbound packet 用三路轮转，取消、session cancellation、idle deadline 仍在外层优先 select。连续 ready 的源至多等待另外两次普通选择；这是工作次数界限，非无条件毫秒延迟保证，socket write 仍受既有有限 timeout 限制。

两个版本各覆盖持续 QoS0→PINGREQ、QoS1→PUBACK→PINGREQ、QoS2→PUBREC/PUBCOMP→PINGREQ、持续 command，共八个 connection 场景；核对 ACK 后旧 packet ID 已从 outbound state 移除。另有 300 次全 ready 轮转、Notify 无 spin、外层取消优先、partial packet 多次 future 取消后保留字节和原 deadline 的虚拟时钟测试。未新增 reader task/队列，未把 outbound 当作 inbound keepalive，未放宽规范断言。

### 目录所有权与依赖

server 在读取两套 recovery 前取得 `.netbaiot.lock` 的 OS advisory exclusive lock；Arc owner 传给 spool、broker 和 blocking jobs，保证 job 未结束前不会释放。锁文件不 unlink。双进程测试使用不同端口，第二实例有限期限失败且不改文件；强制杀死第一个后和启动绑定失败后都能重新取得。

新增生产依赖 fs2 0.4.3、仅 Windows 的 atomicwrites 0.4.4，Unix 显式 libc；锁文件不存事件。审查其 flock/LockFileEx、MoveFileExW 安全封装源码及 MSRV。第三方平台封装内部含 unsafe；项目本身继续 forbid unsafe，无新增 unsafe block。stats_alloc 0.1.10 只在 dev-dependencies 的测试测量使用。

### 红绿复现定位

| 回归 | 修复前运行源码 | RED | GREEN / 最终 workspace |
|---|---|---|---|
| `whole_snapshot_integrity` | f68 起点 + 新测试 | exit 101，7 非法图被接受 | exit 0；最终已选中 PASS |
| active ID / restore whole batch | I/O 修复后、相应责任修复前源码 | 两次 exit 101，覆盖/半批状态 | exit 0；最终已选中 PASS |
| startup owner | 责任修复后，server 暂恢复到 578ec9f 实现，保留同一测试/API | exit 101，任务基线等待超时 | exit 0；24 错误 + Drop PASS |
| MQTT starvation | 9e1eec0 普通固定优先级循环 + 新测试 | exit 101，两个版本超界 | exit 0；8 场景 PASS |
| temp budget（两套 writer） | fb585fa 实现 + 新测试 | 各 exit 101，未拒绝超预算目录 | 各 exit 0，拒绝并保留旧图/修复后重试 |

临时恢复旧实现的复现实验结束后恢复最终文件，未把 red 源码提交或用于最终门禁。原始日志在 `target/strict-audit/`；可审查的脱敏摘录与运行清单在 [evidence](performance/strict-reliability-audit/evidence/README.md)。

### 原生 CI 故障发现与修复

首次 native run [37411015860](https://github.com/sskycn/netbaiot/actions/runs/37411015860) 的 Windows runtime suite 在已有 JWKS HTTPS fixture 的首次认证失败；spool 回归实际通过。fixture 只监听 IPv4，但原客户端依赖系统 localhost 地址选择，且总期限200ms。测试客户端显式 resolve 到该 listener，保留 localhost TLS 主机名、CA、200ms connect/request timeout、outage/oversize/rate-limit 断言；生产 JwtProvider 未改。这消除地址选择变量，不声称已捕获原失败的 DNS/网络时间线。

第二次 [37411658708](https://github.com/sskycn/netbaiot/actions/runs/37411658708) Windows 专项恢复/退出门禁通过，JWKS 也通过，但原 official-client integration 在错误 token 订阅时实际 FAIL（`ConnectionLost`）。源码显示客户端连续写 HELLO/SUBSCRIBE，而服务端认证拒绝后立即 drop TCP stream，未读 pipeline 数据可能让已写 error 被 reset 丢弃。这是依据源码与 [RFC1122 4.2.2.13](https://www.rfc-editor.org/rfc/rfc1122.html#section-4.2.2.13) 的根因推断，未抓包直接确认 RST。

修复只在 legacy business-stream 拒绝/冲突关闭路径：先关闭写半边，再在已有连接任务中有界读至 EOF。最多16KiB discard、1KiB栈缓冲、min(write_timeout,250ms)总期限、取消优先；不创建新 task/queue、不解析/接纳拒绝后的业务数据，不把错误帧写出称为 ACK。恶意超限、网络故障或关机取消仍可提前关闭。新增虚拟时钟/duplex 测试核对 byte ceiling、FIN、deadline 和 cancellation；保持原 SDK 的 Unauthenticated 断言。

第三次 native [37412448455](https://github.com/sskycn/netbaiot/actions/runs/37412448455) 在 f3a5d7c 上三平台 PASS，Windows 原来的两项失败断言均实际 PASS。该提交常规 CI 的 clippy 因新 test module 放在 runtime item 前面 FAIL；未加 allow，已把测试模块移至文件尾，随后本地 stable/MSRV fmt/clippy/full tests 各420 PASS、0 failed、16 ignored。最后测试位置和报告提交继续触发原生与常规 CI；不能把 f3a5d7c 的常规 CI 称为全部通过。

原生 log artifact 通过公开下载代理取得；下载 ZIP 的 SHA256 与 GitHub API 提供的 artifact digest 一致。只读取 bounded 日志文本，不执行下载内容。完整日志、三次 run 的 job/step/commit 及 SHA256 在 target/strict-audit，脱敏摘录/摘要在 evidence。未因日志 API 缺登录返回403而伪造结果。

## 恢复格式与兼容性

NBSP 新 writer=3；新 reader=1/2/3，JSON DeviceEvent schema 不变。v3 header 16 bytes、trailer 52 bytes；大端 u32/u64。digest 覆盖从 magic 到 trailer protected-byte-count，包括 framing/checksums/order；protected count 不含 trailer，自身受 digest 保护。SHA-256 是损坏检测，不是对有权重写文件并重算 digest 的操作者提供认证。

v1/v2 合法 fixtures 保持可读，历史 append-only 重复的相同事件责任仍按已有兼容策略合并。旧格式缺失结束完整性，无法补出被整记录截断的历史；这不是升级后的完整性保证。合法 noncanonical v2 generation 迁移/cleanup 也有回归。

旧版本 reader 不认识 v3；不能把产生 v3 的目录直接交回旧 binary。升级前备份整个 recovery directory（包含 mqtt-runtime.state 和所有合法 legacy spool），在业务消费者 ACK 所有 v3 pending 后，按恢复文档准备回滚。不得手工删除 pending 图来绕过检查。已移除 ConfigAck 返回 IncompatibleSpool，其他损坏/未知格式明确失败，文件保留，readiness 不发布。

Windows 的 ACK cleanup 仍保留合法空 v3 successor，因此仅 `pending_required=0` 不会让旧 reader 自动兼容。停止 gateway 后，必须用当前 reader 校验图确实为零 pending records，再把该空图归档至 active directory 之外；保留完整备份及 MQTT recovery。非空、不可读、未验证的图不能移走以绕过恢复。

NBMQ writer 仍为6，reader 仍兼容1–5，没有因本次修复删除 MQTT 3.1.1/5 行为。两个域各自提交，不是跨文件事务；任何域失败时计划退出保持活着且 unready，按既有限频重试，未观察 ACK 的责任仍需要 spool/replay。一般 SIGKILL/机器断电可丢失非 spooled 内存责任，本次没有改变这项设计。

## 跨平台证据

| 平台 | 结果 | 实际范围 |
|---|---|---|
| macOS 本地 arm64 / hosted macos-latest | **PASS** | 本地 stable/MSRV 各420 passed、0 failed、16 ignored；hosted MSRV完整420/0/16，另先执行恢复/退出专项；本地 MQTT/SDK/fuzz/soak |
| Linux hosted ubuntu-latest 原生 | **PASS** | 本次 MSRV完整420/0/16及恢复/退出专项；常规 CI 另有 stable/MSRV/MQTT/audit。未引用旧 release CI |
| Windows hosted windows-latest 原生 | **PASS** | 首两次 FAIL 保留；第三次 MSRV完整418/0/16及恢复/退出专项。两个 Unix-only权限/符号链接测试不编译，因此少2个，不能计作Windows通过 |

Windows 实现选择 synced+closed temp，然后由 atomicwrites 0.4.4 调用 MoveFileExW(REPLACE_EXISTING|WRITE_THROUGH)，传播 ACL/sharing/replace 错误。没有把 directory sync 改为假成功。ACK cleanup 使用 synced empty v3 successor，与公共空 commit 区分。Unix 仍是 file sync + rename + directory sync。原子可见性、文件同步、rename/write-through 和一般断电持久性不是同一保证；不声称等同一个独立 NTFS directory flush。

依据：[Rust 1.88 Windows fs](https://github.com/rust-lang/rust/blob/1.88.0/library/std/src/sys/fs/windows.rs)、[MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)、[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)、[atomicwrites 0.4.4](https://docs.rs/crate/atomicwrites/0.4.4/source/src/lib.rs)、[fs2 锁契约](https://docs.rs/fs2/0.4.3/fs2/trait.FileExt.html)。这些是实现依据，不能替代 Windows 执行证据。

新增 `.github/workflows/recovery-platform.yml` 使用 Rust1.88，Windows/Ubuntu/macOS native full workspace，job 25 分钟、每个 test step 20 分钟，always 上传包含 exact github.sha 的日志。子进程 kill_on_drop，关键等待3/5秒 timeout。所需九项分别由 spool roundtrip/generation/storage、MQTT storage、server empty drain/required work/storage-repair 用例覆盖；必须看最终 commit 的 Windows 测试执行日志，不能只看 CI 总图标。先单独选中 spool、MQTT recovery、subprocess、ownership、startup，再执行完整 workspace；这些专项与全套都会真实执行，日志中重复出现的同一测试不重复计入完整suite总数。

## 资源安全与最终异常路径自审

| 异常点 | 处理、证据与边界 |
|---|---|
| 接纳后取消 / socket write 取消 | accepted required work 仍在 ActiveEvent；既有 cancelled-inflight test、restart/spool replay 和 ACK 语义回归 PASS。write completion 不转成业务 ACK |
| startup future Drop / 初始化失败 | paused preparation + owned cancel/abort；正常 shutdown join；隔离 task 基线和端口/锁重新取得 PASS |
| 相同 ID 并发、半批非法 restore | 同 mutex 完整 preflight；一成功一 Conflict / 无队列计数副作用 PASS |
| 读中失败、短读、extra tail | 实际 Take ceiling、StorageReader、精确结束和摘要；定向注入/损坏/fuzz PASS |
| 写入失败 / replace 路径被阻塞 | 返回 Storage/Overloaded，旧图/责任保留、temp 清理，repair 后继续；writer tests + 实际 subprocess PASS |
| file sync / Unix directory sync 的底层失败 | 源码所有调用传播错误；本次**没有**真实设备/系统故障或逐个 sync syscall 故障注入，不能宣称全部故障分支动态覆盖 |
| temp unlink 失败 | best-effort 删除之外还有创建前目录预算；超预算拒绝且可修复测试 PASS。没有真实 ACL unlink 故障实测 |
| snapshot 整记录截断 | v3 全图校验拒绝，v1/v2 历史限制明确 |
| 两实例共目录 | server 一份锁贯穿两域；双进程 PASS。仅保证 cooperating local APFS/ext4/NTFS；网络 FS 不承诺 |
| Windows 正常停止 | **PASS**，native专项及完整suite均实际执行空网关和required-work正常停止、失败修复后重试 |
| 持续 outbound 控制包延迟 | 两个版本 fair selector/control/Reader PASS；有限写超时仍决定一次普通工作墙钟耗时 |

每个 sink 的 count/byte/concurrency/timeout/retry 上限保持。restore projections 和 ID 集合受 batch/sink/fanout 硬限约束，不复制 payload；writer 逐条序列化，不新增整批 serialized payload 副本；校验旧 authority 时仍暂存有界的旧恢复批次，峰值需计入该批次、调用方新批次和单条序列化缓冲。reader 的 bytes/decoded ownership 峰值受 configured segment/record/count 上限约束，未进行新的近容量 RSS 实测。blocking I/O 不放 Tokio worker；job 在正常 server lifecycle 顺序执行并持有目录 Arc。drop/abort 不保证已开始 blocking syscall 可立即取消，锁会保留至 job 完成。

raw `RestartSpool::new` / broker 公共 API 为兼容保留，**库的 composition root 必须绑定同一个 owner 并串行安排写/清理**；server 已这样实现。`EventBus::new` 旧公开行为仍需调用 stop_workers；推荐 paused + owned guard，不能声称任意外部调用者只 drop Arc 就安全。文件系统父组件需可信，恶意 live path replacement 不在本次支持范围。

自审未发现本次修改引入新的 false ACK、premature drained、partial restore、stale authority fallback 或无界网络队列。此结论限于上述代码审查/执行范围，不代表穷尽所有调度与硬件故障。

## 命令与日志

所有命令均实际执行；初次最终完整门禁在 `c235f48` 上重新运行（validation-final.json）；native新发现的stream修复及测试模块整理后再运行，详见 `target/strict-audit/validation-stream-final.json`（f3a5d7c + tracked diff SHA）。报告提交的 exact-commit CI 也必须核对。测试退出码0才记 PASS；ignored 不计入通过数。

| 命令/检查 | 状态与结果 | 日志（相对仓库） |
|---|---|---|
| `cargo +stable fmt --all -- --check` / `+1.88.0` 同命令 | PASS，exit0 | `target/strict-audit/{stable,msrv}-fmt-stream-final.log` |
| `cargo +stable clippy --locked --workspace --all-targets --all-features -- -D warnings` / `+1.88.0` | PASS，exit0 | `{stable,msrv}-clippy-stream-final.log` |
| `cargo +stable test --locked --workspace --all-features` / `+1.88.0` | PASS，末轮各420 passed / 0 failed / 16 ignored，exit0 | `{stable,msrv}-tests-stream-final.log` |
| `cargo build --locked -p netbaiot-server`；`cargo build --locked -p netbaiot-device-sdk --example device_mqtt` | PASS，exit0 | `server-build-final.log`、`sdk-build-final.log` |
| `python3 tests/mqtt_protocol_regressions.py --repo . --output target/strict-audit/protocol-results-final.json` | PASS，7/7，含 binary hash + source manifest | `protocol-final.log`、`protocol-results-final.json` |
| `python3 tests/mqtt_conformance/run.py --release-gate --no-build` | PASS，76/76，规范 coverage125/125 | `release-gate-final.log`、`release-gate-results-final.json` |
| `python3 tests/mqtt_conformance/v5_smoke.py` / `v5_mosquitto.py` | PASS，raw + 外部 Mosquitto | `mqtt5-{raw,mosquitto}-final.log` |
| `python3 tests/run_device_profile_mosquitto.py` | PASS，311/5 TCP/TLS、CA/错证书/过期拒绝、各5次持久 reconnect | `sdk-interop-final.log` |
| `python3 tests/measure_device_profile.py` | PASS，有限 SDK memory/task probe；不推导网关容量 | `sdk-measure-final.log` |
| `cargo test --locked -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | PASS，1 selected，12 restart cycles，63.48s | `restart-soak.log` |
| `cargo test --release -p netbaiot-runtime verifier_cache_hit_scaling_audit -- --ignored --nocapture --test-threads=1` | PASS，1 selected，非 production 性能门槛 | `auth-cache-measurement.log`；[JSON](performance/strict-reliability-audit/auth-cache.json) |
| cargo-audit 0.22.2 `audit --json` | PASS，exit0，210 dependencies，0 known vulnerabilities；另有 unmaintained warning | `cargo-audit.json` |
| actionlint（当前 workflows） | PASS，exit0 | `actionlint.log` |
| Windows / Linux / macOS native workflow | PASS；第三次run在f3a5d7c，Windows418/0/16，其余420/0/16；含全部最终生产修复 | native第三次原始log及 [run 37412448455](https://github.com/sskycn/netbaiot/actions/runs/37412448455) |

这里缩写的日志文件均位于 `target/strict-audit/`。RustSec database commit=`ef6173cbc5c50ec8166f9a5b28f07834144373ee`，更新时间2026-10-03；`rustls-pemfile 2.2.0` 的 RUSTSEC-2025-0134 unmaintained 提示保留，没有 ignore advisory。

Fuzz 实际命令（ASan smoke，各10,000 runs，exit0）：先 `python3 fuzz/seed_corpus.py`；随后 `cargo +nightly fuzz run restart_spool -- -runs=10000 -max_len=1048576`，同参数 `mqtt_recovery`；`mqtt_packet` 和 `mqtt_v5_packet` 用 `-max_len=65540`。日志 `fuzz-{spool,mqtt-recovery,mqtt-packet,mqtt5-packet}.log`。这是短 smoke，不是长时间 fuzz；末期 writer/目录预算变化不改变四个纯 decoder 的实现。

首轮 Rust1.88 全 workspace 曾有既有 `jwt_http_uses_https_jwks_and_reports_outage_as_503` 的8秒等待超时（FAIL，exit101）；未改该测试或放宽等待，单项、完整复跑及最终全套均 PASS。保留 `msrv-tests.log` 与 `msrv-jwks-retry.log`，将其视为尚未定位的偶发测试/环境问题。早期受 sandbox 禁止监听导致的 JWKS permissionDenied 通过同代码允许本地监听复跑；没有把它当产品缺陷。复跑协调脚本曾使用错误 example 名和结果复制路径；已更正真实入口并保留相应日志，编排错误不计作产品 PASS 或 FAIL。

未选中项：workspace 16 ignored 中仅 restart soak 和 AuthCache measurement 另行实际执行；其余 broker hotspot/near-capacity recovery/queue-depth/UDP replay 手工 benchmark **NOT RUN**。未做新的整机连接容量、长时间负载 soak、真实断电和全平台磁盘 sync 故障注入，不以 SDK probe 或历史数据替代。

## 变更摘要

逻辑提交依次为：

1. `fb6c9d1` — NBSP v3 whole snapshot integrity。
2. `578ec9f` — 共享有界 recovery I/O、Windows wrapper 和目录 owner。
3. `5823180` — active ID fence / atomic restore。
4. `9e1eec0` — paused startup / owned cancellable workers。
5. `030266d` — MQTT 普通工作轮转。
6. `af76e37` — bounded serialization / legacy generation cleanup。
7. `fb585fa` — test-only AuthCache 测量。
8. `c235f48` — 临时文件目录预算。
9. `6382236` — 完整报告、执行证据和Windows空图回滚说明。
10. `7078f07` — IPv4 HTTPS fixture和恢复专项门禁。
11. `f3a5d7c` — 拒绝流的有界半关闭及回归。
12. 后续提交 — 测试模块位置整理、原生证据和最终报告（不改生产行为）。

I/O 与目录 owner 合并在同一逻辑提交，以便两个域从首次读取到最后 blocking job 共享所有权；Windows replacement 依赖同一 I/O 层。末期自审的 serialization/generation/temp 发现另立提交，便于审查回退；没有顺手重写 AuthCache 算法。

最后代码提交相对基线的完整文件列表和 `git diff --stat`：

```text
.github/workflows/recovery-platform.yml            |  40 ++
 Cargo.lock                                         | 110 ++++
 apps/netbaiot-server/src/business_stream_v1.rs     |  68 +++
 apps/netbaiot-server/src/lib.rs                    | 231 ++++----
 apps/netbaiot-server/tests/server.rs               | 246 +++++++--
 crates/netbaiot-runtime/Cargo.toml                 |   8 +
 crates/netbaiot-runtime/src/auth.rs                | 127 +++++
 crates/netbaiot-runtime/src/event.rs               | 470 +++++++++++++++--
 crates/netbaiot-runtime/src/lib.rs                 |   1 +
 crates/netbaiot-runtime/src/management_auth.rs     |  10 +-
 crates/netbaiot-runtime/src/recovery_io.rs         | 267 ++++++++++
 crates/netbaiot-runtime/src/spool.rs               | 584 +++++++++++++++++----
 crates/netbaiot-transports/src/mqtt/broker.rs      | 194 +++++--
 crates/netbaiot-transports/src/mqtt/fair.rs        |  91 ++++
 crates/netbaiot-transports/src/mqtt/mod.rs         |  67 ++-
 .../netbaiot-transports/src/mqtt/v5_connection.rs  |  15 +-
 crates/netbaiot-transports/tests/end_to_end.rs     | 189 +++++++
 docs/delivery-semantics.md                         |  15 +
 docs/graceful-restart.md                           |  11 +
 docs/mqtt.md                                       |   9 +
 .../performance/strict-reliability-audit/README.md |  31 ++
 .../strict-reliability-audit/auth-cache.json       | 155 ++++++
 docs/restart-spool.md                              |  80 ++-
 fuzz/Cargo.lock                                    | 103 ++++
 fuzz/seed_corpus.py                                |  10 +
 25 files changed, 2788 insertions(+), 344 deletions(-)
```

以上25个文件包含代码、测试、锁文件、4份行为文档、benchmark JSON/README、原生 CI 和 fuzz seeds。本报告及脱敏 evidence 另作文档提交；没有改 release 版本、tag、历史 benchmark 或生产数据。

## 剩余风险

- **已确认未修复的阻塞**：本次范围内没有剩余已知项。原生九项/完整suite实际通过；任何最终分支CI失败仍阻止合并，不能引用中间提交的绿色状态代替最终检查。原旧Windows flush故障没有取得native red，保持静态确认分类。
- **尚未复现/定位**：独立 sync syscall 的失败行为只做传播自审，未全部动态注入；JWKS 一次超时原因未知；I/O/ownership 的所有起点风险未一一做旧实现 red。
- **设计取舍**：旧 NBSP1/2 无法识别历史整记录截断；两域不是跨文件事务；突然终止可丢失非 spooled 内存流量；trusted local directory / cooperating processes 是锁和路径安全前提；bounded I/O 不意味着故障硬件 syscall 有严格墙钟 deadline。
- **非阻塞优化**：AuthCache hit 保留全表 prune。4096 entries 的 median hit203.042µs、mutex202.971µs、每hit2次分配/466,976 bytes。明确支持后续有界过期/顺序维护优化，但不能以本次带统计开销、同时进行其他验证的测量宣称生产吞吐。详见 benchmark README。
- **依赖维护**：rustls-pemfile 的现存 unmaintained warning 待后续迁移；本次不把 warning 称为漏洞，也不屏蔽它。

## 发布判断

**本次审查范围内没有剩余已知阻塞项。** 已有 Windows/Linux/macOS 的原生 first/replace/recover/empty stop/required stop/storage failure+repair、ownership/lifecycle 和完整suite证据，以及上述功能修复前失败、修复后通过的记录。正式合并仍要求最后审计分支提交的全部CI通过；若之后有失败，应保留日志、修复并重新验证 exact final commit。此判断不外推成一般断电持久性、生产容量或全部故障分支已穷尽。

版本和正式发布由作者决定。本任务到此仍保留审计分支用于审查，不自动合并 main、推送 main、打 tag 或发布 Release。
