# NetbaIoT 正确性、资源有界性与可靠性专项审计

日期：2026-09-19。基线：`4fcd393`。环境：macOS arm64、Rust 1.97.1、PostgreSQL 17.11；数据库与 ICU 使用 AGENTS.md 指定的 `/opt/local/lib/pgsql`、`/opt/local/lib/icu`。本报告与修复一起提交；没有 push。验证结束后已停止本任务拥有的临时PostgreSQL集群。

## 1. Executive Summary

| 级别 | 确认问题 | 修复 | 未修复 |
|---|---:|---:|---:|
| P0 | 0 | 0 | 0 |
| P1 | 5 | 5 | 0 |
| P2 | 4 | 4 | 0 |
| 合计 | 9 | 9 | 0 |

“P0=0”表示本次审计没有确认 P0，不表示任意恶意输入的形式化安全证明。所有修复先运行能失败的测试，再修改实现；红灯日志保存在 [audit-evidence](audit-evidence/)。一次 finding 可以包含同一不变量的多个失败场景。

修复集中在命令尝试隔离、租户预算生命周期、HTTP 请求阶段隔离、认证取消、整包截止时间、MQTT filter 语法、Content-Encoding、命令状态单调性及命令重试。未增加 MQTT5/QoS2/persistent session/retained/LWT/wildcard routing/cluster/Redis/Kafka 等功能。生产默认容量、超时和最大重试次数均未增大。唯一新增 Cargo 依赖边是传输集成测试复用已有的 workspace `sqlx`，没有引入新的生产 crate。

已验证的结论包括：真实 PostgreSQL 提交在 PUBACK 和应用回执之前；并发重复只有一个 logical message/outbox；旧命令尝试不能更新新尝试；重连后的旧队列不能获得第二份租户预算；认证期间观察到 EOF/shutdown 后不会注册会话；队列失败、接收者丢弃、连接退出会释放 count/byte permits；过载通过拒绝和有限重试处理。

## 2. Architecture Reviewed

完整读取了 AGENTS.md、implementation-report、architecture、mqtt、device-protocol、delivery-semantics、resource-budgets、根 Cargo.toml、development.json 和全部 migration。随后追踪以下实现及原有测试：

| 模块 | 实际追踪的主路径 |
|---|---|
| core / codecs | 强类型 ID、同步版本化 DeviceCodec、JSON 结构预检、字段/字符串/数字限制、canonical identity |
| runtime/auth | 四传输共享 StaticAuthenticator、Secret/Signed、恒时摘要/HMAC 校验、只读有界 provisioning、独立 admin 权限 |
| runtime/quota | Connections、Admission、RateLimiter、ByteBudget、获取失败回滚和 RAII 释放 |
| runtime/sessions | generation、替换取消、presence、租户预算、sender/receiver/queued item 生命周期 |
| runtime/commands/store/worker | queue→持久化→claim→本地 route→write→PUBACK→execution ACK，outbox 租约/退避/终止/maintenance |
| storage | MemoryStore 锁内原子性；PgStore 所有事务、SQL、配额锁、去重、claim、终态、retention |
| transports/common | accept 前 admission、TLS、Reader、write deadline、listener JoinSet、取消与强制关闭 |
| MQTT | fixed header、Remaining Length、CONNECT、UTF-8、topic ACL、订阅、QoS IDs、上下行 ACK 与会话清理 |
| HTTP/TCP/UDP | header/body/framing、认证、共享 codec/ingress、下行、UDP max+1/HMAC/replay、各自关闭路径 |
| server | 配置边界、TLS 文件验证、全部 bind 后启动、HTTP sink、六个顶层 owner task、SIGINT/SIGTERM |

没有把旧实现报告作为正确性证据。生产源码没有 unbounded channel，也没有每条消息 spawn 等待 semaphore/channel 的路径。生产 std::Mutex 只保护同步操作，guard 不跨 await；数据库 transaction 中没有业务网络调用。codec 不持有会话、数据库或异步任务。

## 3. MQTT Protocol Audit

规范依据：[OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)，尤其 1.5.3、2.2、2.3、3.1、3.3、3.8、3.10、4.7。这里只声明实现的受限 profile。

| 项目 | 实现与证据 |
|---|---|
| CONNECT | 首包只能 CONNECT；MQTT/name、level4、reserved/Will/password flags、长度、非空 client ID；再次 CONNECT 断开。合法 Will/CleanSession=0 返回 CONNACK 5；错误凭据 4；client ID 与 provisioned username 不同返回 2；配额失败 3 |
| 身份所有权 | socket route 按认证产生的 DeviceKey；client ID 必须等于 credential ID；一设备一静态 credential。相同设备重连替换，其他设备不能用 client ID 踢走它。body/topic 从不产生可信身份 |
| 认证 | 认证 deadline；新共享 stream-auth helper 同时观察 EOF、stop，保留有界 pipelined bytes。原先迟到认证能创建僵尸会话，见 A04 |
| fixed header | 支持方向的类型与 flags；QoS3、QoS0+DUP、无效 reserved bits 拒绝；QoS2 PUBLISH/control 拒绝；retained PUBLISH 连接关闭且不 ACK |
| Remaining Length | 1–4 字节及 268435455 编码单元测试；配置 packet cap 包含 fixed header；checked 算术；截断返回 NeedMore；5 字节/continuation/nonminimal 编码错误；检查声明长度后才 split/copy |
| fragmentation | CONNECT、两种 PUBLISH、PUBACK、SUBSCRIBE、UNSUBSCRIBE、PINGREQ、DISCONNECT：每个切分位置、1 byte/read、合并多包、完整包加半包均覆盖。声明 16KiB payload 的三字节 Remaining Length 也遍历所有切分 |
| UTF-8 | 拒绝非法编码、NUL、控制字符、非字符；保留 U+FEFF。控制/非字符拒绝是本 profile 的严格策略；topic/ID 大小与深度有独立限制 |
| topic ACL | exact 字符串由可信 DeviceKey 生成后比较。跨设备、跨租户、相似前后缀、百分号编码、额外/空 segment 均不能授权；超过深度先由 parser 关闭 |
| SUBSCRIBE | 只允许 own down/up_ack，QoS0/1；QoS2、合法 wildcard、shared/foreign topic 返回 0x80。重复 exact subscribe 替换而不累计 |
| filter 语法 | 新增 `+` 必须占整级、`#` 必须占最后整级；畸形 SUBSCRIBE/UNSUBSCRIBE 关闭连接。合法但不支持的 filter 与畸形 packet 分开处理，未实现 wildcard matcher |
| UNSUBSCRIBE | 合法 filter 无匹配也发 UNSUBACK；generation 保护，只清理自己拥有的 exact entry |
| PING / DISCONNECT | 只在 Connected 处理；keepalive 1.5×interval；0 使用独立服务 idle policy；clean-session state 按 RAII 清理 |
| IDs | 非零 u16；inflight 最大32；65535→1 回绕会跳过活跃 ID；释放后可复用；未知非零 PUBACK 忽略。业务 source ID 与 packet ID 独立 |
| QoS0 | 入站无 PUBACK；可选 up_ack；出站按订阅 QoS0 写出；没有 MQTT transport receipt/execution 保证 |
| QoS1 入站 | decode/auth/ACL→store acceptance→PUBACK→可选 up_ack。commit 前、commit 后/PUBACK 前、PUBACK 后/up_ack 前均有真实进程崩溃测试 |
| QoS1 出站 | owner 保留 packet ID、command 元数据、count/bytes permits，PUBACK/断开释放；attempt 与 lease 隔离旧回调。没有在 clean session 内重发 packet 的独立 timer；超时关闭，业务 command_id 可重试 |
| keepalive / read | whole-packet deadline 与 keepalive 分开；buffered tail 不重置起始时间，已到期即使 read 已 ready 也拒绝；没有每个字节刷新 deadline |

真实客户端：Paho MQTT Python **2.1.0** 完成 CONNECT、QoS0/1 publish、exact subscribe、downlink/PUBACK、PING、disconnect/reconnect、bad credentials。不是 MQTT 全规范认证或跨客户端版本兼容矩阵。MQTT5 仅返回拒绝，不能视为支持。

## 4. Resource Budget Map

下表为**默认配置**；压力实验有明确缩小的限额。尺寸是逻辑预算，物理 RSS 不等于这些数的和。

| Resource / owner | 单对象 / device | Tenant | Node/global | 溢出 | 释放 |
|---|---:|---:|---:|---|---|
| Stream sockets / listener+connection lease | 2/device、32/IP | 64 | 256 | spawn/TLS 前 close | task owner drop |
| Stream reservation / Connections | 512KiB/connection | connection cap，最多32MiB | 128MiB | try-reserve 失败 | connection lease drop |
| Reader / connection | MQTT64KiB；TCP64KiB+4；初始≤4KiB | connection cap | reservation+256 sockets | invalid/timeout close | owner drop |
| HTTP / handler | body64KiB、header8KiB/32；request-stage 1/device | 4 handlers | 16 handlers | 413/429/close | body完成、超时、取消 drop |
| Ingress / Admission | 1/device、一条 decode output | 4 | 16；输入 bytes 2MiB | 无等待队列，reject | AdmissionLease drop |
| JSON / codec | depth8、fields64、field256B、input64KiB | ingress/connection cap | 同上 | codec error | 同步栈和 Vec drop |
| Outbound commands / session+QoS state | 32 items、256KiB/connection | 2MiB，跨旧新 owner 共用 | 8MiB | try_send/reject，DB 稍后重试 | ACK/失败/receiver drop/cancel |
| Receipt QoS1 / connection | 与 command 共用32 IDs；receipt accounting ≤256KiB | 64连接派生上界 | 256连接派生上界 | close | PUBACK/owner drop；不保存 receipt payload |
| Subscription index / registry | 2/device/connection；16 filters/packet；topic256B/8级 | 128 | 512 | SUBACK 0x80 | unsubscribe/generation lease drop |
| Credential / static auth | 一设备一 credential，64B secret文本、32B HMAC key | 128 devices | 1024 | 启动失败 | provider drop；变更需要重启 |
| Presence / Sessions | 一项/device；离线TTL24h | provisioning 128 | 1024 | reject | worker TTL cleanup |
| Tenant byte-budget identity / Sessions | weak semaphore identity | 一项/tenant | ≤max_devices；只保留活跃 ownership | reject新tenant | 无 endpoint/permit 后下次 register清理 |
| IP rate table / RateLimiter | 32/s/IP，1秒窗口 | 未认证时未知 | 512/s；1024 entries | reject/drop | 1秒过期，满时拒绝新key |
| Authenticated rate state / Admission | 16/s、maxdevices keys | 128/s、最多maxdevices tenant keys | 512/s | reject | active=0且窗口过期删除 |
| UDP / serial owner | 1201B接收，1200B硬上限 | 单一处理owner | 一buffer/一operation | drop，无响应 | 每次循环复用 |
| Replay / UDP owner | 2 boots/device；64bit序列窗口 | 256 | 1024；TTL120s | 满时拒绝新boot | 只驱逐过期项 |
| Retained commands / Store | 16条×≤16KiB | 128条 | 1024条 | admission reject | expiry+5min批量删除，attempts cascade |
| Command attempts / Store | 每command最多5条 | ≤128×5 | ≤1024×5 | Failed/TTL | command cascade delete |
| Ingress+dedup / Store | 1000条、charge≤2MiB | 10000条/16MiB | 100000条/128MiB | 原子配额检查后reject | 24h TTL，单批≤16 |
| Outbox / Store | 每 ingress 一job；最多5 attempts | ingress quota | ingress quota；worker一次claim1条 | 到期/耗尽 terminal，保留状态 | 随 ingress cascade |
| DB / PgPool | 单caller有deadline | bounded callers共享pool | 8 connections；最多64可配置 | acquire/statement/lock超时 | transaction rollback/drop |
| DB结果集 / workers | claim1；command/device batch16 | command/connection cap | active snapshot≤256；每SQL结果≤batch | limit截断或error | batch scope/drop |
| Codec registry / bootstrap | immutable versioned lookup | — | ≤64条 | 配置错误 | 服务生命周期结束 |

`charge = 16 * canonical_bytes + 8192` 是保守逻辑计费，不是 PostgreSQL 真实磁盘占用。bounded JSON 的 decoded struct/字符串开销还受 fields/count/connection 共同限制。DB/workers 没有与 backlog 等长的内存队列。

**未由应用精确控制的资源**：kernel accept/socket buffers、文件描述符上限、TLS/allocator/runtime 隐含开销、DNS与客户端库内部开销、数据库索引/表膨胀/WAL/autovacuum/磁盘配额。部署需设置 OS/container/PG 限额；本轮没有把逻辑 reservation 宣称为物理内存硬上限。未认证流量只能按 IP/global 限制；分布式来源仍可能占满全局 admission，无法在认证前按租户公平归因。

## 5. Task Lifecycle Map

| 创建者 → owner | 边界 | shutdown | failure policy |
|---|---|---|---|
| server::run → top JoinSet | 3 stream listeners+1 UDP+2 workers，固定6 | drain flag→cancel tokens→等待deadline→abort/join | 任一 service 意外结束使全服务停止；交给外部 supervisor重启 |
| serve_stream → connection JoinSet | 获取 node/IP/memory permit 后才 spawn；全传输合计≤256 | stop accept；子token取消；有限 drain；abort并join | connection错误只关闭该连接；JoinError结构化日志 |
| MQTT/TCP connection → 自身 | 一个读写owner；没有每packet、timer或command任务 | stop/generation cancel；RAII drop queue/IDs/subscriptions/session | 协议/ACL/timeout/write错误关闭 |
| HTTP connection → Hyper service future | 每连接1请求、全局16handler、认证后tenant/device限额 | graceful_shutdown与外层deadline；未提交无成功回执 | 有界HTTP错误或关闭 |
| UDP listener → 自身 | 串行一datagram | 完成已进入的有界operation后停止 | 网络owner失败停止服务，单包错误丢弃 |
| delivery worker → 自身 | 一leased item、一次外部call、一个poll timer | 当前call按deadline结束，未完成lease可回收 | storage错误终止worker/server；网络错误有限退避 |
| command worker → 自身 | bounded snapshot/batches，inline dispatch | cancel后不再claim；已claim可恢复 | storage错误终止；队列失败保留有界重试状态 |
| Tokio/SQLx/HTTP库内部 | pool/connection/bounded调用方派生 | owner/runtime drop | 不将它们计成独立无界业务task |

生产没有裸 `tokio::spawn` 消息循环，任务入口只有两层 JoinSet。顶层硬截止时间与 listener 本地截止时间同时生效；若顶层先 abort listener，JoinSet drop 会请求取消子任务，而这些子任务的析构完成可能晚于 `run()` 返回一个调度步。可执行进程随后关闭 Tokio runtime。**不声称嵌入式调用 `run()` 返回的同一瞬间，所有库内部及孙任务析构都已完成**；本轮测试验证 socket与应用permit最终归零，未对任意 blocking 第三方扩展给出即时析构保证。

## 6. Queue Map

| 队列/过渡 | producer→consumer | count/bytes | 满时 | shutdown |
|---|---|---|---|---|
| socket→Reader | peer→connection | packet/frame硬上限+reservation | close | 丢弃未形成消息的partial bytes |
| HTTP body | Hyper→handler | 1/request，64KiB，分层handler slots | 413/429 | 未接受的body不ACK |
| ingress admission | transport→Store | 16/4/1；2MiB；没有消息mpsc | try-acquire拒绝 | 已admit操作按DB deadline完成或回滚 |
| outbox | atomic ingress→delivery worker | DB count+charged bytes；worker取1 | DB配额拒绝新ingress | 已提交row保留，lease到期可恢复 |
| commands | admin/router→DB worker/HTTP pull | DB 1024/128/16×16KiB | reject | durable记录按TTL继续存在 |
| outbound mpsc | command worker→local writer | 32items+connection/tenant/global bytes | try_send失败，所有部分permit回滚 | receiver/drop取消释放 |
| QoS1 inflight | writer→PUBACK handler | 32 IDs；QueuedCommand仍持有原permits | close，不spawn等位task | clean-session丢弃协议状态，DB command可重试 |
| DB pool waiter | bounded handlers/workers→pool8 | 上游资源限额导出有限caller；无额外消息copy队列 | timeout | 取消transaction/pool future，释放资源 |

## 7. Persistence Semantics

PostgreSQL ingress 在一个事务内写 normalized message、dedup唯一键、outbox以及可选 execution ACK；只有 commit 成功后返回 durable receipt。MemoryStore 用单锁实现相同逻辑，但返回 volatile，崩溃不保留数据。

Canonical 内容包含可信 DeviceKey、source_message_id、occurred_at、typed payload；排除生成UUID与received_at。字段排序稳定。相同 ID+内容返回原 receipt；不同内容 Conflict。去重24h，不续期；到期后允许再次接收为新消息。

配额 admission advisory lock 与数据库 UNIQUE 是不同防线：前者串行化配额/幂等判断，后者保证最终唯一性。32并发相同内容仅一个 first receipt；混合32请求得到16 duplicate、16 Conflict。

Outbox claim 使用 SKIP LOCKED；网络操作发生在 claim commit 后。owner UUID、attempt、leaseexpiry 条件阻止旧 worker覆盖新worker。外部成功但 finish 前崩溃会重复投递同 message_id；下游必须幂等。默认租约30s、外部call5s，失败仅 retryable 类别退避；网络、429、5xx可重试，其他HTTP失败terminal。最多5次/1h TTL。数据库错误不循环重启worker，而是停止服务。

失败/过期job保留可检查状态，最终随24h ingress retention删除。**durable acceptance 不等于保证业务端必然成功**；有限重试、TTL和清理是明确的数据生命周期。需要运营方在保留窗口内检查 terminal失败，不能把这解释为 exactly-once 或零损失永久归档。

清理单批默认16，retention索引存在，attempts随command级联。读取没有加载整表的业务list接口。配额 COUNT/SUM 仍是受容量限制的全表聚合；command claim每条执行save/next/attempt三次SQL，是**有界的3N写往返**，并未宣称已经消除N+1形态的成本。单事务deadline使超时回滚；高延迟DB下这仍是待优化的扩展性限制。

## 8. Command Semantics

| 状态/信号 | 含义 |
|---|---|
| QUEUED | command已存储；PostgreSQL持久，MemoryStore volatile |
| DISPATCHING | 一个有限attempt取得lease；尚未证明write成功 |
| SENT | 本次transport write成功；HTTP在response连接完成后标记 |
| RECEIVED | MQTT PUBACK或合法application ACK证明接收；不是执行成功 |
| execution RUNNING/SUCCEEDED/FAILED | 设备显式command_ack，可信身份与command owner相符后事务更新 |
| delivery FAILED | 最大attempt耗尽或终止；与execution Failed分开 |
| EXPIRED | 未完成command超过TTL，阻止新发送和晚ACK |

`CommandRecord.lease_expires_at` 与 `attempts` 随 claim 返回；outbound和HTTP完成回调携带该attempt。Store在锁/行锁内检查owner、attempt、lease和TTL，旧更新返回显式 `false`，router记录stale回调但不增加成功指标。dispatcher拒绝无claim/到期record，writer不发送过期lease条目。已有存储JSON缺少lease字段可反序列化为None，下一次claim建立新lease；旧版本未知lease的回调不会被当作新尝试。

command重试的 `next_attempt_at = lease_expires_at + exponential full jitter`，最大次数和TTL不变；lease deadline不被backoff延长。抖动由command_id/attempt确定，有限、可复现；不用于安全随机性。离线记录仍受count/bytes/TTL约束。重连/延迟ACK/进程崩溃仍可造成重复下发同command_id，设备必须对执行幂等。

## 9. Findings

### A01 — P1：旧命令发送尝试可以更新新尝试

- 文件/函数：runtime `CommandRouter::state/dispatch/pull`、sessions `QueuedCommand`、两种Store `command_state`、MQTT/TCP/HTTP写完成路径。
- 根因：原API只有device+command_id+state，没有claim attempt/lease；新claim之后收到旧PUBACK会把新attempt从Dispatching标成Received，并写到新attempt历史。
- 复现：claim attempt1→推进逻辑时间并claim2→执行attempt1 Received回调；原状态变Received，测试要求保持Dispatching。
- 影响：跨重连/重试的发送归属被破坏，观察到的接收状态不再描述当前attempt。
- 修复：贯穿持久化record、queue和回调的attempt/lease fencing；应用execution ACK仍按唯一command_id和可信设备处理。
- 回归：`audit_stale_command_attempt_is_fenced`，同一contract在真实PG运行；[红灯](audit-evidence/red-command-attempt.log)。

### A02 — P1：重连可重建租户字节预算

- 文件：runtime/sessions `register`、`SessionLease::drop`、quota ByteBudget。
- 根因：最后一个“当前”session删除tenant budget，但旧队列/inflight permits仍活着；新session获得新semaphore。
- 复现：旧连接占满16B→替换会话→新当前会话退出→再注册→原实现还能入队1B。
- 影响：绕过tenant outbound隔离，虽仍受global预算限制，其他tenant份额会被侵占。
- 修复：tenant registry保留weak semaphore identity；任何endpoint或OwnedSemaphorePermit存活都复用同一预算；registry本身限制max_devices，过期identity清理。最终复查补充了“清理后最后一个permit释放”的交错：若weak已失效，必须把新semaphore身份写回已有entry，防止后续注册再次分配预算。
- 回归：`audit_tenant_budget_survives_superseded_queue`、receiver/drop释放测试；[原问题红灯](audit-evidence/red-tenant-budget.log)；`expired_tenant_identity_replacement_is_shared`确定性覆盖清理与注册间释放最后owner的交错，[修复迭代红灯](audit-evidence/red-weak-identity.log)。后一个问题是本轮修复复查发现，没有重复计入基线finding。

### A03 — P1：HTTP慢请求阶段未按租户隔离

- 文件：transports/http `handle`。
- 根因：global handler slot在body前取得，tenant/device admission在body收完后才取得；一个tenant可通过多个设备耗尽16个handler。
- 复现：占用该tenant唯一request-stage permit时，另一设备HTTP请求仍被原实现202接受。
- 修复：认证后、读body/command pull之前取得共享protocol_admission的tenant/device permit，整个handler持有；不扩大global容量。
- 回归：`audit_http_body_admission_respects_tenant_capacity`；HTTP慢body和关闭测试；[红灯](audit-evidence/red-http.log)。

### A04 — P1：认证完成晚于断开/关闭仍能注册会话

- 文件：common `authenticate_stream`、MQTT/TCP connection。
- 根因：直接await authenticator期间不观察EOF/stop；完成后无draining复查。
- 复现：可控auth gate挂起→连接断开或服务器stop→释放gate；原实现发成功认证回复，或注册旧连接并取消较新会话。
- 修复：一个owner select认证、EOF和stop；pipelined bytes进入原有有界Reader；draining复查。EOF测试在server观察到FIN后再检查新owner，避免把OS FIN调度当作协议保证。
- 回归：`audit_shutdown_during_auth_cannot_register_session`（MQTT/TCP）、`audit_disconnected_auth_cannot_replace_new_session`；[原失败场景](audit-evidence/red-auth.log)。

### A05 — P1：整包截止时间可被缓冲尾部/ready read延长

- 文件：common `Reader::consumed/read_more`。
- 根因：consumed给已到达的尾部字节新起始时间；Tokio timeout可先poll已ready的IO，已经过期仍读成功。
- 复现：暂停时钟，首字节后推进31ms（budget30ms），再给ready字节；以及完整包+partial tail、20ms后consumed、再11ms。
- 修复：非空tail保留原始起始时间；read前显式检查绝对deadline。该保守起点可能较tail实际首字节更早，但不会延后安全截止时间。
- 回归：两个 `common::audit_deadlines`；[红灯](audit-evidence/red-deadline.log)。

### A06 — P2：畸形MQTT filter未作为协议错误

- 文件：mqtt/packet `valid_topic/valid_filter`。
- 根因：filter=true直接跳过全部wildcard语法检查。
- 复现：`a+`、`a/#/b`、`##`、`a/+b`、`a/b#`被parser接受，SUBSCRIBE失败码或UNSUBACK掩盖畸形packet。
- 修复：按整级和末级规则验证语法；合法不支持的wildcard仍由ACL返回0x80，没有增加routing能力。
- 回归：`audit_malformed_wildcards_are_protocol_errors`、真实socket畸形filter、有效种子fuzz；[红灯](audit-evidence/red-filter.log)。

### A07 — P2：HTTP忽略Content-Encoding

- 文件：http `handle`。
- 根因：没有解压支持，但忽略header而把body直接解析为JSON。
- 复现：Content-Encoding:gzip携带明文JSON被202接受。
- 修复：明确拒绝任何Content-Encoding header，返回415；无需分配解压buffer。
- 回归：`audit_http_rejects_unsupported_content_encoding`；[红灯](audit-evidence/red-http.log)。

### A08 — P2：命令终态/尝试历史不一致

- 文件：MemoryStore/PgStore `command_state`。
- 根因：内存适配器在拒绝晚回调前写Expired；PG按传入state写attempt history，而不是应用单调规则后的state。
- 复现：Succeeded/Received命令过TTL后收到Sent，原内存记录变Expired；PG Received后Sent，record仍Received但attempt history变sent。
- 修复：拒绝回调不改变已完成record；PG history写入实际应用的delivery状态。
- 回归：`audit_late_transport_update_preserves_completed_command`、`audit_postgres_attempt_history_cannot_regress`；[内存红灯](audit-evidence/red-command-terminal.log)、[PG红灯](audit-evidence/red-command-history.log)。

### A09 — P2：命令重试缺少backoff/jitter

- 文件：两种Store claim、runtime/worker retry helper。
- 根因：所有command只有固定lease延迟，没有独立retry backoff，同批command会同步再次claim。
- 复现：在lease到期的同一毫秒立即再次claim；原测试取得下一attempt，期望仍处于退避。
- 修复：lease expiry与retry due分离；复用已存在的指数full jitter算法，以command ID分散；不增加attempt上限，不延长发送lease，不越过TTL。
- 回归：`audit_command_retry_uses_bounded_backoff`及真实PG共享contract；`command_backoff_is_positive_bounded_and_spread_across_ids`覆盖64 IDs×100 attempts；[红灯](audit-evidence/red-command-retry.log)。

## 10. Fault Injection Results

| 实验 | 观测结果 / 边界 |
|---|---|
| MQTT commit前SIGKILL | PG trigger在message已插入、outbox事务尚未提交时等待advisory lock；无PUBACK/up_ack；kill后事务回滚；重试首次接受 |
| commit后PUBACK前SIGKILL | 测试Store wrapper在真实PgStore.accept已commit后、返回Ingress前停住；kill；重试duplicate且原message_id |
| PUBACK后up_ack前SIGKILL | 测试AsyncWrite wrapper只拦截up_ack PUBLISH；设备已读PUBACK；kill；重试原receipt且message/outbox各1 |
| outbox claim后、send前SIGKILL | 子进程已完成真实claim commit；lease过期后新owner领取同ID，attempt2，外部副作用仅一次 |
| outbox副作用后、finish前SIGKILL | 测试业务sink文件已经写入稳定ID；kill后重领并再次写入同ID；两次投递、唯一业务ID一个，证明需要下游幂等 |
| 旧outbox owner回写 | owner+attempt+expiry检查拒绝；新owner仍可finish |
| outbox插入失败 | 原PG integration trigger RAISE EXCEPTION；message和command execution一起回滚 |
| 32并发dedup | 一条message、一条outbox；只有一份first receipt；16/16混合内容分别duplicate/Conflict |
| DB锁延迟+pool2耗尽 | 固定16 callers均deadline失败，取消后无部分pressure记录；释放锁后成功接受新消息；没有新增sender task |
| HTTP slow/chunked/malformed | 413/400/401/504；body deadline固定；没有接受失败body；shutdown强制截止释放owner |
| TLS半握手 | timeout关闭；stop中断；无私钥的证书文件启动失败 |
| UDP超长/失败codec/replay | 1201B包拒绝；无效payload不消耗序号，后续同seq合法包可接受；满cache不驱逐活跃boot，序列不回绕 |
| QoS慢消费者 | 12个command，queue/inflight最大4items/1804B，8次queue reject；不读取socket或PUBACK，超时后items/bytes=0 |
| 下游响应1s / deadline500ms | 接受8条达到device持久化限额；4条429；16次有限失败，8条terminal记录；worker内存仅一个leased item |
| shutdown | MQTT已admit ingress完成后可发receipt；认证、HTTP body、TCP半帧、UDP处理中、TLS握手均验证取消/有界drain；SIGTERM smoke通过 |

崩溃测试全部使用独立、父进程拥有的测试进程并强制kill，测试专用wrapper不进入生产源码。outbox副作用sink是有界文件fixture，实际HTTP sink另由smoke及慢下游实验验证。租约恢复通过显式传入“超过lease”的逻辑时刻，未在每例真实等待30秒。没有模拟主机掉电、PG fsync失效、磁盘满或多节点网络分区。

## 11. Fuzz Results

六个目标均直接调用生产parser/codec。MQTT corpus增加合法CONNECT（含不支持但合法flags）、SUBSCRIBE/UNSUBSCRIBE、三种Remaining Length规模的PUBLISH和畸形filter。目标增加消费进度、NeedMore不消耗、输出字段/packet大小界限，TCP也检查消费和输出长度；JSON成功输出只有一个可信设备message。

最终smoke配置：MQTT packet **500,000 runs**；fixed_header、remaining_length、TCP、UDP、JSON各 **100,000 runs**；ASan开启；`-max_len=65540`，没有panic/ASan failure/无限循环发现。此前另跑过500,000 MQTT runs；最终统计不靠重复相加宣传覆盖率。种子生成器：[fuzz/seed_corpus.py](../fuzz/seed_corpus.py)。结果摘要见 [fuzz-summary.json](audit-evidence/fuzz-summary.json)。这是短campaign，不是形式化证明或长时间安全认证。

## 12. Performance Results

### Microbenchmark

原7项和扩展的small/medium/configured maximum、1/64/256 registry规模都使用black_box；每项1000 warmup，原项20000、扩展项10000 samples。baseline源代码从`git archive 4fcd393`放入隔离目录，只复制同一benchmark harness；before/after串行交替三组，记录各组throughput和P50/P95/P99的中位数。最终数据见 [benchmark-summary.json](audit-evidence/benchmark-summary.json)。

一次初始对照观察到MQTT decode P50 125→167ns、encode 83→125ns；把filter grammar拆成单独函数并给小Topic Name检查内联提示后再测。所有语法检查保留，无unsafe、无容量或超时放宽。原始初次对照、拆分与内联单次实验日志也保留，避免只挑选有利结果。完成最后预算身份修复后再次运行三组（`complete-benchmark-*.log`），中位数如下；此前重复实验与最终结果均保留，不能据一次内联实验宣称优化成功。

| Microbenchmark | before P50/P95/P99 ns | after P50/P95/P99 ns | before→after ops/s |
|---|---:|---:|---:|
| mqtt_decode | 125/167/167 | 167/167/209 | 6,537,195 → 5,379,177 |
| mqtt_encode | 83/125/125 | 125/125/208 | 6,367,991 → 7,118,811 |
| topic_acl | 42/125/167 | 42/84/84 | 9,884,475 → 13,343,341 |
| subscription_lookup | 41/42/42 | 41/42/42 | 21,866,885 → 22,102,500 |
| json_codec | 1334/2084/2250 | 1292/2083/2125 | 443,586 → 656,037 |
| tcp_frame | 125/125/166 | 125/125/167 | 6,804,360 → 6,967,629 |
| ingress_admission | 209/250/250 | 209/250/292 | 3,999,200 → 3,989,826 |
| mqtt_decode_small | 125/167/167 | 167/167/209 | 6,108,269 → 5,349,739 |
| mqtt_decode_medium | 375/417/417 | 417/459/459 | 2,342,104 → 2,126,905 |
| mqtt_decode_maximum | 1334/2459/2833 | 1125/2584/3041 | 385,036 → 465,455 |
| json_codec_small | 1625/2792/2917 | 1584/2833/3750 | 320,858 → 450,599 |
| json_codec_medium | 32917/71292/326959 | 33583/85667/336417 | 16,290 → 14,948 |
| json_codec_maximum | 173667/348666/832625 | 177917/736833/3251250 | 4,302 → 2,671 |
| session_lookup_1 | 42/83/84 | 42/84/125 | 15,121,913 → 13,318,523 |
| subscription_lookup_1 | 41/42/42 | 41/42/42 | 21,843,982 → 16,790,271 |
| topic_acl_1 | 84/125/125 | 84/125/125 | 8,325,812 → 8,082,441 |
| ingress_bookkeeping_1 | 209/250/292 | 250/500/500 | 3,995,672 → 3,368,847 |
| session_lookup_64 | 42/125/125 | 42/42/84 | 8,737,440 → 15,468,909 |
| subscription_lookup_64 | 41/42/42 | 41/42/42 | 18,769,086 → 22,095,395 |
| topic_acl_64 | 125/250/250 | 208/250/250 | 3,759,045 → 4,762,376 |
| ingress_bookkeeping_64 | 1417/2250/2250 | 1458/2250/2333 | 610,934 → 586,797 |
| session_lookup_256 | 42/83/84 | 42/125/125 | 14,498,889 → 11,444,371 |
| subscription_lookup_256 | 41/42/42 | 41/42/42 | 21,426,674 → 22,162,678 |
| topic_acl_256 | 125/250/250 | 250/250/250 | 6,147,071 → 3,665,521 |
| ingress_bookkeeping_256 | 5209/7750/16208 | 5250/7791/24458 | 136,144 → 112,377 |

MQTT decode的P50为125→167ns，encode为83→125ns；吞吐见表，存在明显波动。此处修复优先保证协议正确性，代码布局或编译器内联影响尚未通过profile单独归因。该回归作为明确的性能限制保留，未削弱校验来换取基准数字。

Admission bookkeeping随驻留key数增加存在明显线性成本（retain扫描在同步锁内）；session/subscription lookup基本稳定。该结果支持将admission维护作为未来profile对象，不能凭单线程ns数宣称生产吞吐。测量未锁CPU频率，也未采样allocation/profile栈；约41ns的时钟量化与系统调度会影响小函数分位数。

### Integration / SQL measurements

10,000 ingress/outbox、1,024 commands，见 [完整 EXPLAIN ANALYZE](audit-evidence/query-plans.log)：

| 查询 | plan | execution time |
|---|---|---:|
| dedup | composite UNIQUE index scan | 0.014ms |
| outbox claim/update/message join | delivery_due + primary-key index | 0.093ms |
| command batch claim | 小表seq scan + hash join + bounded lock/sort | 0.141ms |
| ingress expiry | ingress_expiry index，LIMIT16 | 0.015ms |
| command retention | commands_retention index，LIMIT16 | 0.014ms |
| expired commands | 小表seq scan，LIMIT16 | 0.012ms |
| quota accounting | 10k-row aggregate seq scan | 2.861ms |

查询计划不是端到端业务benchmark。command表默认上限1024时优化器选择seq scan合理，已有device_due索引；没有通过强制禁用seqscan“美化”结果。quota扫描还受全局advisory lock串行化，不能据此宣称可水平扩展。SQL数据只用于计划，不宣称与真实JSON行宽/WAL完全等价。

### Load / memory attribution

[load.json](audit-evidence/load.json) 来自实际debug executable、loopback明文、单机64连接小实验：

- baseline RSS 12,080KiB；8/16/32/63/64连接分别12,384/12,768/13,088/13,696/13,728KiB，每连接两个exact订阅。64连接+128订阅合计RSS增1,648KiB，约25.8KiB/组合；**没有独立分离订阅的实际heap成本**。
- 达到配置global64后额外16个连接全部关闭。63以内metrics确认MQTT active owner数与连接数相等；64时节点没有空位供metrics HTTP连接，报告实际CONNACK计数，不伪造task采样。
- 关闭后active MQTT=0，RSS14,976KiB仍高于baseline，allocator/metrics请求的驻留内存不等于泄漏；不声称RSS立即返回初始值。
- 慢消费者4items/1804B，RSS增896KiB；下游慢响应，8条持久化job、16总attempt，RSS增1504KiB。均为短时数量级观察，不是长期RSS平台证明。

实测布局（不含heap、Arc引用目标、HashMap bucket、allocator）：SessionEndpoint72B、QueuedCommand112B、DeviceMessage152B、CommandRecord144B；QueuedCommand比基线增加16B用于attempt/lease，CommandRecord增加16B可选lease。保守模型：

```text
RSS ≈ runtime/TLS/base + N_conn × reader/task/socket用户态开销
    + N_sub × (key bytes + device/generation metadata + map overhead)
    + N_qos × pending metadata + 实际queued payload bytes
    + N_ingress × (input + bounded decoded fields + canonical + DB encode)
    + DB pool/worker current items + allocator保留
```

单pending command另有encoded bytes≤16KiB；单ingress逻辑存储计费`16×canonical+8192`；单QoS1 slot保留有限元数据，command slot继续持有原count和bytes预算。这个模型用于解释配置和实验量级，不能当作100k连接容量承诺。

## 13. Validation / reproduction

本轮新增28项独立Rust测试（24项默认、4项需真实PG），另有2个父测试专用子进程入口；Python脚本含4类互操作/负载场景。最终默认workspace测试67通过、0失败、7忽略；忽略项是5个真实PG parent/contract和2个子进程入口。5个PG测试均在独立空库单独通过，两个入口由父测试实际启动。

最终必需检查及结果见 [checks.log](audit-evidence/checks.log)、[workspace-tests.log](audit-evidence/workspace-tests.log)。显式PG测试不能用裸 `--ignored` 一起跑：其中两个入口只允许父测试设置环境启动；每个parent/contract使用独立空数据库。

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --offline -- -D warnings
cargo test --workspace --all-features --offline

NETBAIOT_TEST_DATABASE_URL=postgres://.../fresh_contract cargo test -p netbaiot-storage --test semantics --offline postgres_transaction_and_command_contract -- --ignored
NETBAIOT_TEST_DATABASE_URL=postgres://.../fresh_pressure cargo test -p netbaiot-storage --test semantics --offline audit_postgres_concurrency_leases_pool_pressure_and_cleanup -- --ignored
NETBAIOT_TEST_DATABASE_URL=postgres://.../fresh_history cargo test -p netbaiot-storage --test semantics --offline audit_postgres_attempt_history_cannot_regress -- --ignored
NETBAIOT_TEST_DATABASE_URL=postgres://.../fresh_outbox cargo test -p netbaiot-storage --test semantics --offline audit_outbox_process_crash_recovery -- --ignored
NETBAIOT_TEST_DATABASE_URL=postgres://.../fresh_mqtt cargo test -p netbaiot-transports --test end_to_end --offline audit_postgres_process_crash_boundaries -- --ignored
DATABASE_URL=postgres://.../fresh_smoke python3 tests/smoke_postgres.py
DATABASE_URL=postgres://.../fresh_load /tmp/netbaiot-audit-venv/bin/python tests/audit_load.py

python3 fuzz/seed_corpus.py
CARGO_NET_OFFLINE=true cargo +nightly fuzz run mqtt_packet -- -runs=500000 -max_len=65540
# 其余五个target同样命令，-runs=100000
cargo bench -p netbaiot-transports --bench foundation --offline
```

实际遇到但已解决的执行问题：最初sandbox禁止bind回环，使用已有任务授权申请测试权限后通过；一次Clippy遇到临时ENOSPC，检查可用空间后重跑通过；load fixture最初未同时预留端口，修正为成组预留；慢下游fixture 50ms外部预算不足以完成PG启动，改为500ms并把对端延迟设1s，生产默认值不变；崩溃fixture曾提前drop被写屏障挂起的future，改为pin并保持owner；一次green PG测试错误复用了红灯数据库，按要求使用新空数据库后通过。这些夹具/环境失败没有被计为产品finding。首次strict Clippy指出needless borrow，已修正。

## 14. Remaining Limitations

1. MQTT仍是3.1.1受限子集：QoS0/1、clean session、exact topics；明确不支持MQTT5/QoS2/persistent/retained/LWT/wildcard routing/UDP downlink。
2. At-least-once只在有限TTL/attempt策略中工作；业务端和设备执行必须幂等。terminal失败需运营方检查；没有永久归档/自动补偿保证。
3. 静态credential需要重启更新/撤销；没有动态auth cache TTL/热撤销。已接受的长连接不会自动重新认证。
4. 应用逻辑预算不能替代物理RSS、kernel/TLS、PG WAL/磁盘/FD限制；未认证分布式攻击只能靠IP/global admission保护。共享全局排队/速率仍可能产生竞争。
5. 全局PG配额advisory lock+聚合扫描、有界3N command claim写往返、Admission key扫描是已测量/确认的扩展性限制。本轮没有为吞吐重写存储或引入新缓存。
6. 故障注入覆盖进程kill、锁等待、pool exhaustion、慢sink及关闭，不覆盖真实掉电、PG存储损坏、磁盘耗尽和多节点分区。没有long fuzz campaign、独立形式化验证、TSAN/Loom全交错证明。
7. 强制shutdown会取消listener的子owner；嵌入式run返回的精确瞬间不保证所有孙任务已析构。已提交数据可由lease恢复，未提交或未被设备看到的ACK仍可能是不确定结果。
8. 指标是低基数计数/gauge/累计延迟，尚无P95/P99生产直方图、完整per-budget rejected分类、terminal backlog告警或独立auth/replay cache指标。IP/device/message等没有作为Prometheus label。
9. UDP无加密、无回复，replay bitmap重启清空；HMAC timestamp和持久化source-ID dedup共同约束重放。机器时钟大幅跳变不在本轮故障实验中，需运行环境时间同步。
10. load只到64连接、短时、明文loopback；TLS高负载、长期RSS和独立allocation成本未测。不能据此宣称生产容量或零资源泄漏的数学保证。
