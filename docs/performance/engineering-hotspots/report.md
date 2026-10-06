# NetbaIoT 工程优化报告

测量日期：2026-10-07（本机工作区日期）。基线为 main `a91a910`。按六个功能优化阶段分别提交，然后评估 session 扫描，最后物理拆分 broker；没有改变全局锁模型、协议格式或生产默认上限。第 7 项仅完成测量与评估，**未实现增量 usage / expiry 缓存**。

测量主机：Apple M4，10 CPU，16 GiB，macOS 27.0.1（26A434），arm64；Rust 1.99.0（b940084d7，2026-09-28）。这是同机工程回归证据，不能当成生产容量。原始日志、冻结二进制、逐轮 JSON 均在 `target/engineering-hotspots/`；本目录只保存小型汇总和复现补丁。

## 1. 修改文件

| 文件 | 修改内容 | 原因 |
|---|---|---|
| `crates/netbaiot-runtime/src/event.rs` | CompiledRoutes；有效 global∪tenant fanout 验证；ready FIFO / delayed deadline map；回归及基准 | 配置与真实 admission 一致；去除发布与队列热扫描 |
| `crates/netbaiot-runtime/src/sessions.rs` | touch/query 仅访问目标；容量压力时 prune/oldest-offline eviction；测试及基准 | 已有设备操作平均 O(1)，保留 TTL/generation/active 语义 |
| `crates/netbaiot-runtime/src/quota.rs` | 新 IP 且表满才 retain；全局计费仍先执行；测试及基准 | UDP/请求热路径不再扫描全 IP 表 |
| `crates/netbaiot-runtime/src/lib.rs`、`hotspot_bench.rs` | test-only serial allocator/latency/lock probes | 可比的分配、P50/P95/P99 与 mutex 证据 |
| `crates/netbaiot-transports/Cargo.toml`、`Cargo.lock` | 复用已锁定 stats_alloc，仅 dev-dependency | 测量分配；没有新增生产依赖 |
| `mqtt/mod.rs`、`mqtt/v5_connection.rs` | route 借用消息；直接转移 Bytes/Will；在线 command Vec 所有权转移；UTF-8 检查使用 message.payload | 消除无条件深 clone 和 Vec 中间表示，保留命令 permits/progress |
| `mqtt/codec/v311.rs`、`mqtt/codec/v5.rs` | PUBLISH/Will payload 分离到紧凑 Bytes；buffer ownership 测试 | 小 payload 不得长期持有大读缓冲区；保留一次必要入口复制 |
| `mqtt/broker.rs` → `mqtt/broker/` | 共享不可变 payload、显式兼容 serde；后续物理拆分 | 减少 fanout 深复制；降低单文件维护成本 |
| `mqtt/broker_hotspot_bench.rs` | 保留已有 benchmark，增加 zero-subscriber 和 fresh-input fanout 分配矩阵 | 验证真实存储责任与第一次 Bytes ownership promotion |
| `mqtt/command_tests.rs`、`transports/tests/end_to_end.rs` | Bytes 类型适配，保留测试含义 | 保留命令、真实 sockets、配额与生命周期覆盖 |
| `benches/foundation.rs`、`benches/mqtt_broker_scale.rs` | payload 构造 `.into()` | 保留原有 benchmark 和公开调用路径 |
| `fuzz/fuzz_targets/mqtt_state.rs` | payload 构造适配 Bytes | 保留 state fuzz；未删除任何 target |
| `tests/mqtt_conformance/fixtures/mqtt_recovery/README.md` | 更新兼容测试位置 | 固定历史样本说明对应新模块 |
| `scripts/perf/hotspot_summary.py` | 串行 CSV / E2E JSON 汇总 | 可复核中位数、Δ、锁均值和 RSS |
| `docs/benchmarks.md` | 链接本轮实测与限制 | 区分当前工程证据和历史容量表 |
| 本报告、汇总 JSON、`fresh-input-harness.patch` | 阶段证据与复现说明 | 小型可审阅证据，原始数据留在 target |

broker 子文件职责（路径均相对 `crates/netbaiot-transports/src/mqtt/broker/`）：

| 子文件 | 行数 | 职责 |
|---|---:|---|
| `mod.rs` | 786 | 公共 facade、私有模型、构造；保留 Mutex<BrokerState> |
| `state.rs` | 163 | 派生 usage 汇总及租户统计 |
| `session.rs` | 452 | attach/detach、session 生命周期与集合操作 |
| `subscription.rs` | 317 | 订阅与 trie |
| `routing.rs` | 410 | ACL、preflight、原子 route commit |
| `outbound.rs` | 910 | 发送、packet-ID、ACK、容量唤醒 |
| `inbound_qos2.rs` | 421 | inbound QoS2 operation/incarnation 状态 |
| `retained.rs` | 392 | retained admission、reservation、replace/delete/replay |
| `will.rs` | 515 | Will RAII、delay、pending responsibility |
| `expiry.rs` | 198 | deadline index 与 expiry 清理 |
| `recovery.rs` | 1617 | snapshot/restore、流式 NBMQ read/write、兼容性 |
| `maintenance.rs` | 133 | tick、invalidation、诊断/维护 |

测试分到 `tests/accounting.rs`、`commands.rs`、`expiry.rs`、`inbound_qos2.rs`、`outbound.rs`、`recovery_compat.rs`、`recovery_storage.rs`、`routing.rs`、`sessions.rs`、`will.rs`；`tests/mod.rs` 保留共享 helper。**318 个函数体的规范化摘要完全一致**，模块访问资格、可见性与固定样本相对路径单独调整，没有混入锁或状态机重写。

## 2. 修复的问题

- **Correctness**：在配置提交前拒绝 effective fanout 越界；global/tenant 重叠去重并排序；失败替换保留旧 revision/路由。Byte 共享的回归验证每个独立投递仍占完整配额，任何 required responsibility 无法持有时全体回滚。
- **Performance**：route 从逐事件扫所有 filters 改为预编译查表；presence/IP 常规请求不做整表清理；ready dequeue 不扫描全部 backlog。
- **Memory**：route 前深 clone 消除；payload 在 fanout、QoS、retained/Will/recovery snapshot 中共享。网络 payload 仍做一次紧凑化，避免小 slice 钉住大 buffer；队列 deadline index 有额外固定管理开销，见后文实测。
- **Maintainability**：broker 入口由 12,859 行降至 786 行，实现与测试按责任划分。优先保持原锁、字段、方法体、序列化与 ownership 边界。

## 3. 复杂度变化

| 路径 | Before | After / 保留部分 |
|---|---|---|
| EventBus publish route selection | O(R × F log F)，逐事件创建集合 | 平均 O(1) tenant lookup + O(F) admission/enqueue；编译成本移到配置提交 |
| presence touch/query | 每次 O(HashMap capacity) | 已有/目标查询平均 O(1)；新设备且满时 prune/oldest eviction 仍 O(N) |
| RateLimiter take | 每次 O(IP table capacity) | 平均 O(1)；新 IP 且满时仍 O(N) 回收 |
| zero-subscriber route | O(payload bytes) 深复制 | O(1) 借用/验证/atomic hint，零分配；没有获取 broker mutex |
| fanout payload clone | O(subscribers × payload bytes) | payload 首次所有权 promotion + O(subscribers) 引用管理；topic/properties 等仍需克隆 |
| EventBus ready take | O(N) selection + 中间 VecDeque remove | 普通 ready pop O(1)；promote K due records 为 O(K log D + K)，不能称最坏 O(1) |
| next deadline | O(N) | ready 非空 O(1)，否则 BTreeMap 最早键 O(log D) |
| retry insert | 通常 O(1) VecDeque push | O(log D)，相同 deadline 的 bucket FIFO |
| session usage / message expiry | O(outbound capacity) / O(offline + outbound capacity) | **仍保留扫描**；已有 offline_bytes/state_bytes/集合 len 统计本来就增量/O(1) |

R 为 route 数，F 为实际 fanout，D 为 distinct deadline 数。所有等待记录仍受原 count/byte limits 覆盖，未增加 task/channel。

## 4. MQTT 内存变化

`BrokerMessage.payload` 使用已有 `bytes::Bytes`。业务 DeviceEvent、协议 packet ID 和 MQTT delivery responsibility 仍是不同状态；共享不会改变 ACK 含义或 exactly-once 声明。

消除：`process_publish` 前完整 message clone；各 target / AwaitPuback / AwaitPubrec / retained 等 payload 的反复 Vec 深复制。保留：每个 target 的独立 message/profile/QoS/retain 状态，topic String 与 bounded PublishProperties 克隆，必要的 wire encoding、JSON normalization、入口紧凑化复制，以及独立 bytes/count permits。

decoder 曾返回读缓冲区 slice；直接长期保存该 slice 会持有较大 backing allocation。现在紧凑化在 decoder 中替代原 transport `payload.to_vec()`：**一次入口复制仍在，后续存储克隆共享 payload**。两个测试验证 1B payload 不引用 64KB read buffer，且独占可变容量为 1B。在线 command 使用 `mem::take` 转移 Vec，原 QueuedCommand 的 RAII permits/progress 没有提前释放。

逻辑配额不按物理引用数缩减：per-session/tenant/process state bytes、outbound/offline/retained bytes 仍以每份责任的 `message.bytes()` 累计。1000 × 16KB fanout 仍占 1000 份逻辑责任。共享/byte rollback/retained/outbound ACK 回归测试验证一致性。

兼容性：显式 serde helper 保持 JSON 的 byte-array payload，compact NBMQ message 字节逐字节不变；NBMQ 写版本仍 v6，v1–v5 读兼容及固定非空历史样本通过。MQTT 3.1.1/5、RPC V2/V3、management API、spool、SDK wire 未改。Rust server crate 的 payload 类型及 route_from_session 借用签名是本任务授权的源码 API 变化；工作区所有调用点、examples/tests/bench/fuzz 已适配。`netbaiot-protocol`、client、CLI、device SDK 的公共协议边界不变。

## 5. Benchmark

测量方法：setup、cleanup、样本存储在分配/计时区域外；每组 3 repeats，runtime case 2000/4000 operations，fanout case 100 operations。P99 在 100 样本时接近最大样本，不能据此推导 SLA。fanout fresh input 每次新建 caller-owned buffer，第一次共享 promotion 在测量区域内；caller 构造/decoder 复制不计入 route。旧 `fanout_route_scaling` 的 30 次完整 route+drain/ACK 测量也原样保留并运行。

### 路由

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| route_publish/1 | 1,410,893→2,830,378 (100.609%) | 708→334 (-52.825%) | 834→375 (-55.036%) | 875→459 (-47.543%) |9→8 (-11.11%)|808→616 (-23.76%)|
| route_publish/1024 | 1,116,974→2,915,001 (160.973%) | 875→333 (-61.943%) | 917→416 (-54.635%) | 959→459 (-52.138%) |9→8 (-11.11%)|808→616 (-23.76%)|
| route_publish/256 | 1,896,846→2,826,837 (49.028%) | 541→334 (-38.262%) | 542→416 (-23.247%) | 584→459 (-21.404%) |9→8 (-11.11%)|808→616 (-23.76%)|
| route_publish/64 | 2,006,496→2,901,570 (44.609%) | 500→333 (-33.4%) | 542→416 (-23.247%) | 584→459 (-21.404%) |9→8 (-11.11%)|808→616 (-23.76%)|
### Presence

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| connection_lookup/1 | 3,659,492→3,608,669 (-1.389%) | 250→291 (16.4%) | 375→292 (-22.133%) | 375→333 (-11.2%) |0→0 (—)|0→0 (—)|
| connection_lookup/1024 | 1,295,893→6,106,124 (371.19%) | 750→167 (-77.733%) | 833→208 (-75.03%) | 834→209 (-74.94%) |0→0 (—)|0→0 (—)|
| connection_lookup/256 | 2,726,835→5,165,883 (89.446%) | 375→208 (-44.533%) | 375→250 (-33.333%) | 416→250 (-39.904%) |0→0 (—)|0→0 (—)|
| connection_lookup/64 | 3,414,939→3,949,455 (15.652%) | 292→250 (-14.384%) | 292→292 (0%) | 334→292 (-12.575%) |0→0 (—)|0→0 (—)|
| presence_lookup/1 | 4,201,297→3,532,489 (-15.919%) | 250→292 (16.8%) | 291→292 (0.344%) | 292→334 (14.384%) |0→0 (—)|0→0 (—)|
| presence_lookup/1024 | 1,226,328→6,392,104 (421.239%) | 833→166 (-80.072%) | 834→208 (-75.06%) | 875→209 (-76.114%) |0→0 (—)|0→0 (—)|
| presence_lookup/256 | 2,684,845→5,093,374 (89.708%) | 375→208 (-44.533%) | 417→250 (-40.048%) | 417→250 (-40.048%) |0→0 (—)|0→0 (—)|
| presence_lookup/64 | 3,486,790→3,691,845 (5.881%) | 292→250 (-14.384%) | 292→333 (14.041%) | 333→334 (0.3%) |0→0 (—)|0→0 (—)|
| touch_existing/1 | 2,641,863→3,758,162 (42.254%) | 375→250 (-33.333%) | 416→292 (-29.808%) | 417→292 (-29.976%) |0→0 (—)|0→0 (—)|
| touch_existing/1024 | 1,084,383→7,099,046 (554.662%) | 917→125 (-86.369%) | 958→167 (-82.568%) | 1,000→167 (-83.3%) |0→0 (—)|0→0 (—)|
| touch_existing/256 | 2,282,493→5,659,798 (147.966%) | 417→167 (-59.952%) | 459→209 (-54.466%) | 500→209 (-58.2%) |0→0 (—)|0→0 (—)|
| touch_existing/64 | 2,491,873→4,094,744 (64.324%) | 416→250 (-39.904%) | 417→250 (-40.048%) | 500→292 (-41.6%) |0→0 (—)|0→0 (—)|
| touch_new_free/1 | 2,654,597→2,368,936 (-10.761%) | 375→417 (11.2%) | 417→459 (10.072%) | 500→459 (-8.2%) |0→0 (—)|0→0 (—)|
| touch_new_free/1024 | 1,192,959→5,896,713 (394.293%) | 833→167 (-79.952%) | 917→208 (-77.317%) | 1,000→209 (-79.1%) |0→0 (—)|0→0 (—)|
| touch_new_free/256 | 2,681,256→4,858,366 (81.197%) | 375→208 (-44.533%) | 375→209 (-44.267%) | 417→209 (-49.88%) |0→0 (—)|0→0 (—)|
| touch_new_free/64 | 3,362,028→3,697,425 (9.976%) | 292→250 (-14.384%) | 333→292 (-12.312%) | 334→292 (-12.575%) |0→0 (—)|0→0 (—)|
| touch_new_full/1 | 2,322,843→2,221,006 (-4.384%) | 417→458 (9.832%) | 459→500 (8.932%) | 500→500 (0%) |0→0 (—)|0→0 (—)|
| touch_new_full/1024 | 153,193→195,803 (27.815%) | 6,584→5,458 (-17.102%) | 7,500→6,208 (-17.227%) | 11,458→7,291 (-36.368%) |0→0 (—)|0→0 (—)|
| touch_new_full/256 | 479,084→444,759 (-7.165%) | 2,125→2,125 (0%) | 2,209→2,458 (11.272%) | 2,291→2,500 (9.123%) |0→0 (—)|0→0 (—)|
| touch_new_full/64 | 1,067,399→1,018,897 (-4.544%) | 917→959 (4.58%) | 1,000→1,041 (4.1%) | 1,042→1,042 (0%) |0→0 (—)|0→0 (—)|
### Rate table

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| rate_existing/1 | 2,972,320→3,120,967 (5.001%) | 333→333 (0%) | 375→334 (-10.933%) | 417→375 (-10.072%) |0→0 (—)|0→0 (—)|
| rate_existing/1024 | 56,825→3,586,402 (6,211.31%) | 17,375→291 (-98.325%) | 19,000→292 (-98.463%) | 23,709→292 (-98.768%) |0→0 (—)|0→0 (—)|
| rate_existing/256 | 151,378→4,008,410 (2,547.948%) | 6,375→250 (-96.078%) | 7,458→250 (-96.648%) | 7,500→292 (-96.107%) |0→0 (—)|0→0 (—)|
| rate_existing/64 | 356,665→3,533,388 (890.674%) | 2,333→291 (-87.527%) | 3,500→333 (-90.486%) | 3,542→334 (-90.57%) |0→0 (—)|0→0 (—)|
### 无订阅者借用

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| no_subscriber/1024 | 2,900,895→5,142,181 (77.262%) | 334→208 (-37.725%) | 375→209 (-44.267%) | 375→209 (-44.267%) |2→0 (-100.00%)|1,059→0 (-100.00%)|
| no_subscriber/16384 | 1,435,000→5,152,727 (259.075%) | 708→208 (-70.621%) | 750→209 (-72.133%) | 750→209 (-72.133%) |2→0 (-100.00%)|16,419→0 (-100.00%)|
| no_subscriber/64 | 2,853,932→4,408,894 (54.485%) | 334→209 (-37.425%) | 375→250 (-33.333%) | 416→250 (-39.904%) |2→0 (-100.00%)|99→0 (-100.00%)|
| no_subscriber/65536 | 514,721→4,591,516 (792.04%) | 1,917→208 (-89.15%) | 2,083→250 (-87.998%) | 2,167→291 (-86.571%) |2→0 (-100.00%)|65,571→0 (-100.00%)|
### 共享 payload fanout

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| fanout_1000_qos0/1024 | 3,589→3,881 (8.136%) | 278,791→258,042 (-7.442%) | 296,667→274,500 (-7.472%) | 321,291→310,125 (-3.475%) |5,014→4,015 (-19.92%)|1,812,610→796,634 (-56.05%)|
| fanout_1000_qos0/16384 | 2,115→3,765 (78.014%) | 467,458→265,333 (-43.239%) | 506,542→281,541 (-44.419%) | 533,334→296,125 (-44.477%) |5,014→4,015 (-19.92%)|17,172,610→796,634 (-95.36%)|
| fanout_1000_qos0/64 | 3,871→3,877 (0.155%) | 258,167→259,541 (0.532%) | 280,625→272,666 (-2.836%) | 296,417→317,625 (7.155%) |5,014→4,015 (-19.92%)|852,610→796,634 (-6.57%)|
| fanout_1000_qos1/1024 | 1,445→1,518 (5.052%) | 692,000→655,792 (-5.232%) | 728,708→721,000 (-1.058%) | 758,542→785,292 (3.527%) |10,014→8,015 (-19.96%)|2,900,280→860,304 (-70.34%)|
| fanout_1000_qos1/16384 | 872→1,540 (76.606%) | 1,133,125→648,041 (-42.809%) | 1,242,833→694,792 (-44.096%) | 1,370,792→719,166 (-47.536%) |10,014→8,015 (-19.96%)|33,620,280→860,304 (-97.44%)|
| fanout_1000_qos1/64 | 1,545→1,551 (0.388%) | 652,542→646,250 (-0.964%) | 678,916→678,042 (-0.129%) | 711,250→712,542 (0.182%) |10,014→8,015 (-19.96%)|980,280→860,304 (-12.24%)|
| fanout_100_qos0/1024 | 38,288→43,046 (12.427%) | 26,292→23,584 (-10.3%) | 28,000→26,208 (-6.4%) | 30,250→33,750 (11.57%) |510→411 (-19.41%)|167,498→65,922 (-60.64%)|
| fanout_100_qos0/16384 | 22,873→44,151 (93.027%) | 43,708→23,125 (-47.092%) | 45,458→23,958 (-47.296%) | 47,083→25,583 (-45.664%) |510→411 (-19.41%)|1,703,498→65,922 (-96.13%)|
| fanout_100_qos0/64 | 33,348→44,585 (33.696%) | 30,042→21,666 (-27.881%) | 30,625→23,875 (-22.041%) | 33,167→29,667 (-10.553%) |510→411 (-19.41%)|71,498→65,922 (-7.80%)|
| fanout_100_qos1/1024 | 15,172→16,424 (8.252%) | 63,709→62,458 (-1.964%) | 70,541→63,458 (-10.041%) | 83,083→68,250 (-17.853%) |1,010→811 (-19.70%)|275,968→71,992 (-73.91%)|
| fanout_100_qos1/16384 | 9,595→16,166 (68.484%) | 105,042→62,792 (-40.222%) | 113,833→64,250 (-43.558%) | 127,000→70,208 (-44.718%) |1,010→811 (-19.70%)|3,347,968→71,992 (-97.85%)|
| fanout_100_qos1/64 | 15,183→16,260 (7.093%) | 65,542→62,667 (-4.387%) | 69,167→65,166 (-5.785%) | 78,833→68,542 (-13.054%) |1,010→811 (-19.70%)|83,968→71,992 (-14.26%)|
| fanout_10_qos0/1024 | 163,385→336,271 (105.815%) | 6,084→2,917 (-52.055%) | 6,333→3,250 (-48.682%) | 6,750→3,375 (-50%) |57→48 (-15.79%)|17,376→7,240 (-58.33%)|
| fanout_10_qos0/16384 | 104,045→330,528 (217.678%) | 9,625→3,083 (-67.969%) | 9,917→3,250 (-67.228%) | 10,000→3,417 (-65.83%) |57→48 (-15.79%)|170,976→7,240 (-95.77%)|
| fanout_10_qos0/64 | 133,906→353,306 (163.846%) | 7,417→2,833 (-61.804%) | 7,792→2,958 (-62.038%) | 11,167→3,250 (-70.896%) |57→48 (-15.79%)|7,776→7,240 (-6.89%)|
| fanout_10_qos1/1024 | 81,616→151,763 (85.948%) | 11,750→6,750 (-42.553%) | 13,125→7,000 (-46.667%) | 16,583→7,167 (-56.781%) |107→88 (-17.76%)|28,196→7,820 (-72.27%)|
| fanout_10_qos1/16384 | 62,569→150,405 (140.383%) | 16,167→6,833 (-57.735%) | 16,750→7,000 (-58.209%) | 17,083→7,084 (-58.532%) |107→88 (-17.76%)|335,396→7,820 (-97.67%)|
| fanout_10_qos1/64 | 72,692→156,097 (114.738%) | 13,750→6,250 (-54.545%) | 14,166→7,042 (-50.289%) | 15,459→7,292 (-52.83%) |107→88 (-17.76%)|8,996→7,820 (-13.07%)|
| fanout_1_qos0/1024 | 406,235→1,159,985 (185.545%) | 2,417→875 (-63.798%) | 2,500→917 (-63.32%) | 3,083→1,000 (-67.564%) |10→10 (+0.00%)|2,216→1,224 (-44.77%)|
| fanout_1_qos0/16384 | 381,811→1,123,040 (194.135%) | 2,542→875 (-65.578%) | 2,708→958 (-64.623%) | 3,625→1,000 (-72.414%) |10→10 (+0.00%)|17,576→1,224 (-93.04%)|
| fanout_1_qos0/64 | 371,288→1,150,523 (209.873%) | 2,667→875 (-67.192%) | 2,750→917 (-66.655%) | 4,708→1,625 (-65.484%) |10→10 (+0.00%)|1,256→1,224 (-2.55%)|
| fanout_1_qos1/1024 | 264,812→824,429 (211.326%) | 3,625→1,208 (-66.676%) | 3,833→1,250 (-67.388%) | 8,167→1,375 (-83.164%) |15→14 (-6.67%)|3,298→1,282 (-61.13%)|
| fanout_1_qos1/16384 | 208,568→823,038 (294.614%) | 4,750→1,208 (-74.568%) | 4,958→1,291 (-73.961%) | 7,291→1,334 (-81.703%) |15→14 (-6.67%)|34,018→1,282 (-96.23%)|
| fanout_1_qos1/64 | 247,017→821,868 (232.717%) | 4,000→1,208 (-69.8%) | 4,792→1,250 (-73.915%) | 7,500→1,458 (-80.56%) |15→14 (-6.67%)|1,378→1,282 (-6.97%)|
### ready/delayed queue

三次串行 Release 测量的中位数；ns/op，分配为次数/op 和请求字节/op。

| case | ops/s before→after (Δ) | P50 ns before→after (Δ) | P95 ns before→after (Δ) | P99 ns before→after (Δ) | allocations/op before→after (Δ) | bytes/op before→after (Δ) |
|---|---:|---:|---:|---:|---:|---:|
| deadline_retry0/0 | 4,311,897→4,477,168 (3.833%) | 250→209 (-16.4%) | 250→250 (0%) | 250→250 (0%) |0→0 (—)|0→0 (—)|
| deadline_retry0/100 | 1,524,862→4,027,938 (164.151%) | 666→250 (-62.462%) | 667→292 (-56.222%) | 750→292 (-61.067%) |0→0 (—)|0→0 (—)|
| deadline_retry0/1000 | 328,836→5,282,760 (1,506.503%) | 3,083→208 (-93.253%) | 3,125→209 (-93.312%) | 3,666→209 (-94.299%) |0→0 (—)|0→0 (—)|
| deadline_retry0/4096 | 80,965→6,725,560 (8,206.75%) | 12,375→166 (-98.659%) | 12,458→167 (-98.659%) | 12,709→167 (-98.686%) |0→0 (—)|0→0 (—)|
| deadline_retry10/0 | 5,554,290→5,018,707 (-9.643%) | 167→208 (24.551%) | 209→209 (0%) | 209→209 (0%) |0→0 (—)|0→0 (—)|
| deadline_retry10/100 | 1,849,636→4,215,781 (127.925%) | 542→250 (-53.875%) | 542→250 (-53.875%) | 584→250 (-57.192%) |0→0 (—)|0→0 (—)|
| deadline_retry10/1000 | 335,451→5,781,767 (1,623.58%) | 3,000→167 (-94.433%) | 3,042→208 (-93.162%) | 3,084→209 (-93.223%) |0→0 (—)|0→0 (—)|
| deadline_retry10/4096 | 84,126→7,304,842 (8,583.216%) | 12,083→125 (-98.965%) | 12,125→167 (-98.623%) | 12,833→167 (-98.699%) |0→0 (—)|0→0 (—)|
| deadline_retry50/0 | 3,574,908→3,031,038 (-15.214%) | 291→333 (14.433%) | 292→334 (14.384%) | 292→334 (14.384%) |0→0 (—)|0→0 (—)|
| deadline_retry50/100 | 2,098,116→4,761,701 (126.951%) | 459→208 (-54.684%) | 500→209 (-58.2%) | 500→250 (-50%) |0→0 (—)|0→0 (—)|
| deadline_retry50/1000 | 333,811→6,243,912 (1,770.493%) | 3,083→167 (-94.583%) | 3,125→167 (-94.656%) | 3,167→167 (-94.727%) |0→0 (—)|0→0 (—)|
| deadline_retry50/4096 | 92,705→7,817,600 (8,332.771%) | 10,917→125 (-98.855%) | 10,959→167 (-98.476%) | 11,375→167 (-98.532%) |0→0 (—)|0→0 (—)|
| dequeue_retry0/0 | 3,165,594→3,158,071 (-0.238%) | 333→333 (0%) | 334→334 (0%) | 334→375 (12.275%) |0→0 (—)|0→0 (—)|
| dequeue_retry0/100 | 485,784→2,667,517 (449.116%) | 2,084→375 (-82.006%) | 2,167→375 (-82.695%) | 2,208→417 (-81.114%) |0→0 (—)|0→0 (—)|
| dequeue_retry0/1000 | 105,217→3,915,151 (3,621.025%) | 9,583→250 (-97.391%) | 9,792→292 (-97.018%) | 10,250→292 (-97.151%) |0→0 (—)|0→0 (—)|
| dequeue_retry0/4096 | 27,714→4,817,257 (17,282.034%) | 35,875→208 (-99.42%) | 36,250→209 (-99.423%) | 42,833→250 (-99.416%) |0→0 (—)|0→0 (—)|
| dequeue_retry10/0 | 3,690,582→3,640,673 (-1.352%) | 291→291 (0%) | 292→292 (0%) | 292→292 (0%) |0→0 (—)|0→0 (—)|
| dequeue_retry10/100 | 710,036→3,082,044 (334.069%) | 1,416→333 (-76.483%) | 1,417→334 (-76.429%) | 1,500→334 (-77.733%) |0→0 (—)|0→0 (—)|
| dequeue_retry10/1000 | 122,993→4,184,801 (3,302.471%) | 8,083→250 (-96.907%) | 8,167→250 (-96.939%) | 11,209→250 (-97.77%) |0→0 (—)|0→0 (—)|
| dequeue_retry10/4096 | 30,342→5,329,240 (17,463.905%) | 32,792→208 (-99.366%) | 33,084→209 (-99.368%) | 38,458→209 (-99.457%) |0→0 (—)|0→0 (—)|
| dequeue_retry50/0 | 2,242,042→3,969,892 (77.066%) | 458→250 (-45.415%) | 459→250 (-45.534%) | 459→250 (-45.534%) |0→0 (—)|0→0 (—)|
| dequeue_retry50/100 | 1,238,486→3,464,511 (179.738%) | 792→292 (-63.131%) | 834→292 (-64.988%) | 834→333 (-60.072%) |0→0 (—)|0→0 (—)|
| dequeue_retry50/1000 | 205,243→4,571,376 (2,127.299%) | 4,833→208 (-95.696%) | 4,875→250 (-94.872%) | 6,750→250 (-96.296%) |0→0 (—)|0→0 (—)|
| dequeue_retry50/4096 | 50,515→5,547,712 (10,882.306%) | 19,750→167 (-99.154%) | 19,792→209 (-98.944%) | 22,167→209 (-99.057%) |0→0 (—)|0→0 (—)|


锁均值专项（ns，3 repeat 中位；broker 为 μs 分辨率，另列）：

| case | wait before→after (Δ) | hold before→after (Δ) |
|---|---:|---:|
| connection_lookup/1024 | 16.77→20.56 (+22.57%) | 699.22→76.24 (-89.10%) |
| presence_lookup/1024 | 17.86→20.60 (+15.32%) | 742.88→73.35 (-90.13%) |
| touch_existing/1024 | 18.83→20.50 (+8.90%) | 844.22→58.80 (-93.04%) |
| touch_new_free/1024 | 17.31→19.58 (+13.09%) | 769.42→91.29 (-88.13%) |
| touch_new_full/1024 | 16.77→17.87 (+6.52%) | 6440.72→5033.31 (-21.85%) |
| rate_existing/1024 | 17.09→29.84 (+74.64%) | 17530.04→160.45 (-99.08%) |
| deadline_retry0/4096 | 18.26→21.05 (+15.29%) | 12201.00→44.08 (-99.64%) |
| deadline_retry10/4096 | 17.53→19.48 (+11.11%) | 11782.50→40.83 (-99.65%) |
| deadline_retry50/4096 | 17.68→18.16 (+2.73%) | 10642.29→39.15 (-99.63%) |
| dequeue_retry0/4096 | 15.99→21.43 (+34.06%) | 36030.90→89.23 (-99.75%) |
| dequeue_retry10/4096 | 16.15→19.66 (+21.74%) | 32829.63→81.36 (-99.75%) |
| dequeue_retry50/4096 | 16.08→18.75 (+16.61%) | 19680.97→77.89 (-99.60%) |

锁证据：presence/rate 使用 test-only ns probe，EventBus 使用既有固定 site 的 ns histogram，broker fanout 使用既有 μs histogram（有截断）；zero-subscriber 没有获取 broker mutex。逐轮数据及 RSS 在原始 E2E JSON，汇总保留均值，不能将未测/未锁定标成 0ns。

队列管理内存（StatsAlloc 请求的存活堆字节，不是 RSS）：4096 记录，0/10/50% retry，before 均 3,885,336B；after 为 3,885,600 / 4,001,440 / 4,468,384B。50% retry 增加 583,048B（约 15%）；没有缩减逻辑 count/byte 配额。

共享矩阵并非所有 case 都更快：初次 100 样本矩阵中 100 subscriber/QoS0/1KB 的 P99 +11.6%，1000/QoS0/64B +7.2%，1000/QoS1/1KB +3.5%，1000/QoS1/64B +0.2%。16KB 大 fanout 分配与 latency 收益明确，不能把这个结论推广到所有 metadata/小消息/真实网络场景。

session 评估：offline 0/10/100/128、outbound 1/10/32（当前默认上限）。usage P50 41→83ns，offline deadline 42→125ns；完整 route+PUBACK P50 1666→1834ns，route+PUBCOMP 1875→2292ns。扫描确实增长，但增加缓存/可删除 expiry index 的一致性风险和成本尚缺真实 backlog/egress 收益证据，因此未实施。上限提高或测得 broker lock 被此路径主导时重新评估。

E2E 使用冻结 release server/loadgen，64 MQTT QoS1 connections、256B JSON、固定 20,000/s、20s measurement、3s warmup、2s cooldown，串行且测量时不跑 build/test。before/after 固定 offered load，不是饱和容量；puback 表示 EventAccepted，audit sink ACK 表示 required work 释放。

| 阶段 | before ACK/s | after ACK/s | Δ | P50 ms before→after | P95 ms before→after | P99 ms before→after | P99 Δ | peak RSS KiB before→after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| borrow | 19,986.125 | 19,983.25 | -0.014% | 0.35→0.36 | 0.995→1.015 | 1.47→1.49 | 1.361% | 10,336→10,416 |
| share | 19,990.05 | 19,968.55 | -0.108% | 0.35→0.36 | 0.94→0.98 | 1.28→1.42 | 10.938% | 10,416→10,312 |
| queue | 19,919.375 | 19,913.725 | -0.028% | 0.435→0.435 | 0.98→0.99 | 1.235→1.25 | 1.215% | 10,264→10,312 |
| split | 19,936.7 | 19,936.1 | -0.003% | 0.435→0.43 | 0.98→0.99 | 1.23→1.235 | 0.407% | 10,256→10,232 |

Borrow 最初三组前向 P99 +26% 导致暂缓提交；保留全部结果，补逆序与 ABBA 后 12 pairs 总中位 P99 1.47→1.49ms（+1.36%），最后 6 ABBA 配对变化中位 −0.7%。Shared 的 6 pairs 配对 P99 中位 +3.9%，单组 −17.4% 到 +32.7%；Queue 配对中位约 +1.6%。**没有证明端到端容量增加，尾延迟在这台非隔离主机上仍有不确定性**；不以 microbenchmark 替代 E2E。

route 的固定负载 E2E 中位 ACK/s 19985.42→19990.35（+0.025%），PUBACK P50/P95/P99 0.330/0.950/1.345→0.310/0.900/1.280ms。

presence 的固定负载 E2E 中位 ACK/s 19990.35→19992.05（+0.009%），PUBACK P50/P95/P99 0.310/0.900/1.280→0.350/0.970/1.290ms。

Route 汇总使用 6 次清洁 baseline 与 3 次最终借用快照 candidate，并非同组同时配对；早先与 tests/build 重叠的初始记录排除在清洁比较之外。原第一候选逐 publish Arc clone 的 6 pairs P99 +7.8% 促使改用锁内 slice 借用，最终结果单列，没有掩盖旧候选。

签名 UDP 三组交替 A/B：before/after 均约 20,000 NBA1/s，100% 成功，P99 中位 0.90ms，无丢 ACK。该网络 profile 为同一 source IP，不能代表 1024 IP table 的 CPU 收益。

真实 webhook 超时/恢复对照：8 MQTT clients、1000/s、20s，10s sink delay、测量第 10s恢复，fixture queue/global accepted count=128（原生产默认值未改）。before/after 峰值均 128 events / 50,288 event bytes，均 24 retry，恢复后 accepted/ACK 精确为 8133/8133 与 8118/8118，pending_required=0，server exit=0、未 forced。负载端约 60% overload/unconfirmed 为预期压力结果；成功 acceptance P99 均 0.22ms。只需空 MQTT restart image，无未 ACK EventBus spool。

拆分的初次矩阵出现小 fanout P50/P99 升高，因而暂停提交。使用冻结的前后二进制再做 3 组交替矩阵，第一组前后同时约慢 3 倍、后两组同时恢复：1 subscriber/QoS0/64B，before P50 2583/834/833ns，after 2542/833/834ns；10 subscriber 对应 8208/2750/2750 与 8209/2791/2792ns。不能把初次跨时间结果当成模块回归；也不将具体原因未经证实地归于 CPU core。3 组汇总在 `split-confirm-summary.json`。分配完全相同，函数体和 63 条 public/pub(crate) function signatures 完全一致，未通过 inline hints 或锁变更掩盖差异。

连接内存实际测量：128 active、non-TLS、non-persistent、未订阅，12s idle，单轮 RSS 差值；baseline 为原 main、after 为最终实现。MQTT loaded RSS 10,944→10,768KiB，增量/连接 36,480→35,456B（−2.8%）；TCP loaded RSS 9,984→10,000KiB，增量/连接 28,416→28,800B（+1.35%）。两者 FD 16→144、runtime tasks 6→134，均一连接一个 task。该短时单轮 RSS 不是容量结论，configured 524,288 logical bytes/connection 没有当成实际内存。

有界连续运行：16 MQTT QoS2/TLS connections、1000/s、120s、confirmed webhook，120,000 PUBLISH/PUBREC/PUBREL/PUBCOMP，零 error、断连时零 pending、server exit=0。PUBCOMP P50/P95/P99=0.22/0.37/0.40ms，RSS 样本 7,536–10,608KiB，pending_required 峰值 2。它是两分钟 smoke soak，不是长时生产 soak，也不证明 crash durability。

## 6. 测试

**PASS（实际执行）**

- 各功能优化阶段 `cargo xtask check`；`cargo xtask check release --part rust`。两个入口实际执行 fmt check、workspace/all-target/all-feature Clippy `-D warnings`、workspace/all-feature tests（release --part rust 不是 release test 编译，因此另外运行了 release 专项/benchmark）。
- MQTT 借用、共享与最终拆分后的 `cargo xtask check release --part mqtt`：raw protocol regression、125/125 normative coverage、Mosquitto 3.1.1/5 与 TLS、device SDK/examples。
- 共享/队列/拆分后的 `cargo test --locked -p netbaiot-runtime spool::`、`cargo test --locked -p netbaiot-transports mqtt_recovery`；历史 v1–v5、v6 完整性、nonempty QoS/retained/Will、count/bytes ceiling 与 corruption。
- workspace tests 包含真实 subprocess graceful restart、QoS1/2 resume、spool failure/retry、目录独占、SIGKILL 的三事件 loss-window 回归；未改 crash-durability 声明。
- EventBus 23 tests（3 ignored benchmark 单独 release 运行）；FIFO/same-deadline、later-ready、不泄漏 accounting、concurrency>1、panic isolation、retry exhaustion、stop/spool ownership。
- MQTT changes 每次全部 19 fuzz targets ASan build；mqtt_packet/state/recovery/v5_packet/v5_publish/v5_subscribe 各 2000 runs、max_len=65540；queue 阶段另 json_codec/restart_spool 各 2000 runs。
- 128 idle MQTT/TCP 连接实际 RSS/FD/task 对照；120s QoS2/TLS/confirmed-webhook 连续运行。
- release route/presence/rate/zero-subscriber/fanout/queue/scanning 矩阵；保留并运行原 fanout 与 16,383-depth probe；真实 E2E 与 UDP、timeout/outage/recovery 对照。
- 拆分前后 318 function-body digest 一致；workspace all-target/all-feature check。

**FAIL（修复/复核前，不是最终未解决失败）**

- 新测试初次 Bytes 类型适配编译错误、quota rollback fixture 忽略合法 offline fallback、MQTT 5 CONNECT fixture property 顺序、queue metric 名/字段/类型推断错误：修正后重跑通过。
- 拆分脚本初次漏 helper 和 impl brace 识别：恢复自身备份后修正；最终函数体摘要与功能验证通过。
- 初次 borrow P99 与初次拆分小 fanout 结果触发复核；原始异常数据保留，见最终 A/B 说明。

**BLOCKED / 未运行**

- Mosquitto gate 初次 PATH 不含 broker；本机 `/usr/local/bin` 为客户端，`/usr/local/sbin/mosquitto` 为 broker；补 PATH 后全 gate 通过。没有安装新 runtime broker。
- 没有运行长时生产/跨机 soak、真实生产 saturation/capacity、断电试验、长 fuzz campaign、带最大 metadata 的真实订阅者 RSS/backlog campaign。现有 subprocess SIGKILL/restart 与 bounded loopback 测量不能替代这些证据。
- 第 7 项未实现缓存计数/expiry index，因此没有声称该 O(N) 路径被消除。

## 7. 未解决问题

- 新 presence/IP 在容量压力下仍 O(N)；管理 registry/prune 低频路径仍批量扫。
- session usage/has_outbound_capacity/expiry 仍扫局部 collections；outbound_order 删除仍 retain；复杂度取决于 retained HashMap capacity 与实际配置。
- EventBus due promotion 会一次移动全部到期记录，最坏有 bounded K 的锁持有突发；next-deadline /普通 dequeue 的改善不等于最坏 promotion O(1)。
- broker 全局 Mutex 保留；大 fanout、retained replay、recovery/restore 的同步临界区仍可能有竞争。没有把 Mutex 全换成 Tokio Mutex、没有分片/DashMap/更换 hasher。
- `broker/recovery.rs` 仍约 1617 行，runtime event.rs（含测试/bench）也较大；下一轮可按 reader/writer 分解，但不与恢复算法变更混做。
- metadata/topic 仍克隆；输入紧凑化与 wire encoding/normalization 复制仍在。需要真实 profile 才能决定 Arc<str>/Arc<PublishProperties>。
- E2E 固定负载与短时同机 latency 抖动明显；小矩阵 P99 样本少，未证明 production SLA 或容量。

## 8. 后续建议

- **P0**：持续把 EventAccepted/all-or-nothing、配额守恒、auth/incarnation fencing、planned commit fsync 与 wrong-ACK 回归纳入门禁；不得放宽默认上限或改变失败退出规则掩盖问题。
- **P1**：在隔离主机做真实 MQTT subscribers/backlog/maximum metadata、长 slow-sink/outage/soak，记录 heap/RSS、per-site lock P95/P99；专测大量同时 due retry promotion；只有关键路径证据支持时再实现 usage counters / deletable expiry index，并强制 authoritative debug/test invariant。
- **P2**：继续按职责拆 recovery reader/writer；用 metadata profile 决定 topic/properties 共享；优化 capacity-pressure prune/expiry index 前先证明需求。保留原全局锁，未来并发模型另开经完整原子性审查的任务。

复现关键命令（默认生产 limits 未修改）：

```bash
cargo xtask check
cargo xtask check release --part rust
PATH="/usr/local/sbin:$PATH" cargo xtask check release --part mqtt
cargo test --release --locked -p netbaiot-runtime route_publish_scaling -- --ignored --nocapture --test-threads=1
cargo test --release --locked -p netbaiot-runtime presence_scaling -- --ignored --nocapture --test-threads=1
cargo test --release --locked -p netbaiot-runtime rate_table_scaling -- --ignored --nocapture --test-threads=1
cargo test --release --locked -p netbaiot-runtime ready_retry_queue_scaling -- --ignored --nocapture --test-threads=1
cargo test --release --locked -p netbaiot-transports no_subscriber_route_allocations -- --ignored --nocapture --test-threads=1
cargo test --release --locked -p netbaiot-transports fanout_route_allocations -- --ignored --nocapture --test-threads=1
NETBAIOT_BROKER_EXTRA_BENCH=1 cargo test --release --locked -p netbaiot-transports fanout_route_scaling -- --ignored --nocapture --test-threads=1
NETBAIOT_BROKER_HOTSPOTS=1 cargo test --release --locked -p netbaiot-transports session_recompute_scaling -- --ignored --nocapture --test-threads=1
python3 scripts/perf/event_load.py --server-bin target/engineering-hotspots/stage8-server --loadgen-bin target/engineering-hotspots/loadgen --rate 20000 --duration 20 --warmup 3 --cooldown 2 --connections 64 --sink-mode none
python3 scripts/perf/hotspot_summary.py --before BEFORE.log --after AFTER.log
```

复现 shared-before fresh-input 矩阵：在独立 checkout 的 `4ac37d7` 上应用本目录 `fresh-input-harness.patch`，再跑相同 `fanout_route_allocations`；after 为 `355b974` 或最终 main。切换版本时保留冻结 release 二进制，测量不与 build/test 并发。原始统计的单位、warmup/setup/cleanup、fixture limits 和不确定性应一起保留。

代码阶段提交：`5d612af` routes、`41eb71f` presence、`6b559ac` rate、`4ac37d7` borrow、`355b974` Bytes、`7b4e3c4` ready/delayed、`302912c` physical split。Session 评估没有 runtime 改动。

最终门禁：代码快照 `302912c` 的 fmt/Clippy/workspace tests、MQTT release gate、spool/recovery、fuzz 均通过；报告加入后 `cargo xtask check` 再次通过。Git completion 按 AGENTS 合并任务分支到 main 并推送 origin；最终 merge SHA 由 Git 记录，不在报告中自引用。
