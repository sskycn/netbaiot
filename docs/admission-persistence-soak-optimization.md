# NetbaIoT Admission / Persistence / Soak Optimization

日期：2026-09-19–20。代码基线：`b09754c51a2e50b41a712e2609febbfe26007a13`。

本文记录一次本机、单进程、单 PostgreSQL 实例的专项优化与验证。它不是生产容量承诺。服务、PostgreSQL、负载发生器和 HTTP sink 均在 Apple M4 / 10 cores / 16 GiB RAM / macOS 26.6.2 上通过 loopback 运行；Rust 1.97.1，PostgreSQL 17.11，release build，数据库耐久参数未关闭。本次没有实现新的设备功能，也没有推断未实际测量的吞吐。

## 结论

上一轮首先触发的是 MQTT PUBLISH 外层 `protocol Admission` 的瞬时 16/4/1 个 global/tenant/device permit。这个短期协议保护同时包住 codec、内层 durable-ingress permit、SQLx pool 等待、完整 PostgreSQL transaction/COMMIT、PUBACK 和 `/up_ack` socket write。多个不同资源的等待嵌套在外层小 semaphore 内，连接 ramp、PING 和 PUBLISH 的调度碰撞会在数据库远未饱和时直接断开连接。它是 permit 生命周期错误形成的 convoy，不是服务器只能处理 50 msg/s。

修改后，PUBLISH 只做有界 rate 检查；durable ingress 独立获得 device→tenant→node→byte permit，并允许调用者自身在 16 items、2 MiB、25 ms 三个边界内等待。没有为等待项创建 task。permit 在持久化调用返回时释放，不再跨 PUBACK 排队或 socket write。突发仍可被有界吸收，持续过载仍会明确失败。

数据库的全局 advisory transaction lock 和每次全表 quota aggregate 被同事务精确计数器替代。global→tenant→device 三层计数通过一个有依赖关系的 CTE 以固定顺序更新；消息、outbox 与 charge 在同一事务内一起提交或回滚。quota 更新移动到独立写入之后，使全局行锁只覆盖 quota statement 到 COMMIT 的尾段。这个设计消除了随保留行数增长的扫描，却保留了精确的跨进程全局上限。因而新的首个硬瓶颈是高并发下的 PostgreSQL global quota row 与 transaction/WAL 串行，不是 Admission map。

在本次 60 s 条件下，最终零错误实测点为 QoS0 **500 msg/s**、QoS1 durable **250 msg/s**、downlink **25 commands/s**。紧邻的 QoS0 1,000、QoS1 500、command 50 都出现明确错误或未完成 ACK，因此没有把它们称为 sustainable。上一轮经三次重复认证的点分别是 50/50/10；本阶段还对旧程序重放过 QoS1 100/s 并成功，所以倍率只能理解为相对于上一轮认证点的本机测量变化，不是逐点严格 A/B 的硬件极限。

## 方法与语义边界

机器、数据库和 histogram 精度与 [上一轮容量审计](global-capacity-performance-audit.md) 一致。客户端 application ACK 是 durable ingress receipt，不表示下游业务已完成。command queue-to-receive 从管理请求发起到设备收到命令，包含 200 ms poll 相位；`SENT` 仍不等于 `ACKED`。所有延迟百分位来自固定桶，stage 表中的数值是桶上界。

可持续点要求测量窗内：发布与接受相等、零 client error/ACK timeout、queue/outbox 不持续增长、cooldown 可排空、Admission/pool waiter 回零。60 s 容量点只证明该窗口。长期稳定性另由 4 h mixed soak 检查。

## Stage Timing

新增无 device/message/client 标签的固定低基数 counter 与 20 组微秒 histogram。MQTT QoS1 路径现在可分别观察 socket packet ready→validation、validation→Admission、Admission wait、auth→codec、codec→pool、pool wait、transaction start、dedup、writes、quota accounting/wait/critical-section upper bound、commit、transaction、COMMIT→PUBACK handoff 和 socket write；同时导出 Admission active/wait count+bytes、pool active/idle/waiters、dependency degraded workers、queue、session 与 runtime task gauges。

同为 QoS1 100/s、100 connections、256 B、60 s 的 instrumented A/B 如下。before 为旧 binary 上新增观测后的重放；after 为最终实现。stage P50/P95/P99 均为 histogram 桶上界，单位 µs。

| Stage | Before P50/P95/P99 | After P50/P95/P99 | Before mean | After mean |
|---|---:|---:|---:|---:|
| packet ready→validation | 10/10/10 | 10/10/10 | 0.8 | 1.6 |
| validation→Admission | 10/25/25 | 10/10/10 | 5.5 | 1.6 |
| Admission wait | 10/10/25 | 10/10/25 | 3.0 | 3.3 |
| auth→codec | 10/50/50 | 25/50/50 | 12.6 | 17.9 |
| pool acquire | 100/100/250 | 100/250/250 | 70.9 | 73.1 |
| transaction start | 250/250/250 | 250/250/500 | 134.6 | 150.8 |
| quota accounting | 2500/5000/5000 | 250/500/500 | 1489.5 | 178.2 |
| quota critical-section upper bound | 2500/5000/5000 | 500/1000/1000 | 1959.6 | 358.0 |
| dedup | 250/250/500 | 250/500/500 | 138.4 | 168.2 |
| message/outbox writes | 250/500/500 | 250/500/1000 | 187.6 | 248.3 |
| COMMIT | 250/500/500 | 250/500/1000 | 141.1 | 179.3 |
| whole transaction | 2500/5000/5000 | 1000/2500/2500 | 2230.3 | 1000.8 |
| COMMIT→PUBACK handoff | 10/10/10 | 10/10/10 | 0.0 | 1.6 |
| PUBACK socket write | 10/10/25 | 10/25/25 | 3.3 | 5.3 |

客户端 application ACK 从 **2.38/3.60/4.30 ms** 降至 **1.03/1.99/2.28 ms**。关键变化是 quota mean 1,489.5→178.2 µs、quota critical-section upper bound 1,959.6→358.0 µs、transaction 2,230.3→1,000.8 µs。PostgreSQL 不向 SQLx 暴露 row-lock 实际授予时刻；after 的 critical-section 指标从 quota statement 开始量到 COMMIT，是真实 hold time 的保守上界，quota accounting 则包含该 statement 的 wait + execution。原始数据：[before](performance/optimization/before_q1_100.json)、[after](performance/opt_final5_q1_100.json)。

## Admission 资源图

```text
connection slot + reserved bytes                       connection lifetime
    └─ MQTT parser / fixed packet buffer               one connection owner
       ├─ control packet protocol rate check           lock held only synchronously
       └─ PUBLISH validation + protocol rate check     no protocol permit across await
          └─ ingress bounded waiter                    <=16, <=2 MiB, <=25 ms
             └─ device slot (1)
                └─ tenant slot (4)
                   └─ node slot (16)
                      └─ ingress bytes (2 MiB)
                         └─ auth/codec/store await
                            └─ SQLx pool (8)
                               └─ PostgreSQL transaction
          receipt/PUBACK queue + socket write          ingress lease already released
```

| 名称 | 目的与 scope | 默认 capacity / owner | acquire→release | 跨 await / DB / socket | overflow |
|---|---|---|---|---|---|
| Connection slot/bytes | 限制 socket 与帧内存 | node 1,024；tenant 128；device 2；owner=connection task | accept→connection drop | 是/是/是 | accept reject/close |
| Protocol Admission | packet rate 与 control 短操作 | global 512/s；tenant 128/s；device 16/s | 同步 rate check 内 | 否/否/否 | explicit overload/close/drop |
| Per-connection QoS1 | packet-id/pending command | 32；owner=connection | publish/send→ACK/disconnect | 是/可能/是 | refuse/close |
| Ingress waiter | 吸收调度型短 burst | 16 items + 2 MiB + 25 ms；owner=原调用者 | durable acquire 开始→获得 lease/失败 | 是/否/否 | overload |
| Device ingress | 单设备在途隔离 | 1 | acquire→store returns | 是/是/否 | bounded wait then overload |
| Tenant ingress | 租户在途隔离 | 4 | acquire→store returns | 是/是/否 | bounded wait then overload |
| Node ingress | 节点在途 durable work | 16 | acquire→store returns | 是/是/否 | bounded wait then overload |
| Ingress bytes | 在途 canonical bytes | 2 MiB | acquire→store returns | 是/是/否 | bounded wait then overload |
| SQLx pool | PostgreSQL connections | 8；owner=store call | pool acquire→connection/tx drop | 是/是/否 | external timeout |
| Authentication | 外部 auth call deadline | inline per connection/request；5 s | request→result | 是/否/否 | auth timeout/failure |

固定获取顺序是 device→tenant→node→bytes。先获取的 permit 在后续 acquire 失败、deadline、cancellation、store error 和 task drop 时由 RAII 精确释放。registry 的普通 lookup/insert 为 O(1)；只有遇到新 key 且 map 已到上限才做一次有界清理，不再每条消息扫描所有设备/租户。10/100/1,000/10,000 entries 的 microbenchmark P50/P95/P99 分别为 208/250/250、208/250/250、208/250/250、208/250/291 ns，没有观测到随正常 cardinality 线性上升。

## 持久化配额与数据规模

旧路径先拿跨进程 advisory transaction lock，再对整个 `ingress_messages` 做 count/sum/filter aggregate。这个查询同时计算 global/tenant/device messages+bytes；成本随所有租户的 retained rows 增长，并把无关租户串在同一个长事务锁后。command insert 也扫描整个 `commands` 表。

迁移 `0002_ingress_quota_accounting.sql` 建立 global/tenant/device 精确持久化计数并从现有表回填。插入、重复检测、outbox、command ACK 和 quota 更新保持一个事务；cleanup 同事务减计数。rollback 因此不会留下孤立 charge，成功清理也不会延迟释放。所有 delta 使用范围 predicate，更新结果数必须精确为一，否则整个事务回滚并返回 overload。没有 eventual accounting。

离线历史状态的 `EXPLAIN (ANALYZE, BUFFERS)` 结果如下，单位 ms。fixture 直接建立历史数据，用于查询复杂度，不代表绕过 runtime quota 后的可承载生产数据量。

| 历史行数 | 旧 ingress aggregate | 新 ingress counter | 旧 command aggregate | dedup lookup | outbox claim | command device lookup | command batch claim | bounded ingress cleanup | bounded command cleanup | command-attempt lookup | expired command scan |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10K | 2.770 | 0.036 | 1.443 | 0.013 | 0.138 | 0.086 | 0.185 | 0.379 | 0.333 | 0.014 | 1.215 |
| 100K | 11.043 | 0.035 | 7.921 | 0.017 | 0.144 | 0.245 | 0.880 | 0.373 | 0.333 | 0.015 | 10.697 |
| 1M | 79.205 | 0.035 | 46.370 | 0.015 | 0.146 | 1.445 | 14.577 | 0.383 | 0.374 | 0.013 | 91.753 |

精确 counter lookup 在 10K→1M 保持约 0.035 ms；旧全表 ingress aggregate 从 2.77 增至 79.205 ms。剩余的 expired command scan 与 command batch/device 查询会随 command 历史增长，是后续 retention 风险。原始数据：[10K](performance/optimization/dataset_10000.json)、[100K](performance/optimization/dataset_100000.json)、[1M](performance/optimization/dataset_1000000.json)。

## PostgreSQL Query Inventory

最终普通 QoS1 telemetry 的受限 PostgreSQL statement log 实测每条 **8 statements / 1 transaction**：BEGIN、transaction-local timeout、同 source 的 expired delete、dedup select、message insert、outbox insert、三层 quota CTE、COMMIT。首次 prepare/SQLx pool ping 等 wire protocol 往返不一定出现在 statement log，因此这里报告 SQL statements 与 transactions，不把它伪称为抓包得到的全部物理 RTT。

command lifecycle 到 durable application ACK 的 statement/transaction 数从 **37/5 降至 34/5**；包含 ACK outbox 的后续 delivery claim/finish 后，从 **45/7 降至 42/7**。变化全部来自 enqueue 的 7→5：移除 advisory lock，并以持久化 counter 替代全表 count。claim 仍为 7，SENT 与 RECEIVED 各 6，command ACK ingress 为 10；attempt identity、session generation、lease owner 和状态单调性未合并或削弱。这个 3-statement 节省不是 200 ms queue tail 的主因，因此没有把 command state transitions 重写成难审计的大 CTE。

普通 query 的预期索引和锁行为：dedup 使用 `(tenant_id,product_id,device_id,source_message_id)` unique key；outbox claim 使用 due/lease 索引和 `FOR UPDATE SKIP LOCKED`；command claim 同样批量、有界并跳过锁；quota 精确触达 1 global + 1 tenant + 1 device row，固定顺序加 row lock；cleanup 每轮有 batch limit 并在同事务释放 quota。SQL trace：[uplink](performance/opt_final_sql_uplink_v3_sql.json)、[command](performance/opt_final_sql_command_v3_sql.json)。

## Throughput 与新拐点

| 路径 | 上一轮认证点 | 本阶段最高零错误点 | 相邻失败点 | 最终点 P50/P95/P99 |
|---|---:|---:|---:|---:|
| MQTT QoS0 | 50 msg/s | 500 msg/s，29,999/29,999 | 1,000/s：23,451/23,512，61 errors | 0.47/0.77/0.90 ms |
| MQTT QoS1 durable | 50 msg/s | 250 msg/s，15,000/15,000 | 500/s：9,065/9,149，84 errors | 0.60/0.93/1.28 ms |
| Downlink command | 10/s | 25/s，1,475/1,475 ACK | 50/s：2,930 queued/2,600 ACK，11 errors | queue 89.01/171/174 ms；ACK completion 2.39/3.94/13.41 ms |

QoS0 1,000/s 的三次实现迭代提供了归因链：[quota/update 较早](performance/opt_final2_q0_1000.json)时接受 17,846、P99 36.61 ms；[把 quota 移到事务末端](performance/opt_final3_q0_1000.json)后接受 20,759、P99 30.77 ms；[把 global/tenant/device 合为固定顺序单 statement](performance/opt_final4_q0_1000.json)后接受 23,451、P99 16.70 ms。最终仍过载，quota mean 149.0 µs、quota statement 返回后的 COMMIT tail 109.7 µs、transaction 1,223.3 µs。Admission microbenchmark 在 10K entries 仍只有 291 ns P99，说明下一个限制是 exact global quota row 配合 PostgreSQL transaction/WAL，而不是 Rust registry lock。

pool 不是该拐点的首因：QoS0 500/s 的 sampled active/waiter peak 为 2/1，transaction/quota mean 407.8/67.8 µs；QoS1 250/s 为 3/2、552.2/90.3 µs。失败点 QoS0 1,000/s 与 QoS1 500/s 的 sampled pool waiter peak 都为 0，但 transaction mean 升到 1,223.3/3,051.7 µs，最终分别触发 61/84 次 **ingress bounded-wait deadline**，protocol Admission reject 始终为零。也就是说，新的 rejection 是 16 个 durable permits 被较慢的持久化事务占用后 25 ms 等待边界生效；它由 PostgreSQL 精确全局计数行/transaction/WAL 的服务时间驱动，而不是再次出现外层 protocol permit convoy，也不是 SQLx 8-connection pool queue 满。

原生 `sample` 的 596 个 non-waiting 样本中，SQLx/PostgreSQL path 约 64.60%，runtime 其余 23.66%，allocation/copy 7.72%，JSON 2.85%，MQTT 0.50%，Admission 0.34%，logging 0.34%。这只是 wall-stack 样本分类，不是精确 CPU 百分比，但与 stage/容量拐点相互印证。[原始 profile](performance/opt_final_profile_uplink.sample.txt)。

## Burst、Overload 与恢复

500-in-1s burst 发布 500、接受 463、37 次有界 reject；1,000-in-1s burst 发布 867、接受 722、145 reject。两例 cooldown 后 wait/inflight/pool waiter 均为零。ramp 20→50→100→250→500/s 共发布 18,160、接受 17,975、184 reject，P50/P95/P99 2.19/7.75/27.47 ms。

持续过载恢复例发布 63,248、接受 61,809、明确拒绝 1,438。低速正常段先通过，峰值段触发 reject，回到低速后重新接受；最终 ingress wait count/bytes、inflight count/bytes、queue 和 pool waiter 全为零。系统没有靠永久 backlog 提高接受数，也没有需要进程重启。[原始结果](performance/opt_after_overload_recovery.json)。

## Retention A/B

baseline 与候选 expiry index 两例均在 cleanup 同时完成 18,000/18,000 ingress，零错误，并各清理 10,000 command rows。baseline application ACK P50/P95/P99=1.13/2.23/2.92 ms、cleanup mean=2.211 ms、WAL=58.095 MiB；index 为 1.07/2.24/3.36 ms、2.012 ms、58.176 MiB。候选只让 cleanup mean 改善约 9%，P99 反而增加约 15%，WAL 也略增，没有端到端收益，因此 **REJECT**，未进入 migration。

cleanup 保持有界 batch、外部 deadline、固定 worker lifetime 和 bounded jitter backoff；不会一次删除任意多到期行。1M fixture 中 bounded cleanup 仍约 0.38 ms，但 expired command 全扫描达 91.753 ms，说明 command retention 的下一步应先减少扫描，而不是保留本次候选 ingress index。

## 受控 Soak 与旧 drift 解释

先分别运行 telemetry-only、commands-only、mixed、telemetry+reconnect 四个受控窗口。telemetry 4,500/4,500，P50/P95/P99=1.00/7.20/14.09 ms；首个 325-message sample 为 0.88/8.55/13.59 ms。mixed 4,500/4,500 + 30/30 commands，ACK=1.15/4.33/11.95 ms；首 sample 1.17/6.63/13.42 ms。commands 20/20，queue=176/179/180 ms。reconnect 4,486/4,486，400 次 connect/disconnect 零错误，ACK=2.10/3.25/3.90 ms。所有例最终 Admission、queue、session、pool waiter 与 degraded worker 回零。

上一轮两小时平均 ACK 3.27→25.12 ms 的可复现机制是全表 quota aggregate 随 retained ingress rows 增长，并且它位于跨进程 advisory lock 覆盖的 transaction 内；负载、cleanup/command poll 与宿主 I/O 波动会在这条串行链上放大。10K/100K/1M 的 2.770/11.043/79.205 ms 曲线给出直接规模证据。本次精确 counter 的同一查询保持约 0.035 ms，受控 workload 与下列 4 h run 都未重现同方向的持续漂移。

## 4 小时 Mixed Soak

最终终验使用 TLS、100 devices、5 telemetry msg/s、1 command/30 s、每 30 s 重连 10% devices。固定负载测量 **14,400 s（4 h）**；generator 生命周期 14,445.01 s，runner 含数据库准备与 30 s cooldown 共 14,488.15 s。它是冻结 release binary 上唯一完成的 4 h run；预检曾因并行 compile 污染而在约 13 分钟主动取消，未作为证据，随后一次在负载启动前因同名 disposable DB 存在而失败。

最终 telemetry 发布/接受 **71,761/71,761**，commands queued/application-ACK **480/480**，client error、error sample、ACK timeout、pending-at-disconnect 均为零。另有 480 条 command ACK 本身经过 durable ingress，因此 server ingress、delivery success 与最终 ingress rows 都是 **72,241**。MQTT connect/disconnect 各 4,900，server connect failure、connection reject、protocol/codec violation、delivery/command failure、Admission reject、dependency degraded/recovered 全为零。所有 72,241 delivery jobs 最多尝试一次并成功。

| Application ACK | count | P50 | P95 | P99 | mean | max |
|---|---:|---:|---:|---:|---:|---:|
| 首小时 | 17,941 | 1.84 ms | 3.11 ms | 9.20 ms | 2.069 ms | 517.921 ms |
| 第二小时 | 17,940 | 1.48 ms | 4.15 ms | 7.36 ms | 2.407 ms | 750.419 ms |
| 第三小时 | 17,940 | 2.07 ms | 8.93 ms | 48.17 ms | 3.546 ms | 224.760 ms |
| 末小时 | 17,940 | 2.17 ms | 3.80 ms | 10.60 ms | 2.599 ms | 379.876 ms |
| 全程 | 71,761 | 1.87 ms | 4.14 ms | 17.82 ms | 2.656 ms | 750.419 ms |

首→末小时 P50/P95/P99 分别变化 +0.33/+0.69/+1.40 ms，mean +0.530 ms；没有重现上一轮 3.27→25.12 ms 的 7.7× 单调 drift。第三小时出现独立的尾延迟抬升而第四小时恢复，说明本机 I/O/调度仍有非单调波动，不能声称严格不变。server-side 首→末小时 Admission P50/P95/P99 为 10/10/25→10/25/25 µs，pool 100/250/250→250/250/250 µs，quota 250/500/1,000→500/500/1,000 µs，transaction 2.5/5/10→2.5/5/25 ms；quota/transaction 有温和上升，但没有形成 backlog 或客户端持续恶化。

command 全程 queue-to-receive P50/P95/P99=33.01/36.01/51.01 ms，application-ACK completion=2.48/6.68/34.33 ms；480 条均到 `RECEIVED`/`SUCCEEDED`。终点仅保留 9 条 command/attempt 是 retention worker 正常删除 471 条旧 terminal commands，不是丢失。

| 小时 | ingress rows | DB / ingress / outbox size | server RSS min/median/max | CPU cores | tasks median | FD median | outbox peak / oldest peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 18,078 | 29.19 / 18.41 / 2.20 MiB | 12.73/14.33/15.73 MiB | 0.0167 | 110 | 119 | 1 / 98 ms |
| 2 | 36,125 | 49.94 / 37.10 / 4.27 MiB | 12.41/14.09/14.27 MiB | 0.0154 | 110 | 119 | 1 / 152 ms |
| 3 | 54,185 | 71.01 / 55.89 / 6.55 MiB | 11.98/14.20/14.30 MiB | 0.0176 | 110 | 119 | 1 / 158 ms |
| 4 | 72,235 | 91.41 / 74.09 / 8.77 MiB | 13.84/14.33/14.39 MiB | 0.0174 | 110 | 119 | 1 / 141 ms |

DB/WAL 是预期持久化增长：WAL 四小时增量约 59.53/74.44/86.27/97.43 MiB，PG commits 约 61/s。live server RSS、task、FD、queue 与 waiters 没有随 rows 同步增长。每小时 PostgreSQL lock-waiter peak=0；ingress waiter peak=0；pool waiter 短暂 peak 为 2/1/1/2。cleanup mean 为 1.04/1.40/2.36/1.35 ms，第三小时也出现非单调尖峰并在末小时恢复。

断开与 cooldown 后：MQTT connections=0、registered sessions=0、session tenant entries=0、subscriptions=0、outbound queue count/bytes=0、ingress active count/bytes=0、ingress wait count/bytes=0、DB pool active/waiters=0、degraded workers=0、outbox pending/oldest=0/0 ms，fixed runtime tasks=9。metrics observer 自身占用 1 个 HTTP connection 和 1 个 protocol permit，所以这两个观测值为 1，不是遗留 device owner。RSS 为 14.50 MiB，未要求 allocator 把页面退回 OS。原始结果：[opt_soak_4h.json](performance/opt_soak_4h.json)。

## Runtime DB Failure / Recovery

启动时数据库不可用仍为 fatal。运行时 `Storage`/`Timeout` 使固定 worker 进入 degraded 状态，使用有上限的 exponential full-jitter probe；没有逐消息无限 retry，也没有 RAM telemetry buffer。成功 store call 将 worker 恢复到 healthy；shutdown 可取消 backoff。

故障注入中数据库在 31.15 s unavailable、43.58 s available，服务进程始终存活并正常退出。MQTT 发布 4,500，3,880 收到 durable ACK，620 次客户端错误；HTTP 720 次中 620 durable accepted、100 次 503，未返回虚假的 202。最终 server accepted=4,500、rejected=720、degraded transitions=2、recovered=2、degraded workers=0。约 35.3 s 两个 worker 均 degraded，约 60.2 s 均已恢复并且新 ingress 再次成功。所有 queue/Admission/pool waiter 最终回零。失败的单次 MQTT QoS1 连接没有获得 durable success，设备需按 at-least-once 规则重连重试；dedup 仍由持久化 source ID 保证。[原始结果](performance/opt_after_db_recovery.json)。

## Slow Consumer 与 Tenant Fairness

一个 slow consumer 时，healthy group 9,000/9,000、零错误，application ACK=1.11/2.27/3.00 ms，command queue=58.01/155/156 ms；noisy connection 最多占用 128 条 bounded commands。十个 slow consumer 时 healthy 仍为 9,000/9,000、零错误，ACK=1.08/2.25/3.44 ms，command queue=152/191/193 ms；noisy group 达到 1,024 条全局 command cap。资源有界，十个 slow consumer 增加 command tail，不能称作完全隔离。

跨租户例中 noisy tenant 发布 22,512、接受 11,264、明确失败 11,248；healthy tenant 9,000/9,000、零错误，ACK=1.00/1.98/2.56 ms，command queue=103/203/204 ms。共享 PostgreSQL/global budget 仍存在，但一个租户的 slow/noisy devices 没有使健康租户 ingress 出错。disconnect 后 outbound queue、session、Admission 全归零。

## Connection / Destructor / RSS Regression

重新测量的 plaintext 1,000/2,000/3,400 connections 增量分别为 18.18/17.83/17.56 KiB/connection；TLS 为 26.70/25.66/25.42 KiB/connection，相对 plaintext 约多 8.52/7.83/7.86 KiB。所有 3,400 connections 均建立成功且零错误。强制关闭/cooldown 后 active connections、registered sessions、outbound queue、Admission count/bytes 全部为零，runtime fixed tasks 为 8。RSS 保留 allocator pages，未要求物理 RSS 立即下降；逻辑 accounting 与 owner destruction 是确定性的。

## KEEP / REJECT / DEFER

**KEEP**：stage metrics；缩短 MQTT protocol permit lifetime；16 items/2 MiB/25 ms bounded wait；device→tenant→node→byte RAII 顺序；仅在 capacity 时清理的 O(1) Admission registry；同事务精确 quota counters；固定 global→tenant→device SQL lock 顺序；quota update 延后并合为一个 CTE；有界 cleanup metrics；runtime dependency degraded/recovered supervisor；pool active/idle/waiter gauges；可复现的 burst、dataset、failure、fairness 与 soak tooling。

**REJECT**：仅凭 EXPLAIN 保留 expiry index；任意增大 protocol/DB semaphore 或 channel；无限等待 task；把精确 quota 改成 eventual accounting；用关闭 PostgreSQL durability 换数字；把 1,000/s QoS0、500/s QoS1 或 50/s command 宣称为 sustainable。

**DEFER**：sharded/reservation-based global persistent quota（需要新的精确跨进程语义）；command state transaction consolidation（当前延迟主要是 poll 相位且 fencing 风险更高）；command-history expiry/batch index 重新设计；JSON/MQTT codec 微优化（bench 166/167/208 ns decode、125/125/125 ns encode，不是当前瓶颈）；多节点/远程数据库、24 h+ soak 和真实设备执行时间。

## 剩余风险

- exact global quota row 是有意保留的硬串行点；在本机约 QoS1 250→500/s、QoS0 500→1,000/s 之间首先影响服务质量。
- command poll 的固定相位仍使 queue P95 约 171 ms；1M history 下 command batch claim 14.577 ms、expired scan 91.753 ms，需要独立 retention 设计。
- SQLx acquire 包含 pool wait、连接建立与健康检查；cold connection 曾超过 100 ms，稳定复跑通过，不能把所有 acquire 时间解释为 semaphore wait。
- 一个 runtime DB error 对应的 MQTT connection 会关闭，系统不在内存里永久保留该消息；设备必须重试。
- allocator RSS 可以在 owner/permit 全部释放后保留，不代表 Admission accounting 泄漏；长时间碎片与系统压力仍需更长观测。
- 单机 loopback 4 h 不覆盖集群竞争、远程 RTT、网络分区、24 h retention 周期或真实证书/设备行为。
- global database/pool/WAL 仍为共享资源，测试证明的是本次条件下的量化隔离，不是 perfect tenant isolation。

## Validation

冻结 source 在终验前完成下列验证；之后没有修改生产代码。4 h soak 使用同一 release binary。

- `cargo fmt --all -- --check`：通过。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`：通过。
- `cargo test --workspace --all-features`：通过；第一次 sandbox 内运行仅因 loopback socket `Operation not permitted` 中止，获准使用本机 loopback 后完整套件通过。
- 5 个 fresh disposable PostgreSQL databases：transaction/command contract、pool/concurrency/cleanup pressure、attempt history、outbox crash recovery、MQTT process crash boundaries，**5/5 通过**。
- ASan/libFuzzer smoke：`mqtt_fixed_header` 20K、`mqtt_remaining_length` 20K、`mqtt_packet` 100K、`tcp_frame` 20K、`udp_envelope` 20K、`json_codec` 20K，全部 exit 0。
- 冻结 binary 的最终 QoS1 100/s 校准：12,000/12,000、零错误；application ACK 1.03/1.99/2.28 ms。

机器可读验证记录：[results_1789823074.json](performance/validation/results_1789823074.json)。
