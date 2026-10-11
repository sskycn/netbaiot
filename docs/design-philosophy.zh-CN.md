# 设计理念

[English](design-philosophy.md)

NetbaIoT 是设备入口网关与实时事件路由器，用来把设备连接到已经拥有应用数据和工作流
的业务系统。

## 为什么运行时不使用数据库？

网关不拥有历史遥测、客户记录、分析或工作流状态。让业务系统持有这些责任，各应用就能
选择自己的存储、保留和处理模型。NetbaIoT 的正常事件路径不需要 PostgreSQL、Redis 或
通用消息存储。

唯一运行时持久化是本地计划重启恢复。它记录计划关机无法排空的已接纳工作，容量有界，
不是热路径队列或事件历史。详见[重启恢复](restart-spool.zh-CN.md)。

## 为什么以内存为主？

正常运行时，连接状态、认证缓存、网关路由状态、MQTT broker 状态和投递队列都保存在
内存中。这让数据路径直接、资源所有权可见，也要求网关清楚说明哪些状态可恢复、哪些
不可恢复。restart spool 不会把全部内存状态变成持久状态。

## 业务系统拥有持久状态

应用应负责：

- 持久业务数据及其保留策略；
- 命令意图、离线命令队列和命令历史；
- 领域幂等与工作流状态；
- 设备 desired/reported 配置与协调；
- 分析、仪表盘和业务告警。

网关可以把在线 `DeviceCommand` 路由到已连接 MQTT/TCP 设备，并把 `CommandAck` 作为
`DeviceEvent` 返回。它不会为离线设备保留命令，也不会判断应用状态是否收敛。

## 为什么所有资源都必须有界？

队列、缓存、连接池、报文 buffer、session store、重试 lane、订阅索引、replay window、
retain store、命令队列和恢复文件都需要明确的数量与字节上限。只有数量上限无法约束
可变载荷。等待工作同样消耗容量，因此等待任务和 pending admission 也必须有界。

达到上限时，系统必须拒绝、在有界预算内延迟，或应用配置的 best-effort 丢弃策略。
把过载隐藏在无限队列或任务中只会推迟故障，并让慢 sink 更难隔离。

## 为什么不是 exactly-once？

ACK 丢失、sink 处理后超时或计划重启重放都会让事件重复投递。网关在重试和恢复中保留
`event_id`；消费者应在同一事务中按该 ID 去重并更新业务状态。这是 at-least-once，
不是网关与客户数据库之间的 exactly-once 事务。

MQTT QoS2 是 MQTT 协议握手，不会把业务投递契约改成 exactly-once。

## 计划重启恢复与崩溃持久性

计划停止时，生命周期关闭入口、等待活动 admission、排空 required delivery，并在需要时
把剩余责任提交到有界、带 checksum 的本地 spool。提交包含文件 fsync、原子 rename 和
支持平台上的目录 fsync。成功计划关机不会丢弃已接纳 required 工作。

突然退出、OS 崩溃、断电或硬件故障仍可能丢失尚未提交的有界流量和 MQTT 状态。
NetbaIoT 不承诺 crash durability。Auth cache 与网关控制快照重启后重建，永不写入 spool。
