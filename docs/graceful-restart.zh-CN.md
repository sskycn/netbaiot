# 优雅重启

明确的生命周期依次为 `STARTING`、`RUNNING`、`QUIESCING`、`DRAINING`、可选的 `SPOOLING`，最后到 `DRAINED`。只有处于 `RUNNING` 时 readiness 才为 true；进程退出前 liveness 始终为 true。

`begin_admission` 先检查状态是否为 `RUNNING`，增加活动 guard 计数，然后再次检查状态。Quiesce 会原子关闭状态并等待所有先前的 guard。因此，与关机竞争的工作要么完成 `EventAccepted` 边界并纳入跟踪，要么失败且不会收到成功确认。

Quiesce 会拒绝新的设备事件和命令，并停止监听器/连接。除非客户端发送了 MQTT DISCONNECT，否则 MQTT 连接 owner 会发布自己的 Will；MQTT 3.1.1 的 Will 约定适用于服务端关闭 Network Connection 的场景。所有 owner 分离后，会生成一致的 broker 快照，其中包含持久会话、订阅、离线消息、入站/出站 QoS 状态、packet 分配器位置、retain 消息，以及等待订阅者容量的有界 Will。成功退出前必须提交此快照。

Drain 期间，必需 sink worker 会继续工作。如果待处理必需责任数量降至零，进程无需新建 spool 即可退出。否则会进入 `SPOOLING` 阶段，此时 worker 仍持有工作并可能继续完成投递。只有精确待处理责任已持久提交后，才会停止 worker 并允许优雅退出。

MQTT 快照或 EventBus spool 失败会阻止主动关机。结构性 MQTT 恢复错误会记为严重错误且不会重试，但不会再跳过 EventBus 安全流程：必需事件仍会先完成 drain 或写入 fsync 的重启 spool。之后进程继续存活、保持未就绪，并保留管理接口。可重试的存储故障会按有界周期继续尝试。在此期间被外部 SIGKILL 终止，属于明确允许丢失数据的异常崩溃语义。

启动时先校验/恢复 MQTT broker 快照，再按原 ID 恢复 EventBus spool 段，初始化 sinks/监听器，最后才启用 readiness。只有必需工作全部完成 drain 后才会删除已恢复的 EventBus 段。MQTT 重连仍须先认证再恢复会话；恢复状态不包含凭据。

MQTT 恢复会以增量方式写入 NBMQ v6 有界类型记录，并在末尾附上记录数、字节数和整条流的摘要。只读取 NBMQ v6；旧版及未知版本在 payload 解码前明确失败。完成新 broker 状态替换前，会校验 topic/filter 语法、packet ID、各状态允许的 QoS、离线队列中的非 QoS0 消息、retain 一致性、顺序、重复项、授权/codec 来源，以及 session ACL 所有权。

EventBus 恢复只读写 NBSP v3 权威快照，记录完整事件、稳定 `event_id`、待处理 sink、
路由 revision 与重试元数据，并以逐记录 checksum 和整条流摘要保护。NBSP v1/v2、NBMQ v1–v5
及未知版本均明确失败；当前进程不包含旧版解析器或自动转换器。升级前必须按
[当前协议升级指南](migration/current-protocol-only.zh-CN.md)处理旧文件。

控制面 mutation、当前 Business RPC auth.sync/auth.invalidate、provider 离线撤销、命令提交（含去重重试）与 MQTT/TCP 最终建连均持有 lifecycle admission guard。Quiescing 后拒绝新操作，等待已获准操作完成，再等待 MQTT owner 的 QoS/Will 清理并写入快照。诊断读取和幂等 drain 请求仍可使用。
