# 可靠性与恢复

[English](reliability.md)

本页总结运行时投递和关机保证。它不承诺任意进程或机器故障都不丢数据。

## `EventAccepted`

事件只有在完成认证与授权、协议/codec 校验、路由选择、数量与字节准入、为每个
required sink 原子预留容量并全部入队后，才越过 `EventAccepted`。required fanout
是全有或全无。生产者回执只标记这一边界，不证明业务处理或数据库提交。

required sink 必须显式确认。HTTP webhook 使用配置认可的 2xx；confirmed TCP/RPC
消费者确认匹配的 `event_id`。best-effort sink 可按其有界策略丢弃，通常不会阻止接纳。
每个 sink 的队列、并发、超时、重试和失败策略独立且有界。

## 重试与重复

投递是 at-least-once。sink 已处理事件但 ACK 丢失时，网关可能重试同一事件；计划重启
后重放待处理工作时仍保留稳定 `event_id`。消费者应使用该 ID，让业务更新保持幂等。

慢 sink 不会制造无限进程 backlog。required sink 满载时，事件会在配置预算内被拒绝或
背压；required fanout 不会部分提交。best-effort 行为有意与此不同。

## 优雅关机

计划生命周期先关闭 readiness 和入口接纳，等待已取得的 admission guard，停止新设备
工作，再排空 required delivery。未完成或 inflight ACK 不确定的 required 工作写入本地
restart spool。如果进程仍拥有已接纳工作而持久提交失败，它必须保持存活且 unready；
只有工作已确认或已提交供重放时，才可成功退出。

独立 MQTT 快照保存有界 broker 协议状态，包括符合条件的持久会话、retain、QoS 状态和
pending Will。认证缓存与网关控制快照重启后重建。业务命令和长期历史不由网关保存。

## 恢复能力边界

spool 是本地、有界的计划重启恢复，不是数据库、热路径队列或通用事件日志。进程崩溃、
OS 崩溃、断电或硬件故障可能丢失自上次成功计划快照后仍只在内存中的工作。MQTT 协议
恢复也不承诺业务 exactly-once。

当前只支持 EventBus NBSP v3 与 MQTT NBMQ v6；旧版/未知格式明确失败。更多细节见
[投递语义](delivery-semantics.zh-CN.md)、[EventBus](event-bus.md)、
[重启 spool](restart-spool.zh-CN.md)、[MQTT 会话恢复](mqtt-session-recovery.md)、
[升级指南](migration/current-protocol-only.zh-CN.md)与[运维指南](operations-guide.md)。
