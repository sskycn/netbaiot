# 业务系统集成

## HTTP webhook

内置的需确认 webhook 会发送完整的、与传输无关的事件，并将 `event_id` 作为 `Idempotency-Key`。配置的 2xx 响应即为 sink ACK。系统会限制连接池、拒绝重定向、设置请求超时、限制响应 Content-Length 和流式响应体、限制并发和重试次数。可选 bearer token 从 `NETBAIOT_DELIVERY_TOKEN` 读取，并且不会被记录到日志。

## 当前 Business RPC

[Business RPC V3](business-rpc-v3.zh-CN.md) 是唯一业务流协议。必须显式配置 `business_rpc`，使用当前限额与 mTLS principal 映射；回环开发环境可使用独立 token。bootstrap 是有界的长度前缀 JSON，后续应用消息使用二进制流分帧。没有版本分发、降级或旧版 fallback。

认证后的 EventSubscription 安装 required sink `tcp-rpc` 的唯一活动所有者。filter 决定消费者是否符合资格，不能在事件已经接受后丢弃不匹配的 required 责任。每次投递有稳定 `event_id`、独立 `delivery_id`、`subscription_id` 和 attempt。完成业务事务后才 ACK；socket write 与 WINDOW_UPDATE 均不是业务确认。缺失、格式错误或身份不匹配的 ACK 保留可重试责任。重连和重启可重复相同 `event_id`，业务必须幂等。使用[官方 Rust 客户端](client.zh-CN.md)获得有界缓冲、取消与手动 ACK。

## 在线命令与认证

当前客户端可以独立拥有 Provider 与 EventSubscription 流。Provider 实现 `BusinessAuthHandler`、reset sync、设备认证、UDP verifier 查询和 revisioned invalidation。MQTT/TCP 会话认证后绑定身份，普通上行不逐条调用 provider。revision gap 撤销授权并要求 reset sync；HTTP 和 RPC 共用完整 admission/invalidation fence。

命令 principal 必须允许 `device.command.send` 并限制目标租户；同时消费事件时增加 `sink_id: "tcp-rpc"`。服务就绪后调用 `BusinessRpcV3Client::send_command(&command)`；仅发命令时设置 `provider = false`、`events = false`。`Queued` 代表当前在线 MQTT/TCP 会话接纳，传输 SENT 与后续普通事件 `CommandAck` 的设备执行结果分别处理。`OutcomeUnknown` 由业务决定是否以相同内容及 `command_id` 重试。租户、TTL、count/byte 限额和离线拒绝继续执行。HTTP `/api/v1/devices/commands` 共用进程内去重表；网关不存储离线命令或持久命令历史。见[当前命令示例](../crates/netbaiot-client/examples/business_v3_command.rs)。
