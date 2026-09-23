# 公共协议 v1

`netbaiot-protocol` 是 NetbaIoT 公共 wire 契约的权威、独立于 runtime 的 Rust 模型。它仅依赖序列化、JSON、UUID 和错误模型相关 crate，不依赖 Tokio、HTTP、MQTT、服务端或 runtime 内部实现。Crate SemVer 与 wire `PROTOCOL_VERSION` 是两个独立的兼容性维度；当前 wire 版本为 `1`。

强类型公共标识包括 `TenantId`、`ProductId`、`DeviceId`、`DeviceKey`、`EventId`、`DeliveryId`、`CommandId`、`SinkId` 和 `SubscriptionId`。使用前会验证这些标识。UTC 时间戳使用 Unix 毫秒。

`DeviceEvent` 包含稳定的 `event_id`、来源消息 ID、权威设备身份、接收/发生时间，以及一种有类型的事件：telemetry、heartbeat、设备事件、连接/断开或命令 ACK。重启重放会保留 event ID。`DeliveryId` 标识一次流投递尝试，重连后可能变化。

需确认流使用四字节大端序长度帧封装有界 JSON。客户端先发送包含版本/认证信息的 `hello`，再发送包含有界 `EventFilter` 的 `subscribe`。服务端回复 `ready`，随后发送包含 `EventDelivery` 的 `event` 帧。客户端通过 `ack` 确认处理结果；其中必须包含 `delivery_id`、`subscription_id` 和 `event_id`。写入成功或解码成功都不算 ACK。

管理错误使用 `ApiError { code, message, request_id, required_scope }`。稳定错误码包括认证、授权、请求无效/版本不匹配、设备离线、过载、draining、超时、连接丢失、未找到、冲突、服务端不可用和内部错误。客户端无需解析错误文案。服务端响应和事件模型会容忍 wire v1 中无害的额外 JSON 字段；为避免歧义，在需要保护服务端时，请求解析可继续采用严格策略。官方管理客户端仅允许在 `localhost` 或 loopback IP endpoint 使用明文 HTTP；非 loopback endpoint 必须使用 HTTPS。

控制平面快照和变更共用一把串行化锁。因此每次被接受的变更都会观察最新的已提交状态，并确定性地推进 revision 语义，不会与缓存失效或快照替换操作发生竞态。

设备 JSON v1 对应 `DeviceUplink`。稳定字段包括 `schema_version`、`source_message_id`、可选的 `occurred_at`、`kind` 和 `data`。MQTT QoS1 PUBACK、TCP acceptance 和签名 UDP NBA1都只代表达到 `EventAccepted`。

本次 0.x 清理是有意的 source/control API 破坏性变更：删除 `TransportKind::Http` 和
`ConnectionCounts.http`，传输字符串只剩 `mqtt`、`tcp`、`udp`；status 只输出这三项，
不包含管理连接。UDP 无会话，活动连接数为零。设备 JSON v1、MQTT 3.1.1、TCP 分帧、
NBI1/NBA1 以及业务确认流 v1 均不变，wire version 不变。公共客户端应与服务器一起重新构建。
详见[迁移说明](remove-device-http.md)。
