# Business RPC V3 多流协议

V3 在现有 `business_tcp` listener 上显式启用：保留 `business_rpc.version: 2`，另加 `business_rpc.v3` 限额。V2 Hello 继续使用未修改的四字节长度前缀 JSON 协议；V3 Hello 进入独立二进制流引擎。客户端不会自动降级。V3 Hello 不包含 role；服务端按已认证的 BusinessPrincipal、每个 OPEN 的类型、方法及租户范围授权。生产环境使用 TLS 和客户端证书指纹映射，明文令牌仅用于 loopback 开发。V2 的身份配置和 `BusinessRole` 继续保留。

## Wire 协议

Hello/Ready 是有长度上限的 JSON 握手；Ready 返回新的 `connection_epoch` 和协商限额。后续固定帧头为 12 字节：`payload_length: u32 BE`、`stream_id: u32 BE`（高位为零）、`frame_type: u8`、`flags: u8`、`reserved: u16 BE`（必须为零），随后是 payload。稳定编号：OPEN 1、ACCEPT 2、RESPONSE 3、DATA 4、WINDOW_UPDATE 5、RESET_STREAM 6、CLOSE_STREAM 7、PING 8、PONG 9、GOAWAY 10。`END_STREAM = 1` 仅用于 DATA/RESPONSE；其他 flag 位均非法。读取 payload 前先验证帧类型、标志、ID 和长度。metadata 仍是最多 4 KiB 的 JSON，DATA 是原始业务 body 字节（当前业务 DTO 仍为 JSON）；收齐后严格核对声明的 `content_length`。

Stream 0 仅用于 PING、PONG、连接 WINDOW_UPDATE 和 GOAWAY。客户端 stream ID 为奇数，网关为偶数，同一连接上严格递增且不复用。用尽最后一个合法本地 ID 后，端点发送 GOAWAY NO_ERROR 并重连，不回绕。传输流身份是 `(connection_epoch, stream_id)`，不能替代业务 `request_id`、稳定 `event_id`、`delivery_id` 和 `subscription_id`。GOAWAY 的 `last_stream_id` 表示已考虑的最高对端 stream；重连生成新 epoch。

Provider 和 EventSubscription 是长寿命父流。Provider 下有网关发起的 `device.authenticate`、`device.resolve_verifier` 子 RPC，以及客户端发起的 `auth.sync`、`auth.invalidate` 子 RPC。初始 reset sync 完成且客户端通过 PING/PONG 确认响应后，Provider 才进入 Serving。EventSubscription 下有网关发起的 EventDelivery 子流，每个订阅仍只允许一个 delivery 在途。应用提交后才调用 `BusinessRpcV3Delivery::ack()`，失败时调用 `nack()`。socket 写入、WINDOW_UPDATE、SDK 收到事件都不等于业务 ACK。重试时 `delivery_id` 可变，`event_id` 保持稳定。V3 不增加 Command RPC。

单个 writer 按协商大小惰性切分 body，并在不同流间调度 DATA。RPC 与 Event 的调度份额为 4:1，连续 control 帧最多四个，没有发送 credit 的流会跳过；同一流的 OPEN/RESPONSE 一定先于 DATA。发送 DATA 同时扣除连接和流窗口，WINDOW_UPDATE 实际写出后才返还接收 credit，与应用 Event ACK 分离。RESET_STREAM 结束单流，父流 reset 同时清理子流；连接协议错误和 principal 过期发送 GOAWAY 并关闭。单流错误应通过 RESET_STREAM 隔离。

## 限额和迁移

默认 DATA payload 8 KiB、并发流 256、流窗口 256 KiB、连接窗口 4 MiB、心跳配置 5 秒；硬帧上限 16 KiB、metadata 4 KiB、消息 8 MiB。服务端和 SDK 每连接 outbound body 限额 16 MiB，服务端 inbound reassembly 每连接 16 MiB、全进程 128 MiB。control 队列最多 64 帧与 256 KiB；writer 和 credit 通道各 256 项。这些只是资源边界，不是容量或最优性能实测结论。观察固定名称的 `business_rpc_v3_*` 指标及原有认证、事件指标，不把标识符、令牌或证书内容作为指标 label。

开发环境可在现有有效 V2 配置的 `business_rpc` 中增加：

```json
"v3": {
  "max_frame_payload_bytes": 8192,
  "max_concurrent_streams": 256,
  "initial_stream_window_bytes": 262144,
  "initial_connection_window_bytes": 4194304,
  "heartbeat_ms": 5000
}
```

客户端显式使用 `BusinessRpcV3ClientConfig` 与 `BusinessRpcV3Client::connect`，在依赖 Provider/订阅前等待 `wait_ready()`。重连有有界退避并重建父流；旧事件句柄受 epoch 栅栏保护，不能在新连接上 ACK。TCP 自身仍有队头阻塞，丢包可能暂停所有流；V3 解决的是应用层整帧写入造成的阻塞。迁移时保留 V2，显式启用 V3，先迁移一个客户端，再以相同负载比较认证延迟、ACK 和资源占用。

实际 16 KiB Event 限速短测尚未证明默认 256 KiB 流窗口下有稳定、明显的认证尾延迟改善；原因与原始数字见 [V3 生产就绪测量](business-rpc-v3-production-readiness.zh-CN.md)。
