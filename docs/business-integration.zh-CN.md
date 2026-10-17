# 业务系统集成

[English](business-integration.md)

业务系统负责长期数据、幂等、工作流、离线命令和命令历史。NetbaIoT 负责实时接入、标准化、bounded routing 和向当前在线设备发送命令。

## DeviceEvent 契约

业务 sink 收到的是与 MQTT/TCP/UDP 无关的统一事件：

```json
{
  "event_id": "7eb265e7-bdf1-47c2-97cb-f73592b2ff18",
  "source_message_id": "quickstart:1",
  "tenant_id": "demo",
  "product_id": "sensor",
  "device_id": "device-1",
  "event_type": "telemetry",
  "received_at": 1789980000123,
  "occurred_at": null,
  "payload": {"kind":"telemetry","data":{"temperature":21.5}}
}
```

上述是 webhook envelope。公共 `DeviceEvent` Rust 类型则把身份放在 `device` 字段中。时间戳为 Unix 毫秒；`received_at` 由 gateway 分配，`occurred_at` 可由设备提供。

- `event_id`：NetbaIoT 分配的稳定 delivery identity。sink retry 和 restart replay 保持不变；以它做业务幂等。
- `source_message_id`：设备/应用分配。同一设备消息重试应复用，可用于设备侧关联，但不能替代 gateway delivery 去重。
- `delivery_id`：仅 confirmed stream 的某一次投递尝试；重连可改变，不能作为业务幂等键。

## EventAccepted 与 ConsumerAccepted

事件依次完成认证/授权、codec 验证、路由选择、全局数量/字节准入，以及按稳定 `SinkId` 顺序对所有 required sink 原子预留并入队，才达到 `EventAccepted`。任何 required sink 预留失败都会整体回滚，不产生部分 required fanout。

之后 worker 向 sink 投递。Webhook 2xx 或 TCP 应用 ACK 才是相应 sink 的 `ConsumerAccepted`。业务投递仍是 at-least-once：例如业务事务已提交而 ACK 在网络中丢失，NetbaIoT 必须重试相同 `event_id`。

Runtime 的 `SinkDefinition` 区分 required 与 best-effort：required 参与原子接纳，best-effort 可按 bounded policy 丢弃且通常不阻塞 EventAccepted。当前 `netbaiot-server` 顶层 JSON composition 只安装一个 required sink（webhook、confirmed TCP，或 development audit 三选一），没有可配置 best-effort sink 字段；不要在配置中编造该字段。

推荐数据库事务模式：

```text
BEGIN
INSERT event_dedup(event_id)       -- unique key
  if already exists: COMMIT; ACK
apply business changes
COMMIT
ACK
```

不要先 ACK 再把事件放入未经持久化的后台队列，除非明确接受进程崩溃时丢失。

## HTTP webhook sink

在配置中设置：

```json
"delivery_url": "https://business.example.net/netbaiot/events",
"business_tcp": null
```

非 loopback URL 必须是 HTTPS；URL 不能内嵌 user/password。可选 token 从 `NETBAIOT_DELIVERY_TOKEN` 读取，server 发送 `Authorization: Bearer ...`。每个请求还带 `Idempotency-Key: <event_id>`。

配置的任意 2xx 是 ACK。429 和 5xx 为 retryable；其他非 2xx 为 permanent attempt failure。连接、总请求、响应 body（4 KiB）、并发、重试次数与最大年龄均有界；不跟随 redirect。

零依赖示例：

```bash
python3 examples/business_http_sink.py
```

它只在内存中去重，适合教程，不适合生产。业务 sink unavailable 时，NetbaIoT 使用独立的 count/byte queue 与有界退避重试；required backlog 到达上限会在 EventAccepted 前向设备施加 backpressure，而不是无限吃内存。计划关机时未 ACK 或 ACK 不确定的 required delivery 会被写入 restart spool。

## Confirmed Business RPC

业务监听器只支持 [Business RPC V3](business-rpc.zh-CN.md)，必须显式配置 `business_tcp` 与 `business_rpc`。生产使用 mTLS principal；回环开发配置独立 `NETBAIOT_BUSINESS_RPC_TOKEN`，并在客户端显式设置事件 token。管理或设备凭据不授权这个监听器。

bootstrap 为有界 Hello/Ready JSON；后续是 12-byte header 的二进制 stream framing。Provider、EventSubscription 和在线命令 RPC 分别管理。唯一活动 required sink `tcp-rpc` 所有者每次只有一个事件在途。应用事务成功后调用 ACK，匹配当前 epoch/stream、delivery 与 event identity；socket write 与 WINDOW_UPDATE 都不是业务 ACK。filter 改变不能丢弃已经接受的 required 工作。重连可以重放稳定 EventId，业务必须幂等。

使用下面的官方 Rust 示例，不再保留旧 Python wire 实现。旧客户端和恢复文件必须先完成[升级准备](migration/current-protocol-only.zh-CN.md)。

## Rust 客户端与 CLI

业务事件消费、manual ACK、有界缓冲、重连、取消和错误处理见
[Rust 业务客户端](client.zh-CN.md)；管理、事件订阅和在线命令的完整参数及退出码见
[CLI](cli.zh-CN.md)。可编译示例位于 `crates/netbaiot-client/examples`：

- [完整消费与命令示例](../crates/netbaiot-client/examples/business_complete.rs)。
- [Provider 与事件流](../crates/netbaiot-client/examples/business_rpc_current.rs)。
- [在线命令 RPC](../crates/netbaiot-client/examples/business_rpc_command.rs)。

Provider 与 EventSubscription 可独立启用。Provider 实现 `BusinessAuthHandler`、
reset sync、认证、UDP verifier 查询和 revisioned invalidation；revision gap 撤销
受影响授权并要求 reset sync。MQTT/TCP 普通上行使用连接绑定的身份，不逐条调用 provider。

命令 principal 需允许 `device.command.send` 并限制租户；同时消费事件时增加
`sink_id: "tcp-rpc"`。仅发命令的客户端设置 `provider = false`、`events = false`，
就绪后调用 `BusinessRpcClient::send_command(&command)`。`OutcomeUnknown` 表示结果
不确定，由业务决定是否以相同内容及 `command_id` 重试；HTTP 命令共用进程内去重表。

## Command/downlink

业务系统提交完整 `DeviceCommand`；caller-supplied `command_id` 会原样保留：

```json
{
  "command_id": "00000000-0000-0000-0000-000000000123",
  "device": {"tenant_id":"demo","product_id":"sensor","device_id":"device-1"},
  "expires_at": null,
  "payload": {"name":"relay","arguments":{"enabled":true}}
}
```

- MQTT session：发送到 `v1/t/.../down`，默认 QoS1；设备订阅 QoS 可降低实际 QoS。
- Generic TCP session：发送一个普通 length-prefixed frame，其内容按设备绑定 Codec 编码；
  选择 JSON profile 时是 `DeviceCommand` JSON，厂商布局则是对应二进制数据。设备上行
  接纳回执另用带标记的二进制 ACK v1，不能与命令帧混淆。
- UDP 设备：没有 live downlink session，因此返回 device offline。

队列按 device、tenant、process 的 count/bytes 和 TTL 有界。没有当前本地 live session 时立即返回 `DEVICE_OFFLINE`；不会写数据库、MQTT 离线队列或 restart spool。多节点 session routing 不在当前范围。

`queued`、transport `SENT`、MQTT PUBACK/设备 receipt、设备 execution 是不同状态。设备用 `command_ack` DeviceEvent 返回 `running|succeeded|failed`。因为 timeout/connection loss 可能发生在命令已执行之后，客户端不会自动 retry；只有应用确认重复执行安全时，才可用相同 command ID 重试。

## 错误与重试决策

公共 `ErrorCode`/`ClientError` 如下；服务内部 `Storage` 映射为 `server_unavailable`，并不是单独 wire code。

| 公共错误 | 含义 | 建议 |
|---|---|---|
| `unauthenticated` | token/credential 不正确或已失效 | 不重试旧 secret；修复/rotate |
| `forbidden` | 已认证但无权限/ACL 不匹配 | 修正权限或 topic；不要盲重试 |
| `invalid_request` | JSON、codec、长度或参数非法 | 修复请求 |
| `invalid_protocol_version` | wire version 不兼容 | 升降级客户端；不要重试原请求 |
| `device_offline` | 没有 live MQTT/TCP session | 业务保存策略；等设备在线后决定是否重发 |
| `overloaded` | count/byte/concurrency 容量满 | 指数退避并降载，保留同一应用 identity |
| `service_draining` | 正在 quiesce/drain | 切换实例或等重启完成 |
| `timeout` | 截止时间到；结果可能不确定 | 查询状态；命令尤其不能盲重试 |
| `connection_lost` | stream/HTTP 连接中断 | 事件流可重连并按 event_id 去重 |
| `not_found` | 请求资源不存在 | 校验完整 DeviceKey |
| `conflict` | revision、stream owner 或 replay 状态冲突 | 读取最新状态后重新决策 |
| `server_unavailable` | provider/sink/server/storage 暂时不可用 | 有界退避；观察 readiness/metrics |
| `internal` | 安全的内部错误 | 记录 request_id，检查 server 日志 |

管理错误响应为 `{code,message,request_id,required_scope}`；不要解析 message 文本。管理 HTTP 典型映射为 400/401/403/409/413/429/503/504。
