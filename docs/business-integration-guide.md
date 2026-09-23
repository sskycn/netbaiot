# 业务系统集成指南

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

## Confirmed TCP/RPC stream

该模式适合官方 Rust client 或需要显式应用 ACK 的长连接消费者。配置只能二选一：

```json
"delivery_url": null,
"business_tcp": "127.0.0.1:9100"
```

并设置独立 token：

```bash
export NETBAIOT_BUSINESS_STREAM_TOKEN=business-stream-demo-token
```

当前 business stream 只允许 loopback，因为该监听器尚未单独实现 TLS；跨主机生产集成应优先使用 HTTPS webhook，或在受控 TLS tunnel/sidecar 后使用 stream。当前实现只有一个活动订阅者，顺序发送并逐条等待 ACK。

每帧是 4-byte big-endian 长度 + bounded JSON：

1. Client `hello {version:1, token}`。
2. Client `subscribe {version:1, subscription_id, filter}`。
3. Server `ready`；只有收到它才表示订阅已安装。
4. Server `event {delivery}`。
5. Client 完成业务事务后发送 `ack`，必须同时匹配 `delivery_id`、`subscription_id`、`event_id`。

socket write 不等于 ACK；错误或不匹配 ACK 会让投递失败并可能重放。断线重连可能收到同一 `event_id` 和新的 `delivery_id`。

```bash
python3 examples/business_tcp_client.py \
  --address 127.0.0.1:9100 --token business-stream-demo-token --count 1
```

该示例包含完整 framing、hello/subscribe/ready/event/ack。filter 可限制 tenant/product/device/event types，但它不会在事件接受后改变已存在的 required 责任；重连时使用不匹配 filter 不能静默丢掉旧事件。

## 官方 Rust 业务客户端

`netbaiot-client` 不创建 runtime 或数据库，使用调用方的 Tokio runtime。仓库的可编译示例：

```bash
NETBAIOT_ENDPOINT=http://127.0.0.1:9090 \
NETBAIOT_TOKEN="$NETBAIOT_ADMIN_SECRET" \
NETBAIOT_EVENT_ADDRESS=127.0.0.1:9100 \
NETBAIOT_EVENT_TOKEN=business-stream-demo-token \
cargo run -p netbaiot-client --example business_event_consumer
```

[`business_complete.rs`](../crates/netbaiot-client/examples/business_complete.rs) 还把 confirmed stream、manual ACK、在线 command、自动重连语义和 shutdown 放在一个可编译示例中；运行它时需先保持 demo MQTT/TCP device 在线，并另发一条上行事件：

```bash
cargo run -p netbaiot-client --example business_complete
```

核心 manual ACK 模式：

```rust
use futures_util::StreamExt;
use netbaiot_client::{ClientError, NetbaIoTClient};
use netbaiot_protocol::EventFilter;

let client = NetbaIoTClient::builder()
    .endpoint("http://127.0.0.1:9090")
    .token(management_token)
    .event_address("127.0.0.1:9100".parse()?)
    .event_token(stream_token)
    .connect()
    .await?;

let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    persist_idempotently(delivery.event()).await?;
    delivery.ack().await?;
}
client.shutdown();
```

Manual 是 correctness-first 默认。`AckMode::Immediate` 只适合应用明确接受“进入本地 bounded channel 后即 ACK”的数据丢失窗口。流有 item 和 byte 双重上限，丢弃未 ACK delivery 会关闭连接并触发重放。

断线重连使用 cancellation-aware full-jitter exponential backoff，默认 100 ms–5 s；重新认证并用同一个 subscription ID 订阅。终止性 auth/forbidden/version/protocol 错误不会无穷重试。`client.shutdown()`、丢弃 stream 或最后一个 client 会停止 owned task。

API 模块：`events()`、`commands()`、`devices()`、`configs()`、`runtime()`、`auth_cache()`、`routes()`。Token 的 Debug 输出脱敏，响应体和所有 timeout 有界。

## CLI

Cargo package 是 `netbaiot-cli`，安装/构建后的 binary 是 `netbaiot`。当前没有 clap 风格的独立 `--help` 成功路径；用法错误会打印真实 usage 并返回退出码 2。全局配置：

```bash
export NETBAIOT_ENDPOINT=http://127.0.0.1:9090
export NETBAIOT_TOKEN=$NETBAIOT_ADMIN_SECRET
export NETBAIOT_TENANT=demo
export NETBAIOT_PRODUCT=sensor

cargo run -p netbaiot-cli -- --output json server status
cargo run -p netbaiot-cli -- device status device-1
cargo run -p netbaiot-cli -- config get device-1
cargo run -p netbaiot-cli -- auth invalidate --device device-1
cargo run -p netbaiot-cli -- server drain --yes
```

事件 stream 还需 `NETBAIOT_EVENT_ADDRESS` 和可选 `NETBAIOT_EVENT_TOKEN`：

```bash
cargo run -p netbaiot-cli -- events subscribe \
  --tenant demo --product sensor --device device-1 --type telemetry
```

CLI 刷新 stdout 后才 ACK。`--output json` 输出 JSON/JSONL。命令：

```bash
cargo run -p netbaiot-cli -- command send device-1 \
  --json '{"name":"relay","arguments":{"enabled":true}}'

cargo run -p netbaiot-cli -- config set device-1 \
  --file ./device-config.json --revision 2
```

主要退出码：0 success、2 usage、3 auth、4 forbidden、5 device offline、6 unavailable/其他 runtime failure。

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
- Generic TCP session：发送一个 length-prefixed `DeviceCommand` JSON frame。
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
| `not_found` | 配置等资源不存在 | 校验完整 DeviceKey |
| `conflict` | revision、stream owner 或 replay 状态冲突 | 读取最新状态后重新决策 |
| `server_unavailable` | provider/sink/server/storage 暂时不可用 | 有界退避；观察 readiness/metrics |
| `internal` | 安全的内部错误 | 记录 request_id，检查 server 日志 |

管理错误响应为 `{code,message,request_id,required_scope}`；不要解析 message 文本。管理 HTTP 典型映射为 400/401/403/409/413/429/503/504。

## 常用场景

1. 100 个 MQTT sensor：每设备独立 credential/ClientId，QoS1 上报；业务 webhook 按 event ID 幂等；用实际 payload、TLS、sink latency 做负载验证。
2. TCP 设备：连接时认证，此后发送分帧 codec payload 并等待 EventAccepted；业务仍处理重复。
3. Stream consumer：先 ready，事务后 ACK；断线让官方 client 重连，同 event ID 去重。
4. 离线命令：业务系统持有命令意图，先查询 `devices().connection()`，在线后显式发送；不要期待 gateway 排队。
5. Credential rotation：更新 provider/snapshot，调用对应 auth invalidation，受影响 live/session 状态被移除，设备用新凭据重连。
