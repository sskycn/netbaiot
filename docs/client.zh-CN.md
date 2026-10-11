# Rust 业务客户端

`netbaiot-client` 是面向业务和管理集成的官方异步 Rust 客户端。控制操作使用连接池化的 HTTP 客户端，事件使用当前 Business RPC V3 流。客户端不会创建 Tokio runtime 或数据库。

```rust
let client = NetbaIoTClient::builder()
    .endpoint("https://gateway.example")
    .token(management_token)
    .event_address("127.0.0.1:9100".parse()?)
    .event_token(stream_token)
    .connect()
    .await?;
```

各 API 模块为 `events()`、`commands()`、`devices()`、`runtime()`、`auth_cache()` 和 `routes()`。Secret 的 `Debug` 输出会进行脱敏。连接、请求、流握手和 ACK 写入都具有有限超时时间，并会在 builder 中校验。

事件投递默认使用 `AckMode::Manual`：

```rust
let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    process(delivery.event()).await?;
    delivery.ack().await?;
}
```

每个订阅同时最多有一条等待服务端确认的投递。面向用户的 channel 默认限制为 32 条、1 MiB；RPC command/ACK 通道有界，服务端事件在途窗口为 1。数量容量不会按最大载荷大小预分配。丢弃尚未 ACK 的投递不会发送 ACK；网关按超时/重试策略重放。

连接丢失后使用可取消的有界指数退避，间隔从 100 ms 到 5 s。重连时会重新认证、使用相同 filter 与新的 epoch/subscription identity 重新订阅。客户端不会自行虚构 offset。重放可能使用新的 `delivery_id` 返回相同的 `event_id`；需要持久幂等的应用必须自行保存 event ID。丢弃流会终止其所属任务并关闭 socket。

命令不会自动重试。`DeviceOffline` 与一般服务端故障分别报告，调用方提供的 `command_id` 保持不变。设备 desired/reported 配置和历史由业务系统持久化。配置操作可使用普通命令；`CommandAck` 表示设备执行结果，业务系统自行决定收敛、重试与回滚。`runtime().drain()` 是显式管理操作。

## 当前 Business RPC 客户端

`netbaiot-client::business_rpc` 提供 `BusinessRpcV3Client`、当前 `BusinessAuthHandler`、reset sync、失效、手动 ACK 和在线命令。见[当前协议](business-rpc-v3.zh-CN.md)与[可编译示例](../crates/netbaiot-client/examples/business_rpc_current.rs)。底层 `wait_ready()` 持续等待，应由应用设置超时或取消。根事件 façade 使用同一当前 driver；开发环境显式传 `event_token`，生产用 `event_tls(BusinessRpcTls)`，没有管理 token fallback。旧 API 已移除，见[升级要求](migration/current-protocol-only.zh-CN.md)。
