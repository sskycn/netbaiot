# Rust 业务客户端

`netbaiot-client` 是面向业务和管理集成的官方异步 Rust 客户端。控制操作使用连接池化的 HTTP 客户端，事件使用需确认的分帧 TCP 流。客户端不会创建 Tokio runtime 或数据库。

```rust
let client = NetbaIoTClient::builder()
    .endpoint("https://gateway.example")
    .token(management_token)
    .event_address("127.0.0.1:9100".parse()?)
    .event_token(stream_token)
    .connect()
    .await?;
```

各 API 模块为 `events()`、`commands()`、`devices()`、`configs()`、`runtime()`、`auth_cache()` 和 `routes()`。Secret 的 `Debug` 输出会进行脱敏。连接、请求、流握手和 ACK 写入都具有有限超时时间，并会在 builder 中校验。

事件投递默认使用 `AckMode::Manual`：

```rust
let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    process(delivery.event()).await?;
    delivery.ack().await?;
}
```

每个订阅同时最多有一条等待服务端确认的投递。面向用户的 channel 默认限制为 32 条、1 MiB；待处理 ACK 通道固定为 1。数量容量不会按最大载荷大小预分配。如果丢弃尚未 ACK 的投递，客户端会关闭并重连流，服务端可能重新投递该事件。

连接丢失后使用可取消的指数全抖动退避，间隔从 100 ms 到 5 s。重连时会重新认证、使用相同的 `SubscriptionId` 订阅并继续处理。客户端不会自行虚构 offset。重放可能使用新的 `delivery_id` 返回相同的 `event_id`；需要持久幂等的应用必须自行保存 event ID。丢弃流会终止其所属任务并关闭 socket。

命令不会自动重试。`DeviceOffline` 与一般服务端故障分别报告，调用方提供的 `command_id` 保持不变。配置 revision 使用类型化表示。下载/设置成功与设备应用 ACK 是两件事。`runtime().drain()` 是显式管理操作。
