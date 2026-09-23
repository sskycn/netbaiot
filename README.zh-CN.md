# NetbaIoT

NetbaIoT 是一个无数据库、以内存为主的 IoT 协议网关和实时事件路由器。它通过 HTTP、内嵌 MQTT 3.1.1、通用分帧 TCP 和经过认证的 UDP 接收设备流量，将其规范化为 `DeviceEvent`，并发送到需确认或尽力而为的业务接收端。

运行时不需要 PostgreSQL 或其他数据库。业务系统负责持久化业务数据和离线命令。NetbaIoT 唯一的持久化机制是有界本地重启 spool，仅用于计划内优雅关机无法完成所有已接受的必需投递时。

## 5 分钟 Quick Start

要求 Rust 1.88+、Python 3 和 `curl`；MQTT 命令可选安装 Mosquitto clients。
Mosquitto 在此仅作为客户端，NetbaIoT 自带 MQTT 3.1.1 broker。

```bash
cargo +1.88.0 build --locked
python3 examples/business_http_sink.py
```

另开终端：

```bash
export NETBAIOT_ADMIN_SECRET=abababababababababababababababababababababababababababababababab
cargo run -p netbaiot-server -- configs/tutorial.json
```

开发环境监听地址：

- 单设备入口：`127.0.0.1:8080`（TCP：HTTP/MQTT/通用 TCP；UDP：NBI1）
- 独立管理 HTTP：`127.0.0.1:9090`
- 可选 `business_tcp` 保持独立。

生产可配置 `device_ingress=0.0.0.0:443`：TCP 使用同一证书承载 HTTPS、MQTTS、TLS TCP；
UDP 使用同号端口，仍为 HMAC 认证、不加密。无需 ALPN 或自定义前导。
旧四地址字段已替换为 `device_ingress`，迁移说明见[架构](docs/architecture.zh-CN.md)。

上传设备事件：

```bash
curl --noproxy '*' -i http://127.0.0.1:8080/v1/device/data \
  -H 'Authorization: Bearer demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f' \
  --data '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

HTTP `202` 和 MQTT QoS1 PUBACK 表示事件已越过有界的 `EventAccepted` 边界，不代表业务数据库已存储事件。
Python 终端会打印规范化事件并返回 HTTP 204。继续阅读[10 分钟端到端教程](docs/getting-started.md)，完成 MQTT 收发、在线命令、CLI 和优雅关机。

设置 64 字符的 `NETBAIOT_ADMIN_SECRET` 以启用管理调用。生产配置必须指定需确认的 webhook 或分帧 TCP/RPC 业务接收端。由于重试和重启重放可能造成重复投递，业务接收端必须使用稳定的 `event_id` 去重。

参阅[完整用户指南](docs/user-guide.md)、[架构](docs/architecture.zh-CN.md)、[投递语义](docs/delivery-semantics.zh-CN.md)、[设备协议](docs/device-protocol.zh-CN.md)、[HTTP API](docs/http-api.zh-CN.md)、[业务系统集成](docs/business-integration.zh-CN.md)和[MQTT 指南](docs/mqtt.zh-CN.md)。

## 官方 Rust 客户端

业务系统使用 `netbaiot-client`；事件 ACK 由应用显式发出，并且发生在应用处理之后：

```rust
let client = NetbaIoTClient::builder()
    .endpoint(endpoint)
    .token(token)
    .event_address(event_address)
    .connect()
    .await?;
let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    handle(delivery.event()).await?;
    delivery.ack().await?;
}
```

命令通过 `client.commands().send(&command)` 发送，配置使用 `client.configs()`，运行操作使用 `client.runtime()`。离线设备会返回类型化的 `ClientError::DeviceOffline`；NetbaIoT 不会存储命令。

可选的 `netbaiot-device-sdk` 支持标准 MQTT 遥测/命令和设备 HTTP 上传/配置，不造成厂商锁定。标准 MQTT 3.1.1 客户端仍是一等支持对象。`netbaiot` CLI 提供状态、事件订阅、命令、配置、缓存失效和显式 drain 操作。参阅 [SDK 概览](docs/sdk.zh-CN.md)、[业务客户端](docs/client.zh-CN.md)、[设备 SDK](docs/device-sdk.zh-CN.md)和 [CLI](docs/cli.zh-CN.md)。
