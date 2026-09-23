# NetbaIoT

NetbaIoT 是一个无数据库、以内存为主的 IoT 协议网关和实时事件路由器。它通过内嵌 MQTT 3.1.1、通用分帧 TCP 和经过认证的 UDP 接收设备流量，将其规范化为 `DeviceEvent`，并发送到需确认或尽力而为的业务接收端。

运行时不需要 PostgreSQL 或其他数据库。业务系统负责持久化业务数据和离线命令。NetbaIoT 唯一的持久化机制是有界本地重启 spool，仅用于计划内优雅关机无法完成所有已接受的必需投递时。

## 5 分钟 Quick Start

要求 Rust 1.88+、Python 3、`curl` 和 Mosquitto clients。
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

- 单设备入口：`127.0.0.1:8080`（TCP：MQTT/通用 TCP；UDP：NBI1/NBA1）
- 独立管理 HTTP：`127.0.0.1:9090`
- 可选 `business_tcp` 保持独立。

443 只是便于防火墙部署的端口选择，不表示运行 HTTPS。
生产可配置 `device_ingress=0.0.0.0:443`：TCP 使用同一证书承载 MQTTS、TLS TCP；
UDP 使用同号端口，仍为 HMAC 认证、不加密。无需 ALPN 或自定义前导。
旧四地址字段已替换为 `device_ingress`，迁移说明见[架构](docs/architecture.zh-CN.md)。

使用标准 MQTT 3.1.1 客户端发送设备事件：

```bash
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 \
  -m '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

MQTT QoS1 PUBACK 表示事件已越过有界的 `EventAccepted` 边界，不代表业务数据库已存储事件。
Python 终端会打印规范化事件并返回 HTTP 204。继续阅读[10 分钟端到端教程](docs/getting-started.md)，完成 MQTT 收发、在线命令、CLI 和优雅关机。

设置 64 字符的 `NETBAIOT_ADMIN_SECRET` 以启用管理调用。生产配置必须指定需确认的 webhook 或分帧 TCP/RPC 业务接收端。由于重试和重启重放可能造成重复投递，业务接收端必须使用稳定的 `event_id` 去重。

参阅[完整用户指南](docs/user-guide.md)、[架构](docs/architecture.zh-CN.md)、[投递语义](docs/delivery-semantics.zh-CN.md)、[设备协议](docs/device-protocol.zh-CN.md)、[HTTP API](docs/http-api.zh-CN.md)、[业务系统集成](docs/business-integration.zh-CN.md)和[MQTT 指南](docs/mqtt.zh-CN.md)。


设备 HTTP 已移除，旧客户端须迁移到 MQTT、分帧 TCP 或 UDP；设备主动配置拉取目前没有等价替代。
详见[破坏性变更及迁移](docs/remove-device-http.md)。

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

命令通过 `client.commands().send(&command)` 发送，运行操作使用 `client.runtime()`。离线设备会返回类型化的 `ClientError::DeviceOffline`；NetbaIoT 不会存储命令。

可选的 `netbaiot-device-sdk` 支持标准 MQTT 遥测/命令，不造成厂商锁定。标准 MQTT 3.1.1 客户端仍是一等支持对象。`netbaiot` CLI 提供状态、事件订阅、命令、认证缓存失效和显式 drain 操作。参阅 [SDK 概览](docs/sdk.zh-CN.md)、[业务客户端](docs/client.zh-CN.md)、[设备 SDK](docs/device-sdk.zh-CN.md)和 [CLI](docs/cli.zh-CN.md)。

UDP v1.1 在 EventAccepted 后返回固定 64 字节签名 NBA1 回执。ACK 丢失时重发原始 NBI1 数据报，在有效进程内 replay 窗口内不会重复摄取。详见 [UDP 协议与重试边界](docs/device-protocol.zh-CN.md#udp-acknowledgement-nba1)。

## 发布到 GitHub Releases

确认工作区干净且当前分支已同步到 `origin`，然后传入新版本运行：

```bash
scripts/release.sh v0.1.1
```

脚本会更新 `Cargo.toml` 和 `Cargo.lock` 中的 workspace 版本，单独提交版本变更，再推送当前分支和标签。标签推送后，GitHub Actions 会构建 Linux x86_64/ARM64、macOS Intel/Apple Silicon 和 Windows x86_64 发布包，生成 `SHA256SUMS`，并创建 GitHub Release。预发布版本可使用 `vX.Y.Z-...` 格式；完整用法见 `scripts/release.sh --help`。

NetbaIoT 不持有或持久化设备期望配置。业务系统负责 desired/reported 状态、版本历史、
重试、发布/回滚及离线协调。配置变更可作为普通 `DeviceCommand` 发往在线 MQTT/TCP 设备，
设备通过 `CommandAck` 返回执行结果；是否收敛由业务系统判断。命令仅支持在线投递，
UDP 无会话且没有下行。参阅[职责迁移](docs/remove-device-config.md)。
