# 5 分钟快速开始

[English](quick-start.md)

生产环境使用 Linux x86_64 或 ARM64。macOS 支持开发/测试；Windows 为实验性兼容，
不保证每个版本都有官方压缩包。详见[平台支持与发布政策](platform-support.zh-CN.md)。
本教程使用本地开发凭据；生产配置见[运维指南](operations-guide.md)。

下面的单命令 demo 只需要 NetbaIoT binary。手动流程会启动 webhook receiver、发送真实
MQTT publish，并显示规范化后的 `DeviceEvent`。它只在 loopback 使用仓库内教程凭据；
不要把该凭据或 bootstrap 管理 token 用于本地开发以外的环境。

## 单命令 demo

使用二进制发布包时，这条路径不需要 Python、Rust 或外部 broker：

```bash
./netbaiot demo
./netbaiot demo --once
```

Windows PowerShell 使用 `.\netbaiot.exe demo` 或 `.\netbaiot.exe demo --once`。
源码 checkout 使用 `cargo run --locked -p netbaiot-cli -- demo --once`（Rust 1.88+）。
demo 使用动态 loopback 端口和私有临时恢复目录，验证 MQTT 认证、QoS1
PUBACK/`EventAccepted`、规范化 heartbeat 投递，以及配置的 HTTP 204 sink ACK。
内置 receiver 仅用于开发，不保存持久业务历史，只打印事件元数据。示例凭据禁止复用于生产。

不带 `--once` 时，demo 会持续运行并打印一条使用实际设备端口的 `mosquitto_pub` 命令，
供你自己的 MQTT 客户端使用。Ctrl-C（Unix 也可发送 SIGTERM）采用与 `netbaiot serve`
和 `netbaiot-server` 相同的 drain/spool 生命周期。sink 会存活到网关 drain 完成，随后删除
临时文件。

```bash
./netbaiot --help
./netbaiot version
./netbaiot config limits
./netbaiot config check --config configs/development.json
./netbaiot serve --config configs/development.json
```

`config check` 会执行本地 JSON、静态规则、环境 secret source 和 PEM 检查，但不会绑定
端口、读取/改写恢复快照、启动 worker 或调用远程 provider/sink。检查通过不表示端口已
预留，也不证明远端可达。详见 [CLI 诊断](cli.zh-CN.md)。

## 手动 MQTT 示例

下面继续使用仓库保留的 shell/手动集成 helper。单命令 demo 不需要这些依赖；手动流程
需要 Python 与 Mosquitto client。

## 环境要求

- 只有源码 checkout 需要 Rust 1.88 或更高版本；
- Python 3.9 或更高版本；
- Mosquitto client 工具（`mosquitto_pub`）。

Mosquitto 在本教程中只是客户端。NetbaIoT 自己实现 MQTT 3.1.1 与 MQTT 5.0 broker，
不使用数据库或外部 broker。若依赖尚未缓存，第一次 Cargo build 可能超过五分钟。

## 启动网关与 webhook

在 macOS/Linux 上，从仓库根目录或解压后的二进制包目录打开 Terminal 1：

```bash
./scripts/demo/start.sh
```

脚本优先使用包内 `netbaiot-server`；源码 checkout 会用 Cargo 构建
`target/debug/netbaiot-server`。它启动
[`examples/business_http_sink.py`](../examples/business_http_sink.py)，把所有 listener
绑定到 `127.0.0.1`，并把恢复文件放入临时目录。webhook 打印每条已接纳 JSON 事件并
返回 HTTP 204。教程固定端口为 `8080`（设备 TCP/UDP）、`9090`（管理 HTTP）和
`18080`（webhook）。启动前先停止占用这些端口的进程，发布前等待 `runtime ready` 日志。

内置开发设备为：

```text
tenant / product / device: demo / sensor / device-1
credential id:             demo-device
credential secret:         000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
```

网关还为 demo 设置了可预测的本地 bootstrap 管理 token；发布 MQTT 事件不需要它。生产
环境应使用带 scope 的管理身份、正式设备凭据，并为非 loopback listener 启用 TLS。
详见[安全](security.zh-CN.md)与[运维指南](operations-guide.md)。

## 发布事件

在 Terminal 2 使用标准 MQTT 3.1.1 客户端发布：

```bash
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 \
  -m '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

webhook terminal 会打印下列形状的事件（网关生成的 ID 和接收时间每次不同）。这是 HTTP
webhook envelope；服务端把公开 Rust `DeviceEvent` 映射为这些扁平字段：

```json
{
  "event_id": "2b13d944-bb18-40df-8043-a636807fc023",
  "source_message_id": "demo:1",
  "tenant_id": "demo",
  "product_id": "sensor",
  "device_id": "device-1",
  "event_type": "heartbeat",
  "received_at": 1791203077827,
  "occurred_at": null,
  "payload": { "kind": "heartbeat", "data": { "sequence": 1 } }
}
```

QoS1 PUBACK 表示 NetbaIoT 已在 `EventAccepted` 边界接纳事件，不表示业务数据库已经
提交。demo webhook 的 HTTP 204 是配置的 sink ACK。依赖这些回执前请阅读
[投递语义](delivery-semantics.zh-CN.md)。

要使用 MQTT 5.0，把同一命令的版本改为 `-V mqttv5`。支持的 MQTT 5.0 属性与排除项
见[协议支持矩阵](protocol-support.zh-CN.md)。

## Windows 手动启动

上面的 Bash 脚本用于 macOS/Linux。Windows 为实验性平台：从源码构建，或使用已独立
验证且恰好可用的可选压缩包。在源码或解压目录打开 PowerShell。手动流程需要 Python
3.9+ 和 Mosquitto clients；源码构建还需要 Rust/MSVC。

Terminal 1 启动 webhook：

```powershell
py -3 -u examples/business_http_sink.py --listen 127.0.0.1 --port 18080
```

Terminal 2 使用相同 loopback 教程配置启动包内 server：

```powershell
$env:NETBAIOT_ADMIN_SECRET = "abababababababababababababababababababababababababababababababab"
$env:RUST_LOG = "info"
.\netbaiot-server.exe configs/tutorial.json
```

源码 checkout 先运行 `cargo build --locked -p netbaiot-server`，再改用
`.\target\debug\netbaiot-server.exe configs/tutorial.json`。等待 `runtime ready`。
Terminal 3 通过文件发布，避免 Windows shell 参数引用改变 JSON：

```powershell
'{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}' | Set-Content -Encoding ascii demo-event.json
mosquitto_pub.exe -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 -f demo-event.json
```

Terminal 1 会打印事件。先用 Ctrl-C 停止 server 并等待优雅关机完成，再停止 webhook。
此手动配置把恢复文件放在 `var/tutorial-restart-spool`；与 Bash demo 不同，它不会自动
删除该目录。这些凭据只适用于本地教程。

## 尝试分帧 TCP 或认证 UDP

同一开发 listener 接受仓库内示例客户端：

```bash
python3 examples/device_tcp.py --address 127.0.0.1:8080
python3 examples/device_udp.py --address 127.0.0.1:8080 --sequence 2
```

两条消息都像 MQTT 一样到达同一 webhook。TCP 使用长度前缀帧和认证握手。UDP 用 HMAC
签名每个 NBI1 数据报，并验证签名 NBA1 接纳回执。**UDP 认证不是加密**：网络观察者可见
载荷；UDP 没有 session 或命令下行。这些示例使用开发凭据，只能留在 loopback。

## 停止

在 Terminal 1 按 Ctrl-C，等待 server 完成优雅关机。demo 退出时删除临时配置与 spool
目录。优雅重启可以排空或写盘已接纳 required delivery；任意进程或机器崩溃仍可能丢失
只存在于内存中的工作。

接下来可阅读[10 分钟端到端教程](getting-started.md)、
[设备协议](device-protocol.zh-CN.md)和[运维指南](operations-guide.md)，继续了解设备命令、
客户端 API、持久 MQTT session 与 TLS 配置。
