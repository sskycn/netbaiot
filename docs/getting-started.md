# 10 分钟端到端教程

本章从干净 checkout 开始，最终会看到 MQTT 设备上报到业务 webhook，并从管理 API 向在线设备下发命令。教程配置只绑定 loopback，包含固定演示密钥，**禁止用于生产**。

本章核心流程已由 `scripts/tutorial_smoke.sh` 在实现基线上实际运行；脚本自动选择空闲 loopback 端口，以免开发机已有服务占用文档中的固定端口。固定端口的命令与 `configs/tutorial.json` 一致；若端口冲突，请停止占用者或复制配置后修改全部相关命令。

## 1. 环境要求

- Rust `1.88.0` 或更高；推荐当前 stable。workspace 使用 Rust edition 2024。
- macOS 或 Linux；Windows 未在本次验证环境中实测。
- 构建使用 rustls，不要求系统 OpenSSL 开发包。
- Python 3 用于零依赖教程示例。
- `curl`；MQTT 示例建议安装 Mosquitto clients (`mosquitto_pub`、`mosquitto_sub`)。

Mosquitto 在这里仅是客户端/参考测试工具。NetbaIoT 自己包含 MQTT broker，生产运行不依赖外部 Mosquitto broker。

## 2. 获取并编译

```bash
git clone <your-netbaiot-repository-url>
cd netbaiot
cargo +1.88.0 build --locked
```

日常调试可用上述 debug build。生产构建使用：

```bash
cargo +1.88.0 build --release --locked
```

三个 binary 分别位于 `target/debug/netbaiot-server`、`target/debug/netbaiot` 和 `target/debug/netbaiot-loadgen`。也可直接用 `cargo run -p ...`，以下命令采用这种形式以避免路径混淆。

你应该看到：Cargo 构建成功，没有下载或启动外部 broker、数据库。

## 3. 认识教程身份与端口

[`configs/tutorial.json`](../configs/tutorial.json) 与 development config 使用同一设备：

```text
tenant       demo
product      sensor
device       device-1
credential   demo-device
secret       000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
```

监听地址：设备 HTTP/MQTT/generic TCP 与 UDP 共用 `8080`，management HTTP 独立 `9090`。教程 webhook 使用 `18080`。management token 使用下面的固定 64-hex 演示值。

## 4. Terminal 1：启动业务 webhook

```bash
python3 examples/business_http_sink.py
```

你应该看到：

```text
business webhook listening on http://127.0.0.1:18080/events
```

此示例先按 `event_id` 做进程内去重，再返回 HTTP 204。真实系统应在同一业务事务中写入业务状态和持久化 event ID，然后才返回 2xx。

## 5. Terminal 2：启动 NetbaIoT

```bash
export NETBAIOT_ADMIN_SECRET=abababababababababababababababababababababababababababababababab
RUST_LOG=info cargo run -p netbaiot-server -- configs/tutorial.json
```

成功时日志包含 `runtime ready` 以及五个设备/管理监听地址。健康检查也需要 management bearer：

```bash
curl --noproxy '*' -i http://127.0.0.1:9090/api/v1/ready \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET"
```

你应该看到 HTTP 200 和 `{"ready":true}`。日志文本可能随版本变化，不要依赖整行匹配。

## 6. Terminal 3：让 MQTT 设备等待命令

```bash
export DEVICE_SECRET=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P "$DEVICE_SECRET" -i tutorial-device \
  -t 'v1/t/demo/p/sensor/d/device-1/down' -q 1 -v
```

订阅范围由已认证身份限制；复制 ClientId 或在 filter 中使用通配符都不能逃出当前设备命名空间。

## 7. Terminal 4：发布首个事件

```bash
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P "$DEVICE_SECRET" -i tutorial-publisher \
  -t 'v1/t/demo/p/sensor/d/device-1/up' -q 1 \
  -m '{"schema_version":1,"source_message_id":"quickstart:1","kind":"telemetry","data":{"temperature":21.5,"online":true}}'
```

你应该看到：

- `mosquitto_pub` 正常退出；QoS1 PUBACK 表示 `EventAccepted`。
- Terminal 1 打印完整事件，含服务端生成的 `event_id`、设备身份和 `payload`。
- webhook 返回 204 后该 required delivery 成为 `ConsumerAccepted`。

如果 webhook 未运行，事件不会无限缓存；required sink 失败会在有界队列、并发、超时、尝试次数和年龄限制下重试，并可能对后续接纳施加背压。

## 8. Terminal 4：下发命令

```bash
curl --noproxy '*' -i http://127.0.0.1:9090/api/v1/devices/commands \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET" \
  -H 'Content-Type: application/json' \
  --data '{"command_id":"00000000-0000-0000-0000-000000000123","device":{"tenant_id":"demo","product_id":"sensor","device_id":"device-1"},"expires_at":null,"payload":{"name":"set_interval","arguments":{"seconds":10}}}'
```

你应该看到 HTTP 202、`state:"queued"`，Terminal 3 收到 `.../down` 上的完整 `DeviceCommand` JSON。`queued`/socket write/PUBACK/设备执行是不同阶段；设备执行结果应发布到 `.../down_ack`，使用 codec 的 `command_ack` 载荷。

如果 Terminal 3 已退出，命令返回 `device_offline`。NetbaIoT 不保存离线命令；业务系统决定是否以及何时用同一个 `command_id` 重试，禁止盲目重试不确定的设备动作。

## 9. HTTP ingress 快速确认

```bash
curl --noproxy '*' -i http://127.0.0.1:8080/v1/device/data \
  -H "Authorization: Bearer demo-device:$DEVICE_SECRET" \
  -H 'Content-Type: application/json' \
  --data '{"schema_version":1,"source_message_id":"http:1","kind":"heartbeat","data":{"sequence":1}}'
```

你应该看到 HTTP 202 的 `EventAccepted` JSON，Terminal 1 再收到一条事件。一次性上传、简单设备或上级网关转发适合 HTTP；需要长连接命令、订阅和 MQTT QoS 时用 MQTT。

## 10. CLI 快速确认

```bash
export NETBAIOT_ENDPOINT=http://127.0.0.1:9090
export NETBAIOT_TOKEN=$NETBAIOT_ADMIN_SECRET
cargo run -p netbaiot-cli -- --output json server status
cargo run -p netbaiot-cli -- device status device-1 --tenant demo --product sensor
```

CLI binary 名是 `netbaiot`，Cargo package 名是 `netbaiot-cli`。完整命令见[业务集成指南](business-integration-guide.md#cli)。

## 11. 优雅停止

可在 server terminal 按 Ctrl-C/发送 SIGTERM，或调用：

```bash
curl --noproxy '*' -i -X POST http://127.0.0.1:9090/api/v1/drain \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET"
```

进程会停止新接纳、等待 required sink，必要时提交 EventBus spool 和 MQTT snapshot，成功后打印 `shutdown complete` 并退出。不要在看到 readiness 变 false 后立刻 SIGKILL；那会把计划重启变成允许丢失近期内存工作的突发崩溃。

下一步：设备侧详见[设备接入指南](device-integration-guide.md)，业务 ACK/SDK/CLI 详见[业务集成指南](business-integration-guide.md)，生产部署详见[运维指南](operations-guide.md)。
