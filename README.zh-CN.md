# NetbaIoT

[English](README.md) · [文档导航](docs/README.zh-CN.md) · [发布版本](https://github.com/sskycn/netbaiot/releases)

NetbaIoT 是使用 Rust 编写、无运行时数据库、以内存为主的 IoT 接入网关与实时事件路由器。
它直接实现 MQTT，将 MQTT、分帧 TCP 和认证 UDP 上行规范化为 `DeviceEvent`，再投递给业务系统。
长期数据、工作流、设备配置和离线命令由业务系统负责。

```text
设备 → 传输 → 认证 → 版本化 Codec → DeviceEvent
     → 有界路由 → 独立有界的业务 Sink
```

## 快速开始

源码用户需要 Rust 1.88+：

```bash
cargo run --locked -p netbaiot-cli -- demo --once
```

解压发布包后：

```bash
./netbaiot demo --once
```

Demo 通过真实 MQTT 认证并发布心跳，等待业务 Sink ACK 后排空退出，无需外部 broker、
Python 或 Mosquitto。演示凭据仅限本地开发。去掉 `--once` 可持续运行并获得标准 MQTT
客户端命令。Windows PowerShell 使用 `.\netbaiot.exe`。

创建自己的配置：

```bash
./netbaiot init my-gateway
cd my-gateway
../netbaiot config check --config netbaiot.json
../netbaiot serve --config netbaiot.json
```

手动 MQTT 示例见[快速开始](docs/quick-start.zh-CN.md)，生产配置见[配置参考](docs/configuration.zh-CN.md)。
Linux x86_64/ARM64 是生产平台，macOS 用于开发测试，Windows 为实验性支持，详见[平台支持](docs/platform-support.zh-CN.md)。

## 协议支持

| 边界 | 能力 |
| --- | --- |
| MQTT | 内嵌 3.1.1 与有界 5.0 profile；QoS0/1/2、持久会话、retain、Will、exact/`+`/`#` 订阅 |
| TCP | 长度前缀帧与二进制 EventAccepted ACK v1；与 MQTT 共用设备 TCP listener |
| UDP | NBI1 HMAC 上行、时间戳/replay 校验、签名 NBA1 回执；不加密，无下行 |
| 业务 | 需确认的 HTTP webhook 或 Business RPC V4 |
| 管理 | 独立认证的 HTTP listener，路径为 `/api/v1` |

Payload 支持 JSON V1、CBOR V1、MessagePack V1、Protobuf V1 及显式注册的[厂商 Codec](docs/vendor-codecs.zh-CN.md)。
公网设备 TCP 必须使用 TLS；UDP 认证不提供加密。完整契约见[协议矩阵](docs/protocol-support.zh-CN.md)
与[设备协议](docs/device-protocol.zh-CN.md)。

## 投递与命令

设备回执表示 `EventAccepted`：校验、路由、数量/字节准入和全部 required sink 原子入队
已完成，不表示业务持久化。Required sink 必须显式 ACK；重试与计划重启重放可能重复
相同的 `event_id`，业务端必须据此幂等处理。

计划关机先排空 required 工作，或将其持久提交到有界本地 restart spool。提交失败时
进程保持存活且 unready。突然崩溃可能丢失近期内存工作。详见[投递语义](docs/delivery-semantics.zh-CN.md)
与[重启恢复](docs/restart-spool.zh-CN.md)。

命令只发往本节点当前在线的 MQTT/TCP 会话。离线返回 `DEVICE_OFFLINE`，不保存离线命令。
传输 SENT、设备收到与执行 ACK 分别处理。详见[业务集成](docs/business-integration.zh-CN.md)。

## 当前限制

当前为单节点网关，不支持集群和跨节点在线会话路由；不提供仪表盘、遥测数据库、OTA 或配置协调。
MQTT over WebSocket、共享订阅、MQTT-SN、Topic Alias、Subscription Identifier、Enhanced
Authentication、bridge mode 与 `$SYS` 不在支持范围。HTTP 仅用于管理，不是设备入口。
当前生产容量尚未确定，详见[测量范围](docs/benchmarks.zh-CN.md)。

文档跟随 `main`。Workspace 版本仍为 `0.3.3`，但 `main` 已包含 `v0.3.3` 标签之后尚未发布的
整数 Decimal 与诊断改进。升级前请阅读[版本与升级说明](docs/README.zh-CN.md#版本与升级)。

## 开发与许可

开发和验证见 [CONTRIBUTING.md](CONTRIBUTING.md)，架构约束见 [AGENTS.md](AGENTS.md)。
部署前阅读[安全参考](docs/security.zh-CN.md)，漏洞通过 [SECURITY.md](SECURITY.md) 私密报告。

采用 [AGPL-3.0-or-later](LICENSE) 许可。CLA 草案提供[英文](CLA.md)与[中文](CLA.zh-CN.md)，
状态与签署要求见 [CONTRIBUTING.md](CONTRIBUTING.md)。
