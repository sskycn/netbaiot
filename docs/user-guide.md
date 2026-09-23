# NetbaIoT 完整用户指南

> 当前职责边界与验证见[设备配置职责移除报告](remove-device-config.md)。<br>
> Workspace 版本：`0.1.0`；Rust MSRV：`1.88`。

本指南面向第一次部署 NetbaIoT 的开发者、设备开发者和业务系统开发者。它以当前代码、测试和配置 schema 为准；若与更早的文档冲突，以当前代码和链接的迁移报告为准。

## NetbaIoT 是什么

NetbaIoT 是无数据库、以内存为主的 IoT 协议网关和实时事件路由器。它直接实现 MQTT 3.1.1 broker，同时接收分帧 TCP 和带 HMAC 的 UDP 设备流量，把它们转换为统一的 `DeviceEvent`，再投递到有明确容量上限的业务 sink。

它不是遥测仓库、工作流引擎、通用消息持久化系统或离线命令队列。长期业务数据、设备 desired/reported 配置及版本历史、离线协调、分析、工作流、离线命令和幂等记录由业务系统负责。唯一运行时持久化是计划重启所用的本地 recovery spool。

```text
Device
  │  MQTT / TCP / UDP
  ▼
NetbaIoT
  ├─ Authenticate
  ├─ Decode (versioned DeviceCodec)
  ├─ Normalize
  ├─ DeviceEvent
  ├─ bounded EventBus / Router
  └─ required or best-effort Business Sink
                                   │
                                   ▼
                            Business System

Business System ── Command ──> NetbaIoT ──> Live MQTT/TCP Session
```

所有传输遵守同一条运行路径：`CONNECT -> AUTHENTICATE -> DECODE -> NORMALIZE -> ROUTE -> SEND`。MQTT/TCP 只在连接时认证一次，之后使用绑定且不可变的认证上下文；UDP 每个包验证 HMAC，但其 verifier 可缓存。

## 从哪里开始

1. [10 分钟端到端教程](getting-started.md)：编译、启动、收到首个事件、下发首个命令。
2. [设备接入指南](device-integration-guide.md)：认证、codec、MQTT QoS/持久会话/retain/Will、TCP/UDP、设备 SDK。
3. [业务系统集成指南](business-integration-guide.md)：DeviceEvent、webhook、confirmed TCP、Rust 客户端、CLI、命令和错误处理。
4. [运维与生产部署](operations-guide.md)：配置、资源上限、TLS、管理 API、监控、重启、systemd、性能与压测。
5. [故障排查](troubleshooting.md)：按症状定位认证、ACK、会话、sink、spool 和 TLS 问题。

## 三个必须先理解的边界

| 边界 | 含义 | 不代表什么 |
|---|---|---|
| TransportAccepted | 传输层报文合法并进入处理；并非所有入口都单独暴露此状态 | 未必已选择路由或预留 sink |
| EventAccepted | 认证、codec、路由、数量/字节准入完成，所有 required sink 容量已原子预留且入队 | 不代表业务数据库已处理或持久化 |
| ConsumerAccepted | required webhook 返回配置认可的 2xx，或 confirmed TCP 客户端发回完整匹配的 ACK | 不代表全系统 exactly-once |

TCP/NBA1 回执、MQTT QoS1 的 PUBACK、QoS2 的完成握手都围绕 `EventAccepted`，而不是业务持久化。业务投递是 **at-least-once**：重试、ACK 丢失和计划重启重放都可能带来重复；消费者必须按稳定的 `event_id` 幂等。

```text
if event_id already processed:
    ACK
else:
    transaction:
        update business state
        record event_id
    ACK
```

`event_id` 由 NetbaIoT 创建，重试和 restart replay 保持不变；当前实现使用 UUID，但公共契约应只依赖“稳定且唯一的 ID”，不要依赖具体 UUID 版本。`source_message_id` 由设备/应用提供，用于关联设备侧重试或业务消息；它不等于 `event_id`。

## 当前组件

- binaries：`netbaiot-server`、`netbaiot`（crate 为 `netbaiot-cli`）、`netbaiot-loadgen`。
- 公共协议：`netbaiot-protocol`。
- 业务客户端：`netbaiot-client`。
- 可选设备 SDK：`netbaiot-device-sdk`。
- 当前 codec：`netbaiot-json` version `1`。
- 业务 sink：required HTTP webhook，或单个 required confirmed TCP/RPC stream；开发配置可使用即时 ACK 的 audit sink。
- MQTT：3.1.1 QoS0/1/2、CleanSession 0/1、persistent session、exact/`+`/`#`、retain、Will、计划重启恢复。

## Reference 文档

教程讲“怎么用”，详细契约仍以 reference 为准：[架构](architecture.zh-CN.md)、[HTTP API](http-api.zh-CN.md)、[设备协议](device-protocol.zh-CN.md)、[MQTT profile](mqtt.zh-CN.md)、[投递语义](delivery-semantics.zh-CN.md)、[公共协议](public-protocol.md)、[资源预算](resource-budgets.md)、[恢复格式](restart-spool.md)、[性能基线](performance-baseline.md)。
