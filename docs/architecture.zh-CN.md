# 架构

NetbaIoT 是一个无数据库、事件驱动的 IoT 网关。运行路径如下：

```text
HTTP / MQTT / TCP / UDP 设备
          |
          v
传输分帧与生命周期
          |
          v
认证 / ACL / 已绑定的 AuthContext
          |
          v
带版本的 DeviceCodec
          |
          v
DeviceEvent(event_id)
          |
          v
有界 EventBus 和必需 sink 原子接纳
          |
          +------ 需确认的 HTTP webhook
          |
          +------ 需确认的分帧 TCP/RPC 流
```

普通 MQTT/TCP 遥测只使用 socket 解析状态、连接绑定的可信认证上下文、共享 codec/配置快照和有界内存路由。它不会访问数据库、文件系统、远程认证服务或控制平面。HTTP 仅在缓存未命中时可能调用认证 provider。UDP 会验证每个签名数据报，并维护有界的本地重放窗口。

在 MQTT 传输内部，报文解析、协议状态、会话存储、订阅路由、retain 状态和 QoS 与 IoT 绑定相互隔离。持久 MQTT 会话使用已认证的 DeviceKey 和 ClientId 作为键，只保存有界协议状态，不持有已断开的 socket 或任意 `DeviceEvent` 历史。MQTT QoS 和 EventBus 投递语义是独立契约。

命令沿相反方向流动：从管理 HTTP 到 `CommandRouter`，然后直接进入本地活动 MQTT/TCP 会话的有界命令队列（按数量和字节数限制）。设备离线时返回不可用；不会保留离线命令。

设备 HTTP 和管理 HTTP 使用不同监听器和授权。运行时配置是带 revision 的不可变控制快照。认证和配置缓存分别设限，并在重启后重建。

计划关机过程为 `RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED`。在关闭监听器前先关闭接纳闸门。已接受的必需投递要么收到 ACK，要么通过文件 fsync、原子重命名和目录 fsync 写入有界本地重启 spool。同一恢复目录还保存单独的原子 MQTT 协议快照，用于 retain 和持久会话状态。突发崩溃可能丢失有界的、尚未写入 spool 的内存流量以及最近的 MQTT 修改。

工作区模块职责：

- `netbaiot-protocol`：公共 wire/domain 类型、稳定错误、路径和版本管理。
- `netbaiot-client`：业务 HTTP API 和需确认事件流的客户端管理。
- `netbaiot-device-sdk`：可选的标准 MQTT/设备 HTTP 便捷客户端。
- `netbaiot-core`：强类型领域/事件/命令类型和同步 codec trait。
- `netbaiot-codecs`：有界的厂商/设备协议 codec。
- `netbaiot-runtime`：缓存、资源接纳、事件总线、会话、生命周期、命令、指标和重启 spool。
- `netbaiot-transports`：HTTP、内嵌 MQTT、TCP 分帧、UDP、ACL 和连接生命周期管理。
- `netbaiot-server`：经过校验的组件组合、webhook/TCP 业务 sink、监听器、恢复和优雅关机。
- `netbaiot-cli`：只通过 `netbaiot-client` 实现的运维命令行工具。

公共客户端 crate 只依赖 `netbaiot-protocol` 和网络依赖，不依赖 runtime、transport、broker、session 或 server 实现 crate。

系统刻意不包含存储 crate、SQL migration、数据库连接池、持久 outbox、持久命令状态或运行时消息历史。
