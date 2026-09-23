# 架构

## Single Device Ingress（单设备入口）

`device_ingress` 在同一个地址、相同端口号绑定一个 TCP listener 和一个 UDP socket。
开发示例为 `127.0.0.1:8080`，生产可配置 `0.0.0.0:443`。TCP 通过同一证书承载
标准 MQTT 3.1.1 TLS 和通用分帧 TLS TCP。TLS 握手后才识别应用协议，
不要求 ALPN、自定义前导或修改客户端 wire protocol。UDP 同端口继续使用 NBI1/HMAC，
只认证不加密，不涉及 DTLS/QUIC。

Management HTTP（`management_http`，通常为 `127.0.0.1:9090`）和可选的
`business_tcp` 继续独立监听与授权。管理 HTTP 是控制面协议，不参与设备协议分类。
非 loopback TCP 入口必须配置 TLS；开发模式强制 loopback，允许本地明文测试。

`device_ingress` 是唯一设备地址，旧分离监听字段会触发配置错误。443 只是部署选择，不代表 HTTPS。
设备入口收到 HTTP 字节后直接关闭，不返回 HTTP 响应。详见[迁移说明](remove-device-http.md)。

NetbaIoT 是一个无数据库、事件驱动的 IoT 网关。运行路径如下：

```text
MQTT / TCP / UDP 设备
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

共享 TCP listener 在启动所属连接任务前预留现有全局数量/字节和每 IP 配额，并执行有界 IP 限流。
分类后原 lease 转为协议计数，错误/EOF/取消只释放一次。MQTT 配额和设备/租户 admission 保持原语义。管理 HTTP 保留有界请求 slots 和全局/IP/字节 lease，
但不计入设备 `active_connections`；没有增加协议独占连接池，原有全局/IP 池仍共享，因此不承诺协议间绝对无饥饿。

探测缓冲固定 12 字节：1 字节包头、最多 4 字节 Remaining Length、2 字节协议名长度、4 字节名称及 1 字节版本。MQTT 复用 Remaining Length 解码，
校验 `00 04 MQTT` 和版本字节，版本 4 正常处理，其他版本交原解析器返回标准 CONNACK=1 后关闭。
通用 TCP 校验 1..max_tcp_frame_size 长度和 JSON 对象/空白起始。现有合法帧上限为 1 MiB，
长度首字节为零，与 MQTT 0x10 不冲突。失败后不切换解析器，已读前缀完整回放。

TLS、探测和首包共享从接纳时开始的 `connect_timeout_ms` 截止时间。
认证阶段仍有界；管理 HTTP 独立保留请求头、请求和写出超时。探测失败/超时使用固定指标名与 debug 日志。
Quiesce 关闭接纳，停止共享 TCP/UDP 并等待连接结束，然后进行 MQTT 恢复快照和必需投递 drain/spool，
最后才停止管理监听器。

普通 MQTT/TCP 遥测只使用 socket 解析状态、连接绑定的可信认证上下文、共享 codec/配置快照和有界内存路由。它不会访问数据库、文件系统、远程认证服务或控制平面。UDP 会验证每个签名数据报，并维护有界的本地重放窗口。

在 MQTT 传输内部，报文解析、协议状态、会话存储、订阅路由、retain 状态和 QoS 与 IoT 绑定相互隔离。持久 MQTT 会话使用已认证的 DeviceKey 和 ClientId 作为键，只保存有界协议状态，不持有已断开的 socket 或任意 `DeviceEvent` 历史。MQTT QoS 和 EventBus 投递语义是独立契约。

命令沿相反方向流动：从管理 HTTP 到 `CommandRouter`，然后直接进入本地活动 MQTT/TCP 会话的有界命令队列（按数量和字节数限制）。设备离线时返回不可用；不会保留离线命令。

管理 HTTP 使用独立监听器和管理授权。运行时配置是带 revision 的不可变控制快照。认证和配置缓存分别设限，并在重启后重建。

计划关机过程为 `RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED`。在关闭监听器前先关闭接纳闸门。已接受的必需投递要么收到 ACK，要么通过文件 fsync、原子重命名和目录 fsync 写入有界本地重启 spool。同一恢复目录还保存单独的原子 MQTT 协议快照，用于 retain 和持久会话状态。突发崩溃可能丢失有界的、尚未写入 spool 的内存流量以及最近的 MQTT 修改。

工作区模块职责：

- `netbaiot-protocol`：公共 wire/domain 类型、稳定错误、路径和版本管理。
- `netbaiot-client`：业务 HTTP API 和需确认事件流的客户端管理。
- `netbaiot-device-sdk`：可选的标准 MQTT 便捷客户端。
- `netbaiot-core`：强类型领域/事件/命令类型和同步 codec trait。
- `netbaiot-codecs`：有界的厂商/设备协议 codec。
- `netbaiot-runtime`：缓存、资源接纳、事件总线、会话、生命周期、命令、指标和重启 spool。
- `netbaiot-transports`：管理 HTTP、内嵌 MQTT、TCP 分帧、UDP、ACL 和连接生命周期管理。
- `netbaiot-server`：经过校验的组件组合、webhook/TCP 业务 sink、监听器、恢复和优雅关机。
- `netbaiot-cli`：只通过 `netbaiot-client` 实现的运维命令行工具。

公共客户端 crate 只依赖 `netbaiot-protocol` 和网络依赖，不依赖 runtime、transport、broker、session 或 server 实现 crate。

系统刻意不包含存储 crate、SQL migration、数据库连接池、持久 outbox、持久命令状态或运行时消息历史。

## UDP 接纳回执

```text
设备 -- NBI1 --> HMAC + 版本 + 时钟 + 有界 replay
  New:               ingest -> EventAccepted -> replay commit -> 签名 NBA1
  AcceptedDuplicate: 跳过 codec/presence/EventBus -------------> 签名 NBA1
设备 <-- NBA1 -- 非阻塞发送（失败绝不回滚接纳）
```

单一接收循环拥有 replay 与回执处理，无 ACK queue、每包任务、UDP session、命令 endpoint 或 ACK spool。Quiesce 等待活跃数据报接纳 guard，只有已接纳业务投递需要 drain/spool。认证失效阻止旧 signer 发回执，不做第二次 provider 查询。详见 [wire、重试和安全边界](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。
