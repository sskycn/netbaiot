# Rust 设备 SDK

`netbaiot-device-sdk` 是可选组件。它直接使用 NetbaIoT 标准 MQTT 3.1.1 / MQTT 5.0 topic，不引入隧道或第二套认证机制。

Builder 必须配置 `mqtt_endpoint`；SDK 定位为 MQTT 便捷客户端。SDK 在调用方的 Tokio runtime 上运行，仅在内存中保留凭据，并对 `Debug` 输出进行脱敏；它会校验所有资源上限，并为 `mqtts` endpoint 启用 TLS 验证。MQTT 已使用仓库内 `netbaiot-mqtt-wire` 和独立 Device Profile 状态机替换 `rumqttc`；默认 MQTT 3.1.1（可通过 `protocol_version(MqttProtocolVersion::V5)` 选择 MQTT 5.0），以及对已进入有界应用 channel 的命令进行 broker 手动确认。MQTT 5 可设置 `session_expiry_interval` 和 `message_expiry_interval`。连接时，`connect().await` 会等到收到初始 CONNACK 和成功的命令 topic SUBACK，或达到配置的连接超时时间。SUBACK 授权失败不会被呈现为 Connected。因此，connect 成功后可以安全地立即发布。

支持的流程包括 QoS0/QoS1 事件发布、QoS1 遥测、接收命令、命令执行 ACK、通过 `publish(DeviceUplink, PublishQos)` 发布 heartbeat。规范 topic 使用现有的 `v1/t/.../up`、`down` 和 `down_ack` 命名空间。

当前唯一且默认的策略是 `OfflinePublishPolicy::Reject`。MQTT 断开后新提交的发布会返回 `Offline`；断线前已接纳的工作可在内存中有界保留，用于会话恢复。`publish` 成功表示消息已进入有界 MQTT 客户端，不代表服务器已经接纳或业务存储已完成。

NetbaIoT 不持有或持久化设备期望配置。业务系统负责 desired/reported 状态、版本历史、
重试、发布/回滚及离线协调。配置变更可作为普通 `DeviceCommand` 发往在线 MQTT/TCP 设备，
设备通过 `CommandAck` 返回执行结果；是否收敛由业务系统判断。命令仅支持在线投递，
UDP 无会话且没有下行。参阅[职责迁移](remove-device-config.md)。

丢弃最后一个客户端会取消唯一的 MQTT event-loop 任务。命令缓冲默认最多 16 条。命令格式错误或命令队列溢出会强制断开连接，且不确认 MQTT 投递。持久会话中的 QoS1 命令可由 broker 重新投递；QoS0 没有这个保证。

连接丢失时会清除可观察到的连接状态，然后使用有界指数全抖动退避重试（默认 100 ms 至 5 s）。每次成功重连都会显式重新订阅命令 topic，即使 broker 已不再保留上一个持久会话。应用可通过 `mqtt_connected()` 和 `wait_until_connected()` 协调后续断线恢复期间的工作。

UDP 详见 [NBI1/NBA1 可靠上行](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。Rust 设备 SDK 仅使用 MQTT；Python UDP 示例演示有界原包重试和签名接纳回执。

## Profile、迁移和资源上限

| 能力 | MQTT 3.1.1 | MQTT 5.0 |
| --- | --- | --- |
| TCP、验证证书的 TLS、现有用户名/密码 | 支持 | 支持 |
| 固定 up/down/down_ack topic、QoS0/1 上行、QoS1 命令订阅 | 支持 | 支持 |
| 持久会话恢复 | CleanSession=0 | Clean Start=0，默认 Session Expiry 3600 秒 |
| 流控与过期协商 | 不适用 | Receive Maximum、Maximum Packet Size、Server Keep Alive、Message Expiry |

SDK 不开放 QoS2、任意 topic、保留消息发布、LWT 配置、Topic Alias、WebSocket、
增强认证或离线磁盘队列；这些范围限制不影响网关 broker 对标准 MQTT 客户端的支持。
原有 `DeviceClient`、Builder、凭据、错误、`PublishQos`、协议版本、离线与重连策略，
以及 `publish`、`publish_telemetry`、`commands`、`ack_command`、
`mqtt_connected`、`wait_until_connected`、`metrics`、`shutdown` 仍可调用。
新增 `publish_receipts()`、`shutdown_with_timeout()`、`mqtt_ca_pem()` 和显式条数/字节上限配置。
`DeviceSdkError::SessionStateMismatch` 是新增公开枚举分支；下游对
`DeviceSdkError` 的穷尽匹配需增加分支。一般旧会话冲突由 SDK 自动修复，只有 broker
在清理会话时仍返回矛盾状态，应用才通常看到此错误。

`publish` 成功仍仅表示本地有界接纳。`publish_receipts()` 的有界广播会报告
`Written`、`Puback`、`Rejected(reason)`、`SessionLost`、`Expired`、`Uncertain`；
需要结果时应在发布前订阅，慢接收方会收到广播滞后错误。QoS0 `Written` 是 socket
写入，QoS1 `Puback` 是 MQTT broker 确认。仅对于 NetbaIoT broker，PUBACK 代表
EventAccepted，绝非业务处理或命令执行。应用执行命令后另行调用 `ack_command`，经
`down_ack` 上报。SDK 先校验固定 topic 与完整 DeviceKey、成功放入有界应用队列，
才发送下行 MQTT PUBACK。设备进程在 PUBACK 后崩溃，仍可能丢失未执行的内存命令。

默认 payload 上限 65,536 字节、完整 MQTT packet 上限 131,072 字节；发送队列、
QoS1 在途表和命令队列各 16 条，命令字节预算 1 MiB。握手总截止时间 10 秒、
KeepAlive 15 秒、重连全抖动 100 毫秒至 5 秒。Builder 可设置
`max_payload_bytes`、`max_packet_bytes`、`mqtt_queue_items`、
`qos1_inflight_items`、`command_buffer_items`、`command_buffer_bytes`、
`mqtt_connect_timeout`、`reconnect_policy`。没有新增离线发布队列。
`shutdown_with_timeout(timeout)` 停止新接纳，在超时时间内等待已接纳工作，发送
DISCONNECT 并回收驱动；同步 `shutdown()` 请求立即关闭。

同一进程的网络恢复保留可恢复的 QoS1 Packet Identifier 与 payload。MQTT 5 的
Session Present=0 会以 `SessionLost` 报告旧在途工作；Session Present=1 使用原 ID、
DUP 和递减的 Message Expiry 重发。新进程若看到 broker 旧会话但本地无状态，会以
Clean Start 重建一致状态。MQTT 3.1.1 遇到本地无对应状态的旧 broker 会话时，先用临时
CleanSession=1 连接清理，再建立新的 CleanSession=0 持久会话；同一进程内重连保留
原有 QoS1 重发状态。
SDK 不把客户端会话状态持久化到磁盘，设备进程重启不是完整会话恢复。

生产凭据应使用 `mqtts://host:port`。SDK 验证证书链与主机/IP 名称，
`mqtt_ca_pem(path)` 可增加私有 CA；嵌入凭据、路径、查询或片段的 endpoint 被拒绝。
[可运行示例](../crates/netbaiot-device-sdk/examples/device_mqtt.rs)以
`NETBAIOT_MQTT_VERSION=5` 选择 MQTT 5，以 `NETBAIOT_MQTT_CA_PEM` 配置私有 CA。
设置 `NETBAIOT_WAIT_COMMAND=1` 可等待一条命令；示例只执行 `example_noop`，并用
`ack_command` 回报结果。`NETBAIOT_RECONNECT_CHECK=1` 用于 broker 重启验证。
[Mosquitto 测试](../tests/run_device_profile_mosquitto.py)覆盖双版本 TCP/TLS、持久重连，以及未受信、主机名不符和过期证书拒绝。
[验证记录](device-profile-validation.md)包含本机 RSS、线程、QoS1 延迟与重连测量及复现命令。
