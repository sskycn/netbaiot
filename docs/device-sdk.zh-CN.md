# Rust 设备 SDK

`netbaiot-device-sdk` 是可选组件。它直接使用 NetbaIoT 标准 MQTT 3.1.1 / MQTT 5.0 topic，不引入隧道或第二套认证机制。

Builder 必须配置 `mqtt_endpoint`；SDK 定位为 MQTT 便捷客户端。SDK 在调用方的 Tokio runtime 上运行，仅在内存中保留凭据，并对 `Debug` 输出进行脱敏；它会校验所有资源上限，并为 `mqtts` endpoint 启用 TLS 验证。MQTT 使用维护中的 `rumqttc` 0.25 客户端（Apache-2.0）、有界请求 channel、默认 MQTT 3.1.1（可通过 `protocol_version(MqttProtocolVersion::V5)` 选择 MQTT 5.0），以及对已进入有界应用 channel 的命令进行 broker 手动确认。MQTT 5 可设置 `session_expiry_interval` 和 `message_expiry_interval`。连接时，`connect().await` 会等到收到初始 CONNACK 和成功的命令 topic SUBACK，或达到配置的连接超时时间。SUBACK 授权失败不会被呈现为 Connected。因此，connect 成功后可以安全地立即发布。

支持的流程包括 QoS0/QoS1 事件发布、QoS1 遥测、接收命令、命令执行 ACK、通过 `publish(DeviceUplink, PublishQos)` 发布 heartbeat。规范 topic 使用现有的 `v1/t/.../up`、`down` 和 `down_ack` 命名空间。

当前唯一且默认的策略是 `OfflinePublishPolicy::Reject`。MQTT 断开时发布会返回 `Offline`；SDK 不会积累离线 RAM 队列。`publish` 成功表示消息已进入有界 MQTT 客户端，不代表服务器已经接纳或业务存储已完成。

NetbaIoT 不持有或持久化设备期望配置。业务系统负责 desired/reported 状态、版本历史、
重试、发布/回滚及离线协调。配置变更可作为普通 `DeviceCommand` 发往在线 MQTT/TCP 设备，
设备通过 `CommandAck` 返回执行结果；是否收敛由业务系统判断。命令仅支持在线投递，
UDP 无会话且没有下行。参阅[职责迁移](remove-device-config.md)。

丢弃最后一个客户端会取消唯一的 MQTT event-loop 任务。命令缓冲默认最多 16 条。命令格式错误或命令队列溢出会强制断开连接，且不确认 MQTT 投递，以便 broker 对持久会话重新投递。

连接丢失时会清除可观察到的连接状态，然后使用有界指数全抖动退避重试（默认 100 ms 至 5 s）。每次成功重连都会显式重新订阅命令 topic，即使 broker 已不再保留上一个持久会话。应用可通过 `mqtt_connected()` 和 `wait_until_connected()` 协调后续断线恢复期间的工作。

UDP 详见 [NBI1/NBA1 可靠上行](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。Rust 设备 SDK 仅使用 MQTT；Python UDP 示例演示有界原包重试和签名接纳回执。
