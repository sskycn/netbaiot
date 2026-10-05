# NetbaIoT project descriptions

These descriptions are copy-ready starting points. Keep claims aligned with the
current protocol matrix and measured evidence.

## One sentence

NetbaIoT is a Rust IoT ingress gateway that accepts MQTT, framed TCP, and authenticated UDP, normalizes device traffic into bounded DeviceEvent delivery, and leaves durable business state to your backend.

## Short description

NetbaIoT is a database-free, memory-first IoT ingress gateway and real-time event router written in Rust. It accepts MQTT 3.1.1, a bounded MQTT 5.0 profile, framed TCP, and authenticated UDP, then normalizes uplinks into `DeviceEvent`s for business services. Required sink delivery has an explicit acceptance boundary, and runtime queues and caches have count and byte limits. NetbaIoT keeps durable business data, offline command intent, analytics, and workflows in your existing backend. It is a single-node gateway; planned-restart recovery does not make abrupt crashes durable.

## Long description

NetbaIoT is an IoT ingress gateway and real-time event router written in Rust. It
is designed to sit in front of an application's existing backend and remain
responsible for device connectivity, authentication, protocol decoding, event
normalization, routing, and online command delivery.

Devices can connect through the embedded MQTT 3.1.1 broker, a bounded MQTT 5.0
profile, generic length-prefixed TCP, or authenticated UDP. The device uplinks use
a configured versioned codec and converge on the public `DeviceEvent` model. MQTT
and TCP authenticate at connection setup. UDP verifies each signed datagram with
HMAC, checks timestamps and a bounded replay window, then returns a signed
acceptance receipt. UDP payloads are not encrypted.

The gateway does not require a database or external message broker on its normal
runtime path. The application owns durable telemetry and business records,
offline command queues, command history, domain idempotency, analytics, workflows,
and desired/reported device configuration. Commands from NetbaIoT are delivered
only to a live local MQTT/TCP session. Device execution results return through the
normal event path.

`EventAccepted` has a specific meaning: authentication, validation, routing,
count/byte admission, atomic reservation for required sinks, and enqueue have
completed. A QoS1 PUBACK or TCP/UDP acceptance receipt does not mean an application
database committed the event. Required sink acknowledgements are explicit, and
retries or planned-restart replay may deliver the same stable `event_id` again.
Consumers should deduplicate that ID with their business update.

Connections, caches, queues, event and sink work, commands, subscriptions, replay
state, and recovery files are bounded. A planned graceful shutdown drains required
work or commits pending work to a local recovery spool. Abrupt process, operating
system, or power failure can lose recent state that was only in memory; NetbaIoT
does not claim crash durability.

NetbaIoT is currently single-node. It is not a complete IoT cloud platform and
does not include dashboards, a time-series database, OTA management, clustered
sessions, MQTT over WebSocket, shared subscriptions, MQTT-SN, or broker bridge
mode. The workspace is pre-1.0. Read the protocol support, delivery, security, and
operations documents before selecting it for a deployment.

## 中文一句话

NetbaIoT 是一个使用 Rust 开发的 IoT 设备接入与实时事件路由网关，接收 MQTT、分帧 TCP 和认证 UDP，将上行数据归一为有界投递的 `DeviceEvent`，并由业务后端负责持久化状态。

## 中文短介绍

NetbaIoT 是一个无需运行时数据库、以内存为主的 Rust IoT 接入网关和实时事件路由器。它支持 MQTT 3.1.1、受限的 MQTT 5.0 profile、分帧 TCP 和经过认证的 UDP，并将设备上行数据转换为统一的 `DeviceEvent`。必需 Sink 的接纳边界明确，运行时队列和缓存有数量与字节上限。长期业务数据、离线命令意图、分析和工作流仍由现有业务后端负责。NetbaIoT 当前为单节点；计划重启恢复不代表能够抵御突然崩溃。
