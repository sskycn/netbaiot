# 协议支持

设备载荷支持 JSON V1、CBOR V1、MessagePack V1、Protobuf V1，详见[wire 格式、认证选择与扩展](codecs.zh-CN.md)。

[English](protocol-support.md)

本页汇总当前 workspace 实现的协议 profile。MQTT 细节见
[MQTT profile](mqtt.zh-CN.md)，JSON 上行与 TCP/UDP wire 格式见
[设备协议](device-protocol.zh-CN.md)。

## 设备入口

| 协议 | 已支持行为 | 边界 |
| --- | --- | --- |
| MQTT 3.1.1 | QoS 0/1/2、CleanSession 0/1、持久会话、保留消息、Will、精确/`+`/`#` 订阅、计划重启恢复 | 内嵌 broker；不支持 WebSocket、共享订阅、MQTT-SN、bridge、`$SYS` 或集群 |
| MQTT 5.0 | QoS 0/1/2、Clean Start、会话/消息过期、Will Delay、Receive Maximum、Maximum Packet Size、No Local、Retain As Published、Retain Handling，以及受限元数据 | 不支持 Topic Alias、Subscription Identifier、共享订阅、Enhanced Authentication 和 WebSocket |
| 分帧 TCP | 4 字节大端长度 + 一条有界的认证 Codec 载荷；首个 JSON 帧认证连接；支持上行与在线命令 | TCP 是字节流；非 loopback 必须使用 TLS；没有 HTTP 设备入口 |
| 认证 UDP | NBI1 HMAC-SHA256 上行；检查 credential version、时间戳和重放；越过 `EventAccepted` 后返回 64 字节签名 NBA1 | 已认证但未加密；没有长期会话、命令下行或分片 |

MQTT 3.1.1 与 MQTT 5.0 使用同一个设备 TCP listener。跨协议 level 切换不会恢复
原会话。MQTT 5 User Properties 和其他元数据均有数量/字节限制；依赖具体属性前应
阅读详细 profile。

三种设备上行都使用已认证设备身份和配置的版本化 codec，最终归一为公开
`DeviceEvent`。传输协议不会泄漏为业务事件载荷的一部分。

## MQTT 5.0 属性矩阵

| MQTT 5 能力 | 状态 |
| --- | --- |
| Session Expiry Interval 与 Clean Start | 支持，并限制会话状态 |
| Message Expiry Interval | 支持；保存为 deadline，并清除过期消息 |
| Will Delay Interval | 支持，并限制待处理 Will 状态 |
| Receive Maximum 与 Maximum Packet Size | 双向支持且有界 |
| No Local、Retain As Published、Retain Handling | 支持的订阅选项 |
| Payload Format Indicator 与 Content Type | 作为有界传输元数据支持 |
| Response Topic、Correlation Data、User Properties | 作为有界传输元数据支持 |
| Topic Alias | 不支持 |
| Subscription Identifier | 不支持 |
| 共享订阅 filter | 不支持 |
| Enhanced Authentication | 不支持 |
| WebSocket 传输 | 不支持 |

不支持的能力不会被静默当作已生效。协议允许时，broker 使用 MQTT 5 reason code。
精确报文/属性行为和恢复兼容性见 [MQTT profile](mqtt.zh-CN.md)。

## 认证与传输保护

MQTT/TCP 在建连时认证，并把不可变的可信设备身份绑定到连接。普通 MQTT 报文和
TCP 帧不会调用远程认证服务。UDP 对每个数据报验证 HMAC，并应用有界时间戳/重放
策略。公网 TCP 流必须使用 TLS；HMAC 不会加密 UDP 载荷。

管理 HTTP 使用独立 listener 和独立授权。它不是设备协议，设备凭据不能授权管理操作。

## 业务出口

服务端组合有界业务 sink。已确认 HTTP webhook 以配置认可的 2xx 为 ACK；已确认的
分帧 TCP/RPC 投递要求应用发送匹配 `event_id` 的 ACK。socket write 不是业务确认。
尽力而为 sink 按各自有界溢出策略处理。详见
[投递语义](delivery-semantics.zh-CN.md)与[业务系统集成](business-integration-guide.md)。

## 当前业务与恢复格式

| 接口面 | 支持格式 | 明确拒绝的格式 |
| --- | --- | --- |
| Business RPC | 仅 V3 | V1、V2 与未知版本 |
| MQTT 重启恢复 | 仅 NBMQ v6 | NBMQ v1–v5 与未知版本 |
| EventBus 重启 spool | 仅 NBSP v3 | NBSP v1/v2 与未知版本 |
| 设备 MQTT | 3.1.1 与 5.0 | 见上面的实现 profile |
| 管理 HTTP | `/api/v1/...` | 与 RPC wire version 相互独立 |

本轮清理没有改变当前格式的 wire/file 字节。旧客户端必须升级；旧恢复文件必须在升级前
由合适旧版本完成或转换。当前 runtime 不包含转换器或 fallback。详见
[破坏性变更与升级步骤](migration/current-protocol-only.zh-CN.md)。
