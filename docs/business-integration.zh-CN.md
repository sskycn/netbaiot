# 业务系统集成

## HTTP webhook

内置的需确认 webhook 会发送完整的、与传输无关的事件，并将 `event_id` 作为 `Idempotency-Key`。配置的 2xx 响应即为 sink ACK。系统会限制连接池、拒绝重定向、设置请求超时、限制响应 Content-Length 和流式响应体、限制并发和重试次数。可选 bearer token 从 `NETBAIOT_DELIVERY_TOKEN` 读取，并且不会被记录到日志。

## 分帧 TCP/RPC 流

可选的业务监听器提供长期运行、由应用 ACK 的数据流。每帧由四字节网络序长度和有界 JSON 组成。公共 v1 契约使用 `netbaiot-protocol` 定义的 `hello`、`subscribe`、`ready`、`event` 和 `ack` 版本化帧。客户端先通过 `hello` 认证，在 `subscribe` 中发送筛选条件，并等待服务端 `ready` 后再接收事件。每条投递都有区别于稳定 `event_id` 的 `delivery_id`，以及 `subscription_id` 和尝试次数。建议使用 `netbaiot-client` 处理自动重连、重新订阅、有界缓冲和显式应用 ACK。

客户端必须在应用处理完成后才返回匹配的 `ack`。socket 写入成功不等于 ACK。格式错误或不匹配的 ACK 会导致投递失败。当前实现只允许一个活动订阅者，并按顺序确认投递，以限制流控和不确定状态。

这是全局必需流模型。订阅者筛选条件决定连接是否符合接入条件，不会在事件接受后参与路由决策：每个已接受事件仍由唯一的 TCP sink 承担必需投递责任。若重连后的筛选条件不同，该连接就不能 ACK 之前不匹配的事件；在符合条件的订阅者显式 ACK 之前，该事件会产生可重试的投递失败。不会因为当前筛选条件变化而在 `EventAccepted` 之后静默丢弃事件。

Hello、subscribe、事件 ACK 读取和写入都有硬性截止时间和帧大小限制。格式错误的握手只会影响该连接，不会停止监听器。认证、版本和校验失败会返回结构化 v1 流错误，便于官方客户端区分终止性错误和暂时性故障。服务端会先安装有界订阅，再发送 `ready`，因此握手成功代表真正完成接纳，不存在 ready 之后的路由空窗。

此分帧 RPC 是当前提供的高吞吐流式通道。本版本尚未实现原生 gRPC 和 WebSocket 适配器。未来可将 WebSocket 用于 dashboard，但除非它增加应用级 `ACK event_id`，否则必须作为尽力而为的通道。

消费者必须按 `event_id` 实现幂等：sink 可能已处理事件，但在计划重启前丢失 ACK，导致相同 ID 被重放。

## Business RPC V2

双向 V2 协议、独立认证 provider、mTLS 映射、配置和 Rust SDK 见 [Business RPC Stream V2](business-rpc-v2.zh-CN.md)。没有 `business_rpc` 配置时默认保留 V1 行为。
