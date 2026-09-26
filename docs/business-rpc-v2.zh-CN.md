# Business RPC Stream V2

显式启用 V3 后，同一 listener 仍支持 V2。V2 multiplexed 以完整消息/帧为单位调度，不提供 V3 的独立应用流或 DATA 交错发送。参见 [Business RPC V3](business-rpc-v3.zh-CN.md)。

生产环境的 mTLS、同端口双角色连接、网络延迟与长时门禁证据见 [Business RPC V2 生产就绪门禁](business-rpc-v2-production-readiness.zh-CN.md)。

V2 在现有 `business_tcp` 监听地址上承载设备认证、UDP verifier 获取、认证失效控制、需确认事件及在线设备命令。每帧是四字节大端长度加 JSON；长度不含前缀。零长度、超限、畸形 JSON 和未知字段会被拒绝。`hello.version = 2` 与 V1 公共协议版本独立。准确帧结构和 DTO 见 `netbaiot-protocol::business_rpc`。

## 角色、身份和连接

`Hello` 申请 `events`、`auth_control`、`commands`、`application` 或 `multiplexed`；服务端验证业务身份后以 `Ready` 返回连接代际和协商限制。事件通过 `Subscribe` / `Subscribed` 和 `Event` / `EventAck`、`EventNack` 处理。事件窗口固定为 1。一个事件等待应用 ACK 时，同一条 `multiplexed` 连接仍可处理多个 `device.authenticate`、`device.resolve_verifier` 请求和心跳。认证 provider 向网关调用 `auth.sync`、`auth.invalidate`。服务端分别限制控制、命令、认证和事件输出队列的条数与字节数；每连接命令工作与响应队列各有 16 条、256 KiB 上限；优先级只影响尚未写出的帧，不能消除 TCP 本身的队头阻塞。生产环境若需隔离大事件字节流，可以在同一端口建立 `auth_control` 与 `events` 两条连接。

生产 V2 必须使用 mTLS。服务端先验证客户端证书，再把证书 SHA-256 指纹映射为显式配置的 `BusinessPrincipal`；单凭受信 CA 证书不会获得权限。SDK 验证服务端名称和 CA，并出示客户端证书。principal 约束角色、`primary` provider 或 `tcp-rpc` sink、方法、全局或租户范围及可选过期时间；不信任 Hello 自报身份。开发明文模式只可监听回环地址，必须单独设置 `development_token_env`，不能使用管理或 V1 token。没有 `business_rpc` 时仍按旧配置运行 V1；显式 `allow_v1: true` 时仅回环明文监听器可按首帧版本同时接纳 V1/V2。TLS V2 不回退 V1。V1/V2 共用稳定的 `tcp-rpc` sink，订阅 owner 仍唯一。

## 认证、失效与重连

`device_auth: "business_rpc"` 复用原有 AuthCache、正负 TTL、singleflight 和本地失效 epoch。MQTT/TCP 连接时认证一次，正常消息不逐条调用 provider。UDP 命中 verifier 缓存时在本地验 HMAC，只有 miss 才远程查询。一个 provider 通过代际租约注册；已有 provider 不被新连接无条件覆盖，旧连接退出不能注销新租约。认证响应须匹配 request ID、连接代际、方法、资源范围和最低业务修订。只有明确的设备拒绝可以负缓存；provider 不可用、超时、过载、越权或畸形响应均不会冒充设备拒绝。

每次 provider 建连须先做 `auth.sync(reset)`。服务端用与管理 HTTP 相同的完整失效边界清理缓存、在线设备连接和 MQTT 持久会话；客户端收到 sync 响应并以 `Ping` / `Pong` 确认后才进入 Serving。同步中新的远程认证 miss 失败关闭。业务权威先提交授权变更，再发送递增修订的 `auth.invalidate`，并等待服务端完成确认；重复修订可幂等处理，修订缺口或权威 incarnation 变化要求 reset 同步。业务修订不代替网关本地 AuthCache epoch。

`max_auth_control_offline_ms` 默认 30000，最大 86400000，可设为零。provider 断开后在途 RPC 立即失败，新的 miss 不等待重连；仅未过期的正缓存可在有限宽限内继续使用，且原 TTL 不延长。宽限到期通过完整失效路径撤销旧授权和会话。断线期间不承诺实时吊销。重连 reset 可能使设备重新认证，这是覆盖断线期间可能遗漏吊销的明确代价。认证缓存、凭据和 verifier 不进入 restart spool。

## 事件与持久性

应用完成必要业务事务后才调用 `BusinessDelivery::ack()`；写入 socket 或进入回调均非 ACK。NACK、断线、超时和未观察到的 ACK 交由 EventBus 已有有界重试和 required 责任处理。重试、计划重启 replay 保留 `event_id`，`delivery_id` 可以变化；业务消费者按 `event_id` 幂等。计划重启须 ACK 或按既有规则成功写入 restart spool；突然崩溃可能丢失有限的未入 spool 内存事件，不承诺 exactly-once 或崩溃不丢失。订阅 filter 变化不得在 EventAccepted 后静默丢弃 required 事件。

## 配置与 SDK

四项职责分开：`business_tcp` 是监听地址；`business_rpc` 定义 V2/TLS/身份和限制；`device_auth` 是 `static`、`http` 或 `business_rpc`；`event_delivery` 是 `http`、`business_rpc` 或开发审计。支持 RPC 认证+RPC 事件、RPC 认证+HTTP webhook、HTTP 认证+RPC 事件、静态认证+RPC 事件。仅在 HTTP 认证时设置 `auth_provider_url`，仅在 HTTP 事件出口时设置 `delivery_url`；冲突的新旧配置会被拒绝。回环开发环境可在 `configs/development.json` 基础上设置 `business_tcp`、`business_rpc: {"version":2,"tls":null,"development_token_env":"NETBAIOT_BUSINESS_RPC_TOKEN"}`，并显式选择认证来源与事件出口。生产需给 `business_rpc.tls` 配置服务端证书、私钥、client CA 和 `require_client_certificate: true`；`identities` 中配置证书指纹、principal、角色、provider/sink ID、方法、范围和可选过期时间。

默认值为连接 8、认证在途 128、事件在途 1、Hello 4 KiB、认证帧 16 KiB、事件帧 8 MiB、心跳 5 秒、离线宽限 30 秒。RPC 总截止时间沿用认证超时，事件 ACK 沿用 sink 超时；这些是配置限制，不代表实测容量。官方 `BusinessRpcClient::connect(config, handler)` 启动独立驱动；`wait_ready()` 等同步和订阅完成，用户不轮询事件也能执行认证 handler。`invalidate(revision, scope)` 等待失效完成确认，`BusinessDelivery` 默认需手动 ACK，`shutdown()` 终止任务。可运行内存演示：

```sh
cargo run --locked -p netbaiot-client --example business_rpc_v2 -- multiplexed
```

模式还有 `dual`、`auth_webhook`、`invalidate`。先设置 `NETBAIOT_BUSINESS_RPC_ADDRESS`、开发 token 或示例的 CA/客户端证书变量、`DEMO_DEVICE_SECRET`、`DEMO_VERIFIER_KEY_HEX`、`DEMO_CREDENTIAL_ID`、`DEMO_TENANT_ID`、`DEMO_PRODUCT_ID`、`DEMO_DEVICE_ID`。`auth_webhook` 需要另配 HTTP 事件出口。输入 `disable` / `enable` 会变更演示授权并调用失效。真实业务系统须以自己的授权状态替换内存演示。

## 错误、指标与范围

`device_rejected` 是明确拒绝设备；`unavailable`、`timeout`、`overloaded`、`forbidden`、`invalid_request`、`stale_revision`、`internal` 分别表示服务、时限、容量、权限、请求、同步或内部故障。响应保留 request ID 与方法。SDK 对认证/协议终止性错误停止重连，对暂时性连接故障做有界退避。`/api/v1/status` 报告 `business_auth_serving` 和 pending 数；`/api/v1/metrics` 提供连接、同步、失效、超时、过载、迟到响应、队列条数/字节、pending 资源、ACK 延迟等固定名称序列。设备 ID、凭据和 request ID 不作为指标标签。provider 未 Serving 时先检查 reset sync；超时和队列持续升高时检查应用处理、证书映射与角色权限。

管理 HTTP 命令接口继续服务运维和兼容用途。本版没有 provider 负载均衡、消费者组、多节点会话路由、gRPC、WebSocket、QUIC、新二进制编码或更宽事件窗口。

## 在线设备命令

`device.command.send` 的请求体是 `DeviceCommandSendRequest { command }`，响应体是 `DeviceCommandSendResponse { dispatch }`。`Queued` 只代表网关已把命令交给当前在线的 MQTT/TCP 会话，不代表设备收到或执行成功。MQTT PUBACK/PUBCOMP 与 socket 写入只是传输状态；设备执行结果仍通过现有 `CommandAck` 事件流返回，业务处理完成后再发 `EventAck`。离线、MQTT 未订阅匹配 `/down`、旧会话或网关 draining 返回 `unavailable`；容量不足返回 `overloaded`；输入非法返回 `invalid_request`；方法或租户越权返回 `forbidden`；同 ID 不同内容返回 `conflict`。租户授权先于设备会话查询。

业务方为重试保留同一个 `command_id`；`request_id` 只标识一次 RPC 尝试。管理 HTTP 与 Business RPC 共用 `CommandService` 的进程内幂等表。键为租户和命令 ID；SHA-256 指纹覆盖调用方提交的设备、命令内容、ID 与原始 `expires_at`（包括 `null`）。相同内容返回原 dispatch，不再次下发；并发重复共用一个进行中预留；失败请求不占用 ID。保留时长为 `limits.command_dedup_ttl_ms`（默认 300 秒），至多 `limits.command_dedup_max_entries` 条（默认 4,096）；满额时新 ID 返回 `overloaded`。进程重启后幂等记录消失，突然崩溃不承诺 exactly-once。网关不保存离线命令。`Cancel(request_id)` 只能阻止尚未被接受的请求，不会撤销已下发命令。SDK 不透明重试有副作用的命令，响应丢失时报告 `OutcomeUnknown`。

生产 mTLS 身份可配置 `role: "commands"`、无 provider/sink、空 `provide_methods` 及 `call_methods: ["device.command.send"]`；要同时接收事件则用 `role: "application"` 和 `sink_id: "tcp-rpc"`。`global`/`tenants` 限定可控制的租户。旧 `multiplexed` 仍只拥有认证控制和事件能力，不自动获得命令权限。仅回环开发模式可用 `business_rpc.development_role` 选择 `commands` 或 `application`；省略时保持原 `multiplexed` 默认值。
