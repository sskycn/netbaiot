# 内嵌 MQTT 3.1.1 / MQTT 5.0 broker

## Single Device Ingress（单设备入口）

`device_ingress` 在同一个地址、相同端口号绑定一个 TCP listener 和一个 UDP socket。
开发示例为 `127.0.0.1:8080`，生产可配置 `0.0.0.0:443`。TCP 通过同一证书承载
标准 MQTT 3.1.1 / MQTT 5.0 TLS 和通用分帧 TLS TCP。TLS 握手后才识别应用协议，
不要求 ALPN、自定义前导或修改客户端 wire protocol。UDP 同端口继续使用 NBI1/HMAC，
只认证不加密，不涉及 DTLS/QUIC。

Management HTTP（`management_http`，通常为 `127.0.0.1:9090`）和可选的
`business_tcp` 继续独立监听与授权。管理 HTTP 是控制面协议，不参与设备协议分类。
非 loopback TCP 入口必须配置 TLS；开发模式强制 loopback，允许本地明文测试。

`device_ingress` 是唯一设备地址，旧分离监听字段会触发配置错误。443 只是部署选择，不代表 HTTPS。
设备入口收到 HTTP 字节后直接关闭，不返回 HTTP 响应。详见[迁移说明](remove-device-http.md)。

NetbaIoT 直接实现 MQTT 3.1.1 和 MQTT 5.0，不依赖外部 broker 或数据库。子系统分层包括按版本隔离的增量 packet codec、连接状态机、认证后的会话挂接、有界会话存储、topic trie、retain 存储、QoS 引擎，最后才是 IoT 绑定/EventBus。

支持的控制报文包括 CONNECT/CONNACK、PUBLISH、PUBACK/PUBREC/PUBREL/PUBCOMP、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP 和 DISCONNECT。已实现 QoS0、QoS1 以及明确的入站/出站 QoS2 状态机。MQTT 5 支持会话过期、消息过期、Will Delay、接收上限、报文大小上限、订阅选项和有界 PUBLISH 属性。MQTT-SN、WebSocket、共享订阅、Topic Alias、Subscription Identifier、Enhanced Authentication、bridge 模式和 `$SYS` 服务不在当前范围内。详细能力矩阵见[英文 MQTT 文档](mqtt.md#compatibility-and-mqtt-5-profile)。

CONNECT 阶段通过有界 AuthCache 认证一次。得到的 `Arc<AuthenticatedDevice>` 会绑定到连接；后续普通 MQTT 报文不会再调用远程认证。MQTT ClientId 不作为可信身份。持久会话以 `(Authenticated DeviceKey, ClientId)` 为键，因此其他设备或租户不能仅凭复制 ClientId 继承或删除会话。空 ClientId 仅在 CleanSession=1 时接受，并会生成仅对当前连接有效的值。

CleanSession=1 会删除该认证身份的旧会话，并始终返回 Session Present=0。CleanSession=0 会在 socket 销毁后保留订阅、离线 QoS1/2 投递、入站 QoS2、出站 QoS1/2 和 packet ID 分配状态。MQTT 3.1.1 默认断开会话保留策略为 24 小时；MQTT 5 使用 Session Expiry Interval。会话有效期在新连接挂接和定期维护时检查；与此同时所有集合仍受硬性容量限制。

订阅使用 topic trie 支持精确 topic filter、`+` 和末尾的整层 `#`。根级通配符不会匹配以 `$` 开头的 topic。重复订阅会更新已有条目。订阅请求 QoS 和 publish QoS 通过 `min(publish_qos, subscription_qos)` 合并。授权只允许绑定设备命名空间内的有效 filter。发布仅限以下规范 topic：

```text
v1/t/{tenant}/p/{product}/d/{device}/up
v1/t/{tenant}/p/{product}/d/{device}/up_ack
v1/t/{tenant}/p/{product}/d/{device}/down
v1/t/{tenant}/p/{product}/d/{device}/down_ack
```

Retain 发布、替换、通配符重放和零载荷删除均已实现，并受数量/字节/消息数/租户上限约束。retain 存储有界，但通配符 retain 重放当前会扫描该有界存储；这是经过权衡的简化方案，已作为扩展性限制记录。

空载荷且带 retain 的 PUBLISH 在 QoS0/1/2（包括 MQTT 5）都是 broker 删除操作：仍需主题授权和 QoS 握手，但不解码为 JSON 设备事件。待完成的 QoS2 retain 替换会保守地预留完整条目的数量与字节；即使旧值先被其他操作删除，该责任仍保持到提交或事务释放。

每租户 retain 数量与字节使用派生索引维护，接纳时无需逐条扫描 retain 存储。

CONNECT 阶段会校验 Will Topic、二进制载荷、QoS、retain 标志、大小、语法和授权。EOF、网络/协议错误、keepalive 超时和连接替换都会恰好发布一次 Will。DISCONNECT 与计划内服务端关机的语义不同：MQTT DISCONNECT 会删除 Will，而计划内服务端关机会在恢复快照写入前发布 Will。此行为符合 MQTT-3.1.2-8；服务端有序关机不等同于客户端发送 MQTT DISCONNECT。已接受的 Will 还会在 CONNECT 时预留有界的 broker 投递责任。如果异常断开时因持久订阅者容量已满，无法原子路由 Will，则 Will 会留在有界待处理队列中，在计划重启时保留，并在 broker 容量变化后重试；不会出现部分路由。

普通 QoS0/QoS1 规范上行会先完成 retain 更新和有界 broker 路由，然后 IoT 绑定才跨过 `EventAccepted`；之后的 broker 故障不会把已接受的事件改成生产者可见的失败。IoT 接受前失败可能导致 MQTT 投递按通常的至少一次语义重放。入站 QoS2 会单独保存 `EventAccepted` 待路由阶段，包括计划重启期间的状态，因此可以完成 retain/订阅者责任而不重复发出业务事件。对于 retained QoS2 流程，会在 PUBREC 前预留 retain 容量，并在路由时原子释放。`EventAccepted` 表示每个必需 EventBus sink 都已预留数量/字节容量并完成入队；它不是数据库提交。MQTT QoS2 可避免同一已存 MQTT 流重复进入 IoT 绑定，但不承诺业务层恰好一次：EventBus 恢复采用至少一次投递，消费者仍须实现幂等。

SUBSCRIBE 会先针对会话、租户、全局、离线队列和活动 channel 容量完整预检 retain 重放，之后才插入订阅映射和 trie 节点。因此 SUBACK 失败不会留下能接收后续实时发布的隐藏订阅。

入站 QoS2 完成后若释放租户 inflight 容量，broker 会通过有界待发送索引推进订阅者副本。活动下行帧在等待和写入 socket 期间持有连接、租户、进程三级字节许可；retain 重放和重连恢复帧也受同一预算约束。

重连恢复帧若暂时等不到全局字节容量，会留在有界待发送索引中；后续 socket 写入释放容量或定期维护会重试未发送的 QoS 帧。

每个已解码 MQTT 报文在进入状态机分支前都经过协议处理限流，包括 retain 删除、QoS2 重复 PUBLISH 和 MQTT 5 校验失败路径。大 PUBLISH 按每 4096 字节增加计费单位。ACK、PINGREQ、DISCONNECT 使用独立的有界控制预算；只有创建新设备事件时才使用业务事件接纳预算。

持久 MQTT 会话的离线订阅投递与命令 API 相互独立。显式 MQTT 管理命令要求当前代次的在线连接已订阅匹配的 `/down` 主题；缺少订阅时在命令入队前返回不可用。旧代次连接不能向接管后的新连接发布命令。活动发送配额或 channel 满时拒绝命令，不会将其转为 MQTT 离线队列。报文写入、正面传输 ACK 和应用 CommandAck 是不同阶段。

命令发送指标只在 MQTT 报文写入成功后增加。普通订阅投递的 PUBACK 不计入命令收到数；MQTT 5 的负面 PUBACK/PUBCOMP 计为命令失败。取消订阅会阻止新命令，但不删除已开始的 QoS 握手。

慢速活动消费者使用有界 sender。队列满时可以舍弃 QoS0。对 QoS1/2，broker 会将所有匹配会话、租户/全局 inflight 限额、离线队列、会话字节数以及可选 retain 更新合并为一个路由计划并预检。只要任一持久目标无法接管其责任，就不会更改任何目标或 retain 值，也不会确认源发布。只有整个计划成功后才提交，因而多订阅者路由不会部分投递后静默漏掉某个订阅者。计划仅保存紧凑的逐目标决策：只进行一次有界全局记账，不会复制已存载荷，也不会针对每个匹配项重新扫描所有会话。

持久会话会保存授权来源信息（credential version、auth generation、permissions、codec 标识和版本，不含密钥），以及单调递增的 session incarnation。CleanSession=0 接管会保留 incarnation；CleanSession=1 会创建新的 incarnation。入站 QoS2 完成和路由必须匹配相同的 incarnation、packet identifier 和 operation token。授权变更后重连会重置旧会话并返回 Session Present=0。管理失效操作会在同一个有界控制操作中删除匹配的持久状态。

计划重启时会增量写入紧凑的 NBMQ v6 记录：带校验和的头部、每条有界记录的长度/校验和，以及包含权威记录数量、字节数和 SHA-256 摘要的全镜像校验尾部。v6 保存协议版本、会话/消息过期、订阅选项、发布属性、下行 QoS 首次传输状态及延迟 Will 状态。载荷字节保持二进制，不会复制完整快照或创建整个镜像的序列化缓冲区。仍可按各版本独立上限读取 NBMQ v1/v2/v3/v4/v5 镜像；所有新写入均使用 v6。缺少完整授权/codec 来源信息的旧会话不会暴露在订阅索引中，并会在连接挂接时安全重置。文件使用受限权限，并执行文件 fsync、原子重命名和目录 fsync。镜像不包含密码或 socket/TLS/task 状态。恢复 `(DeviceKey, ClientId)` 会话前必须重新认证。突发崩溃可能丢失上一次计划快照之后的修改；broker 不承诺崩溃持久性。

实现证据和恢复细节见英文版 [MQTT 3.1.1 一致性清单](mqtt-3.1.1-conformance.md)和 [MQTT 会话恢复说明](mqtt-session-recovery.md)。
