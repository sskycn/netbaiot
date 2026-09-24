# 设备协议：netbaiot-json-v1

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

认证过程会选择 codec ID `netbaiot-json`、版本 `1`。MQTT/TCP/UDP 载荷都由同一个同步 codec 解码。设备不能在载荷中自行声明可信身份；未知的信封字段会被拒绝。

```json
{"schema_version":1,"source_message_id":"boot-7:42","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}
```

`source_message_id` 为必填项（1–64 个符合命名空间要求的 ASCII 字符）。内容相同的重试应复用该 ID。`occurred_at` 是可选的 Unix 毫秒时间戳；`received_at` 和稳定的 `event_id` 由服务端分配。codec 保留有类型的数值、布尔和文本标量字段；任意 JSON 对象不是领域载荷。

其他 `kind` / `data` 组合：

```json
{"kind":"event","data":{"name":"boot","value":true}}
{"kind":"heartbeat","data":{"sequence":42}}
{"kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000001","execution":"succeeded"}}
```

这些示例仅展示 kind/data 部分；实际请求还需包含 `schema_version` 和 `source_message_id`。命令执行状态可以是 running/succeeded/failed。需要时由业务系统按 `command_id` 关联并持久化命令/应用结果。

Codec 默认限制：输入/编码后字节数 64 KiB、每次输出一条消息、遥测字段 64 个、字段名/文本 256 字节、嵌套深度 8。结构成员数预检会在 serde 分配前限制内存；遥测字段名重复时会被拒绝。无效 UTF-8、未知字段、格式错误的 JSON 和超限结构都会失败。未来若支持多消息 codec，还需配套设计原子批量回执；当前入口每次只接受一条消息。

## 通用 TCP

每个帧由 `u32` 大端序载荷长度和后续载荷组成。长度必须在 1..max_tcp_frame_size 范围内。首帧是认证握手：

```json
{"credential_id":"demo-device","secret":"<64-hex-character-key>"}
```

服务端返回分帧的 `{"authenticated":true}`。后续帧是 JSON 上行消息。服务端帧包含回执或通用 `DeviceCommand` JSON。命令包含 command_id、device、expires_at 和 `{name,arguments}` 载荷。执行 ACK 使用共享 codec。读取分片时会保留未收全的帧；EOF 会关闭连接并释放连接所属资源。厂商自有分帧格式可单独实现 `TcpFramer`。

## UDP v1.1 签名可靠上行

NBI1（设备 → 网关）请求格式保持不变。不建立 session、endpoint registry，不支持命令下行、加密或应用层分片。数据报最大 1200 字节；整数采用网络字节序：

| 字段 | 字节数 |
|---|---:|
| magic `NBI1` | 4 |
| credential ID 长度 | 1 |
| credential ID | 1–64 |
| credential version | 4 |
| boot ID | 16 |
| sequence | 8 |
| Unix 毫秒时间戳（有符号 i64） | 8 |
| payload 长度 | 2 |
| JSON v1 payload | length |
| HMAC-SHA256 | 32 |

HMAC 覆盖此前所有字节，密钥是**解码后的 32 字节 credential key**，不是 hex ASCII。每个包（包括重复包）都必须通过 HMAC、权限、credential version 和时间戳检查；默认时钟偏差为 ±30 秒。

每个 `(DeviceKey, credential_version, boot_id)` 保留有界的 64 序号位图。新序号进入 codec/Ingress/EventBus，只有 **EventAccepted 后**才提交 replay。已接受的重复序号直接重新 ACK，不解码、不生成 event_id、不更新 presence、不再次发布事件。滑出 64 槽窗口的序号静默丢弃。记录默认 120 秒过期（严格大于两倍时钟偏差），仅删除过期记录；重复 ACK 不刷新过期时间。设备/租户/全局容量限制也计算不同凭据版本，满载时拒绝新记录。摄取失败不消耗序号。

## UDP acknowledgement: NBA1

NBA1（网关 → 设备）是固定 **64 字节签名接纳回执**：

| 偏移 | 字节数 | 字段 |
|---:|---:|---|
| 0 | 4 | magic `NBA1` |
| 4 | 4 | credential_version，u32 大端 |
| 8 | 16 | boot_id |
| 24 | 8 | sequence，u64 大端 |
| 32 | 32 | 对前 32 字节的 HMAC-SHA256 |

HMAC 使用与 NBI1 相同的已解码 32 字节密钥。NBA1 仅表示 **EventAccepted**，与 MQTT QoS1 PUBACK、通用 TCP acceptance receipt 处于同一接纳层级；不表示 required sink 最终 ACK、数据库提交、业务处理完成或设备命令执行。codec 的 `CommandAck` 是另一类应用事件，NBA1 仅确认该事件被接纳。

设备必须依次检查：长度恰好 64、magic 为 NBA1、预期 credential version、当前 boot ID、待确认 sequence，以及常量时间 HMAC 验证。不能只信任来源 IP 或序号。NBA1 不携带 status 或 event_id。

### 重试与消息身份

在认证设备范围内，`(credential_version, boot_id, sequence)` 表示同一条不可变消息。没有收到合法 NBA1 时，在有效窗口内**重发完全相同的 NBI1 数据报**，包括相同 credential version、boot ID、sequence、timestamp、payload 和 HMAC。不要刷新时间戳，也不要为 ACK 重试换新序号。复用已接纳身份表达不同内容属于协议违规；服务器采用 first accepted message wins，不复制 payload 或保存每序号 event_id。

使用有界指数退避和少量 jitter，例如 100、200、400、800、1600 ms。考虑实际时钟偏差，在原时间戳失效前结束重试，也要避免新流量将待确认序号推出 64 槽窗口。超过任一边界仍未确认，状态是 **uncertain**，不能断言失败。

Replay 只存在内存中。这一保证限于同一 runtime 实例、同一有效 replay 窗口；不提供跨意外重启 exactly-once。计划重启也重建 replay。必须保留稳定的 `source_message_id` 并由业务消费者幂等处理；EventBus replay 保持 event_id，但重启后重新摄取 UDP 可能生成新的 event_id。

### 失败、资源和安全边界

格式错误、未认证、未知/过期凭据、错误时钟、过旧 replay、codec/权限/admission/EventBus 失败及 draining 全部**静默丢弃**，无 NACK。接纳后先提交 replay，再非阻塞 `try_send_to`；发送失败只计数并丢弃 ACK，不撤销接纳，重复包可以再次获得回执。凭据失效会阻止在途旧 signer 发送 ACK。服务端没有 ACK queue、重传任务、ACK drain 或 ACK spool；UDP 不注册 session。

NBA1 固定 64 字节，最小结构有效 NBI1 为 76 字节，载荷字节放大比最多 **64/76 ≈ 0.842**。未认证流量绝不回复。捕获的有效签名包仍可在有效时间窗内被伪造来源地址重放，造成有限 authenticated reflection；固定小回包及原有来源 IP/进程限速阻止字节放大。重复 ACK 同样经过限速。载荷仍未加密。

`udp_datagrams`、`udp_accepted`、`udp_accepted_duplicates`、`udp_acks_sent`、`udp_ack_send_failures` 分别统计入包、新接纳数据报、已接受重复包、发送成功、发送失败或被抑制回执。socket 发送成功不等于设备收到。指标不新增设备 ID 等高基数标签。

当前 0.x 版本的破坏性清理已移除 `config_ack`，旧载荷会被拒绝，不保留兼容别名。
信封结构未变，继续使用 `schema_version=1`。应用配置操作通过普通 MQTT/TCP 命令与
`command_ack` 表达。详见[迁移说明](remove-device-config.md)。
