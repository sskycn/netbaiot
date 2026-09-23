# 设备协议：netbaiot-json-v1

## Single Device Ingress（单设备入口）

`device_ingress` 在同一个地址、相同端口号绑定一个 TCP listener 和一个 UDP socket。
开发示例为 `127.0.0.1:8080`，生产可配置 `0.0.0.0:443`。TCP 通过同一证书承载
HTTPS、标准 MQTT 3.1.1 TLS 和通用分帧 TLS TCP。TLS 握手后才识别应用协议，
不要求 ALPN、自定义前导或修改客户端 wire protocol。UDP 同端口继续使用 NBI1/HMAC，
只认证不加密，不涉及 DTLS/QUIC。

Management HTTP（`management_http`，通常为 `127.0.0.1:9090`）和可选的
`business_tcp` 继续独立监听与授权。设备 HTTP 不提供管理 API。
非 loopback TCP 入口必须配置 TLS；开发模式强制 loopback，允许本地明文测试。

配置中的 `device_http`、`mqtt`、`tcp`、`udp` 四个旧字段替换为 `device_ingress`。
旧字段将触发配置错误；迁移时须明确选择新地址并修改所有设备目的端口和防火墙规则，
不会静默选择旧配置中的某一个端口。

认证过程会选择 codec ID `netbaiot-json`、版本 `1`。HTTP/MQTT/TCP/UDP 载荷都由同一个同步 codec 解码。设备不能在载荷中自行声明可信身份；未知的信封字段会被拒绝。

```json
{"schema_version":1,"source_message_id":"boot-7:42","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}
```

`source_message_id` 为必填项（1–64 个符合命名空间要求的 ASCII 字符）。内容相同的重试应复用该 ID。`occurred_at` 是可选的 Unix 毫秒时间戳；`received_at` 和稳定的 `event_id` 由服务端分配。codec 保留有类型的数值、布尔和文本标量字段；任意 JSON 对象不是领域载荷。

其他 `kind` / `data` 组合：

```json
{"kind":"event","data":{"name":"boot","value":true}}
{"kind":"heartbeat","data":{"sequence":42}}
{"kind":"config_ack","data":{"revision":7,"status":"applied","error":null}}
{"kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000001","execution":"succeeded"}}
```

这些示例仅展示 kind/data 部分；实际请求还需包含 `schema_version` 和 `source_message_id`。命令执行状态可以是 running/succeeded/failed。需要时由业务系统按 `command_id` 关联并持久化命令/应用结果。

Codec 默认限制：输入/编码后字节数 64 KiB、每次输出一条消息、遥测字段 64 个、字段名/文本 256 字节、嵌套深度 8。结构成员数预检会在 serde 分配前限制内存；遥测字段名重复时会被拒绝。无效 UTF-8、未知字段、格式错误的 JSON 和超限结构都会失败。未来若支持多消息 codec，还需配套设计原子批量回执；当前入口每次只接受一条消息。

## HTTP

发送 `POST /v1/device/data`，并携带 `Authorization: Bearer <credential-id>:<key>`。成功接受时返回 202；错误映射为 400/401/403/409/413/429/503/504。Hyper 和适配器负责限制请求和请求头大小。任何 `Content-Encoding` 请求头都会导致 415；不支持请求解压缩。认证后的请求阶段许可会在设备、租户和节点层面限制慢速请求体。在当前版本中，HTTP/1 每条连接只处理一个请求；请求头/请求体/响应截止时间用于限制慢客户端。HTTP 设备没有离线命令队列。应将类型化的配置和命令结果分别 POST 到 `/v1/device/config/ack` 和 `/v1/device/commands/ack`。

## 通用 TCP

每个帧由 `u32` 大端序载荷长度和后续载荷组成。长度必须在 1..max_tcp_frame_size 范围内。首帧是认证握手：

```json
{"credential_id":"demo-device","secret":"<64-hex-character-key>"}
```

服务端返回分帧的 `{"authenticated":true}`。后续帧是 JSON 上行消息。服务端帧包含回执或通用 `DeviceCommand` JSON。命令包含 command_id、device、expires_at 和 `{name,arguments}` 载荷。执行 ACK 使用共享 codec。读取分片时会保留未收全的帧；EOF 会关闭连接并释放连接所属资源。厂商自有分帧格式可单独实现 `TcpFramer`。

## UDP v1

UDP 不维护会话，不发送回复，不支持命令下行或应用层分片。数据报最大为 1200 字节。字段采用网络字节序：

| 字段 | 字节数 |
|---|---:|
| magic `NBI1` | 4 |
| credential ID 长度 | 1 |
| credential ID | 长度，1–64 |
| credential version | 4 |
| boot ID | 16 |
| sequence | 8 |
| Unix 毫秒时间戳（有符号 i64） | 8 |
| payload 长度 | 2 |
| JSON v1 payload | 长度 |
| HMAC-SHA256 | 32 |

HMAC 覆盖此前所有字节，使用**解码后的 32 字节密钥**，而不是 ASCII 十六进制文本。credential version 必须与设备注册配置一致。时间戳偏差最多 30 秒。每台设备/每个 boot 使用一个 64 序号位图；它允许有限乱序，同时拒绝重复包和更旧的包。重放记录在 120 秒后过期（超过时间戳偏差的两倍）；只会淘汰已过期记录。设备/租户/全局容量已满时会拒绝新 boot。只有摄取成功后才提交重放状态，因此失败的摄取不会消耗序号。

若 UDP 发送结果丢失或不确定，客户端重试时应使用新序号。服务端不会响应任何数据报，包括未认证流量；因此不存在放大攻击或伪造应用回执的通道。需要回执时应使用 HTTP/MQTT/TCP。载荷经过认证但不加密。重放状态仅保存在进程内，重启后会重建；因此在时间戳有效期内重放的有效签名包仍可能再次被接受。存在该风险时，业务消费者应按稳定的应用标识去重。
