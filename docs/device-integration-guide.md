# 设备接入指南

本章使用 [`configs/tutorial.json`](../configs/tutorial.json) 中的 `demo-device`。除特别注明外，先按[入门教程](getting-started.md)启动 webhook 和 server，并设置：

```bash
export DEVICE_SECRET=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
export UP_TOPIC=v1/t/demo/p/sensor/d/device-1/up
export DOWN_TOPIC=v1/t/demo/p/sensor/d/device-1/down
```

## 身份、认证和 codec

权威设备身份是 `DeviceKey = (TenantId, ProductId, DeviceId)`。设备提交 `credential_id` 和 secret，认证结果绑定：

- `credential_version` 与 `auth_generation`：凭据/授权世代；变更会使旧会话失效。
- `permissions.publish`、`permissions.commands`：上报与命令能力。
- `codec_id=netbaiot-json`、`codec_version=1`：wire payload 解释方式。

设备不能通过 topic 或 payload 伪造身份。MQTT/TCP 在连接时认证一次；正常 PUBLISH/frame 不调用远程 auth provider。认证缓存独立并限制正/负条目的数量、字节和 TTL。未知或过期的 cache miss 遇到 provider 故障会 fail closed；已绑定会话和未过期正缓存可继续。

演示 secret 是 32 字节密钥的 64 个十六进制字符，只能用于 loopback 教程。生产应使用独立随机凭据、TLS、受保护的 provider 和显式 rotation/invalidation；不要把 secret 放入日志、metric label 或命令行历史。

### netbaiot-json-v1

四种设备传输共用同一个 codec。合法 envelope：

```json
{
  "schema_version": 1,
  "source_message_id": "boot-7:42",
  "occurred_at": 1789980000000,
  "kind": "telemetry",
  "data": {"temperature": 25.3, "door_open": false, "mode": "eco"}
}
```

`occurred_at` 可省略，单位为 Unix 毫秒且不能为负数。`source_message_id` 必填、1–64 个安全 ASCII 字符；同一设备消息的应用级重试应复用它。当前支持：

```json
{"schema_version":1,"source_message_id":"m:1","kind":"event","data":{"name":"boot","value":true}}
{"schema_version":1,"source_message_id":"m:2","kind":"heartbeat","data":{"sequence":42}}
{"schema_version":1,"source_message_id":"m:4","kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000123","execution":"succeeded"}}
```

Telemetry 必须是非空 scalar map，不支持数组/任意嵌套对象。默认上限为输入/输出 64 KiB、64 个字段、字段名/文本 256 字节、JSON 深度 8；未知字段、重复字段、无效 UTF-8、非有限数或错误 schema version 会被拒绝。

要增加厂商 codec，应在 `netbaiot-codecs` 实现同步、可替换的 `DeviceCodec`，并由控制面绑定版本；传输 framing 不能混入 codec。

## MQTT 3.1.1

NetbaIoT 直接实现 broker。所有示例显式使用 `-V mqttv311`，避免客户端默认版本变化。

### Topic 与 ACL

```text
v1/t/demo/p/sensor/d/device-1/up        设备上行
v1/t/demo/p/sensor/d/device-1/up_ack    设备命名空间内的 broker topic
v1/t/demo/p/sensor/d/device-1/down      管理命令下行
v1/t/demo/p/sensor/d/device-1/down_ack  设备命令执行 ACK 上行
```

设备只能向自己的 `up` 发布；有 commands 权限时也可向自己的 `down_ack` 发布。订阅可以使用设备根之下的 exact、`+` 和末级 `#`，例如 `$DOWN_TOPIC`、`v1/t/demo/p/sensor/d/device-1/+`、`v1/t/demo/p/sensor/d/device-1/#`。以下会收到 SUBACK `0x80`：

```text
v1/t/demo/p/sensor/d/other-device/#
v1/t/+/p/sensor/d/device-1/#
#
```

MQTT 标准规定根级 `#`/`+` 不匹配 `$` 开头 topic；broker matcher遵守该规则，但设备 ACL 本身已把订阅限制在 `v1/.../d/{device}/`。

### QoS0、QoS1、QoS2

- QoS0（at most once）：无 PUBACK；适合可丢的高频数据。过载时可被有界策略舍弃，命令/required 业务数据通常不应选它。
- QoS1（at least once）：broker 只有在 broker 路由和 `EventAccepted` 完成后发 PUBACK。失败/断线时客户端重发可能产生业务重复。
- QoS2：执行 `PUBLISH -> PUBREC -> PUBREL -> PUBCOMP`。同一 MQTT QoS2 operation/DUP 重传不会仅因 DUP 再创建 `DeviceEvent`；但 EventBus 的 sink retry/restart replay 仍是 at-least-once，不是业务 exactly-once。

可分别运行：

```bash
for qos in 0 1 2; do
  mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
    -u demo-device -P "$DEVICE_SECRET" -i "qos-$qos" \
    -t "$UP_TOPIC" -q "$qos" \
    -m "{\"schema_version\":1,\"source_message_id\":\"qos:$qos\",\"kind\":\"heartbeat\",\"data\":{\"sequence\":$qos}}"
done
```

你应该看到业务 webhook 收到三条事件。QoS1/2 命令正常退出只证明 `EventAccepted`，不是业务数据库提交。

### 持久会话与离线 QoS

Mosquitto CLI 的 `-c` 表示 MQTT 3.1.1 `CleanSession=0`，且必须使用稳定、非空 ClientId。

1. 建立持久订阅并在 SUBACK 后断开：

   ```bash
   mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
     -u demo-device -P "$DEVICE_SECRET" -i persistent-demo -c \
     -t "$UP_TOPIC" -q 1 -E
   ```

2. 订阅者离线时发布 QoS1：

   ```bash
   mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
     -u demo-device -P "$DEVICE_SECRET" -i offline-publisher \
     -t "$UP_TOPIC" -q 1 \
     -m '{"schema_version":1,"source_message_id":"offline:1","kind":"heartbeat","data":{"sequence":10}}'
   ```

3. 用同一 ClientId 和 `-c` 恢复，读取一条后退出：

   ```bash
   mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
     -u demo-device -P "$DEVICE_SECRET" -i persistent-demo -c \
     -t "$UP_TOPIC" -q 1 -C 1 -v
   ```

CONNACK 的 Session Present 应为 1（加 `-d` 可查看）。离线队列只保存 QoS1/2，并受 session/tenant/global 数量与字节上限约束；不是无限历史。默认断开 session idle policy 是 24 小时。相同 `(DeviceKey, ClientId)` 才能恢复；credential version、auth generation、permissions 或 codec provenance 改变会重置旧状态。

### 持久 UNSUBSCRIBE（兼容 Mosquitto 2.0.x/2.1.x）

Mosquitto 2.0.x 拒绝只有 `-U` 而没有任何 `-t` 的调用，2.1.x 则接受。可移植写法提供一个**不同的、合法且不匹配目标的** `-t`：

```bash
mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P "$DEVICE_SECRET" -i persistent-demo -c -d -E \
  -t "$DOWN_TOPIC" -q 1 -U "$UP_TOPIC"
```

以 debug 输出出现 UNSUBACK 为完成边界。之后再离线发布到 `$UP_TOPIC`，恢复时不要重新 `-t "$UP_TOPIC"`，否则那是在创建新订阅，无法验证旧订阅已删除。

### Retained message

Retain 使用普通、合法的上行 payload：

```bash
# 创建
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P "$DEVICE_SECRET" \
  -t "$UP_TOPIC" -q 1 -r \
  -m '{"schema_version":1,"source_message_id":"retain:1","kind":"heartbeat","data":{"sequence":1}}'

# 新订阅者立即得到 retained replay（其 PUBLISH retain flag 为 1）
mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P "$DEVICE_SECRET" \
  -t "$UP_TOPIC" -q 1 -C 1 -v

# 替换
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P "$DEVICE_SECRET" \
  -t "$UP_TOPIC" -q 1 -r \
  -m '{"schema_version":1,"source_message_id":"retain:2","kind":"heartbeat","data":{"sequence":2}}'

# MQTT 3.1.1 用 retained zero-length payload 删除；此特殊操作不会生成 DeviceEvent
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P "$DEVICE_SECRET" \
  -t "$UP_TOPIC" -q 1 -r -n
```

实时 retain publish 发给当前订阅者时 retain flag 为 0；新订阅触发的 retained replay 为 1。存储受全局、tenant、消息数和字节数限制。

### Last Will and Testament

另开一个订阅者观察 `$UP_TOPIC`，再启动带 Will 的客户端：

```bash
mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P "$DEVICE_SECRET" -t "$UP_TOPIC" -q 1 -v &
WATCH_PID=$!

mosquitto_sub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P "$DEVICE_SECRET" -i will-demo -t "$DOWN_TOPIC" \
  --will-topic "$UP_TOPIC" --will-qos 1 \
  --will-payload '{"schema_version":1,"source_message_id":"will:1","kind":"event","data":{"name":"unexpected_disconnect","value":true}}' &
WILL_PID=$!
kill -KILL "$WILL_PID"
wait "$WILL_PID" 2>/dev/null || true
kill "$WATCH_PID"
wait "$WATCH_PID" 2>/dev/null || true
```

异常断线、keepalive 超时和连接替换发布 Will；客户端发送 MQTT DISCONNECT 会 suppress Will。注意：计划内服务端关机关闭 network connection，不等于客户端 DISCONNECT，因此当前实现会发布仍 armed 的 Will，然后保存 MQTT snapshot。Will 在 CONNECT 时预留有界责任；订阅者压力下可进入有界 pending-Will 队列并在容量释放/计划重启后继续。

## Generic TCP device ingress

协议是 `u32` 大端 payload 长度 + JSON payload，长度必须为 `1..=max_tcp_frame_size`。首帧：

```json
{"credential_id":"demo-device","secret":"<64-hex-secret>"}
```

服务端返回分帧 `{"authenticated":true}`。此后每个上行帧是 codec payload，服务端以分帧 `EventAccepted` 回执；管理命令以完整 `DeviceCommand` JSON 帧下行。socket write 只是 `SENT`，设备执行 ACK 必须通过正常 `command_ack` 事件返回。

```bash
python3 examples/device_tcp.py
python3 examples/device_tcp.py --wait-command
```

第二种方式保持连接最多 60 秒等待命令，可在另一终端调用 management command endpoint。示例的 `recv_exact` 演示了 TCP 分片读取；不要假设一次 `recv()` 等于一帧。

## UDP v1.1 ingress

UDP 无连接、无命令下行、无分片；接纳后返回签名 NBA1。运行：

```bash
python3 examples/device_udp.py --sequence 1
```

数据报为网络字节序：`NBI1`、1-byte credential ID 长度、credential ID、u32 credential version、16-byte boot ID、u64 sequence、i64 Unix 毫秒、u16 payload 长度、payload、32-byte HMAC-SHA256。HMAC 覆盖此前全部字节，key 是 secret hex 解码后的 32 字节，不是 64 个 ASCII 字符。

默认 datagram 上限 1200 bytes、clock skew ±30 秒；每 `(DeviceKey, credential_version, boot ID)` 保留 64-bit replay window。未收到有效 NBA1 时，在有效时间和序号窗口内重发完全相同的 NBI1（含原时间戳及 HMAC）；已接纳重复包重新 ACK，不再次摄取。NBA1 只表示 EventAccepted，不表示业务最终处理。重启会丢失 replay，仍需稳定 source_message_id 和业务幂等。设备必须验证 ACK 长度、magic、版本、boot、待确认序号及 HMAC；详见 [完整协议](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。

## 官方 Rust 设备 SDK

SDK 不创建 runtime、数据库或无界离线队列；调用方必须已有 Tokio runtime。development 身份可这样运行仓库示例：

```bash
export NETBAIOT_DEVICE_CREDENTIAL_ID=demo-device
export NETBAIOT_DEVICE_SECRET=$DEVICE_SECRET
export NETBAIOT_MQTT_ENDPOINT=mqtt://127.0.0.1:8080
cargo run -p netbaiot-device-sdk --example device_mqtt
```

仓库示例使用 tutorial 身份 `demo/sensor/device-1`，可直接连接上述配置。示例均会被 workspace `--all-targets` 编译验证。

SDK 支持 MQTT QoS0/1 publish、接收命令、`ack_command`，以及通过 `publish(DeviceUplink, PublishQos)` 上报 heartbeat。MQTT 断开时默认 `OfflinePublishPolicy::Reject`；重连使用可取消、有界的 full-jitter exponential backoff（100 ms–5 s），重连后重新订阅命令 topic。`connect()` 会等待成功 CONNACK 和 command SUBACK；`shutdown()` 或丢弃最后一个 client 会停止所属任务。

普通 MQTT 3.1.1 客户端始终是一等支持对象，不要求使用 SDK。

NetbaIoT 不持有或持久化设备期望配置。业务系统负责 desired/reported 状态、版本历史、
重试、发布/回滚及离线协调。配置变更可作为普通 `DeviceCommand` 发往在线 MQTT/TCP 设备，
设备通过 `CommandAck` 返回执行结果；是否收敛由业务系统判断。命令仅支持在线投递，
UDP 无会话且没有下行。参阅[职责迁移](remove-device-config.md)。
