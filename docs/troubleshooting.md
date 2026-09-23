# 故障排查

先固定四项信息：`git rev-parse HEAD`、实际 config 路径、server 启动日志、客户端明确使用的 protocol/address。再查询：

```bash
curl --noproxy '*' -i http://127.0.0.1:9090/api/v1/ready \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET"
curl --noproxy '*' http://127.0.0.1:9090/api/v1/status \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET"
curl --noproxy '*' http://127.0.0.1:9090/api/v1/metrics \
  -H "Authorization: Bearer $NETBAIOT_ADMIN_SECRET"
```

不要把 credential/token 写入 issue、日志或 metric label。API error 的 `request_id` 可以安全地用于关联 server 日志。

## 1. Server 启动即退出

**症状**：`Configuration`、listener bind 或 recovery error。

**常见原因**：JSON 未知字段/类型错误；`development:true` 却绑定非 loopback；非 loopback MQTT/TCP/management HTTP 没有 TLS；production 没有 required sink；端口占用；credentials/provider 均为空；spool 空路径。

**确认/解决**：从 `configs/development.json` 最小差异修改；用 `lsof -nP -iTCP:<port> -sTCP:LISTEN` 查冲突；检查 PEM、目录权限和 server 日志。不要删除损坏 spool 来“修复”，除非明确接受丢失责任并经过事故审批。

## 2. MQTT connection refused / timeout

**症状**：TCP connect 失败，没有 CONNACK。

**原因**：server 未 ready、地址/端口错误、TLS/plaintext 混用、防火墙、connection/IP/global capacity 满。

**确认/解决**：检查 `runtime ready`、`/ready`、`connections_rejected`；loopback 教程用 `127.0.0.1:8080` plaintext，生产 `mqtts` 客户端需正确 CA/hostname。

## 3. Bad username or password / CONNACK 4

**症状**：Mosquitto 报 bad username/password。

**原因**：username 不是 `credential_id`；password 不是配置中的 64-char hex 文本；credential rotated/invalidated；provider unavailable 且 cache miss/expired。

**确认/解决**：只比较长度和 credential ID，避免打印 secret；查看 `auth_failures`、cache miss 与 provider 健康；修复凭据后新建连接。正常 PUBLISH 不会远程重认证。

## 4. SUBACK 0x80

**症状**：连接成功但订阅被拒绝。

**原因**：filter 逃逸已认证设备 root、没有 commands permission、filter 语法/长度/depth 非法、subscription/retain replay 容量不足。

**确认/解决**：从 exact `v1/t/<tenant>/p/<product>/d/<device>/down` 开始；确认 identity 与 path 完全一致；检查 overload/reject metrics。通配符只能位于该设备 root 之下。

## 5. PUBLISH 后没有 PUBACK/PUBCOMP

**症状**：QoS1/2 客户端等待或重连重发。

**原因**：topic ACL、JSON codec、required sink admission、persistent subscriber 容量或 EventBus count/bytes 失败；server draining；协议状态错误。

**确认/解决**：先用文档中的 heartbeat payload 和 exact `/up`；查看 `codec_failures`、`ingress_rejected`、`queue_rejects`、`events_rejected`、sink counters。PUBACK 被延迟/缺失意味着不能假定 EventAccepted，客户端应按 MQTT 语义重试。

## 6. 管理 HTTP 返回 401/403/400/413/415/429/503/504

**症状与处理**：401 检查精确 `Bearer credential-id:secret`；403 检查 permission/management token；400 检查严格 JSON/endpoint kind；413 降低 body；415 移除 `Content-Encoding`；429 是 admission/rate/queue overload，退避；503 检查 draining/provider/device offline/storage；504 检查 request/sink timeout。

## 7. 业务系统没收到事件

**原因**：运行的是 development audit sink 而不是 webhook/stream；`delivery_url` 错误；stream filter 不匹配或未收到 ready；业务 endpoint 拒绝/超时；事件尚未 EventAccepted。

**确认/解决**：检查实际 config；比较 `events_accepted` 与 `sink_acks/retries/failures`；查看 `/status.pending_required`；用 `examples/business_http_sink.py` 隔离 business service。不要只看设备客户端“write 成功”。

## 8. 重复收到事件

**症状**：相同业务内容或相同 `event_id` 多次出现。

**原因**：sink 事务完成但 ACK 丢失、timeout、reconnect、planned restart replay；设备应用重试也可能产生不同 event ID/相同 source ID。

**解决**：以 `event_id` 做持久 unique key，在业务事务后 ACK；`source_message_id` 用于额外的设备域关联。重复是 at-least-once 契约，不是 server 自动 exactly-once 的缺陷。

## 9. `DEVICE_OFFLINE`

**症状**：command HTTP 503，code 为 `device_offline`；CLI exit 5。

**原因**：没有当前本节点 live MQTT/TCP session；只有 persistent offline session；设备只用 UDP；旧 generation 已被新连接替换。

**解决**：查询 `/api/v1/devices/connection`；让设备建立 live MQTT/TCP；业务系统保留离线命令意图。NetbaIoT 不会把管理命令塞进 MQTT offline queue。

## 10. `overloaded`

**症状**：管理 HTTP 429、MQTT 无成功 ACK、connection 被拒绝。

**原因**：connections、ingress wait、rate、EventBus、sink、command、subscription、offline、retain 或 replay state 任一 count/byte limit 满。

**确认/解决**：查 reject counters、pending required 和 sink latency；先修慢 consumer/突发流量，再根据测量调 limits。指数退避并加 jitter；不要立即循环重试制造 retry storm。

## 11. 503 / service draining

**症状**：readiness 503，新 upload/command 被拒绝。

**原因**：已收到 SIGTERM/Ctrl-C/drain；或 recovery/spool 故障让实例保持 unready。

**解决**：把新流量切到 ready 实例；等待正常退出。若长期不退出，查 sink ACK、spool fsync/rename/权限/容量和 MQTT recovery 错误，不要直接 SIGKILL 除非接受 crash loss window。

## 12. Persistent session 没恢复

**原因**：漏了 `-c`、ClientId 改变/为空、DeviceKey 改变、clean session 连接删除旧状态、24h idle expiry、授权/codec provenance 变化、auth invalidation、session capacity 或恢复文件失败。

**确认/解决**：用 `-d -V mqttv311 -c -i stable-id` 看 Session Present；确保同一 credential identity；检查启动 recovery 日志。重连时重新 `-t` 会创建/更新订阅，不能用来证明原 session 已保存。

## 13. Persistent UNSUBSCRIBE 似乎无效

**原因**：Mosquitto 2.0.x 的 `-U` 调用缺少 `-t` 而根本没有发包；验证重连又 `-t` 了目标 topic；忽略 CLI 非零退出。

**解决**：使用[兼容命令](device-integration-guide.md#持久-unsubscribe兼容-mosquitto-20x21x)，为 `-t` 指定不同的 authorized topic，观察 UNSUBACK，再不订阅目标地验证。

## 14. Retained message 没出现

**原因**：发布未带 `-r`、payload/ACL/EventAccepted 失败、被 zero-length retained publish 删除、订阅 filter 不匹配、retain replay preflight 容量不足、使用 `-R` 排除了 retained。

**确认/解决**：用 exact topic、合法 codec payload、`-q 1 -r` 创建；新 client 用 `-C 1 -v` 订阅。实时 publish 的 retain flag 为 0，新订阅 replay 才为 1。

## 15. Will 没触发

**原因**：客户端正常发送 MQTT DISCONNECT；Will topic/payload/ACL 不合法导致 CONNECT 被拒；观察者 filter 不匹配；测试 process 实际优雅退出。

**解决**：先确认带 Will 的连接成功；payload 必须是 `/up` 可接受的 codec JSON；用隔离测试 process 的 SIGKILL 或真实 network loss。注意服务端 planned shutdown 当前会发布 armed Will。

## 16. TCP device 卡住或 frame invalid

**原因**：把 TCP 当 message stream；长度不是 4-byte big-endian；长度含了 header；首帧不是 handshake；长度为 0/超限；只读一次 `recv()`。

**解决**：对照 `examples/device_tcp.py`，使用 `recv_exact`，长度只计算 JSON payload。认证 reply、EventAccepted、command 都同样分帧。

## 17. UDP 无回执或被丢弃

**症状**：`sendto` 成功但业务无事件。

**原因**：未通过验证或接纳时静默丢弃；HMAC key 错把 hex ASCII 当 key、timestamp 超 30 秒、credential version 错、序号已滑出 replay 窗口、datagram 超 1200 B、codec/ingress 拒绝。

**解决**：对照 `examples/device_udp.py`；同步时钟；每 boot 使用单调 sequence；查 `udp_datagrams`、`udp_accepted_duplicates`、`udp_acks_sent`、`udp_ack_send_failures` 与 auth/codec/ingress counters。有效窗口内重发原始数据报；只有验证通过的 NBA1 才确认 EventAccepted，超窗未确认属于 uncertain。

## 18. Confirmed TCP stream 连接后无事件

**原因**：没有先 hello/subscribe、token 错、未等待 ready、已有另一个 owner（conflict）、filter 不匹配、frame/ACK 字段错误。

**解决**：用 `examples/business_tcp_client.py`；确认 config 为 `business_tcp` 而非 webhook；一次只运行一个 consumer；ACK 三个 ID 必须完整匹配。

## 19. Spool/recovery 启动失败

**原因**：目录不可读写、磁盘满、checksum/length/trailer/版本损坏、配置上限低于已提交镜像、错误复用远程/不支持 fsync 的存储。

**解决**：保留文件做事故证据；检查 owner/mode、free space、inode 和 server 精确错误。EventBus v2 可读 v1；MQTT writer v3 可读 v1/v2/v3。未知/部分/损坏状态会 fail loudly，不能静默忽略责任。

## 20. Management endpoint 无法访问

**原因**：连错 device port、未设置 server 进程的 `NETBAIOT_ADMIN_SECRET`、token 不是 64-hex、缺少 Bearer、非 loopback 未配置 TLS、防火墙。

**解决**：management 默认 `9090`、device ingress 默认 `8080`；即使 health/ready 也带 management bearer。设置环境变量后必须重启 server。

## 21. TLS hostname / CA 错误

**原因**：证书 SAN 不含访问 hostname、缺中间证书、客户端不信任 CA、把 TLS port 当 plaintext、证书过期。

**解决**：用部署 hostname 而不是随意 IP；配置完整 chain；把 CA 安装到客户端 trust store或显式 `--cafile`；检查时间。不要用 `--insecure` 掩盖生产问题。

## 22. CLI 命令和文档不一致

**症状**：`--help` 不是常规成功输出、package/binary 名混淆。

**解决**：`cargo run -p netbaiot-cli -- ...` 启动的 binary 名为 `netbaiot`。当前 CLI 在 usage error 时打印帮助，先设置 `NETBAIOT_ENDPOINT`/`NETBAIOT_TOKEN`；以[业务集成命令表](business-integration-guide.md#cli)和 `apps/netbaiot-cli/src/main.rs` 为当前事实来源。
