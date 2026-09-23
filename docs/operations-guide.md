# 运维与生产部署指南

## 配置模型

Server 接受一个 JSON 路径；未提供时默认 `configs/development.json`：

```bash
netbaiot-server /etc/netbaiot/server.json
netbaiot-server --print-default-limits
```

配置使用 `deny_unknown_fields`，拼错字段会让启动失败。主要字段：

| 字段 | 含义 | development | production |
|---|---|---|---|
| `device_ingress` | TCP：设备 HTTP/MQTT/通用 TCP；UDP：NBI1 | loopback `8080` | 同号 TCP/UDP `443`；TCP 必须 TLS，UDP 只认证不加密 |
| `management_http` | `/api/v1/...` listener | loopback `9090` | 优先 loopback/管理网；非 loopback 必须 TLS |
| `business_tcp` | confirmed stream listener | null | 当前只允许 loopback，且与 webhook 二选一 |
| `development` | 强制所有 listener loopback | true | false |
| `limits` | `Limits` 的覆盖字段 | `{}` 使用默认 | 按测量调优，不可设无界 |
| `tls` | PEM certificate/private key | null | 对非 loopback HTTP/MQTT/TCP 必填 |
| `delivery_url` | required webhook | tutorial 为 loopback | HTTPS business endpoint |
| `auth_provider_url` | 外部认证 provider | null + static credentials | HTTPS provider；loopback 可 HTTP |
| `spool_directory` | planned-restart recovery | `./var/...` | 独立、本地、受监控、权限受限目录 |
| `credentials` | static demo/bootstrap credential | 固定演示值 | 建议 provider/安全生成的配置，不进 Git |
| `device_configs` | bootstrap config snapshots | 示例一台设备 | 控制面 snapshot 管理 |

Environment variables：

| 名称 | 用途 |
|---|---|
| `NETBAIOT_ADMIN_SECRET` | 64-hex management bearer；未设置时 loopback management 存活但所有请求 forbidden |
| `NETBAIOT_DELIVERY_TOKEN` | webhook bearer |
| `NETBAIOT_BUSINESS_STREAM_TOKEN` | `business_tcp` hello token；配置 stream 时必填 |
| `RUST_LOG` | tracing filter，如 `info` 或 `netbaiot_server=debug` |
| `NETBAIOT_PERF_LOCK_METRICS=1` | 仅性能实验的额外 lock timing；生产默认关闭 |

## 最小开发与生产方向配置

最小开发配置见 [`configs/development.json`](../configs/development.json)。它使用即时 ACK 的 in-process audit sink；适合验证 ingress，但看不到业务 consumer。端到端教程使用 [`configs/tutorial.json`](../configs/tutorial.json) 和本地 webhook。

生产方向示例（证书/URL/容量只是占位，必须按环境修改）：

```json
{
  "device_ingress": "0.0.0.0:443",
  "management_http": "127.0.0.1:9090",
  "business_tcp": null,
  "development": false,
  "limits": {},
  "tls": {
    "certificate": "/etc/netbaiot/tls/server-chain.pem",
    "private_key": "/etc/netbaiot/tls/server-key.pem"
  },
  "delivery_url": "https://business.internal.example/netbaiot/events",
  "auth_provider_url": "https://identity.internal.example/device-auth",
  "spool_directory": "/var/lib/netbaiot/recovery",
  "device_configs": [],
  "credentials": []
}
```

同一 `tls` 接受器用于 device HTTP、management HTTP、MQTT 和 generic TCP。TLS 验证必须使用真实 CA/hostname；不要在生产客户端长期使用 insecure 选项。UDP payload 不加密。

## 资源限制与 backpressure

权威默认值在 [`configs/resource-limits.json`](../configs/resource-limits.json)，可用 `--print-default-limits` 与 binary 当前值比较。重要默认上限：

| 类别 | 默认值摘要 |
|---|---|
| connections | 256 node / 64 tenant / 32 IP / 2 device |
| packet/body/frame | MQTT/HTTP/TCP 64 KiB；UDP 1200 B |
| ingress | 16 active、2 MiB；16 waiters、25 ms |
| commands | 16/device、128/tenant、1024/process；16 KiB/command；TTL 5 min |
| persistent sessions | 4096 global / 512 tenant；idle policy 24 h |
| subscriptions | 32/session、64/device、128/tenant、512 global |
| offline MQTT | 128 + 1 MiB/session；16384 + 128 MiB global |
| retained | 4096 + 64 MiB global；64 KiB/message |
| EventBus | 16384 events / 64 MiB |
| each sink | 4096 / 16 MiB；concurrency 8；timeout 5 s |
| retry | 5 normal attempts；max age 1 h；100 ms–30 s backoff |
| spool | 100000 records / 256 MiB；1 MiB/record |

设计目标不是无限缓存，而是明确且可预测的 backpressure。每个可变 payload 资源都有 count/bytes 约束；required sink 超载在 EventAccepted 前拒绝，best-effort sink 可按策略丢弃。等待工作本身也有上限，不会 spawn 无限 semaphore waiter。

调参方法：

1. 开发/小型环境先保留默认值，只降低与预期设备数明显不符的上限以暴露错误。
2. 中等规模按连接数、真实 payload 分布、QoS、persistent/offline/retain 使用率和最慢 required sink 的服务时间做容量表，再调整层级上限。
3. 大型部署先在目标 CPU、allocator、TLS、真实网络、auth provider 和 business sink 上跑 load/soak/failure test。确保 global >= tenant >= device/session 的层级关系，spool/MQTT recovery ceiling 可覆盖合法 admitted state。

配置最大值不是实际容量。逻辑 memory reservation 也不是预分配 RSS。

## Management API

所有 endpoint，包括 health/ready，都要求独立 management bearer：

```bash
ADMIN=abababababababababababababababababababababababababababababababab
curl --noproxy '*' http://127.0.0.1:9090/api/v1/status \
  -H "Authorization: Bearer $ADMIN"
```

| 方法/路径 | 行为 |
|---|---|
| `GET /api/v1/health` | 进程 liveness |
| `GET /api/v1/ready` | 仅 RUNNING 返回 ready true/200 |
| `GET /api/v1/status` | lifecycle、EventBus/cache 使用量、task、transport connection counts |
| `GET /api/v1/metrics` | Prometheus text counters/histograms |
| `GET /api/v1/connections?offset=0&limit=100` | 本节点 bounded pagination，limit 最大 256 |
| `POST /api/v1/devices/connection` | 完整 DeviceKey 的 live status |
| `POST /api/v1/devices/commands` | 只发 live MQTT/TCP session |
| `POST/PUT /api/v1/devices/config` | 获取/更新 revisioned config |
| `POST /api/v1/auth/invalidate` | cache/live/persistent MQTT auth invalidation |
| `POST /api/v1/config/invalidate` | 移除一台设备的 config cache |
| `PUT /api/v1/control/snapshot` | 完整验证并原子替换 snapshot |
| `PUT /api/v1/routes` | revisioned route replacement |
| `POST /api/v1/drain` | 开始 graceful shutdown |

Auth invalidation 请求由 `scope` tag 选择：

```bash
# device
curl -X POST http://127.0.0.1:9090/api/v1/auth/invalidate \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  --data '{"scope":"device","device":{"tenant_id":"demo","product_id":"sensor","device_id":"device-1"}}'

# product / tenant / credential version / auth generation / all
# {"scope":"product","tenant_id":"demo","product_id":"sensor"}
# {"scope":"tenant","tenant_id":"demo"}
# {"scope":"credential_version","version":1}
# {"scope":"auth_generation","generation":1}
# {"scope":"all"}
```

响应分别报告 `invalidated_cache_entries`、`disconnected_connections` 和 `invalidated_mqtt_sessions`（另保留 legacy `invalidated`/`disconnected`）。Invalidation 与 session registration/MQTT attachment 共用同步边界；完成前已返回的 stale provider result 不能随后注册。

Management 应只暴露在 loopback、受控管理网或强认证的 reverse proxy 后。当前 static admin token 是 all-or-nothing，不要虚构不存在的细粒度 RBAC。

## 可观测性

```bash
RUST_LOG=info netbaiot-server /etc/netbaiot/server.json
curl -H "Authorization: Bearer $ADMIN" http://127.0.0.1:9090/api/v1/metrics
```

稳定、低基数 counters 包括 connections accepted/rejected、MQTT connect/packets/publishes/subscriptions/PUBACK/protocol violations、HTTP/TCP/UDP traffic、auth cache/failures、codec/ingress/admission/queue rejects、command lifecycle、events accepted/rejected/bytes、sink ACK/retry/failure/drop、spool/recovery 和 timeouts。Histogram 提供关键阶段延迟。

`/status` 提供当前 transport connection counts、EventBus `event_count/event_bytes/pending_required`、auth/config cache 和 runtime task 数。当前公共 metrics **没有直接导出** persistent session 数、offline message 数、QoS inflight 数、retained 数或逐 sink backlog gauge；不要在 dashboard 中假装这些指标存在。可用 rejection/sink counters、status、日志和外部 black-box probe 监控，若运维必须精确观测这些状态，应单独提出受控指标扩展。

日志不得包含 password、token、Authorization、HMAC key、raw credential 或完整敏感 spool。device/event/command/client ID、revision、sink URL 可进入受控日志/trace，但不能成为 metric label。高流量 debug 会增加开销，不应长期启用。

## Graceful shutdown 与 planned restart

生命周期：

```text
STARTING -> RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED -> EXIT
```

Quiesce 先让 readiness false、关闭 admission gate、等待活动 guard，然后停止新 connection/upload/command/config mutation。已接受 required delivery 继续 drain；未 ACK 或 inflight ACK 不确定的工作会写入 EventBus recovery file。MQTT persistent session/retain/QoS/Will 状态写入独立 snapshot。

计划重启步骤：

1. 从负载均衡摘除实例并观察 `/ready`。
2. POST `/api/v1/drain`，或发送 SIGTERM。
3. 等待进程自己退出；不要额外设置比 `shutdown_timeout_ms` 更短的强杀 deadline。
4. 用同一配置和 spool directory 重启。
5. 等 `/ready` 200；恢复的 required event 使用原 `event_id` 重放，MQTT client 仍需重新认证后恢复 session。

如果 spool fsync/rename/directory fsync 失败且仍有 accepted work，进程保持存活、unready、有界频率重试，不宣称成功退出。结构性 MQTT recovery failure 不会跳过 EventBus 安全流程，但最终仍会阻止 voluntary exit。

EventBus 当前 writer 为 v2，并可读 legacy v1。MQTT 当前 writer 是 streaming NBMQ v3，读取 v1/v2/v3；snapshot 包含权威 record-count/byte-count/digest trailer。目录默认应为 0700、文件 0600；监控容量、权限、inode 和本地磁盘错误，不要把它当 hot-path queue 或一般 event store。

SIGKILL、process/OS crash、断电可能丢失仍仅在内存中的 bounded recent traffic 和最近 MQTT state。NetbaIoT 不是 crash-durable database；恢复能力只承诺正确完成的 planned shutdown。

## systemd 示例

仓库当前没有 installer；以下路径是部署示例：

```ini
[Unit]
Description=NetbaIoT gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=netbaiot
Group=netbaiot
ExecStart=/opt/netbaiot/bin/netbaiot-server /etc/netbaiot/server.json
Environment=RUST_LOG=info
EnvironmentFile=/etc/netbaiot/secrets.env
WorkingDirectory=/var/lib/netbaiot
Restart=on-failure
RestartSec=5s
TimeoutStopSec=90s
KillSignal=SIGTERM
LimitNOFILE=65536
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=/var/lib/netbaiot/recovery

[Install]
WantedBy=multi-user.target
```

确保 certificate/private key、config、environment file 和 spool 权限允许 service user 最小读取/写入。`LimitNOFILE` 必须依据连接规模和 host 全局限制测量；不是越大越好。

当前仓库没有官方 Dockerfile/docker-compose deployment。本教程不额外引入未经维护的容器入口；如自行容器化，必须正确处理 SIGTERM、readiness、只读 rootfs 例外、持久 spool volume、secret injection、FD limits 和 TLS 文件。

## 性能与安全压测

当前 [`performance-baseline.md`](performance-baseline.md) 是 release、单机 IPv4 loopback、Mac mini M4、server 与 load generator 共享主机的实验，不是 SLA 或生产容量承诺。吞吐受 QoS、publisher 数、payload、TLS、fanout、sink latency、auth provider 和 host limits 显著影响。

`netbaiot-loadgen` 接受一个 JSON 文件，不提供复杂 CLI flag parser：

```bash
cargo build --release --locked -p netbaiot-loadgen
./target/release/netbaiot-loadgen /tmp/netbaiot-small-load.json
```

安全的本地起点：

```json
{
  "transport":"mqtt",
  "address":"127.0.0.1:8080",
  "connections":10,
  "ramp_per_sec":10.0,
  "warmup_secs":2.0,
  "duration_secs":10.0,
  "cooldown_secs":1.0,
  "publish_rate":50.0,
  "payload_bytes":256,
  "qos":1,
  "subscribe":false
}
```

未给出的字段使用 loadgen 默认值。只对你有权测试的隔离环境运行；先小规模，再监控 CPU/RSS/FD/sink/backpressure 逐步升载。生产前还应覆盖 slow sink、provider outage、restart cycles、SIGKILL loss window、TLS、memory、长时间 soak 和真实业务 fanout。

## 生产上线 checklist

- release + locked build；记录 binary SHA、config revision、Rust/toolchain。
- device public stream 启用 TLS；management 保持 loopback/管理网；UDP 使用受控网络。
- demo secret 全部替换；secret 不进 Git/日志；验证 rotation 与 invalidation。
- 配置真实 required sink；消费者按 event ID 持久幂等。
- 依据 measurement 设置所有 count/byte/time limits；禁止无界旁路。
- spool 使用本地可靠磁盘、私有权限、容量/错误告警；演练恢复与损坏失败。
- 提升并验证 FD/kernel limits；监控 connections、rejects、pending required、sink retry/failure、timeouts。
- 部署工具等待 graceful exit；演练 SIGTERM 与 unavailable sink。
- 验证 command offline/timeout 策略；业务拥有离线队列。
- 在目标环境完成负载、故障、重启和 soak；baseline 不当作 SLA。

## UDP v1.1 回执观测

NBA1 仅确认 EventAccepted。观察 `udp_accepted`、`udp_accepted_duplicates`、`udp_acks_sent`、`udp_ack_send_failures`，结合 `udp_datagrams` 与 ingress/codec/admission counters 判断重试及丢回执；发送成功不等于设备收到。未认证及其他拒绝均静默，socket 压力下不排队 ACK。重复包仍受来源 IP/进程限速。捕获合法包后的来源伪造仍可能有限反射，但 64 字节 ACK 小于最小 76 字节 NBI1，无字节放大。详见 [协议与安全边界](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。
