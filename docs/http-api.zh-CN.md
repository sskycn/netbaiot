# 管理 HTTP API

管理 HTTP 使用独立监听器和通过 `NETBAIOT_ADMIN_SECRET` 配置的 64 字符管理密钥。
请求头、请求体、处理器并发、响应和超时均有界，不接受内容编码。设备凭据不能授权管理操作；
设备入口不分派 HTTP。

## 管理监听器

| 方法/路径 | 说明 |
|---|---|
| `GET /api/v1/health` | 进程存活状态 |
| `GET /api/v1/ready` | 仅在 RUNNING 阶段就绪 |
| `GET /api/v1/status` | 生命周期和有界缓存/事件用量 |
| `GET /api/v1/metrics` | 低基数 Prometheus 文本指标 |
| `GET /api/v1/connections?offset=&limit=` | 分页查询本节点活动会话；最多 256 条 |
| `POST /api/v1/devices/connection` | 按 `DeviceKey` 查询非敏感连接/在线信息 |
| `POST /api/v1/devices/commands` | 立即向本节点活动会话发送命令 |
| `POST /api/v1/devices/config` | 读取一份类型化设备配置 |
| `PUT /api/v1/devices/config` | 设置 revision 更新的类型化设备配置 |
| `POST /api/v1/auth/invalidate` | 使指定范围失效并断开受影响会话 |
| `POST /api/v1/config/invalidate` | 移除一份设备配置缓存 |
| `PUT /api/v1/control/snapshot` | 校验并原子替换带 revision 的快照 |
| `PUT /api/v1/routes` | 按 revision 校验并替换路由 |
| `POST /api/v1/drain` | 请求优雅关机 |

连接查询有界。命令请求体必须包含权威的完整 `DeviceKey`；设备不可用时返回 503，且不会将命令放入离线队列。连接响应会返回活动传输类型、`connected_at`、`last_seen` 和会话 generation，但不会暴露 socket 状态。仅当当前 MQTT/TCP 会话仍处于连接状态时才会提供 `connected_at`。

错误使用稳定的 `ApiError` JSON 结构，字段包括 `code`、安全的 `message`、可选 `request_id` 和可选 `required_scope`。例如，离线命令使用 `device_offline`，而不是含糊的内部 500。设备和管理接口的授权相互独立；当前静态管理令牌只支持全有或全无授权，公共错误模型中的 scope 字段为未来的细粒度授权 provider 预留。
