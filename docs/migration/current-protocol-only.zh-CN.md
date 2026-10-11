# 破坏性变更：只保留当前协议

[English](current-protocol-only.md)

本轮清理移除了 Business RPC V1 与 V2 实现；Business RPC V3 是唯一接受的业务流。
MQTT 恢复只接受 NBMQ v6，独立 EventBus spool 只接受 NBSP v3。NBMQ v1–v5、NBSP
v1/v2 reader、JSON recovery、迁移路径和历史 fixture 都已移除。旧版/未知 header 会返回
`UnsupportedRecoveryVersion(version)`，或触发协议拒绝/关闭连接。当前 framing、payload
字节、checksum/trailer、权限和资源限制继续生效。MQTT 3.1.1/5.0 与管理 `/api/v1`
不受此清理影响，仍然支持。

## 客户端与配置

部署当前 listener 前，把业务客户端升级到 `BusinessRpcV3Client` 或当前
`NetbaIoTClient` event façade。旧公共 stream/V2 frame/client/config type 已删除，没有
deprecated wrapper。Python V1 示例与 V1 capacity harness 已移除；使用当前 Rust 示例与
RPC loadgen。

包含 `version`、`allow_v1`、`v3`、`v3_send_ahead` 或
`v3_experiment_socket_send_buffer_bytes` 的旧配置会因 `deny_unknown_fields` 失败。
应改用 `limits`、`send_ahead` 和 `experiment_socket_send_buffer_bytes`。启用
`business_tcp` 时必须显式配置 `business_rpc`；省略它不会启用旧协议。把
`NETBAIOT_BUSINESS_STREAM_TOKEN` 替换为配置的当前 loopback development token 环境
变量；生产使用 mTLS 和明确映射的 principal。管理/设备凭据不能替代业务凭据。

当前 client 继续提供有界数量/字节 buffer、应用手动 ACK、稳定 EventId 重放、epoch
fencing、取消和命令结果语义。`StaleRevision` 要求 Provider reset sync。底层 readiness
wait 是持续等待；应用应自行设置 timeout/cancellation。loadgen 不再暴露 V2 topology 或
空的 V2-only handshake/sync latency 字段。旧 Event/Command response-queue metric class
已移除；当前 control-queue gauge 和 `business_rpc_v3_*` metric 保留。

## 恢复目录升级

1. 从外部停止新入口，备份完整恢复目录与原配置。保留每一份已提交责任；不得通过删除
   文件绕过校验。
2. 升级前运行能理解旧记录且能完成责任或写出当前格式的合适旧版本。基线 `21a6945`
   可读当时支持的 NBMQ v1–v5 与 NBSP v1/v2，并写出 NBMQ v6/NBSP v3。更早已删除的
   业务事件类型可能还需要更早、兼容的 consumer/release 先完成工作。
3. 让 required consumer ACK 工作，用该旧版本检查 `pending_required` 和已提交文件。
   请求计划 drain/shutdown 并确认持久提交和成功退出。成功关机仍可能留下 pending spool，
   不证明业务已全部处理。部署新版本前，使用旧版本的当前格式 reader 验证剩余文件。
4. 使用已验证的 NBMQ v6/NBSP v3 文件部署当前 client/config/gateway。MQTT client 用正确
   身份重新认证连接。两个恢复域与 required setup 全部验证后才能发布 readiness。

当前网关不包含转换器、迁移服务或自动 downgrade。不支持的文件会保留；不得把其字节
重新解释为当前格式。当前 NBMQ 文件中显式 unknown-profile marker 不能在未重新认证时
恢复订阅；系统不会伪造 codec/authorization provenance，也不承诺旧 MQTT 状态可无缝重放。

回滚需要能同时读取两种当前文件格式的版本，或者先完成所有责任再切换到不兼容 reader。
保留原备份。SIGKILL、断电和 OS 崩溃仍可能丢失近期内存流量；此次清理没有增加 crash
durability、跨域事务或离线命令存储。
