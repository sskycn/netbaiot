# 配置与项目初始化

[English](configuration.md)

```bash
netbaiot init my-gateway
cd my-gateway
netbaiot config check --config netbaiot.json
netbaiot doctor --config netbaiot.json
netbaiot serve --config netbaiot.json
```

`init` 只创建 `netbaiot.json`、`.env.example`、`README.md` 和 `var/`。development
配置使用 loopback、`127.0.0.1:18080/events` 示例 HTTP receiver 和现有开发凭据。
运行 `serve` 前先启动自己的 receiver；要体验自包含流程可用 `netbaiot demo`。命令不会
生成或打印管理密钥；`.env.example` 只含空值变量名，也不会自动加载。应从受保护来源
提供 secret。

`netbaiot init --production my-production` 创建可被 parser 读取的骨架，其中包含 TLS
占位、HTTPS provider/sink 占位，不含设备凭据或固定生产 token。它**不能直接运行**。
填入 TLS/auth/sink 后执行 `config check`；骨架不会允许公网明文网关直接启动。

默认情况下，`init` 拒绝替换任何受管理文件。`--force` 只替换上述三个普通文件；它会
拒绝 symlink/special file，绝不删除整个目录或无关文件。每个文件分别 staging、sync、
原子发布，但三个文件之间不是事务：较晚的文件系统错误可能留下会明确报告的部分生成。
生成的恢复路径是项目绝对路径，移动项目后应更新。其他配置文件路径仍按 server 当前
工作目录解析。

## IDE schema 关联

[生成的 JSON Schema](schema/netbaiot-config.schema.json)通过可选 `schema` feature 从真实
Rust `Config`/serde 类型生成，遵循 serde default、enum 拼写和 `deny_unknown_fields`。
资源标量限制与 runtime 的正数/u32 范围一致；`secret_hex` 标记为 `writeOnly` 且必须为
64 个十六进制字符。生产 server/client binary 默认不启用 schema 生成。
`netbaiot config schema` 可直接打印仓库提交的生成产物，不需要 compiler。

配置会拒绝未知字段，包括 `$schema`，所以不要把 `$schema` 插入 gateway JSON。VS Code
可在 editor settings 中关联：

```json
{
  "json.schemas": [{
    "fileMatch": ["netbaiot.json"],
    "url": "./netbaiot-config.schema.json"
  }]
}
```

把 `netbaiot config schema` 输出以该文件名保存在配置旁，或设置合适的绝对 editor schema
路径。二进制发布包也包含该 schema。

Schema 校验不能替代 `netbaiot config check`。跨字段 listener 安全、身份唯一性、
role/permission 约束、环境 secret source、真实 PEM/key 匹配以及运行时目录/端口所有权
仍需单独检查。[生成字段与默认值参考](configuration-fields.md)由
`cargo xtask config-reference` 重建；[运维指南](operations-guide.md)包含手写安全和部署建议。

## 当前 Business RPC 配置

`business_tcp` 只接受 Business RPC V3；启用 listener 时必须显式配置 `business_rpc`。
当前字段是 `limits`、`send_ahead`、`experiment_socket_send_buffer_bytes`、TLS/identity、
开发 token env 和连接/认证上限。旧 `version`、`allow_v1`、`v3`、`v3_send_ahead` 与
`v3_experiment_socket_send_buffer_bytes` 会因未知字段失败。生产使用 mTLS 和明确映射的
principal；无 TLS 只允许 loopback 开发 token。详见
[协议升级指南](migration/current-protocol-only.zh-CN.md)与[Business RPC V3](business-rpc-v3.zh-CN.md)。

## 每租户事件 backlog

`limits.event_queue_max_count_per_tenant` 与
`limits.event_queue_max_bytes_per_tenant` 限制每个 tenant 的未完成 EventBus 事件，包括
queued、retrying 和 inflight delivery。序列化事件字节无论 fanout 数量都只计一次；只有
最后一个 sink responsibility 完成才释放所有权。required sink 失败期间仍占用 quota。

两项默认值与既有全局事件限制相同：16384 条、67108864 字节。旧配置仍可解析，默认
容量不变；之前调高全局限制的部署，若单个 tenant 也需要超过默认值，必须显式设置租户
上限。可把租户值设低以给其他 tenant 保留全局容量。全局和 sink 限制仍独立生效；租户
上限高于全局上限不会增加容量。接纳和重启重放会在提交前预检全局、租户及 required
sink quota。
