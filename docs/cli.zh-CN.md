# `netbaiot` CLI

[English](cli.md)

统一 CLI 可以运行网关、体验开发 Demo、检查本地配置，并通过 `netbaiot-client`
管理运行中的网关。`serve` 与兼容入口 `netbaiot-server` 使用相同的服务库、信号、
排空和恢复逻辑。网络管理功能复用客户端，不复制 HTTP/流协议实现。

```bash
netbaiot --help
netbaiot serve --help
netbaiot demo --help
netbaiot config --help
netbaiot serve --config configs/development.json
netbaiot demo --once
netbaiot init my-gateway
netbaiot init --production my-production
netbaiot doctor --config netbaiot.json
netbaiot config check --config configs/tutorial.json
netbaiot --output json config check --config configs/tutorial.json
netbaiot config schema
netbaiot config limits
netbaiot version
```

`serve`/`config check` 默认读取 `configs/development.json`；文件相对路径按当前工作目录
解析。`config limits` 输出真实默认值；`version` 输出 binary 的 Cargo 版本，不能据此
推断未发布 `main` 与 Release 行为相同。本地命令不要求管理 endpoint/token。

Demo 使用标准 MQTT SDK、真实认证/路由和 loopback HTTP sink，动态端口及私有临时恢复
目录。Sink 只处理一条连接，body 64 KiB、通知队列 16 条/1 MiB、连接 deadline 两秒。
Demo 分别报告 PUBACK/EventAccepted 和 Sink ACK；不需要 Python、Mosquitto 或外部 broker。
`--once` 完成一条后排空退出；持续模式打印普通 Mosquitto 命令，等待 Ctrl-C（Unix 也处理
SIGTERM）。打印的设备凭据仅供本地开发，随机管理密钥不打印。

## 配置诊断

`config check` 共用服务端静态校验、真实有界 PEM 读取/密钥匹配和管理认证构造，尽可能
收集独立错误。它不绑定端口、不启动 worker、不投递事件、不访问远端、不读取/修改恢复
快照或获取恢复锁，只检查恢复路径元数据。最终端口、远端行为和运行时所有权另行验证。

JSON stdout 形如 `{"valid":true,"diagnostics":[]}`；错误项契约为
`code/path/message/help`，不输出配置值、未知字段名或原始 serde 错误文本。
人类可读输出包含相同提示。解析/类型错误停止依赖该值的检查，静态独立错误可以汇总。

| Code | 含义 |
| --- | --- |
| NBI-CFG-001 | 非法/尾随 JSON 或不兼容字段类型 |
| NBI-CFG-002 | 未知配置字段 |
| NBI-CFG-003 | 无效监听地址或开发监听范围 |
| NBI-CFG-004 | 公网设备入口缺少 TLS |
| NBI-CFG-005 | 非 loopback 管理入口缺少 TLS |
| NBI-CFG-006 | TLS 文件/CA/key 解析或匹配错误 |
| NBI-CFG-007 | 管理认证、mTLS 映射或 admin secret source 错误 |
| NBI-CFG-008 | Business RPC/listener/identity 配置不一致 |
| NBI-CFG-009 | 缺少/不安全的业务地址或投递选择 |
| NBI-CFG-010 | 数量/字节/期限或层级限制错误 |
| NBI-CFG-011 | 设备认证、凭据或 Codec 绑定错误 |
| NBI-CFG-012 | 恢复路径无效/不可访问 |
| NBI-CFG-013 | 配置文件缺失、不可读或超过 16 MiB |
| NBI-CFG-015 | Codec 注册/声明式布局、数值证明或资源预算错误 |
| NBI-CFG-016 | 整数旧 scale/bias，含 null；路径和 help 提供精确迁移建议 |

`NBI-CFG-016` 属于[尚未发布的 main](documentation.zh-CN.md)。迁移时删除旧字段，
非恒等映射使用字符串 `decimal_exact`，恒等映射省略 `conversion`。
非法 JSON、错误类型/浮点值、未知字段保留原类别。见[完整迁移](migration/integer-decimal.zh-CN.md)。
HTTP provider/sink 地址要求 HTTPS（literal loopback/localhost 可 HTTP），禁止 URL 凭据，
运行时仍拒绝重定向；诊断不会发请求。

## 远程管理

```text
netbaiot server status
netbaiot server drain --yes
netbaiot device status DEVICE --tenant TENANT --product PRODUCT
netbaiot events subscribe [--tenant ...] [--product ...] [--device ...] [--type ...]
netbaiot command send DEVICE --json JSON
netbaiot command send DEVICE --payload-file PATH
netbaiot auth invalidate --device DEVICE --tenant TENANT --product PRODUCT
```

全局参数可在子命令前后：`--endpoint`、`--token`、`--api-key`、`--event-address`、
`--event-token`、`--output human|json`。对应环境变量为 `NETBAIOT_ENDPOINT`、
`NETBAIOT_TOKEN`、`NETBAIOT_API_KEY`、`NETBAIOT_EVENT_ADDRESS`、`NETBAIOT_EVENT_TOKEN`，
设备命令另支持 `NETBAIOT_TENANT`/`NETBAIOT_PRODUCT`。显式凭据优先，否则 TOKEN 优先于
API_KEY。生产使用受保护 secret source，避免命令行凭据进入进程列表；未提供 token-file。
Token 不出现在帮助、Debug 或错误中。

Clap 拒绝未知参数/命令、重复 singleton、缺值、非法标识符/地址/输出格式，以及同时传
token/API-key 或 JSON/file。`--help`/`--version` 成功返回 0；Drain 必须有 `--yes`。
手动 ACK 前刷新事件输出，取消订阅停止所属任务；不增加离线命令队列或盲目重试。
`--output json` 的 stdout 只包含 JSON/JSONL，日志与错误走 stderr；Demo 保持交互式输出。

退出码：0 成功、2 用法/配置、3 认证、4 禁止访问、5 设备离线、6 不可用/运行时/I/O。
兼容 server 保留原非零失败约定、默认/位置配置路径与 `--print-default-limits`。
项目初始化见[配置](configuration.zh-CN.md)，环境诊断见 [doctor](doctor.md)，
开发维护另见 [cargo xtask](maintenance.md)。
