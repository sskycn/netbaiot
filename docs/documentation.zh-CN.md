# 文档状态与权威参考

[English](documentation.md)

**2026-10-17** 核对基线为 `origin/main` 的 `a26ab86`。
[Cargo.toml](../Cargo.toml) 的 workspace 版本仍为 `0.3.3`；当天发布的
[v0.3.3](https://github.com/sskycn/netbaiot/releases/tag/v0.3.3) 是最新公开 Release。
文档与示例跟随 `main`，其中已包含该标签之后尚未发布的整数精确 Decimal 修改
（`028b358` 至 `a26ab86`）；本文描述的诊断修复也尚未发布。
不能仅凭 Cargo 版本推断 Release 内容。见[整数迁移](migration/integer-decimal.zh-CN.md)。

## 阅读顺序与权威来源

| 主题 | 权威参考 | 配套指南 |
| --- | --- | --- |
| 首次体验与初始化 | [快速开始](quick-start.zh-CN.md)、[配置](configuration.zh-CN.md) | [教程](getting-started.md)、[CLI](cli.zh-CN.md) |
| 运行职责与接纳 | [架构](architecture.zh-CN.md)、[投递语义](delivery-semantics.zh-CN.md) | [完整用户指南](user-guide.md) |
| 设备 wire 契约 | [协议矩阵](protocol-support.zh-CN.md)、[设备封包](device-protocol.zh-CN.md)、[MQTT](mqtt.zh-CN.md) | [设备接入](device-integration-guide.md) |
| Payload 与精确数值 | [内置 Codec](codecs.zh-CN.md)、[厂商 Codec](vendor-codecs.zh-CN.md) | [整数迁移](migration/integer-decimal.zh-CN.md) |
| 业务与在线命令 | [Business RPC V3](business-rpc-v3.zh-CN.md)、[公共协议](public-protocol.zh-CN.md)、[客户端](client.zh-CN.md) | [业务集成](business-integration.zh-CN.md) |
| 配置字段与默认值 | [生成 Schema](schema/netbaiot-config.schema.json)、[生成参考](configuration-fields.md) | [可运行配置](../configs/development.json)、[厂商示例](../configs/vendor-codec.json) |
| 安全与运维 | [管理认证](management-auth.zh-CN.md)、[安全](security.zh-CN.md)、[资源预算](resource-budgets.md) | [运维](operations-guide.md)、[故障排查](troubleshooting.md) |
| 恢复与升级 | [Spool](restart-spool.zh-CN.md)、[MQTT 恢复（英文）](mqtt-session-recovery.md)、[协议迁移](migration/current-protocol-only.zh-CN.md) | [优雅重启](graceful-restart.zh-CN.md) |
| 测试与测量 | [维护命令（英文）](maintenance.md)、[基准](benchmarks.zh-CN.md)、[生产验证](production-readiness.zh-CN.md) | [证据规则与索引](performance/README.md)、[复现脚本](../scripts/perf/README.md) |

中英文配对参考共用契约并互相链接。部分较长中文指南沿用不带 `.zh-CN` 的文件名，
这只是命名约定，不代表另一套英文规范。简短概览链接至详细参考，避免重复定义不同语义。

## 保留历史与复现检查

保留 `docs/releases/` 的全部发布说明、`docs/migration/` 的兼容性指南，以及
`docs/performance/` 的测量、Manifest 与验证记录。其版本、源码哈希、测量窗口限定
适用范围；日期报告中的“当前”指当时 revision，不是今天的 `main`。
更新当前说明时不改写原始证据或替换其哈希。本次审核没有删除历史文件。

```bash
python3 scripts/check_docs.py --inventory target/docs-audit/inventory.json
cargo xtask schema --check
cargo xtask config-reference --check
cargo xtask check evidence
cargo xtask check release --part preflight
```

清单枚举根目录、docs/configs/scripts/.github 中全部 Markdown 与 `configs/` 的全部
JSON，区分当前参考、生成参考、兼容性指南与历史证据。检查器验证本地目标和标题锚点，
不验证外部 URL、远端分支内容或语义声明。Preflight 会运行该检查和单元测试。
可运行服务配置使用 `netbaiot config check`；纯 limits、独立 Codec 和 loadgen 配置
分别使用对应类型的解析器，与 CLI Schema 和负载工具测试一致。Command-readiness profile
是 harness 输入，不是独立服务配置，详见[示例清单](../configs/README.md)。只有 Rust 配置结构或
描述改变时才重新生成 Schema/参考。

CI 成功、短 Smoke、loopback 基准、完整长期 Soak 是不同类别的证据，都不能单独
认证生产容量。必需 Sink ACK、设备 EventAccepted、计划重启重放与突然崩溃丢失必须
分别理解。命令要求本节点在线会话，当前没有多节点会话路由。
