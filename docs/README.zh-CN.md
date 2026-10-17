# 文档导航

[English](README.md) · [项目介绍](../README.zh-CN.md)

先读[快速开始](quick-start.zh-CN.md)，再按接入边界查阅参考。配置结构和默认值以从 Rust
生成的 [Schema](schema/netbaiot-config.schema.json) 与[字段参考](configuration-fields.md)为准。

| 任务 | 参考 |
| --- | --- |
| 启动与配置 | [快速开始](quick-start.zh-CN.md)、[端到端教程](getting-started.md)、[配置](configuration.zh-CN.md)、[CLI](cli.zh-CN.md)、[环境诊断](doctor.md)、[配置示例清单](../configs/README.md) |
| 理解运行时 | [架构](architecture.zh-CN.md)、[投递语义](delivery-semantics.zh-CN.md)、[EventBus 实现（英文）](delivery-semantics.md#eventbus-ownership-and-accounting)、[资源预算](resource-budgets.md) |
| 设备接入 | [接入指南](device-integration-guide.md)、[协议矩阵](protocol-support.zh-CN.md)、[MQTT](mqtt.zh-CN.md)、[设备封包](device-protocol.zh-CN.md)、[设备 SDK](device-sdk.zh-CN.md) |
| Payload 解码 | [内置 Codec](codecs.zh-CN.md)、[厂商 Codec](vendor-codecs.zh-CN.md) |
| 业务集成 | [集成指南](business-integration.zh-CN.md)、[Business RPC V4](business-rpc.zh-CN.md)、[Rust 客户端](client.zh-CN.md)、[公共协议](public-protocol.zh-CN.md) |
| 管理与安全 | [HTTP API](http-api.zh-CN.md)、[管理认证](management-auth.zh-CN.md)、[认证缓存（英文）](authentication-cache.md)、[控制面](control-plane.zh-CN.md)、[安全](security.zh-CN.md)、[UDP 密钥配置（英文）](device-key-security.md) |
| 部署与恢复 | [运维指南](operations-guide.md)、[故障排查](troubleshooting.md)、[平台支持](platform-support.zh-CN.md)、[优雅重启](graceful-restart.zh-CN.md)、[Spool](restart-spool.zh-CN.md)、[MQTT 恢复（英文）](mqtt-session-recovery.md) |
| 验证与维护 | [维护命令（英文）](maintenance.md)、[MQTT 一致性（英文）](mqtt-3.1.1-conformance.md)、[测量范围](benchmarks.zh-CN.md)、[生产验证](production-readiness.zh-CN.md)、[测量证据](performance/README.md)、[复现脚本](../scripts/perf/README.md) |

中英文配对参考共用契约。`getting-started.md`、`device-integration-guide.md`、
`operations-guide.md` 与 `troubleshooting.md` 沿用不带语言后缀的中文文件名。

## 版本与升级

文档跟随 `main`，当前 Workspace 版本为 `0.4.0`。
[v0.4.0](releases/v0.4.0.md) 要求 Business RPC V4，并采用不带版本号的 Rust API、配置与监控命名。
业务客户端应与网关一起升级，V3 及更早版本的握手会被拒绝。

整数精确 Decimal 转换与配置诊断自 [v0.3.4](releases/v0.3.4.md) 引入，`v0.4.0` 继续包含这些改动。
从 `v0.3.3` 或更早版本升级时，加载现有厂商 Codec 配置前请完成[整数转换迁移](migration/integer-decimal.zh-CN.md)。

当前仅支持 Business RPC V4、MQTT 恢复 NBMQ v6 与 EventBus 恢复 NBSP v3。
旧版或未知版本直接失败，不自动回退或迁移；升级前阅读[协议升级指南](migration/current-protocol-only.zh-CN.md)。
MQTT 3.1.1/5.0 与管理 `/api/v1` 继续支持。

目录保留[当前发布说明（英文）](releases/v0.4.0.md)、此前的 [v0.3.4](releases/v0.3.4.md)
与 [v0.3.3](releases/v0.3.3.md) 说明，以及[发布模板](release-template.md)。
其他发布历史通过 Git 及 [GitHub Releases](https://github.com/sskycn/netbaiot/releases)查阅。

## 精简与维护约定

每个主题保留一份参考，教程链接参考，避免重复维护参数或协议说明。保留可运行示例、生成参考、
升级指南和测量来源；删除重复概览、已完成任务报告与过时说明，移除内容仍可从 Git 历史查阅。
已发布测量依赖的源码哈希、Manifest 和验证记录应保留，日期报告只代表记录的 revision。
新增原始日志放在被忽略的 `target/` 下。

修改后运行[文档检查](maintenance.md#documentation-checks)。检查覆盖本地链接、标题锚点和
生成参考漂移；外部 URL、语义声明与生产容量需要另行审核和证据。
