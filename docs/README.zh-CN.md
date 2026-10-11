# NetbaIoT 中文文档导航

本页按使用场景组织中文文档。当前 workspace 版本为 `0.2.4`，最低 Rust 版本为
`1.88`。除明确标注为历史报告的页面外，以下文档描述当前实现。

> 当前只支持 Business RPC V3、MQTT 恢复 NBMQ v6 和 EventBus 恢复 NBSP v3。
> 旧版或未知协议/恢复文件不会自动回退或迁移。升级现有部署前先阅读
> [当前协议升级指南](migration/current-protocol-only.zh-CN.md)。

## 第一次使用

1. [5 分钟快速开始](quick-start.zh-CN.md)：单命令 demo，或手动发布第一条 MQTT 事件。
2. [10 分钟端到端教程](getting-started.md)：启动 webhook、接收事件、下发在线命令。
3. [完整用户指南](user-guide.md)：理解接纳、确认、幂等和组件边界。
4. [配置与项目初始化](configuration.zh-CN.md)：`init`、JSON Schema、`config check`。
5. [CLI](cli.zh-CN.md)与[环境诊断](doctor.md)：检查配置和运行状态。

## 设备与业务集成

- [设备接入指南](device-integration-guide.md)：身份、codec、MQTT/TCP/UDP 和设备 SDK。
- [协议支持矩阵](protocol-support.zh-CN.md)：当前支持与明确不支持的能力。
- [MQTT 3.1.1 / 5.0 profile](mqtt.zh-CN.md)与[设备 wire 格式](device-protocol.zh-CN.md)。
- [业务系统集成指南](business-integration-guide.md)：webhook、Business RPC V3、命令和错误处理。
- [业务 RPC V3](business-rpc-v3.zh-CN.md)、[业务客户端](client.zh-CN.md)、
  [设备 SDK](device-sdk.zh-CN.md)。
- [管理 HTTP API](http-api.zh-CN.md)与[管理认证](management-auth.zh-CN.md)。

## 正确性、可靠性与生产运行

- [投递语义](delivery-semantics.zh-CN.md)：`EventAccepted`、required sink 与重复投递。
- [可靠性与恢复](reliability.zh-CN.md)、[优雅重启](graceful-restart.zh-CN.md)、
  [恢复文件格式](restart-spool.zh-CN.md)。
- [运维与生产部署](operations-guide.md)、[安全](security.zh-CN.md)、
  [故障排查](troubleshooting.md)。
- [平台支持](platform-support.zh-CN.md)、[资源预算](resource-budgets.md)、
  [控制面](control-plane.zh-CN.md)。

## 架构与项目定位

- [架构](architecture.zh-CN.md)、[设计理念](design-philosophy.zh-CN.md)、
  [按项目定位比较](comparison.zh-CN.md)。
- [公共协议](public-protocol.zh-CN.md)与[配置字段参考](configuration-fields.md)。
- [基准与测量说明](benchmarks.zh-CN.md)。历史性能和审计报告只证明其标注的
  commit、环境与负载，不能当作当前生产容量。
- [v0.2.4 中文发布说明](releases/v0.2.4.zh-CN.md)。

## 文档约定

部分早期中文主文档沿用不带 `.zh-CN` 的文件名，例如 `getting-started.md`、
`operations-guide.md` 和 `troubleshooting.md`；它们仍是中文当前文档。带日期、
baseline、audit、report 或 performance 路径的页面通常是历史证据，保留当时术语与
数据。部署决策应以本导航中的当前文档、代码、配置 schema 和测试为准。
