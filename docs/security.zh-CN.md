# 安全概述

[English](security.md)

未公开漏洞请遵循[安全报告政策](../SECURITY.md)，通过仓库的私密报告入口提交；不要在
公开 issue 中发布利用细节。

## 设备认证

MQTT 与 TCP 在建连时认证，并把不可变设备身份、credential version、permissions 和
codec 选择绑定到连接。普通报文/帧不会逐条调用远程 auth provider。认证缓存同时限制
条目数与字节数，支持显式失效；配置 provider 不可用时，cache miss 会 fail closed。

UDP 无连接。每个 NBI1 数据报包含 credential ID、credential version、时间戳、序号、
载荷与 HMAC-SHA256。网关检查凭据、时间容差、签名和有界重放窗口。数据报正文没有
加密：**已认证 UDP 不等于保密 UDP**。需要载荷机密性时应使用受保护网络。

## TLS 与 listener 边界

非 loopback 设备 TCP 必须启用 TLS。MQTT 与通用分帧 TCP 在 TLS 握手后共享设备 listener。
development 模式只允许 loopback 明文。管理 HTTP 使用独立 listener 和授权服务；设备
凭据永远不能获得管理权限。非 loopback 管理接口也必须启用 TLS 和授权 provider。

webhook 与 Business RPC/TCP 有各自的传输和认证配置。应按部署配置保护这些路径；
设备凭据不是业务服务身份。

## 密钥与授权状态

日志、metric label 或支持材料中不得包含密码、token、HMAC key、原始凭据、Authorization
header 或完整敏感 spool 记录。凭据应由受保护的 secret source 注入，不能提交到教程外的
生产配置。仓库教程密钥是公开开发 fixture。

管理接口支持全局 bootstrap token、带 scope 的 API Key、使用配置 JWKS 的 RS256 JWT，
以及可选管理 mTLS 身份映射。bootstrap token 具有 Global 权限；迁移到 scoped identity 后
应禁用。管理授权与设备认证相互独立。

Auth cache 与网关控制快照相互分离、各自有界，并在重启后重建。认证失效会栅栏 stale
session registration，同时删除匹配的有界持久 MQTT 会话状态。

## 资源与解析器保护

所有网络、管理、MQTT、codec、spool 和 sink 输入都按敌意输入处理。长度在分配前检查；
buffer、请求、队列、缓存、重试、连接、订阅、retain、replay 和恢复记录都有硬数量/字节
上限。MQTT 使用增量解析、严格 UTF-8、报文 deadline 和 Remaining Length 上限。TCP
framing、UDP 数据报与管理 HTTP 使用相互独立的限制。

配置细节见[设备协议](device-protocol.zh-CN.md)、[MQTT profile](mqtt.zh-CN.md)、
[管理认证](management-auth.zh-CN.md)与[运维指南](operations-guide.md)。
