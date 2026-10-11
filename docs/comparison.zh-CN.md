# 按项目定位比较

[English](comparison.md)

以下项目解决相邻但不同的问题。这是定位说明，不是功能排名；产品版本与部署选项会变化，
选型时还应查阅各项目当前官方文档。

| 项目 | 主要定位 |
| --- | --- |
| [Eclipse Mosquitto](https://mosquitto.org/) | 面向 MQTT 客户端与应用的轻量通用 MQTT broker |
| [EMQX](https://docs.emqx.com/en/emqx/latest/) | 提供 broker 集群和多种部署选项的 MQTT 消息平台 |
| [ThingsBoard](https://thingsboard.io/docs/) | 集设备管理、数据采集处理、可视化和规则工作流于一体的 IoT 平台 |
| NetbaIoT | 放在现有业务后端之前的单节点设备入口网关与事件路由器；接收 MQTT/TCP/UDP，输出统一 `DeviceEvent` |

NetbaIoT 不打算替代所有 MQTT broker 或 IoT 平台。需要聚焦 MQTT broker 时可选择
Mosquitto；集群 MQTT 消息是核心需求时可评估 EMQX；需要一体化设备/应用能力时可选择
ThingsBoard 一类平台。当业务后端已经存在，缺少的是资源有界、多传输接入和明确投递
语义时，可以考虑 NetbaIoT。

本网关不提供横向集群、仪表盘、时序数据库、OTA 管理或规则引擎 UI，也不支持 MQTT over
WebSocket、共享订阅、MQTT-SN 或 bridge 模式。集成决策前请阅读
[当前限制](../README.zh-CN.md#当前限制)与[协议支持](protocol-support.zh-CN.md)。

以上外部项目描述依据其官方概述： [Mosquitto](https://mosquitto.org/)、
[EMQX 文档](https://docs.emqx.com/en/emqx/latest/)与
[ThingsBoard 文档](https://thingsboard.io/docs/)。
