# Project comparison by intended role

[中文](comparison.zh-CN.md)

These projects solve related but different problems. This is a description of
their intended roles, not a feature ranking. Product editions and deployment
options change; check each project's current documentation for requirements.

| Project | Intended role |
| --- | --- |
| [Eclipse Mosquitto](https://mosquitto.org/) | Lightweight, general-purpose MQTT broker for MQTT clients and applications. |
| [EMQX](https://docs.emqx.com/en/emqx/latest/) | MQTT messaging platform with broker clustering and deployment options for larger MQTT workloads. |
| [ThingsBoard](https://thingsboard.io/docs/) | IoT platform for device management, data collection and processing, visualization, and rule-based workflows. |
| NetbaIoT | Single-node device ingress gateway and event router in front of an existing business backend. It accepts MQTT/TCP/UDP and emits normalized `DeviceEvent`s. |

NetbaIoT is not intended to replace every MQTT broker or IoT platform. Choose
Mosquitto when a focused MQTT broker is the requirement; evaluate EMQX when
clustered MQTT messaging is central; choose a platform such as ThingsBoard when
its integrated device and application capabilities fit. Consider NetbaIoT when
the business backend already exists and the missing piece is bounded multi-
transport ingress with explicit delivery semantics.

The gateway does not provide horizontal clustering, a dashboard, a time-series
database, OTA management, or a rule-engine UI. It also does not offer MQTT over
WebSocket, shared subscriptions, MQTT-SN, or bridge mode. See [current
limitations](../README.md#current-limitations) before making an integration
decision.

Project descriptions above follow the projects' own overviews: [Mosquitto](https://mosquitto.org/),
[EMQX documentation](https://docs.emqx.com/en/emqx/latest/), and
[ThingsBoard documentation](https://thingsboard.io/docs/).
