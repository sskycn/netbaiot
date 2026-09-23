# 网关控制平面

网关控制负责凭据、可信身份、权限、auth generation、产品/codec 映射、路由及已安装 sink。
`GatewayControl` 通过 `Arc` 共享不可变快照，与认证缓存分别设限。
`ControlSnapshot` 仅包含 `revision`、`products`、`routes`，不存储设备业务期望或上报状态。

启动时校验控制快照、构建 sink/路由并恢复已提交的重启工作，然后进入 ready。
外部认证请求有超时和并发边界；MQTT/TCP 会话绑定认证结果，普通报文不调用 provider。

快照替换校验 revision、产品键唯一性、非零 profile/codec 版本、产品数量和序列化字节数。
管理更新串行校验已安装 sink 和 fanout 上限，再修改控制/路由状态。
仅替换路由会保留产品映射；过期或超限更新不改变现有快照。
默认 `control_max_products=4096`、`control_max_bytes=16 MiB`、`max_routing_filters=256`。
认证缓存及控制快照在重启后重建，不写入投递 spool。

产品映射元数据不会覆盖已建立会话的不可变认证 codec 绑定；撤销会话仍使用 auth invalidation。

设备配置持久化、desired/reported revision、历史、重试、发布/回滚及离线协调由业务系统负责。
在线变更使用普通 MQTT/TCP `DeviceCommand`，结果通过 `CommandAck` 返回。
网关不解释命令名，也不比较业务 revision。详见[迁移说明](remove-device-config.md)。
