# 控制平面与配置缓存

控制平面负责设备定义、凭据、产品/codec 绑定、设备配置、路由和 sink 定义。NetbaIoT 运行时只保留有界快照。

启动时会校验静态引导快照、创建 sinks 和路由、恢复已提交的重启 spool 记录，然后才进入 `RUNNING`/就绪状态。可配置外部 HTTP 认证 provider；其请求有超时和并发上限，不会无限重试。

`ControlSnapshot` 有单调递增的 revision，并包含产品、设备配置和路由。替换快照前会完整校验数量、字节数、唯一键、产品引用和 revision，然后再原子替换不可变索引快照。路由更新会串行执行，并在修改配置/事件路由状态前针对已安装的 sinks 完成校验。

设备配置值以 `Arc<DeviceConfigSnapshot>` 共享。设备 GET 使用 revision/ETag；设备应用结果通过独立的 `ConfigAck` 事件返回。重启后认证/配置缓存均为空，并且不会写入投递 spool。
