# SDK 生态

NetbaIoT 提供一个公共 wire 模型 crate 和两个用途明确的客户端：

- `netbaiot-protocol`：可移植的公共 ID、消息、版本、路径和错误类型；
- `netbaiot-client`：业务事件消费和管理 API；
- `netbaiot-device-sdk`：可选的标准 MQTT 3.1.1 / MQTT 5.0 便捷客户端；
- `netbaiot-cli`：完全基于 `netbaiot-client` 的运维/调试客户端。

两个客户端都不依赖 `netbaiot-runtime`、`netbaiot-transports` 或 `netbaiot-server`。协议 crate 不依赖任一客户端。这使未来的 Go、Java、Python、TypeScript 和 C/C++ 客户端也能使用同一公共契约。

服务端仍是无数据库的连接和实时路由进程。客户端 crate 不增加持久化、离线命令存储、后台 runtime 线程或无界队列。标准 MQTT 客户端始终受完整支持；设备 SDK 只是便捷工具，并非专有接入要求。

编译示例：

```bash
cargo check --workspace --all-targets
```

业务示例位于 `crates/netbaiot-client/examples`；设备示例位于 `crates/netbaiot-device-sdk/examples`。

UDP 详见 [NBI1/NBA1 可靠上行](device-protocol.zh-CN.md#udp-acknowledgement-nba1)。Rust 设备 SDK 仅使用 MQTT；Python UDP 示例演示有界原包重试和签名接纳回执。
