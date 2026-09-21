# `netbaiot` CLI

CLI 是运维/调试工具，也是 `netbaiot-client` 的自用客户端。它不会自行实现 HTTP 或流式协议。

可通过 `--endpoint`、`--token`、`--event-address`、`--event-token` 和 `--output human|json` 配置，也可使用 `NETBAIOT_ENDPOINT`、`NETBAIOT_TOKEN`、`NETBAIOT_EVENT_ADDRESS` 和 `NETBAIOT_EVENT_TOKEN` 环境变量。设备命令还接受 `NETBAIOT_TENANT` 和 `NETBAIOT_PRODUCT`。Token 不会打印。

已实现的命令：

```text
netbaiot server status
netbaiot server drain --yes
netbaiot device status DEVICE --tenant TENANT --product PRODUCT
netbaiot events subscribe [--tenant ...] [--product ...] [--device ...] [--type ...]
netbaiot command send DEVICE --json JSON
netbaiot command send DEVICE --payload-file PATH
netbaiot config get DEVICE
netbaiot config set DEVICE --file PATH --revision N
netbaiot auth invalidate --device DEVICE
```

手动 ACK 前会先刷新事件输出。`--output json` 时 stdout 只包含 JSON/JSONL。Drain 命令必须带 `--yes`。退出码：0 成功、2 用法错误、3 认证失败、4 禁止访问、5 设备离线、6 不可用或其他运行时故障。
