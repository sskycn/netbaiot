# Configuration examples / 配置示例

These files target `main`; its exact integer Decimal changes are
[unreleased since v0.3.3](../docs/README.md#versions-and-upgrades).
示例跟随 `main`；整数精确转换尚未包含在公开 `v0.3.3` 中，见[迁移](../docs/migration/integer-decimal.zh-CN.md)。
All device credentials in these fixtures are public development values.

| Files | Parser / use | 注意事项 |
| --- | --- | --- |
| [development.json](development.json) | `netbaiot config check --config configs/development.json` | Loopback + in-process audit sink；无业务持久化 |
| [tutorial.json](tutorial.json) | Server Config / CLI check | Requires the tutorial webhook；需先运行示例接收器 |
| [multi-codec.json](multi-codec.json) | Server Config / CLI check | Four built-in V1 codecs on separate products；开发凭据 |
| [vendor-codec.json](vendor-codec.json) | Server Config / CLI check | Embeds the standalone vendor plan；整数使用 decimal_exact |
| [codecs/acme-v1.json](codecs/acme-v1.json) | `DeclarativeCodec::from_json` | Standalone fictional wire layout, not a server Config；不交给 config check |
| [resource-limits.json](resource-limits.json) | `Limits` / `netbaiot config limits` | Complete current defaults, not measured capacity；不可单独启动服务 |
| [business-rpc-load.example.json](business-rpc-load.example.json) | `business_rpc` loadgen Config | Requires a prepared gateway；不是 server Config |
| [smoke-30m](business-rpc-production-readiness/smoke-30m.json), [readiness-1h](business-rpc-production-readiness/readiness-1h.json), [optional-6h](business-rpc-production-readiness/optional-6h.json) | `business_rpc` loadgen Config, `topology: rpc_dual` | TLS paths are placeholders; supply real files and gateway first；配置时长不表示已经完成测试 |
| [smoke-60s](business-rpc-command-readiness/smoke-60s.json), [fault-storm-20s](business-rpc-command-readiness/fault-storm-20s.json), [readiness-30m](business-rpc-command-readiness/readiness-30m.json), [optional-1h](business-rpc-command-readiness/optional-1h.json) | Profile input to [command readiness harness](../tools/netbaiot-loadgen/run_business_rpc_command_readiness.py) | Harness generates gateway/loadgen configs; these are not standalone configs；require_* 是测试断言，不是服务端字段 |

CLI Schema tests parse and validate all four server examples, compare the limits
snapshot with `Limits::default()`, and compare embedded/standalone vendor plans.
Loadgen tests parse and validate its four complete examples without opening TLS
files or running their declared load durations. Command profiles are read by the
Python harness, which supplies addresses, TLS and other required fields.

Default refresh / 默认值更新：

```bash
cargo run --locked -p netbaiot-cli -- config limits > configs/resource-limits.json
cargo xtask schema --check
cargo xtask config-reference --check
```

Production values require protected credentials, real TLS, target-specific resource
measurement and confirmed business sinks. See [configuration](../docs/configuration.md)
and [operations](../docs/operations-guide.md).
