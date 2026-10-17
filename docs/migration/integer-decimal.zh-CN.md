# 尚未发布的 main：厂商整数转换迁移

[English](integer-decimal.md)

本说明适用于公开 `v0.3.3` 之后的 `main`，workspace 版本仍为 `0.3.3`。
它不改写该 Release 的历史行为，也不改变设备传输封包。见[版本状态](../README.zh-CN.md)。

遥测、Event 值和 Command 参数中的整数**值字段**统一使用精确 Decimal 运算。
删除旧数字型 `scale` 和 `bias` 成员，包括显式 `null`、`1`、`0`。例如：

```json
{"offset":9,"type":"i16","conversion":{"mode":"decimal_exact","scale":"0.1","bias":"0"}}
```

恒等转换省略 `conversion`。迁移旧浮点值时先确认预期的十进制值，不要机械地转成字符串。
启动时证明完整整数区间同时满足带检查算术、公共 Decimal 64 字节和
`CodecLimits.field_bytes`；不能满足全部约束的配置会被拒绝。
`f32/f64` 保留 IEEE 754 行为与数字型 `scale/bias`，显式 null 仍等同省略。
浮点、文本和 bool 不可使用 `decimal_exact`。

`source_message_id` 是独立且不缩放的元数据。也要删除其整数旧成员，但不能添加
非空 `conversion`。无符号 ID 支持完整 `u64`；固定 UTF-8 ID 遵守公共 1–64 字节
ASCII 标识符字符规则，不受业务 Scalar 的 Decimal 输出长度规则约束。

整数值解码返回 `Scalar::Decimal`，恒等映射也一样。命令参数建议使用
`{"decimal":"1.001"}` 保留精度。有限 `Scalar::Number` 便捷输入使用最短可往返
十进制表示，不能恢复此前已丢失的 f64 精度；普通文本不是数值输入。
JSON、CBOR、MessagePack 使用显式 Decimal 对象；Protobuf 使用既有 Scalar oneof
字段 6（`string decimal`）。本次诊断修复不改变字段编号或 wire 编码。
从公开 `v0.3.3` 升级时，Rust 消费端的穷举匹配需要处理新增 `Scalar::Decimal`。

使用同一份 `main` 源码编译的 binary 检查：

```bash
cargo run --locked -p netbaiot-cli -- config check --config configs/vendor-codec.json
cargo run --locked -p netbaiot-cli -- --output json config check --config configs/vendor-codec.json
```

已识别的整数旧参数返回 `NBI-CFG-016`，两种输出均含完整成员路径和迁移建议。
非法 JSON、错误字段类型/浮点值和未知字段保留既有诊断类别；编译错误使用
`NBI-CFG-015`。诊断不包含输入值和未知字段名，不启动监听器、不访问 provider、
不修改恢复状态。变更 Codec profile 需正常排空重启；已接受事件重放保留 `event_id`，
业务消费者继续负责幂等。
