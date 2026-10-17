# 厂商自定义 Payload Codec

[English](vendor-codecs.md)

厂商 Codec 复用同步 `DeviceCodec` 与 `CodecRegistry`，只负责 Payload 转换。
认证、传输封包、ACK、EventAccepted、背压和命令排队继续由现有模块负责。
认证结果及产品控制配置明确选择 `codec_id`／`codec_version`，不猜测格式、
不在解析失败后尝试其他 Codec。现有 JSON、CBOR、MessagePack、Protobuf 不变。

## 原生 Rust 接入

在独立版本模块实现 `DeviceCodec`：上行恰好生成一个事件，身份取自
`DecodeContext`，保留来源消息标识；下行保留完整 `command_id`。
`validate_payload` 与 `decode` 应共用完整解析函数，预验证不生成事件 ID。
先限制字节、字段和分配，再访问数据。原生实现自行负责这些约束；注册接口
不为任意 Rust 代码提供沙箱。

`vendor::demo::AcmeV1` 是独立实现的原生参考，支持示例遥测及
`set_temperature` 命令，须显式注册。声明式示例还覆盖心跳、事件和命令执行回执。

```rust,ignore
use netbaiot_codecs::vendor::{demo::AcmeV1, registration};
use netbaiot_core::CodecLimits;
use std::sync::Arc;

config.native_codecs.push(registration(
    "vendor-acme", 1,
    Arc::new(AcmeV1::new(CodecLimits::default())?),
)?);
config.validate()?;
// netbaiot_server::run(config, cancellation).await?;
```

`Config.native_codecs` 是 Rust 组合入口，不参与 JSON 序列化或 Schema，JSON
不能请求加载原生代码。必须在校验和启动之前注册；重新读取配置时要重新注册。
独立 CLI 的 `config check` 只认识内置 Codec 和 JSON 声明；定制宿主调用自身
配置的 `validate()`。自行组合运行时也可把注册元组加入 `builtins(limits)`，
再传给 `CodecRegistry::new`。

厂商 ID 使用 `vendor-<名称>`，沿用现有 ID 字符规则和 64 字节上限；版本必须
大于零。同一 ID 可同时注册不同版本。重复 ID／版本、原生与声明式冲突、
覆盖内置 Codec 都会在启动时失败。总计最多 64 个注册项，其中四个为内置项。
可信认证和产品配置必须引用相同 ID／版本。切换运行中的产品协议需要排空、
重启并更新认证配置，不能偷偷改变已认证连接的解释规则。

可运行的组合示例：`cargo run -p netbaiot-server --example native_vendor`。

## 声明式二进制协议

完整定义见 [acme-v1.json](../configs/codecs/acme-v1.json)，可启动配置见
[vendor-codec.json](../configs/vendor-codec.json)。后者将声明嵌入
`payload_codecs` 并配置开发凭证。这是虚构示例协议，不代表商业设备兼容性。

服务器在监听网络之前检查配置并编译字段区间及消息／命令查找表。连接共享
只读计划；运行时直接读取 Payload 切片，不解析配置、不复制整帧、不做 I/O
或创建任务，只分配标准化输出字段、标识和有界下行缓冲。

| 配置 | 含义 |
| --- | --- |
| `id`、`version` | 认证绑定的厂商协议 ID／版本 |
| `header` | 从偏移 0 开始的 1～64 字节固定头 |
| `message_type` | 用于选择上行 `tag` 的无符号整数字段 |
| `length` | 可选长度字段；线上的值加有符号 `adjustment` 必须等于整个 Payload 长度 |
| `messages` | 唯一类型值、精确 `size`、必填来源标识、可选时间戳、事件映射及校验和 |
| `commands` | 唯一命令名／类型值、精确长度、完整 16 字节命令 UUID、可选过期时间、参数及校验和 |

所有偏移均相对于 Payload，不含 MQTT/TCP/UDP 封包。每种消息长度固定且精确，
不同类型可有不同长度。未映射区间为保留字节，必须为零；多余尾部、截断、未知
类型、重叠和越界都会拒绝。TCP 分帧仍归传输层。本版不支持变长数组、位域、
嵌套结构或工业链路封包；复杂布局使用有界原生 Codec。不引入 WASM、动态库或脚本。

整数元数据支持 `u8/u16/u32/u64`；普通字段另支持 `i8/i16/i32/i64`、
`f32/f64`、`bool`、`utf8`。`endian` 可为 `big`（默认）或 `little`。
UTF-8 必须配置精确字节 `length`，不带填充和终止符；其他类型禁止 `length`。
文本拒绝控制字符，布尔只接受 0／1，浮点拒绝 NaN／无穷。

数值映射是 `wire * scale + bias`，默认比例 1、偏移 0。比例必须有限且非零，
偏移和结果必须有限；文本和布尔禁止缩放。标准 `Scalar::Number` 使用 f64，
因此普通整数值超出 ±2^53 时拒绝；来源 ID、心跳序列和整数元数据保留完整 u64。
下行执行逆变换，检查整数范围，拒绝小数截断，只允许很小的浮点计算舍入误差
（相对原始信号量级，且不超过 1e-7 个原始单位）来选取整数或 f32 候选值；
该预算随原始信号趋近零，因此非零原始值不得舍入或下溢为零。写入后按相同
归一化规则回读：未缩放数值必须精确相等；缩放数值另用数值量级四倍 f64
机器精度的相对预算校验乘加舍入，非零归一化信号不得塌缩为 bias。把原始域
量化和归一化算术分开校验，可让带 bias 的普通
十进制缩放通过，同时拒绝较大的量化误差、溢出、下溢及精度损失。
参数名、数量和类型必须精确匹配，没有隐式默认值或忽略额外参数。

来源标识为不缩放的无符号整数十进制文本，或最长 64 字节、符合现有 ID 规则
的固定 UTF-8 字符串。厂商须确保同一设备的来源 ID 在各种消息类型间保持唯一。
Payload 不改变认证身份。正式解码生成新事件 ID；已接受事件的 EventBus 重试
和恢复重放仍保留该 ID。`occurred_at` 和 `expires_at` 单位为 Unix 毫秒，
不得超出公共时间戳范围。`expires_at` 必须使用 `u64`，覆盖完整的非负 i64
毫秒范围（0 到 2^63−1），较窄类型在编译时拒绝。配置了过期字段但命令未携带期限时编码为零；无论
线上是否包含过期字段，网关命令 TTL／资源准入继续生效。

事件映射支持：

- `telemetry`：非空、唯一字段名的标量集合。
- `heartbeat`：完整无符号 `sequence`。
- `event`：固定非空名称及可选标量值。
- `command_ack`：完整 RFC 4122 字节序 UUID，及三个不同整数对应
  `running/succeeded/failed`，其他状态拒绝。它表示设备执行，区别于传输 SENT
  和 EventAccepted。

校验算法为 `sum8`（模 256 加和）、`xor8`、`crc16_modbus`。CRC 参数为初始值
FFFF、反射多项式 A001、xorout 0000；标准向量 `123456789` 的结果为 4B37。
校验值默认大端；常规 Modbus 存储须显式选择 little。覆盖范围 `start` 包含、
`end` 不包含，必须非空且排除校验字段自身；仅覆盖配置的区间。校验和不能
替代认证，现有 TLS/HMAC 保护不变。

## 资源和运行语义

每个协议最多 64 个消息／命令布局，总帧长不超过 64 KiB，每个遥测／命令布局
最多 64 字段，字段名及文本最长 256 字节，并继续受 `CodecLimits` 更小限制。
所有偏移加法检查溢出；结构预算按每布局 512 字节、每值字段 128 字节及其
线长／名称字节的三倍计入 `decoded_bytes`。所有编译计划另计区间元数据，
每个协议总预算不超过 256 KiB。该预算不是进程 RSS 测量值。

独立 `from_json` 接口限制配置 256 KiB；服务器使用现有 16 MiB 配置文件上限、
最多 60 个自定义协议以及相同编译预算。Schema 校验类型和简单范围，
`netbaiot config check` 进一步检查重叠、唯一性、长度和跨字段预算，错误定位到
`payload_codecs[0].commands[0].expires_at.type` 等配置路径；不输出配置值或未知字段名。

命令布局在启动时检查 `max_command_bytes`。所有 Codec（含原生实现）的编码结果
在入队前还需符合会话传输容量：TCP 帧／写缓冲上限，或含主题、封包及过期属性的
MQTT 包／写缓冲上限，以及 MQTT 5 客户端声明的 Maximum Packet Size。
MQTT 即使使用 QoS0 也预留两字节报文标识符，避免重订阅为 QoS1/2 后已入队命令无法发送。
超限命令在占用队列前拒绝，不会因此断开设备连接。

QoS2 预验证与正式解码共用同一函数，包含来源标识、校验和、保留位、数值边界
和执行状态。预验证不是 EventAccepted；所有必需 sink 的原子预留、QoS1 PUBACK、
TCP 接受回执和 UDP NBA1 的语义保持不变。

## 验证及性能

```bash
cargo test -p netbaiot-codecs --all-features
cargo test -p netbaiot-transports --test multi_codec
cargo test -p netbaiot-server --lib vendor
cargo bench -p netbaiot-codecs --bench vendor
cargo +nightly fuzz run vendor_codec -- -max_total_time=30 -max_len=65537
cargo +nightly fuzz run vendor_config -- -max_total_time=30 -max_len=262145
```

基准分别测量原生／声明式解码、QoS2 预验证、编码、拒绝路径及启动编译，输出
耗时、分配次数和累计分配字节。热路径样本不包含编译时间。这些是本地微基准，
不代表网关吞吐量、尾延迟、容量或生产性能承诺。
