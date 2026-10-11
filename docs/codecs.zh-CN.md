# 设备载荷 Codec

[English](codecs.md)

服务器启动时统一注册四种同步 Codec，所有上行均生成现有 `DeviceEvent`，
下行由在线设备绑定的认证信息选择编码。每条上行只生成一条事件。

| Codec ID | 版本 | 上行 | MQTT/TCP 下行 |
| --- | --- | --- | --- |
| netbaiot-json | 1 | Telemetry / DeviceEvent / Heartbeat / CommandAck | 原有 JSON DeviceCommand |
| netbaiot-cbor | 1 | 同上 | 原生 CBOR Map |
| netbaiot-msgpack | 1 | 同上 | 原生 MessagePack Map |
| netbaiot-protobuf | 1 | 同上 | 原生 Protobuf DeviceCommand |

## 认证与产品配置

静态凭据、HTTP 认证及 Business RPC V3 均通过可信 `AuthenticatedDevice` 中的
`codec_id` / `codec_version` 选择 Codec。返回未知 ID、版本 0 或未注册版本时，
在建立会话前拒绝，并在最终注册的认证失效同步边界内再次检查。普通 MQTT/TCP
数据包不访问远程认证服务。UDP 每个数据报仍验证 HMAC、版本、时间戳与 replay。
载荷不能覆盖身份、选择 Codec 或触发自动探测、降级和失败后的其它解码尝试。

同一租户/产品只能配置一个 Codec ID/版本；静态冲突在启动、config check、doctor
报错。已配置产品与动态认证信息不一致时拒绝认证。控制快照中没有该产品时，支持的
动态认证结果决定不可变会话的编码。新增产品配置若与在线会话冲突，也原子拒绝。

动态控制快照完整验证所有 Codec，保留 revision 与原子替换机制。V1 显式拒绝在线
修改或删除已经配置的产品 Codec；失败保留原快照与会话。切换需要使受影响产品的
认证缓存/会话失效，重新配置认证，并安排 drain/计划重启。重新认证后的 MQTT
profile 与恢复状态的 Codec ID/版本、凭据版本、generation 或权限不一致时，原有
provenance 机制重置旧持久会话。禁止修改已有认证对象来偷偷切换编码。

[多编码配置示例](../configs/multi-codec.json) 使用原有配置字段，为四种编码分别
配置产品；公开凭据仅用于开发。没有增加配置字段或修改公共领域协议，JSON Schema
保持原样。配置检查中的错误不会显示凭据或载荷。

## JSON V1

[原有 JSON wire](device-protocol.zh-CN.md) 保持不变，包括字段、数值转换、UUID 文本、
可选时间戳、命令序列化、未知字段验证和大小限制。公开 `DeviceUplink` 仍是 JSON
便捷类型，不要求普通 MQTT 客户端使用 SDK。

## CBOR / MessagePack V1

两种格式均为原生文本键 Map，字段与 JSON 逻辑信封一致：

| 字段 | 类型与规则 |
| --- | --- |
| schema_version | 必填整数，必须为 1 |
| source_message_id | 必填，1–64 字节 namespace-safe ASCII 文本 |
| occurred_at | 可选，非负 i64 Unix 毫秒；null/nil 表示缺省 |
| kind | telemetry / event / heartbeat / command_ack |
| data | 对应消息类别的 Map |

CBOR 采用 RFC 8949 definite-length 子集：整数、有限 half/single/double 浮点、
布尔、UTF-8 文本、Map 和允许缺省处的 null。不支持 indefinite length、tag、
byte string、array、undefined 或其它 simple value。MessagePack 使用具名 Map、
整数、有限 float32/64、布尔、UTF-8 文本和可选 nil；拒绝 array、bin、ext 与保留标记。
Map 顺序与整数编码宽度不携带语义；重复键、未知字段、非法 UTF-8、尾随字节均拒绝。

| kind | data |
| --- | --- |
| telemetry | 非空 Map，键为非空字段名称，值为 Scalar |
| event | 必填非空 name；可选 value Scalar，null/nil 表示不存在 |
| heartbeat | 必填 sequence，无符号整数，支持完整 u64 范围 |
| command_ack | 必填 UUID 文本 command_id；execution 为 running/succeeded/failed |

Scalar 整数仅接受闭区间 **[-2^53, 2^53]**，精确转换为 Number(f64)；区间外的整数
表示一律拒绝，包括偶然能精确表示的整数。需要浮点语义时显式使用有限浮点表示。
有限浮点映射 Number，布尔映射 Boolean，UTF-8 文本映射 Text。Scalar 不支持
复杂结构、bytes、null/nil、非有限数和控制字符文本。名称/文本按 UTF-8 字节计限。
此严格整数策略仅适用于新二进制 Codec，JSON 原有策略保持不变。

下行 Map 字段固定为 schema_version=1、UUID 文本 command_id、device（含
 tenant_id/product_id/device_id 文本）、expires_at（毫秒或 null/nil）、payload
（name 与 arguments Scalar Map）。CommandRouter 继续负责 TTL、授权、在线检查和
有界队列。编码器检查目标身份、字段及编码长度。SENT 与设备执行 CommandAck 分开；
UDP 无下行，也没有离线命令队列。

## Protobuf V1

[正式 schema](../crates/netbaiot-codecs/proto/device_v1.proto) 在正常 Cargo 构建时由
锁定的 prost-build 与 protoc-bin-vendored 生成，无需用户安装 protoc 或手工生成。
`netbaiot_codecs::protobuf::v1::wire` 提供生成类型，公共 protocol crate 不引入这些
依赖；其他语言客户端从 `.proto` 生成自己的绑定。没有完整 JSON bytes 包装。

| 消息 | 稳定字段编号 |
| --- | --- |
| Uplink | schema_version=1，source_message_id=2，optional occurred_at=3；oneof telemetry=10 / event=11 / heartbeat=12 / command_ack=13 |
| Scalar | oneof double number=1 / boolean=2 / text=3 / sint64 signed_integer=4 / uint64 unsigned_integer=5 |
| Field | name=1，Scalar value=2 |
| Telemetry | repeated Field fields=1 |
| DeviceEvent | name=1，可选 Scalar value=2 |
| Heartbeat | uint64 sequence=1；省略时按 proto3 默认为 0 |
| CommandAck | UUID 文本 command_id=1，Execution execution=2 |
| DeviceKey | tenant_id=1，product_id=2，device_id=3 |
| DeviceCommand | schema_version=1，UUID 文本 command_id=2，device=3，optional expires_at=4，name=5，repeated Field arguments=6 |

Execution 枚举 UNKNOWN=0（拒绝），RUNNING=1，SUCCEEDED=2，FAILED=3。
Uplink 必须携带 schema_version=1、合法消息标识和一个已知类别；Scalar 必须包含一个
已知 value。Telemetry 用 repeated Field 保留重复名称检查能力，名称和值均必需。
整数 Scalar 范围同上；有限 double 直接映射 Number。未知枚举、缺失类别、非法字段
类型、非有限数、负 occurred_at 拒绝。

Protobuf 的未知字段采用可演进策略：wire type 0/1/2/5 在预算内跳过，不分配未知内容
副本，也不进入业务事件；仅包含未知类别时仍拒绝。已知 singular/oneof 重复字段明确
拒绝，不使用 last-one-wins 或 merge；repeated Field 合法但受数量限制，重复名字拒绝。
Groups、溢出 varint、越界长度均拒绝。该策略与 CBOR/MessagePack 的未知键规则不同。

Codec / schema 版本独立于 crate SemVer。禁止复用字段号、枚举值，删除后 reserve
名字与编号。兼容性新增 optional Protobuf 字段可以在预算内被 V1 忽略；改变语义或
必填字段需要新 Codec 版本。注册表支持同 ID 多个正整数版本，最多 64 个唯一组合。
设备通过可信认证迁移版本，禁止失败后换解码器。

## 资源边界与扩展

新增解码器在库分配前进行零分配预检：输入/decoded_bytes/output_messages，
长度与剩余字节，Map 字段数，UTF-8，结构深度与不支持类型。结构预算按每节点 128
字节、文本字节的 3 倍计费；Protobuf 未知 length-delimited 内容也计费。嵌套最多
min(nesting_depth,64)，Protobuf 按嵌套消息计数。预算累计，不能假定每项最大值能
同时使用；decoded_bytes 是保守的可变结构预算，不是进程 RSS 或栈测量。
独立 Codec 默认 64 KiB、64 字段、256 字节文本、深度 8、单事件；服务器保持已有
传输派生字节上限。UDP 1200 字节限制涵盖整个 NBI1 信封。

下行 writer 从小缓冲增长，到 decoded_bytes 停止；Protobuf 在复制命令结构前检查
结构预算，在输出分配前检查 encoded_len。Codec 无网络/磁盘访问、无任务/队列。
QoS2 预验证与正式 decode 使用同一解析函数。EventAccepted、Sink ACK、NBSP v3、
NBMQ v6 均不变；非计划故障仍可能丢失有界的未 spool 内存流量。

新厂商协议可放在 `src/vendor`，独立实现同步 `DeviceCodec`，共用验证，库解码前
完成有界预检，validate_payload 和 decode 使用同一 parser，身份来自 DecodeContext，
只输出一个事件，并实现 encode。在 builtins 中注册唯一 ID/版本，添加独立 wire
向量、边界测试、fuzz 和真实传输测试。工业协议 framing（例如 Modbus）属于独立
适配器，不注册为通用序列化 Codec。

## 可运行示例

```bash
cargo run -p netbaiot-codecs --example multi_codec
cargo test -p netbaiot-codecs
cargo test -p netbaiot-transports --test multi_codec
cargo build -p netbaiot-server
python3 tests/multi_codec_interop.py
cargo bench -p netbaiot-codecs --bench codecs
cargo +nightly fuzz run cbor_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run msgpack_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run protobuf_codec -- -max_total_time=30 -max_len=65536
```

固定向量由 tests/fixtures/generate.py 直接按规范独立生成，不使用 Rust Codec 做
自我 roundtrip。示例展示同一温湿度数据的四种编码及统一事件输出。interop 工具
依赖测试用 mosquitto_pub，直接发送原生文件，经 MQTT 3.1.1/5.0 QoS0/1/2 后核对
真实 confirmed webhook；网关运行时没有外部 Broker。可选 SDK 的 JSON 便捷接口
保持原样，二进制设备使用标准 MQTT 原始 payload 即可。

实际验证与微基准见[开发报告](multi-codec-report.zh-CN.md)。
