# 多 Codec 实现与验证报告

日期：2026-10-11。基于本地 main 的 `7ff7fe4`，保护已有 11 个本地提交。
[英文规范](codecs.md) / [中文规范](codecs.zh-CN.md) / [验证证据](performance/multi-codec/evidence/validation.json)。

## 1. 实际完成与支持矩阵

同一网关的真实 socket 测试同时连接四种 Codec 设备。全部上行经过原有 Ingress、
EventBus、必需 Sink 接纳，生成现有 DeviceEvent；下行由 CommandRouter 选定设备的
Codec，经原有 MQTT/TCP 队列发送。

| Codec ID / 版本 | 四类上行 | MQTT 3.1.1/5.0 QoS0/1/2 | TCP | UDP NBI1/NBA1 | MQTT/TCP 命令 |
| --- | --- | --- | --- | --- | --- |
| netbaiot-json / 1 | 通过 | 通过 | 通过 | 通过 | 通过 |
| netbaiot-cbor / 1 | 通过 | 通过 | 通过 | 通过 | 通过 |
| netbaiot-msgpack / 1 | 通过 | 通过 | 通过 | 通过 | 通过 |
| netbaiot-protobuf / 1 | 通过 | 通过 | 通过 | 通过 | 通过 |

四类为 Telemetry、DeviceEvent、Heartbeat、CommandAck；UDP 无下行。真实传输的
跨格式测试主要使用温湿度 Telemetry，另有 MQTT 外部客户端及直接 Ingress 测试
覆盖其它类别。没有声称把每个类别与每个传输的全部排列都逐一测试。

## 2. 主要模块

- codecs/src/json/v1.rs：原 JSON 实现与测试原样迁移，复用等价领域验证函数；原性能测试保留。
- codecs/src/common.rs：领域验证、精确整数转换、唯一字段和具名信封、受限输出 writer。
- codecs/src/binary.rs：CBOR/MessagePack 零分配结构预检与长度预算。
- codecs/src/{cbor,msgpack,protobuf}/v1.rs：独立 DeviceCodec 实现，共用同一 parser 做预验证/解码。
- codecs/proto/device_v1.proto、build.rs：正式 schema，锁定库与随构建提供的 protoc。
- codecs/src/lib.rs、vendor/mod.rs：统一启动 catalog、厂商扩展位置。
- runtime/src/{ingress,control,sessions}.rs：注册前 profile 校验、控制快照完整验证、注册同步边界。
- server/src/{bootstrap,diagnostics,auth_provider}.rs：注册全部内置 Codec、静态产品冲突拒绝、HTTP 认证测试。
- transports/src/{management_http,udp}.rs：控制发布同步、UDP 可信 profile 检查；未改变 wire framing。
- codecs/tests、transports/tests/multi_codec.rs、tests/multi_codec_interop.py：固定向量与实际链路。
- fuzz、benches、examples、configs/multi-codec.json、中英文规范与现有文档入口。

公共 protocol/client/SDK 领域模型没有修改。内部 GatewayControl::apply 现在要求
CodecRegistry 参数，调用处均已更新；运行中改变/删除既有产品 Codec 返回 Conflict。

## 3. 实现方式与 wire

JSON 使用原 serde_json wire；CBOR 使用 ciborium 0.2.2 原生 definite-length 文本键
Map；MessagePack 使用 rmp-serde 1.3.1 原生具名 Map；Protobuf 使用 prost 0.14.4，
互斥类别与 Scalar 用 oneof，遥测字段用 repeated Field 保留重复名称验证。

Protobuf 构建由 prost-build 0.14.4 + protoc-bin-vendored 3.2.0 自动生成，无需手工
生成或运行时服务。四种格式均提供独立 Python 规范编码器生成的固定上行向量与
精确下行字节向量；下行也通过独立 DTO 解码核对身份、UUID、TTL、参数类型。

CBOR/MessagePack 拒绝未知键；Protobuf 在预算内跳过未知 wire type 0/1/2/5 字段，
拒绝未知类别/枚举、重复已知 singular/oneof 与 groups。严格整数 Scalar 范围为
[-2^53,2^53]，浮点必须有限；Heartbeat 保留完整 u64。JSON 旧数值行为不变。
详见规范与正式 proto。参考 [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)、
[MessagePack 规范](https://github.com/msgpack/msgpack/blob/master/spec.md)、
[Protobuf wire 编码](https://protobuf.dev/programming-guides/encoding/)。

## 4. 编码选择与一致性

仅由可信 AuthenticatedDevice.codec_id / codec_version 选择。静态、HTTP、Business
RPC 都验证后才可正式注册；最终注册在现有 auth-registration gate 内再次检查。
未知 Codec 和错误版本不能建立必然不可用的会话。普通 MQTT/TCP 上行没有远程认证。
UDP 保留每数据报 HMAC/replay，新增 profile 检查不创建 session。

一个 tenant/product 对应一个 ID/版本；静态冲突不能再由 or_insert 静默决定。
动态快照在完整校验后原子更新，新增 profile 与现有连接冲突时拒绝。暂不提供在线
Codec 切换/删除：使用失效、重新配置和计划重启，保留旧连接编码不可变以及 MQTT
持久状态 provenance/generation/incarnation 机制。未配置的动态产品可使用认证方
返回的已注册 profile。错误缓存结果需按原有显式 invalidation/TTL 机制更新。

## 5. 兼容性与可靠性

JSON V1、领域事件及业务 wire 没有改动。所有 decode 只产生一条事件，Ingress 原有
单事件检查保留。EventAccepted、原子必需 fanout、Sink ACK、命令 TTL/授权/在线队列
与 transport SENT / 设备执行区别保持原样。

MQTT 原 PUBLISH payload 可携带二进制；3.1.1/5.0 能力均保留。TCP 原长度前缀、JSON
认证与 JSON 接纳回执不变，后续数据/命令按绑定 Codec 编码。UDP 只替换 NBI1 内部
payload，HMAC 签名范围、replay、NBA1 回执保持不变。没有自动降级、工业协议嗅探、
数据库、外部 Broker、离线命令持久化或常规磁盘队列。

跨格式 spool 测试验证每个被接纳的 CommandAck 都保持原 event_id、pending Sink 与
revision，并在重复重放时仍保持该 ID。既有 NBSP v3、NBMQ v6、SIGKILL 丢失窗口、
spool 失败留存与重启恢复回归均通过。

## 6. 实际验证

以下最终命令加 --locked / --offline 运行（fuzz 与安全公告拉取需要已缓存或网络）：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo +1.88.0 check --workspace --all-targets --all-features --locked --offline
cargo test -p netbaiot-transports --test multi_codec
cargo run -p netbaiot-codecs --example multi_codec
cargo bench -p netbaiot-codecs --bench codecs
cargo xtask schema --check
cargo xtask config-reference --check
cargo run -p netbaiot-cli -- config check --config configs/multi-codec.json
# 提供开发 admin secret 并允许本地端口探测
netbaiot doctor --config configs/multi-codec.json
python3 tests/multi_codec_interop.py
python3 tests/run_mosquitto_cli_interop.py
python3 tests/mqtt_conformance/v5_mosquitto.py
python3 tests/mqtt_protocol_regressions.py --repo <repo> --output <tmp>/results.json
python3 tests/mqtt_conformance/run.py --release-gate --no-build
cargo test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --exact
cargo test -p netbaiot-server --test server engineering_soak::engineering_tls_qos1_http_sixty_second_soak -- --ignored --exact
cargo +nightly fuzz run cbor_codec -- -max_total_time=20 -max_len=65536
cargo +nightly fuzz run msgpack_codec -- -max_total_time=20 -max_len=65536
cargo +nightly fuzz run protobuf_codec -- -max_total_time=20 -max_len=65536
cargo audit --json
```

结果：fmt、Clippy、全工作区测试通过；**528 通过、0 失败、45 原有 ignored**。
另执行了上面两项 ignored soak，分别约 64.72 秒和 60.24 秒。
Rust 1.88 全目标/全 feature 编译通过。Schema 与字段参考无漂移，旧配置仍可用。
新增 integration 六项通过，包括动态 RPC、静态认证、profile 注册竞态、权限、
QoS2 错误 PUBREC 后复用 packet ID、spool duplicate replay。HTTP provider 单元
测试覆盖全部 profile 与未知/错误版本。恶意二进制长度声明测试观察到拒绝前 **0
heap allocations**；这只针对这些固定恶意长度样本，未声称所有错误均零分配。

外部客户端为 mosquitto_pub/sub 2.1.2（libmosquitto 2.1.0），测试用参考工具。
多 Codec 外部链路收到 40 条 confirmed webhook，内容一致；现有 3.1.1 CLI 矩阵与
MQTT5 retained/offline/expiry/delayed Will 通过。额外原始报文回归 7/7，完整
conformance 77/77，MQTT 规范追溯覆盖 125/125。

CBOR fuzz 2,447,657 次、MessagePack 1,876,384 次、Protobuf 1,649,153 次；每项
实际 21 秒，ASan，无发现失败。保留原 JSON/MQTT/TCP/UDP fuzz，没有移除旧目标。
此为有种子的短期 fuzz，非长期安全审计或资源上限证明。

cargo-audit 原环境缺失，临时安装到 <tmp> 后实际检查：0 个漏洞，保留已有
RUSTSEC-2025-0134 / rustls-pemfile 停止维护的 informational warning；新 Codec
依赖没有报告漏洞。原始回归脚本第一次缺少 --repo，补齐参数后通过。外部多 Codec
测试第一次在第 32 次建连触发默认每 IP rate limit，测试专用配置增加预算后通过；
生产限速没有调整。最初受 sandbox 限制的 socket/doctor 检查在许可环境重跑通过。

## 7. 性能与分配测量

环境：本机 macOS aarch64，rustc 1.99.0，release/optimized，单线程同步 Codec。
无生产流量、TLS/network 或数据库；不是吞吐容量结论。每种格式使用同一组 2/16/64
个数值字段，固定上行 wire 与等价下行命令，预热 1000 次，3 轮各 20,000 次。
表中时间是 **3 个每轮均值的中位数**，不是请求延迟 P50。stats_alloc 在全局
allocator 中计数，因此时间包含其计数开销；decode 包含正常 event_id 生成。
最后一轮在上述长任务完成后执行，未设置 CPU affinity 或控制系统其它进程。

| 字段数 | Codec | 上行字节 | 命令字节 | decode ns | encode ns | decode 分配次数/字节 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2 | json | 104 | 196 | 1062.3 | 255.8 | 6 / 739 |
| 2 | cbor | 96 | 174 | 1351.6 | 437.0 | 9 / 1489 |
| 2 | msgpack | 96 | 186 | 1045.2 | 386.5 | 9 / 1484 |
| 2 | protobuf | 55 | 101 | 992.6 | 237.6 | 7 / 936 |
| 16 | json | 298 | 390 | 2324.6 | 620.0 | 22 / 2013 |
| 16 | cbor | 326 | 320 | 3455.9 | 898.3 | 27 / 4545 |
| 16 | msgpack | 328 | 418 | 2347.2 | 835.0 | 27 / 4518 |
| 16 | protobuf | 356 | 401 | 2726.7 | 840.3 | 23 / 2808 |
| 64 | json | 1018 | 1110 | 7751.8 | 1822.9 | 76 / 5613 |
| 64 | cbor | 1143 | 849 | 10909.0 | 2442.6 | 85 / 12673 |
| 64 | msgpack | 1144 | 1234 | 7249.9 | 2325.1 | 85 / 12598 |
| 64 | protobuf | 1412 | 1457 | 8539.9 | 2886.9 | 77 / 8760 |

完整每轮时间与 encode/decode 分配见 [codecs.csv](performance/multi-codec/evidence/codecs.csv)。
CBOR 的命令浮点采用可无损缩小的表示；上行固定向量使用 float64，因此两种方向
不能仅凭字段数直接推断字节大小。不同字段数量下 Protobuf/Map 的字段名与结构成本
也不同，二进制格式不保证总是更小或更快。没有运行真实 MQTT 改前/改后性能对照，
没有当前生产容量、连接 RSS 或长时间高负载结论。

## 8. 已知限制与未覆盖项

- 每条载荷一条事件；没有批量接纳、自动探测、Codec 热插件或 Modbus。
- CBOR 为明确的有限 profile；不接收 indefinite/tag/bytes/array。MessagePack 不接收 bin/ext/array。
- Protobuf 未知内容跳过，groups 拒绝；已知 singular/oneof 重复拒绝，属于本网关 V1 契约。
- 产品 Codec 切换/删除需计划重启和认证失效。此运行中限制已在配置/控制文档说明。
- 普通 MQTT 客户端无需 SDK；现有 SDK JSON 便捷 API 未新增二进制生成接口。
- 未执行小时/天 soak、跨节点/生产网络 load、全连接 RSS 容量测量、所有外部 CBOR/MessagePack/Protobuf 库互操作组合。
- 用独立规范编码器、固定上下行向量及成熟外部 MQTT 客户端补齐互操作证据；不是各语言 SDK 全认证。
- 45 个原有 ignored 测试没有全部执行，其中两项恢复 soak 已单独运行；其它多为历史/测量用基准。
- 计划退出的持久 spool 保证不等于崩溃持久性；突发故障仍可能丢失有界未 spool 流量。

## 9. Git 交付

任务分支 `codex/multi-codec-v1`，提交消息 `feat(codecs): add bounded native device codecs`。
依仓库流程将验证后的任务提交合并到 main，推送 origin/main，再移除任务分支。
保留 main 开始时已有的 11 个本地提交。最终提交哈希与推送状态见本任务最终消息；
也可使用 `git log -1 --oneline main` 核对。未创建额外 worktree。
