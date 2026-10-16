# 原生 TCP 下行类型歧义：复现、修复与验证

## 先证明问题

基线为 `40ce7c7942cbd6ed8c65c91959041a4616bd6296`。先新增
`vendor_command_identical_to_ack_body_has_distinct_frame_type`，尚未修改生产代码时运行。
该测试用合法 ACK 的 42 字节正文作为厂商命令，分别经过真实 `tcp_ack::encode` 和
`LengthPrefixFramer::encode`。正文相同，两个帧头也都是 `[0, 0, 0, 42]`，
断言帧头不同失败。完整 46 字节完全相同，接收端不存在可靠的正文识别算法。
原始失败日志保存在 `target/performance/tcp-frame-types/collision-before.log`。

审查结果：

| 路径 | 原有行为与结论 |
|---|---|
| TCP 服务端 | ACK 和 Codec 命令共用纯长度帧头；`DeviceCodec::encode` 返回值没有保留正文前缀要求，厂商命令可以合法返回冲突字节 |
| Rust 常规负载器、Business RPC 负载器中的 TCP 设备 | 根据正文首字节 `0x01` 尝试解码 ACK；完整冲突会被吞为 ACK，部分相似正文会被当成损坏 ACK |
| Rust mixed 负载器 | 默认下行都是 ACK；需同时验证帧类型，避免把冲突命令计为接纳 |
| Rust density 负载器 | 只统计下行包，不确认 EventAccepted；仍需更新帧长度读取 |
| Python 示例与 Codec campaign | 按预期时序直接读取 ACK/命令，没有独立类型判定；示例需支持命令与 ACK 交错 |
| 四种内置 Codec | 命令外层首字节分别是 JSON `7b`、CBOR `a5`、MessagePack `85`、Protobuf `08`，正文不与 `01 01` ACK 相同；不需要修改 Codec 生产逻辑 |
| 公共 Rust 客户端/设备 SDK | 前者是管理及 Business RPC 客户端，后者使用 MQTT；没有原生设备 TCP 接收实现，无需引入跨层依赖 |

## 最小修复与兼容性

使用现有大端 4 字节帧头的最高位表示类型：Payload=0、EventAccepted=1；低 31 位表示
正文长度。ACK 帧头改为 `80 00 00 2a`，42 字节 ACK 正文、46 字节完整帧、正文版本 1
及所有字段保持不变。Payload 头和字节原样保留。零长度、配置上限及超时仍有效；
Payload 的理论长度上限为 `0x7fffffff`。服务端在读正文前拒绝带 ACK 标记的上行。
现有配置验证已把 TCP 正文上限限制在 1 MiB，因此复用最高位不会缩减任何已允许的运行时帧。

对任意合法厂商 Payload P，命令帧最高位必为 0，ACK 帧最高位必为 1，两集合不相交。
即使 P 恰好等于合法 ACK 正文，或者内嵌任意帧头，也不影响外层分流。客户端先校验类型与
长度，再读取正文；只有 ACK 类型调用 ACK 解码，Payload 才交给绑定 Codec。

这属于不向后兼容的分帧修复，服务器和设备客户端必须同步升级，包括之前已使用二进制 ACK
的客户端。没有协商、JSON ACK 回退或正文嗅探兼容路径。

服务端接纳和写入循环未改动。认证绑定、EventAccepted 边界、顺序、背压、写超时、错误退出
及已接纳事件所有权保持原有实现。没有新增任务、队列、锁、依赖或 unsafe；ACK 仍为固定
栈数组。帧头辅助函数不分配内存，命令保持原来的分配及复制次数。

## 验证

- `cargo fmt --all -- --check` 通过。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` 通过。
- `cargo test --workspace --all-features`：619 通过、0 失败、61 默认忽略。
- 新增真实厂商 Codec TCP 用例发送 ACK 形状的命令，与连续真实 ACK 交错，并用分片读取核对
  类型、正文和 EventId；验证一次认证、四次真实接纳，伪造命令不会增加接纳计数。
- Rust 负载器覆盖每个双段分片位置、连续 ACK、ACK 形状及近似正文命令、混合下行和异常帧头。
  协议测试覆盖长度边界、未知 ACK 正文版本/类型；现有测试继续验证错误摄取无 ACK、慢读端
  写超时、required sink 背压、资源清理、生命周期、重启/spool 故障及 SIGKILL 语义。
- 四 Codec 真实网络测试验证 ACK→命令→ACK，且命令字节保持原 Codec 编码结果。
- `python3 -m unittest discover -s scripts/perf -p 'test_*.py'`：43 通过。
  包括同一黄金向量、伪造冲突、连续/交错帧、逐位置截断、错误类型和有界长度读取。
- 更新后的 Python 客户端连接真实 Release 服务端：四 Codec × 心跳/命令，共 8 个用例，
  错误均为零；心跳经过 required webhook ACK，命令用例验证设备收到的命令字节。
- 显式执行 Release `mixed_codec_reconnect_and_control_soak`：60.01 秒、241 轮、3856 个
  接纳事件，通过；该用例覆盖 MQTT/TCP/UDP 四 Codec 重连、控制更新及资源归零。
- TCP fuzz 初次 31 秒运行 312537 次；补充带类型 ACK、冲突 Payload 和非法长度种子后，
  再运行 31 秒、316703 次，无崩溃。上限：输入 65540 字节、RSS 2048 MiB、单次 5 秒。
- 显式运行 Release `tcp_ack_encoding`：三轮各 100000 次，新 ACK 编码均为 0 次分配、
  0 分配字节。该既有微基准的另一分支是历史 JSON 实现，不作为本次二进制前后对比。

完整日志及冻结二进制在忽略目录 `target/performance/tcp-frame-types/`。
默认忽略项没有被冒充为已运行；本次另行执行了上述分配测量和 60 秒 soak。
未执行 Linux、异机网络、TLS 性能或长时间 soak；不据此宣称生产容量或长期稳定性。

## Release 性能对比方法

以修改前后分别冻结的 Release 服务端及匹配负载器进行交替运行。当前 Apple M4/macOS
ARM64 同机回环，服务端 10 个 Tokio worker，负载器 4 个；8 个 TCP 连接、256 字节上行、
目标 110000/s、每版 3 × 20 秒，预热 3 秒、爬升 1 秒，顺序为旧/新、新/旧、旧/新。
使用实际 EventBus required AuditSink；速率限制由同一测量配置提高至 2000000/s，
不修改生产默认配额。测量期间无并行编译、测试或 profiler。

吞吐按成功 ACK 数/测量秒数；CPU/事件使用同一采样窗口的服务端 CPU 增量与
EventAccepted 增量；延迟仅覆盖成功 ACK；RSS 为采样峰值。桌面同机短测不能替代生产测量。

完整 plan、冻结二进制哈希、源文件哈希见 [manifest.json](manifest.json)。复现时为 plan
更换 `name`，避免覆盖原始记录，并使用各自匹配的客户端：

```sh
caffeinate -i python3 scripts/perf/device_protocol_benchmark.py \
  --plan target/performance/tcp-frame-types/plan.json \
  --server target/performance/tcp-frame-types/bin/server-before \
  --candidate target/performance/tcp-frame-types/bin/server-after \
  --loadgen target/performance/tcp-frame-types/bin/loadgen-before \
  --candidate-loadgen target/performance/tcp-frame-types/bin/loadgen-after \
  --baseline 40ce7c7942cbd6ed8c65c91959041a4616bd6296 \
  --label tcp-frame-types --output target/performance/tcp-frame-types/paired
```

### 110000/s：每版三轮的中位数

| 指标 | 修改前 | 修改后 |
|---|---:|---:|
| ACK/s | 109999.05 | 109926.90 |
| CPU/事件 | 31.68 µs | 30.55 µs |
| ACK P99 | 1.42 ms | 1.43 ms |
| 峰值 RSS | 9344 KiB | 9984 KiB |
| 已发送消息确认率 | 100% | 100% |
| ACK/目标发送量 | 99.9991% | 99.9335% |

吞吐变化 −0.066%，CPU/事件 −3.56%，P99 +0.70%；RSS **增加 640 KiB（6.85%）**。
全部轮次正常退出，冷却后事件、字节、pending_required、活动连接归零，任务数回到空闲值；
无协议错误、断连或未确认消息。旧版累计发送/确认 6600099 条，新版 6594708 条。

轮间范围：CPU/事件旧版 29.66–33.33 µs、新版 26.56–31.41 µs；P99 旧版
1.37–1.45 ms、新版 1.31–1.67 ms。新版第三轮 P99 较高，三轮累计 `schedule_missed=1885`、
`client_window_full=3386`（旧版为 65、0）。这些反映客户端未发出的计划工作，并非成功发送后
丢失。不能只以确认率声称零调度损失，也不把 CPU 变化归因于一个类型位。
原始逐轮摘要见 [measurements.json](measurements.json)。

### 60000/s 较低压力复测：每版三轮的中位数

保持其余参数和交替顺序相同，每版再运行 3 × 20 秒。复现时使用 manifest 中的
`low_pressure_plan`（本次保存为 `target/performance/tcp-frame-types/low-plan.json`），
输出目录为 `target/performance/tcp-frame-types/paired-low`。

| 指标 | 修改前 | 修改后 |
|---|---:|---:|
| ACK/s | 59999.50 | 59999.05 |
| CPU/事件 | 42.96 µs | 42.14 µs |
| ACK P99 | 0.95 ms | 0.94 ms |
| 峰值 RSS | 8032 KiB | 8560 KiB |
| 已发送消息确认率 | 100% | 100% |
| ACK/目标发送量 | 99.9992% | 99.9984% |

吞吐变化 −0.00075%，CPU/事件 −1.91%，P99 −1.05%；RSS **增加 528 KiB（6.57%）**。
CPU/事件范围旧版 42.39–44.10 µs、新版 41.88–42.83 µs；P99 旧版 0.93–0.96 ms，
新版 0.92–0.98 ms。所有轮次发送均得到确认、无协议错误、正常退出且资源归零。
累计发送/确认旧版 3599956 条、新版 3599177 条；新版仍有 `schedule_missed=437`、
`client_window_full=401`，旧版分别为 46、0。逐轮见 [measurements-low.json](measurements-low.json)。

两档短时测量均未出现明显吞吐或 P99 中位数退化，CPU/事件也未持续上升。RSS 增量在较低
压力下仍然存在，本次没有定位其来源，不声称整个进程内存无变化或一定是测量噪声。
固定 ACK 编码的零分配是单独测得的事实，不能代替进程 RSS 结果。
内核 TCP 计数不可用，未声称零内核丢包/重传；以上确认率仅指应用层已发送消息。
