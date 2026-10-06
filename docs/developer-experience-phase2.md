# NetbaIoT Developer Experience 第二阶段验收

日期：2026-10-06。按用户指定顺序，第一阶段通过本地与三平台原生CI后，再实施本阶段。第二阶段本地完整gate与最终代码提交f9e1047的三平台原生/常规CI全部PASS。用户随后授权将审计与DX分支合并并推送main、清理分支；合并结果由交付记录补充。未创建tag/Release，未操作生产数据。

## Baseline

- 第二阶段起点：干净的 `9ea710dbbe1fd0d4fe5d7d9bc954d54b4af35e8e`，分支 `codex/developer-experience-phase1`。名称沿用已授权DX任务分支，两个阶段独立提交。
- 本阶段实现提交：`bd08ca785e814b2408085dae49760505a2115a32`；合并前最后代码提交：`f9e10475afea2d8709dc41489997b411d9061abf`。报告与执行证据单独提交；合并后补充的V3关闭修复见下文，最终main SHA由交付记录给出。
- 合并前main/origin/main基线为f68bfd4；workspace0.2.3；macOS arm64 / stable1.99.0 +实际Rust1.88.0。
- 起点已经实际具备serve/demo/check/limits/version、共享server入口与诊断、兼容server；没有只依照“第一阶段应已完成”的假设。检查了当前tools/scripts/config/docs/workflows/fuzz及依赖图。

## Init

```bash
netbaiot init my-gateway
netbaiot init --production my-production
netbaiot init --force my-gateway
```

开发项目只生成netbaiot.json、.env.example、README.md与var/。配置来自真实development Config，loopback、已有演示设备凭据、示例HTTP sink；真实config check PASS。管理secret不生成/打印，env例子只有空变量名、不自动加载。用户需要自己的HTTP接收器；自包含体验仍首选demo。

默认任何managed文件存在即拒绝；force只替换这三个regular文件，拒绝symlink/special路径和var symlink，保留unmanaged文件。create-new staging+sync；default用同目录hardlink防止检查/使用竞态覆盖，force复用安全atomic replacement。提交原子性是per-file，并非跨三个文件事务；晚期I/O失败可能留下明确失败的部分生成，不声称全目录事务。Recovery path固定为生成项目的绝对var路径，迁移项目后需要调整，避免在从其他cwd检查时误探测别处var。

实际生产生成（exit0）：

```text
Created <temporary>/prod: netbaiot.json, .env.example, README.md
Production skeleton is NOT ready to run until TLS/auth/sink values are supplied.
Next: netbaiot config check --config netbaiot.json
```

生产配置development=false、非loopbackTLS ingress、真实Config接受的PEM/HTTPS占位路径、credentials=[]；没有固定生产secret/token或demo credential。实际后续检查exit2：

```text
Configuration invalid: 1 problem(s)

NBI-CFG-006 tls
TLS files cannot be loaded, parsed or matched.
Help: Check bounded PEM certificate, private key and client CA files and their permissions.
```

没有把生成成功称为production configuration valid。覆盖拒绝exit6；force后unmanaged keep.txt字节不变。

## Doctor

```bash
netbaiot doctor --config netbaiot.json
netbaiot --output json doctor --config netbaiot.json
netbaiot doctor --config netbaiot.json --network
```

默认只做local preflight。复用同一config validator/真实PEM/key/CA和secret-source检查，无另一套validator。模块分别处理report、filesystem、TLS、network。输出PASS/WARN/FAIL/SKIP，JSON字段ok/checks/configuration，stable id/status/code/message；noFAIL exit0，configFAIL2，环境FAIL6，WARN不导致失败。

| id/code | 实际行为 |
|---|---|
| configuration / NBI-CFG-* | 共享真实诊断，不输出serde值、secret或未知用户字段名 |
| device/management/business_port / NBI-DOC-002 | 2秒有限bind，立即释放TCP/设备UDP；available now，不保留端口 |
| recovery / NBI-DOC-001 | directory/type/probe create-write-sync-delete、同一exclusive owner、真实bounded MQTT/spool decoder；既有图byte不变 |
| device/management/business TLS及client_ca / NBI-DOC-005 | 实际PEM/key/CA检查+UTC validity，过期/未生效FAIL、<30天WARN；不打印private key |
| auth_provider/business_sink_network / NBI-DOC-003 | 默认SKIP；--network仅DNS/TCP/验证TLS，不发HTTP/credential/command/event |
| business_acknowledgement | SKIP/NOT TESTED；reachability不是业务ACK |
| authentication_configuration | 共享local setup检查；不宣称设备认证语义通过 |
| system_time / NBI-DOC-004 | 当前UTC Unix秒，可信时钟同步NOT CHECKED |

实际healthy报告exit0（摘录）：

```text
configuration PASS
 device_port PASS (available now)
 management_port PASS (available now)
 recovery PASS (private probe removed; snapshots preserved)
 TLS SKIP (not configured)
 provider/sink network SKIP (--network not supplied)
 business acknowledgement SKIP (NOT TESTED)
 system synchronization SKIP (NOT CHECKED)
```

完整实际输出含JSON在[CLI证据](performance/developer-experience/evidence/phase2-actual-cli-acceptance.json)。占用端口实际FAIL/exit6；不可达本地endpoint加--network实际FAIL/exit6并受3秒deadline限制；默认无任何连接/HTTP提交由本地mock断言。

DNS采用真实异步resolver，1秒query timeout/1attempt/1concurrency/cache0/最多16candidate IP，外层每endpoint3秒总期限；避免Tokio blocking系统lookup超时后拖住runtime退出。TLS用公共roots/hostname验证，无insecure模式或猜测健康HTTP端点。已有private CA remote endpoint会明确TLS FAIL。

Filesystem可创建缺失目录和runtime `.netbaiot.lock` 元数据；不chmod现有目录，不unlink锁inode。只删除自己的随机probe，真实快照不truncate/rename/replace/delete。另一个gateway持有锁时FAIL。父组件/目录必须是可信local FS；不新增一般硬件I/O墙钟或network FS保证。检查通过不保证serve时端口/文件状态未变化。

## JSON Schema / reference

[Schema](schema/netbaiot-config.schema.json)由真实Config/serde类型生成，optional schema feature经server/runtime/transports/core/protocol传播；生产默认不链接生成逻辑。CLI config schema输出提交的静态artifact，binary用户不需要源码/compiler。

- schemars反映default/rename/required/deny_unknown_fields；对应strict对象additionalProperties=false。
- Limits scalar范围与runtime同一常量1..u32MAX；defaults来自真实Limits::default，非手写列表。
- credential secret_hex writeOnly、64hex；不新增inline管理secret字段。
- 示例Config实例全PASS；unknown/type/required/limit0错误被真正JSON Schema validator拒绝。
- 两次生成/--check确定性通过；当前SHA256=`c0a95062dde9bab342e49c1bfaf9e36bc0568c778d1061e8780e3fc57fdbb157`。
- cargo xtask config-reference从同一schema生成全部对象字段/类型/default/description表，[手写部署指南](configuration.md)分离复杂安全说明。

Schema不替代config check：cross-field TLS/loopback、role/identity、secret sources、真实文件/ownership/port需要checker/runtime。Config格式未增加$schema，仍deny unknown；[IDE说明](configuration.md)使用editor association，不推荐在生产JSON插入$schema。

## Xtask

.cargo/config.toml让cargo xtask真实可用；tool只有clap/serde_json，不依赖server/runtime。Schema通过专用feature-gated server binary生成，没有复制Config或提前新建config crate。

| 命令 | 底层内容 |
|---|---|
| check | fmt +locked workspace all-targets/all-features clippy -D warnings +all-features tests；--audit可加audit |
| check mqtt | 已有Pythonprotocol/unit/release gate、MQTT5 raw/Mosquitto、SDKbuild/interop/measure |
| check release | Cargo MSRV与stable全部检查、audit、MQTT suite、schema/reference drift、preflight/toolingunits、hostpackage/archive smoke |
| schema --check | regenerate exact Rust schema比较，drift FAIL |
| config-reference --check | 同源字段reference比较，drift FAIL |
| package --target | Cargo/cross已有构建模型与现有packager/layout；host自动识别；no-build用于CI已构建target |

全用Command::args，无shell secret字符串拼接，无GitHub API/publishing。每条底层command可见，失败/缺required tool显式FAIL/BLOCKED非零，不silentSkip。解析、子程序失败传播、missing tool和真实drift test PASS。Windows基础check/schema支持；Bash archive smoke在Windows明确BLOCKED，native DX验证binary。

## CI and release topology

- release-verify：rust/audit/preflight改调check release --part；matrix仍MSRV/stable，RELEASE_TAG仍必须匹配workspace。
- mqtt-interop：环境安装仍YAML；内容由同一mqtt函数编排，原两份artifact仍always保留。
- ci：host archive part实际package+extract smoke。
- release：verify/build/checksums/smoke/publish结构/权限/触发不变；cross构建仍YAML，no-build package使用既有输出；--archive检查真实下载artifact，不重建另一份充数。
- 三平台DX：环境准备后cargo xtask check；实际init/check/doctor/schema/once以及drift检查，动态端口fixture；日志exact github.sha。

本次未触发任何可能正式发布的workflow；仅用户已授权的DX branch CI。

## Dependencies and compatibility

- [schemars1.2.2](https://docs.rs/schemars/1.2.2/schemars/) MSRV1.74；optional std/derive，不影响protocol/runtime默认依赖边界。
- x509-parser0.18.1 defaultfeatures关闭，MSRV1.67.1，用于certificate validity；没有新crypto signature/unsafe绕过。
- Hickory resolver/net/proto0.26.3，MSRV1.88，tokio/system-config，禁defaultFeatures/缓存。最初0.25.2的audit FAIL保留；按[上游NSEC3公告](https://github.com/hickory-dns/hickory-dns/security/advisories/GHSA-3v94-mw7p-v465)和[编码公告](https://github.com/hickory-dns/hickory-dns/security/advisories/GHSA-q2qq-hmj6-3wpp)升级修复版，未ignore。
- jsonschema0.33.0仅CLI dev-dependency，MSRV1.71.1，用真实validator测试artifact；default net features关闭。
- 最终audit PASS、0 known vulnerabilities；既有rustls-pemfile unmaintained warning保留。

第一阶段命令/server/API保持；newschema traits不改变serde/wire/API layout；client仍不依赖server，protocol无Tokio/HTTP/server依赖。Rust1.88/unsafe_code=forbid保持；无DB/message store、离线命令、WebUI、plugin/YAML/TOML配置或IoT新功能。

## Tests and real execution

| 项 | 状态/证据 |
|---|---|
| cargo xtask schema/config-reference generate +--check | PASS，deterministic/drift/source defaults |
| cargo +stable xtask check | PASS，433 passed/0failed/16ignored |
| cargo +stable xtask check release | PASS，Rust1.88/stable各433/0/16，fmt/clippy/audit/MQTT/SDK/preflight/package/archive全实际执行，301.7秒 |
| CLI真实init/default拒绝/force/unmanaged preservation/production/check/doctor/schema | PASS，11个command case，含预期FAIL exit2/6；JSON stdout可解析 |
| default无远端、local DNS/TLS timeout、expired/missing certificate、path非目录/corrupt spool byte-preserved | PASS，定向unit/E2E |
| Schema示例/unknown/type/required/limits/defaults | PASS，实际jsonschema validator |
| actionlint / release-tooling / archivelayout | PASS；empty PATH archive包含新CLI操作链和旧Bash/MQTT/TCP/UDP |
| 最终f9e1047原生Linux/macOS/Windows | PASS，Linux/macOS各433/0/16，Windows430/0/16；实际CLI及schema/reference drift checks均PASS |
| Windows真实Ctrl-C、near-expiry30天WARN fixture、kernel-fsync硬件故障、长负载/fuzz | NOT RUN；不宣称覆盖或容量 |

source指纹含新模块，actual CLI、full logs、manifest在[证据目录](performance/developer-experience/evidence/README.md)，未修改结果。16ignored不计PASS，本阶段未选择这些manual benchmark；第一阶段单独soak已经执行。

过程中两个真实FAIL：依赖audit已升级修复；一次旧V3wait_ready超时的子进程只记录Unavailable。原单项复跑PASS；发现fixture只保留TCP，已同时配对预留UDP，保持原10秒/全部业务断言，加listener/error-kind日志。未抓到当次具体失败listener，因果边界保留；随后原七项与完整gate PASS。没有通过放宽时间、删除assertions或skip取得绿色。

本阶段首次最终CI在62d0fc8上，常规Rust/audit/MQTT/preflight通过，native Linux通过，macOS在旧V3 cleanup轮询失败；另中间纯文档9ea710d的Windows旧V2 grace测试失败。原始artifact已校验摘要并保留failure摘录，未称这些run整体PASS。V3在默认32/IP/s下20ms轮询可能自触发限流，现改100ms并增加单请求期限，保留原5秒及全部四个零usage断言；V2把即时connected断言放在发起cancel后、等待driver join前，保留1500ms grace和5秒最终撤销断言。join完成并不代表仍在grace窗口，未改生产策略或延长规范期限。最终f9e1047的三平台完整回归均PASS。

62d0fc8的Windows完整workspace与实际demo/init/check/doctor均PASS，但schema --check出现文件drift。生成文件已用gitattributes固定LF，xtask比对仅归一化CRLF/LF（不忽略其他内容）；新增行尾等价与内容变化仍不等价断言。最终f9e1047的Windows schema/reference检查均PASS；这证明修复有效，不把旧run的失败改写为成功。

## Cross-platform

macOS arm64本阶段上述本地检查全部实际PASS，包含host archive。最终f9e1047的[三平台原生CI](https://github.com/sskycn/netbaiot/actions/runs/37457928928)和[常规Rust/release gate](https://github.com/sskycn/netbaiot/actions/runs/37457929637)均PASS。原生平台完整workspace计数为Linux/macOS各433 passed、Windows430 passed，均0 failed/16 ignored；Windows少3项为Unix限定测试，不计为PASS。每个平台均实际执行demo/init/check/doctor/schema与两个drift检查。官方artifact ZIP SHA256校验及原始日志摘要已保存于[最终CI证据](performance/developer-experience/evidence/phase2-final-ci-verification.json)。第一阶段结果仍单独记录。

## 合并后的V3关闭回归

首次合并main提交`bcf55097bee7225ce52109c96da5166fcbf86641`的Rust/release gate、三平台DX、decoder fuzz均PASS；[原生恢复工作流](https://github.com/sskycn/netbaiot/actions/runs/37462049715)的Linux/Windows PASS，macOS完整workspace在旧V3 lost-response测试失败。最后采样为active_connections=1，active_streams/queued_bytes/reassembly_reserved_bytes均0；保留[失败日志及官方artifact摘要](performance/developer-experience/evidence/post-merge-v3-close-evidence.json)。本地原测试单次及4并发共20次均PASS，不能据此否认CI失败，也不能从该日志断定当次writer具体停在哪个await。

代码检查发现两个可确定复现的关闭缺口：writer的socket write与GOAWAY drain deadline没有响应owner cancellation。新增两个paused-time测试，保留10秒write deadline，同时要求取消后100毫秒内join；修复前均因Elapsed失败，修复后均PASS，后者也断言全部byte permits与queued-byte gauge归零。真实V3全部7项随后PASS，原lost-response测试5秒deadline及四项零usage断言未改。

修复只让gateway V3 writer在socket write/GOAWAY等待中响应其owner cancellation；reader遇到EOF/Unavailable时不再等待无法完成的GOAWAY交换，由connection取消并join writer。正常关闭仍先按原有期限完成GOAWAY/drain，再取消writer；未观察ACK的required delivery仍由sink/EventBus拥有，不能因连接取消称为业务ACK。没有修改wire、身份/代际、默认limits、生产rate limit或断言。此修复解决已由确定性测试证明的问题；首次macOS失败的精确I/O状态没有现场日志，不夸大因果结论。

修复后完整stable/Rust1.88检查均PASS，各435 passed、0 failed、16 ignored，包含fmt和严格clippy；[命令/耗时/原始日志摘要](performance/developer-experience/evidence/post-merge-v3-close-validation.json)和执行摘录已保存，preflight/source manifest也PASS。修复后main原生CI由最终交付记录补充；[修复后的源码指纹](performance/developer-experience/evidence/post-merge-v3-close-source-manifest.json)与原f9e1047证据分别保留。未改decoder；原main decoder fuzz smoke已实际通过。未重跑长负载、硬件I/O或Windows控制台Ctrl-C，不新增容量或硬件故障覆盖结论。

## git diff --stat

实现提交相对9ea710d（不含庞大的脱敏执行日志）：

```text
.cargo/config.toml                              |    2 +
 .github/workflows/ci.yml                        |    7 +-
 .github/workflows/dx-platform.yml               |   13 +-
 .github/workflows/mqtt-interop.yml              |   26 +-
 .github/workflows/release-verify.yml            |   16 +-
 .github/workflows/release.yml                   |    9 +-
 .gitignore                                      |    1 +
 CONTRIBUTING.md                                 |    9 +
 Cargo.lock                                      |  830 +++++++++-
 Cargo.toml                                      |    3 +-
 README.md                                       |    5 +
 README.zh-CN.md                                 |    4 +
 apps/netbaiot-cli/Cargo.toml                    |   10 +
 apps/netbaiot-cli/src/args.rs                   |   17 +
 apps/netbaiot-cli/src/doctor/filesystem.rs      |  103 ++
 apps/netbaiot-cli/src/doctor/mod.rs             |  162 ++
 apps/netbaiot-cli/src/doctor/network.rs         |  187 +++
 apps/netbaiot-cli/src/doctor/tls.rs             |  101 ++
 apps/netbaiot-cli/src/init.rs                   |  161 ++
 apps/netbaiot-cli/src/main.rs                   |   56 +-
 apps/netbaiot-cli/tests/dx2.rs                  |  105 ++
 apps/netbaiot-cli/tests/schema.rs               |   47 +
 apps/netbaiot-server/Cargo.toml                 |   10 +
 apps/netbaiot-server/src/bin/schema.rs          |   28 +
 apps/netbaiot-server/src/config.rs              |   19 +
 apps/netbaiot-server/src/diagnostics.rs         |   28 +-
 apps/netbaiot-server/src/server.rs              |   19 +-
 apps/netbaiot-server/tests/business_rpc_v3.rs   |   27 +-
 crates/netbaiot-core/Cargo.toml                 |    4 +
 crates/netbaiot-core/src/lib.rs                 |    2 +
 crates/netbaiot-protocol/Cargo.toml             |    4 +
 crates/netbaiot-protocol/src/business_rpc.rs    |    1 +
 crates/netbaiot-protocol/src/business_rpc_v3.rs |    1 +
 crates/netbaiot-protocol/src/lib.rs             |    3 +
 crates/netbaiot-runtime/Cargo.toml              |    4 +
 crates/netbaiot-runtime/src/auth.rs             |    2 +
 crates/netbaiot-runtime/src/limits.rs           |    6 +-
 crates/netbaiot-runtime/src/management_auth.rs  |    6 +
 crates/netbaiot-runtime/src/recovery_io.rs      |    7 +
 crates/netbaiot-transports/Cargo.toml           |    4 +
 crates/netbaiot-transports/src/business_rpc.rs  |    1 +
 docs/cli.md                                     |    8 +
 docs/configuration-fields.md                    |  326 ++++
 docs/configuration.md                           |   60 +
 docs/developer-experience-phase1.md             |    2 +
 docs/doctor.md                                  |   43 +
 docs/maintenance.md                             |   46 +
 docs/schema/netbaiot-config.schema.json         | 1827 +++++++++++++++++++++++
 scripts/release_preflight.py                    |    2 +-
 tests/release_archive_smoke.py                  |   25 +-
 tools/netbaiot-xtask/Cargo.toml                 |   12 +
 tools/netbaiot-xtask/src/main.rs                |  435 ++++++
 52 files changed, 4769 insertions(+), 67 deletions(-)
```

详细文件职责见configuration/doctor/maintenance；schema/reference均generated。审查完成后按用户授权合并审计与DX分支；版本/tag/Release不变。

## Remaining work / limits

- 最终代码提交三平台/常规CI已核验；硬件I/O/Windows控制台Ctrl-C/近30天WARN现场证据仍未覆盖。
- Init per-file原子而非跨3文件事务；目录移位需修正绝对recovery path，env example不自动装载。
- Doctor只证明当下local preflight和显式network reachability；不证明业务ACK、真实时钟同步、运行中的gateway健康或未来端口可用。
- 下一阶段可考虑support-bundle、shell completion、interactive wizard；本轮不实现，也不建议重写runtime。
