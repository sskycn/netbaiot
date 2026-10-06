# NetbaIoT Developer Experience 第一阶段验收

日期：2026-10-06。第一阶段实现、本地及三平台原生验收完成。经作者单独授权只推送DX任务分支验证CI；未合并或推送main、未打tag或发布Release。第二阶段在完成本阶段验收后开始。

## 1. Current baseline

- 起点：干净的审计分支 `codex/strict-reliability-audit`，`e7d8bfed81b31c7a5895b52e1a72fe4f67844117`，上一轮最终原生/常规 CI 已核验通过。当前 main/origin/main 仍为 `f68bfd4a8f563e265673ae76e70c7c795b27ee86`；没有把历史 main 当作最新实现。
- 新分支：`codex/developer-experience-phase1`。第一阶段生产/测试实现提交：`9cf9018801a4705206fffdb5d93529727378cb92`；报告另作提交，不改变生产行为。包含全部本阶段生产修改的 `4cb93af56dea579c10322e379abfe950b088bc9e` 已通过 [原生 CI](https://github.com/sskycn/netbaiot/actions/runs/37443851778) 和 [常规 CI](https://github.com/sskycn/netbaiot/actions/runs/37443853586)。后续本次提交仅补充平台证据，不改生产行为。
- workspace version：0.2.3；未改 SemVer、tag 或 Release。
- 本机 macOS arm64 / Darwin 27；stable rustc/cargo 1.99.0，另实际执行 Rust/Cargo 1.88.0。详见 `target/dx-phase1/baseline.json`。
- 已阅读当前 Cargo/lock、CLI/server/client/runtime/transports、configs/demo/examples、两份 README、CLI/Quick Start/tutorial/operations/CONTRIBUTING/AGENTS 和 release/CI 入口。

## 2. CLI architecture

```text
netbaiot serve ─────┐
                   ├─ server::serve_path -> local preflight -> run_until_signal
netbaiot-server ────┘                              -> existing runtime composition
netbaiot config check -> same Config diagnostics / constructors / TLS loaders
netbaiot config limits -> shared Limits::default() JSON function
netbaiot demo -> supervised server + development-only sink + standard MQTT SDK
operator commands -> netbaiot-client -> management HTTP / confirmed event stream
```

没有复制服务器、信号、配置校验或管理客户端实现。旧 `run` / `run_with_credentials` API 保留；新增 bounded oneshot readiness API `run_with_credentials_ready`，只在准备和 worker ownership 完成后发送实际绑定地址。Config、TLS、HTTP auth provider、delivery、bootstrap、entry、server composition 分模块；普通函数/struct，没有引入 factory/validator traits 或新 config crate。

## 3. New commands

```bash
netbaiot --help
netbaiot serve --config configs/development.json
netbaiot demo
netbaiot demo --once
netbaiot config check --config configs/tutorial.json
netbaiot --output json config check --config configs/tutorial.json
netbaiot config limits
netbaiot version
```

`serve` / `config check` 默认 `configs/development.json`；version 使用 Cargo compile-time package version。实际执行全部 help/version/limits/check 命令，详见 `actual-cli-acceptance.json`。Demo 不需要 operator endpoint/token，也不依赖 Python、Mosquitto 或 Cargo 可执行文件。

## 4. Compatibility

实际启动并管理排空了三个路径：默认 `netbaiot-server`（临时 cwd 的 configs/development.json）、显式 `netbaiot-server PATH`、`netbaiot serve --config PATH`；均 exit0，并检查可重新绑定监听端口。旧 `--print-default-limits` 与新 `config limits` 输出逐字段一致。

现有 status/device/command/auth/events/drain 集成 smoke 保留并通过。CLI 仍用 netbaiot-client；事件 stdout flush 后才 ACK；离线命令不存储/重试。client 不依赖 server，protocol/SDK API 与 MQTT wire 不变。

有意收紧/调整的边界：未知参数、重复 singleton、token/API-key 与 JSON/file 冲突现在 exit2；错误帮助文字由 clap 生成。运行日志移到 stderr，JSON stdout 只含数据。HTTP auth URL 现在与 sink 一样禁止 URL username/password。默认/位置参数 server、systemd/Docker/manual脚本和 archive 两个 binary 保持。

## 5. Config diagnostics

稳定编号和字段路径见 [CLI 文档](cli.md)。静态校验由 `Config::diagnostics()` 统一驱动旧 validate API；本地检查复用实际 StaticAuthenticator、management auth、codec registry、RPC token读取和 TLS loaders。独立错误尽量汇总；无效依赖子树停止，不显示 serde 值/未知用户字段名/secret。

实际成功（exit0）：

```text
Configuration valid
```

实际失败 1（exit2）：

```text
Configuration invalid: 1 problem(s)

NBI-CFG-001 config
Malformed JSON or a value does not match the configuration schema.
Help: Check the JSON syntax and documented field types; values are omitted to protect secrets.
```

实际失败 2（exit2，一次显示两个独立问题）：

```text
Configuration invalid: 2 problem(s)

NBI-CFG-003 development
Development listeners must use loopback addresses.
Help: Use 127.0.0.1 or ::1, or configure authenticated TLS production listeners.

NBI-CFG-004 device_ingress
Non-loopback device ingress requires TLS.
Help: Configure tls or bind device_ingress to loopback.
```

实际失败 3（exit2）：

```text
Configuration invalid: 1 problem(s)

NBI-CFG-009 delivery_url
HTTP endpoints require HTTPS (loopback HTTP is allowed) and must not contain URL credentials.
Help: Use an HTTPS URL and supply service credentials through protected environment variables.
```

JSON 为 `{"valid":true,"diagnostics":[]}`，失败 diagnostics 含 code/path/message/help。配置检查不绑定、不启动任何 worker、不发送 webhook/provider 请求、不取得 recovery 锁或读写快照；实际断言检查后 recovery path 未创建。检查通过不承诺端口仍空闲、远端可用或恢复图合法。相对文件路径保留 cwd 语义。

## 6. Demo

实际 `demo --once` 在空 PATH（无外部工具可执行入口）成功，关键输出为：

```text
Gateway started
Demo device authenticated
Heartbeat EventAccepted (MQTT QoS1 PUBACK)
Business sink acknowledged
Shutdown completed
Demo completed successfully.
```

完整实际记录包含动态地址与稳定 event_id，见 `actual-cli-acceptance.json`。标准 SDK `publish()` 本身只表示本地 enqueue；demo 另等待 PublishReceipt::Puback，再确认收到匹配 source_message_id 的真实 normalized heartbeat，最后通过管理状态观察 required pending=0 才打印 sink acknowledged。没有把 socket write 说成业务 ACK。

仅 development=true / loopback / private temp spool；配置复用 development.json，不另造生产配置或鉴权绕过。随机管理 token 不打印；固定演示设备凭据仅在明确 warning 后为外部标准 MQTT 示例展示。交互模式持续处理外部事件 metadata，直到 Ctrl-C；Unix SIGTERM 也走共享信号实现。

sink 单连接、header数量/缓冲和2秒总期限、body64KiB、16条通知/1MiB编码字节预算；overload返回503，不 false ACK。supervisor 只有一份资源 ownership，调用 future Drop 先取消，仍由 supervisor 完成 server drain，再停 sink、移除 temp；isolated runtime 测试核对任务基线与端口/目录。startup/sink bind/sample auth failure、future cancellation、正常 once、Unix SIGINT/SIGTERM 都有真实测试。异常删除失败会返回 cleanup 阶段错误，不声称清理成功。

## 7. Files changed

- CLI args/main：clap 命令树、本地/管理分派、保留结构化 client errors 和退出码。
- demo.rs / tests/dx.rs：真实标准 MQTT 示例、有界 HTTP sink、拥有者清理和真实 binary 验收。
- server config/diagnostics/tls/auth_provider/delivery/bootstrap/entry/server 模块：移动既有职责、共享入口与准备/就绪边界；runtime核心与transport协议实现保持。
- README/CLI/Quick Start/tutorial/operations/CONTRIBUTING：首选单命令；保留普通 MQTT 与手动脚本。
- release preflight/archive smoke：打包 development.json，两个 binary 保留；新增空 PATH native CLI smoke，旧 Bash MQTT/TCP/UDP smoke 保留。
- dx-platform.yml：三平台 Rust1.88 全 workspace + 实际version/limits/check/demo，带有限超时与 exact-SHA artifact。未运行则不计 PASS；本轮矩阵已实际执行并核对原始artifact摘要。

## 8. Dependencies

- [clap 4.6.7](https://docs.rs/clap/4.6.7/clap/)：成熟 derive parser；实际读取 manifest rust-version=1.85，并用 Rust1.88编译/测试。关闭 default features，只开 std/derive/help/usage/error-context；不启用 color、env、suggestions 或 completion。CLI环境回退在边界处理，保持旧 precedence。
- serde_path_to_error 0.1.20：JSON反序列化字段定位；只输出 schema字段，原始serde message不展示。
- tempfile 3.27.0（已有锁定依赖，新增CLI直接使用）：私有临时目录、明确 cleanup；manifest MSRV1.63。
- 其余为现有 workspace 的 server/runtime/SDK、Tokio、Hyper、bytes、UUID、tracing 依赖复用；没有新 runtime broker、DB 或消息存储。
- workspace `unsafe_code=forbid` 保留。cargo audit exit0，0 known vulnerabilities；保留既有 rustls-pemfile unmaintained warning，没有 ignore。

## 9. Tests

| 检查 | 实际状态 | 记录（target/dx-phase1） |
|---|---|---|
| stable / Rust1.88 fmt | PASS，exit0 | stable-fmt.log / msrv-fmt.log |
| stable / Rust1.88 clippy all-targets/all-features -D warnings | PASS，exit0 | stable-clippy.log / msrv-clippy.log |
| stable / Rust1.88 workspace all-features | PASS，各424 passed/0 failed/16 ignored | stable-tests.log / msrv-tests.log |
| CLI命令树/严格错误、真实配置检查、demo+清理、旧管理 smoke | PASS | focused.log / cli-tests.log / actual-cli-acceptance.json |
| 一分钟子进程restart soak（单独选择ignored用例） | PASS，65.14秒 | restart-soak.log |
| MQTT protocol regressions | PASS，7/7，含binary SHA和source manifest | protocol-results.json / protocol.log |
| MQTT 3.1.1 release gate | PASS，76/76，normative125/125 | mqtt-release-gate.log |
| MQTT5 raw / Mosquitto | PASS | mqtt5-raw.log / mqtt5-mosquitto.log |
| Device SDK interop / bounded measurement | PASS；测量不推导生产容量 | sdk-interop.log / sdk-measure.log |
| cargo audit | PASS，0漏洞；unmaintained警告保留 | cargo-audit.json |
| actionlint / release tooling / preflight | PASS | actionlint.log / release-tooling.log / release-preflight.log |
| macOS arm64 release build +实际archive smoke | PASS，新CLI空PATH +旧Bash/MQTT/TCP/UDP | release-build.log / package.log / archive-smoke.log |
| 本轮Linux/Windows/macOS native CI | PASS；Linux/macOS424/0/16，Windows421/0/16，真实version/limits/check/once也通过 | native-ci-verification.json / native-*-latest.log |
| fuzz/新生产容量/长时负载 | NOT RUN；没有修改纯协议/recovery decoder，不作新容量声明 | — |

16 ignored 中仅 restart soak 在本阶段另行选择并实际执行；其余手工 benchmark 不计通过。完整命令/退出码/摘要在 validation.json；包含新未跟踪模块的源文件指纹在 validated-source-manifest.json。

过程中真实 FAIL：首轮workspace的既有警告测试只读stdout，移到实际stderr捕获后保持警告与secret断言并通过；Rust1.88 clippy指出等价布尔表达式简化，修正后通过。独立验收脚本最初无SO_REUSEADDR，误把TIME_WAIT视为端口残留；按Tokio listener同样的reuse设置修正probe后全部通过。这些日志保留，未跳过测试或减少业务断言。

## 10. Cross-platform

macOS arm64及hosted macos-latest：上述全套实际 PASS。Linux hosted ubuntu-latest：完整424/0/16及实际命令PASS，提取archive Quick Start也实际PASS。Windows hosted windows-latest：完整421/0/16及实际version/limits/check/once PASS；少的3项为Unix-only权限/符号链接及SIGINT/SIGTERM测试，不计作Windows通过。Windows真实Ctrl-C控制台行为仍为NOT RUN，不能用Unix signal测试替代。

ZIP均通过公开下载代理取得，SHA256与GitHub官方artifact digest一致；仅读取有界日志，不执行下载内容。完整job/step/SHA和摘要见target/dx-phase1/native-ci-verification.json。

## 11. git diff --stat

相对起点e7d8bfe的实现提交：

```text
.github/workflows/dx-platform.yml         |   39 +
 CONTRIBUTING.md                           |    3 +-
 Cargo.lock                                |   70 ++
 README.md                                 |   44 +-
 README.zh-CN.md                           |   39 +-
 apps/netbaiot-cli/Cargo.toml              |   19 +-
 apps/netbaiot-cli/src/args.rs             |  230 +++++
 apps/netbaiot-cli/src/demo.rs             |  489 +++++++++
 apps/netbaiot-cli/src/main.rs             |  759 ++++++--------
 apps/netbaiot-cli/tests/dx.rs             |  172 +++
 apps/netbaiot-cli/tests/smoke.rs          |    2 +-
 apps/netbaiot-server/Cargo.toml           |    1 +
 apps/netbaiot-server/src/auth_provider.rs |  272 +++++
 apps/netbaiot-server/src/bootstrap.rs     |   47 +
 apps/netbaiot-server/src/config.rs        |  350 +++++++
 apps/netbaiot-server/src/delivery.rs      |   87 ++
 apps/netbaiot-server/src/diagnostics.rs   |  516 +++++++++
 apps/netbaiot-server/src/entry.rs         |   86 ++
 apps/netbaiot-server/src/lib.rs           | 1610 +----------------------------
 apps/netbaiot-server/src/main.rs          |   50 +-
 apps/netbaiot-server/src/server.rs        |  765 ++++++++++++++
 apps/netbaiot-server/src/tls.rs           |   67 ++
 apps/netbaiot-server/tests/server.rs      |    6 +-
 docs/cli.md                               |  106 +-
 docs/getting-started.md                   |    3 +
 docs/operations-guide.md                  |   11 +-
 docs/quick-start.md                       |   42 +-
 scripts/demo/start.sh                     |    1 +
 scripts/release_preflight.py              |    2 +-
 tests/release_archive_smoke.py            |   14 +
 30 files changed, 3774 insertions(+), 2128 deletions(-)
```

报告单独提交，未修改main/审计分支、历史benchmark、release版本或生产数据。

## 12. Remaining issues

- 三平台原生本阶段门禁已通过；Windows真实控制台Ctrl-C仍未实测，已明确区分它与CancellationToken/once清理测试。
- 配置检查只是local preflight；端口占用/证书期限/remote probes不在第一阶段验证范围，也不保证recovery内容合法。
- token-file属于可选项，未扩大本轮范围；仍建议生产secret使用保护的环境来源。
- 没有已知未修复的第一阶段本地验收失败。下一阶段依用户顺序，在第一阶段验收完成后实施init/doctor/Schema/xtask，在本阶段完成后继续代码改造。

本地target构建/日志目录在第二阶段期间消失，原因未确定；原本本地日志不再可用，不重造原始记录。第一阶段GitHub原生日志重新下载并对照官方SHA256核验，摘要/执行摘录已保存在[证据目录](performance/developer-experience/evidence/)。完整重新取得的原生日志保留在独立临时证据目录，后续阶段另行记录执行结果。
