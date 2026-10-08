# Linux 优先平台调整交付报告

基线：`bceb1de`。本次只调整平台政策、构建发布工具、CI 与文档。

## 1. 平台支持结果

- Linux x86_64 / ARM64：正式生产支持，两个 GNU 发布包均为必需目标。
- macOS x86_64 / Apple Silicon：开发与测试，保留源码运行、自动 native 验证与独立可选构建。
- Windows x86_64 MSVC：实验性兼容，源码构建与手动独立验证，无生产稳定性或每次发布二进制承诺。

## 2. 修改文件

| 文件 | 作用 |
| --- | --- |
| `scripts/release_targets.json`, `release_targets.py` | 唯一必需/可选目标、runner、二进制架构清单；生成 Actions matrix |
| `scripts/release_preflight.py` | 引用统一清单，强制包含平台说明 |
| `scripts/release_package.py` | Linux 必需集合、可选资产完整验证、架构识别、精确 SHA256 验证与发布资产清单 |
| `tools/netbaiot-xtask/src/main.rs` | 构建前从统一清单校验目标，主机/显式目标打包接口不变 |
| `.github/workflows/linux-packages.yml` | 两架构原生质量/恢复、release build、真实包 smoke、SHA256 门禁 |
| `.github/workflows/ci.yml`, `release.yml` | 复用 Linux 流程，正式发布只依赖 Linux 必需链 |
| `.github/workflows/optional-platforms.yml` | 手动 macOS/Windows 真正验证、可选 Actions artifacts，失败如实失败 |
| `.github/workflows/dx-platform.yml`, `recovery-platform.yml` | 保留 Linux/macOS 自动验证，Windows 从自动矩阵拆出 |
| `tests/test_release_tooling.py`, `test_release_platform_policy.py` | 必需/可选集合、损坏/误标架构、校验和与真实工作流依赖图测试 |
| `README.md`, `README.zh-CN.md` | 对齐三档支持政策 |
| `docs/platform-support.md`, `quick-start.md`, `operations-guide.md`, `release-template.md` | 生产平台、使用方式、发布操作与管理员 Required Checks 说明 |
| `docs/releases/v0.2.3.md`, `v0.2.3-validation.md` | 明确旧五平台策略的历史性，不改历史验证数据或远端 Release 资产 |
| `docs/linux-first-platform-report.md` | 本报告 |
| `docs/linux-first-platform-validation.json` | 已核实的双架构实际运行、SHA256 与质量门禁证据 |

## 3. CI/CD 变化

必需链：`release-verify`（MSRV/stable fmt/Clippy/workspace、MQTT、audit、preflight）
→ `linux-packages`（原生 x86_64 与 ARM64 完整质量/恢复、构建、打包、真实执行）
→ 合并两包、验证归档及 SHA256 → tag 发布。任一失败，后续门禁/发布停止。
CI 与正式 Release 使用同一 reusable workflow，ARM64 在原生 Ubuntu runner 执行。

macOS 自动开发/恢复检查及可选平台手动流程均不在上述 `needs` 链中。
Windows 诊断工作流仍手动；可选失败保持 failure，无 `|| true`、忽略断言或
`continue-on-error`。自动化测试直接读取依赖图，验证 Linux 失败阻断、Windows/macOS
独立失败不阻断、可选流程不会伪装成功。

## 4. 发布结果

正式集合至少包含 Linux x86_64 与 ARM64 两个 `.tar.gz` 和精确对应的 `SHA256SUMS`。
默认 checksums 缺少任一 Linux 包都失败；出现可选包时也验证结构、架构、路径和摘要。
未知资产、错误版本、坏包、路径穿越、链接绕过、重复成员、错误架构或摘要/行集合不一致均失败。
`--optional-only` 专用于独立非 Linux artifacts，不能含不完整 Linux 集合或被正式发布使用。
发布使用验证后的文件清单，不上传无关 dist 文件。

本任务不创建/移动 tag、不发布正式版本、不修改或删除历史 Releases。实际 Linux 验证结果见下表。

## 5. 兼容性影响

MQTT/TCP/UDP、认证/ACL、DeviceEvent、EventBus、Business RPC V3、ACK、在线命令、
TLS/管理边界、恢复格式 NBSP v3 / NBMQ v6、资源预算、公开 API 与配置格式均不变。
Cargo.toml / Cargo.lock 无改动，没有新增依赖。Windows recovery_io、原子替换/reparse-point
保护及 server 的有界 WSAEACCES 自动端口逻辑保持原样。

工具行为变化：默认 checksum 集合由强制五包改为必需两包加所有出现的可选包；
新增 `verify-checksums`、`assets` 和显式 `--optional-only`。既有单目标打包与资产命名不变，
但新增二进制架构检查，防止把 x86_64 ELF 误标 ARM64。

## 6. 测试结果

| 验证 | 状态 |
| --- | --- |
| 本机 cargo fmt / locked workspace all-targets all-features Clippy / tests / xtask check | PASS：496 workspace tests |
| 发布工具及平台策略 Python tests | PASS：20 tests，保留旧全五目标安全用例并增加 Linux-only 用例 |
| 发布 preflight / schema-reference / YAML 语法 | PASS |
| macOS 原生可选包、SHA256、实际 archive smoke | PASS：CLI、MQTT/TCP/UDP、graceful shutdown，无 Cargo 运行回退 |
| Linux x86_64 / ARM64 原生完整构建、实际运行、精确 SHA256 | PASS：两种原生 runner 完整检查、构建、包验证、真实 CLI/MQTT/TCP/UDP/graceful smoke、合并 SHA256；本机 Linux 执行 NOT RUN（macOS 主机），由 CI 真实验证 |
| Windows/macOS 可选手动发布工作流 | NOT RUN：独立按需；不影响 Linux 正式链 |
| 新 tag / 正式发布 / 历史资产修改 | NOT RUN：未授权发布，本次不执行 |
| 长期 soak / 新 fuzz campaign | NOT RUN：运行时、协议、spool decoder 未改变；workspace 的现有恢复/网络测试照常执行 |

不把格式 stub 测试当成实际二进制验证，不把交叉构建或配置上限当成运行能力。
对应[完整 Linux CI](https://github.com/sskycn/netbaiot/actions/runs/37734523362)
的九个门禁 job 全部 PASS；小型[验证记录](linux-first-platform-validation.json)保存逐 job 链接。
当前没有未解决的 FAIL / BLOCKED。原生 ARM runner 配置参考
[GitHub 官方 runner 列表](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)。

## 7. 剩余问题与管理员操作

管理员需审查 branch protection / rulesets / tag / environment required checks，保留 Linux
质量、两架构构建运行、精确 SHA256 门禁，移除非生产 Windows/macOS 检查的强制绑定。
YAML 不自动修改外部规则，本任务未修改这些设置。主机发行版需符合 Ubuntu 24.04 GNU
产物的 loader/glibc 要求并完成环境验证；没有宣称认证所有发行版或生产容量/SLA。
