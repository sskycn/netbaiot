# Platform support / 平台支持

[完整中文说明](platform-support.zh-CN.md)

| Platform | Support level | Release policy |
| --- | --- | --- |
| Linux x86_64 / ARM64 (aarch64) | Production / 正式生产 | Both GNU Linux archives required; any Linux gate failure blocks publishing |
| macOS x86_64 / Apple Silicon | Development and testing / 开发与测试 | Source builds, automatic native development/recovery checks, optional independent artifacts |
| Windows x86_64 MSVC | Experimental compatibility / 实验性兼容 | Local builds and manual checks; no production stability or per-release binary guarantee |

Linux is recommended for production. Both architectures build and run natively
on Ubuntu 24.04 in release CI, with full workspace quality/recovery tests and
extracted-binary MQTT/TCP/UDP/graceful-shutdown smoke. GNU binaries require the
runner's Linux loader/glibc compatibility. Other distributions must verify these
requirements, TLS, resource settings and recovery; they are not individually certified.

推荐 Linux 生产部署。两种架构在 Ubuntu 24.04 原生构建、测试并运行实际发布包。
其他发行版需确认 GNU loader/glibc、TLS、资源配置和恢复行为。macOS 用于开发、调试和测试。
Windows 保留本地编译与兼容实现，但不保证生产稳定性或每次 Release 都有官方二进制。
macOS/Windows 独立流程失败不阻断 Linux 发布。

## Build and use

```sh
cargo +1.88.0 build --locked
cargo run --locked -p netbaiot-cli -- demo --once
cargo xtask package --target x86_64-unknown-linux-gnu
# On a native Linux ARM64 host:
cargo xtask package --target aarch64-unknown-linux-gnu
```

Use the host's matching architecture; a cross-build is not execution evidence.
macOS can use `cargo xtask package` for its detected host, or either Darwin target
with the corresponding Rust target/linker setup. Windows PowerShell can run
`cargo build --locked` then `.\target\debug\netbaiot.exe demo --once`; source and
release-tool work needs normal MSVC build tools and Python. No global platform
restriction is added. Existing Windows recovery/reparse-point protections, atomic
replacement and bounded WSAEACCES automatic-port handling remain intact.

发布包必须匹配主机架构，交叉编译成功不等同实际运行验证。macOS 保留源码运行与主机打包。
Windows 可在 PowerShell 使用上述 Cargo 命令；原有恢复安全逻辑和自动端口处理保留，
显式端口失败仍报错，没有增加重试或放宽运行时预算。

## Release and independent verification

`scripts/release_targets.json` is the single target/runner/architecture source.
`release-verify.yml` keeps MSRV/stable fmt, Clippy, workspace tests, audit and full
MQTT interoperability on Linux. CI and tag release both call `linux-packages.yml`:
both native Linux targets must build, validate, execute their archive and pass
exact-set SHA256 verification. Publishing depends only on required Linux jobs.
Manual release rehearsal creates no tag or Release; historical assets stay intact.

The official asset set contains:

- `netbaiot-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`
- `netbaiot-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz`
- `SHA256SUMS`, covering exactly the present validated archives

`checksums` requires both Linux archives and validates every optional archive
present. Unknown, incomplete, wrong-architecture, unsafe or corrupt assets fail.
`verify-checksums` checks digest and filename/row completeness without overwriting.
Package names and contents remain compatible; Windows/macOS are optional.

```sh
python3 scripts/release_package.py checksums --tag vX.Y.Z --dist dist
python3 scripts/release_package.py verify-checksums --tag vX.Y.Z --dist dist
(cd dist && sha256sum -c SHA256SUMS)
```

`optional-platforms.yml` is manual, readonly and independent. It builds both
Darwin targets and/or Windows, performs real checks and retains Actions artifacts
with their own SHA256SUMS. `--optional-only` explicitly verifies development
artifacts and cannot waive an official Linux requirement. Windows diagnostic
workflows remain manual. Failures remain failures; no ignored assertions or
`continue-on-error` masks them.

正式 Release 至少需要两个 Linux 包和 SHA256SUMS，缺少任意 Linux 包都失败。
可选包出现时也完整验证，但不进入 Linux 发布依赖链。独立手动构建提供 Actions artifacts，
不承诺每次正式发布都有 macOS/Windows 二进制。历史资产、协议/API/配置和恢复格式不变。

## Repository administrator checks

YAML does not change branch protection, rulesets, tag restrictions or environment
approvals. Administrators should require Linux MSRV/stable/MQTT/audit/preflight,
both `Build and run <Linux target>` jobs and `Verify required Linux archives and
SHA256`. Remove macOS development and Windows experimental names from mandatory
production/tag checks and release-environment dependencies; keep them visible
independently. Inspect actual generated check names before changing rules.

管理员仍需检查分支保护、rulesets、tag 和发布环境审批：保留全部 Linux 门禁，移除
Windows/macOS 非生产检查的 Required Status Checks 绑定。本任务不自动修改仓库设置。
