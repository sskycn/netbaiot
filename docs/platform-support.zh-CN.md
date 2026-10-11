# 平台支持

[English / bilingual summary](platform-support.md)

| 平台 | 支持级别 | 发布政策 |
| --- | --- | --- |
| Linux x86_64 / ARM64（aarch64） | 正式生产 | 两个 GNU Linux 包都必须产出；任一 Linux 门禁失败都会阻止发布 |
| macOS x86_64 / Apple Silicon | 开发与测试 | 支持源码构建、原生开发/恢复自动检查和独立可选产物 |
| Windows x86_64 MSVC | 实验性兼容 | 支持本地构建与手动检查；不承诺生产稳定性或每个版本都有二进制 |

生产环境推荐 Linux。两个架构都在 Ubuntu 24.04 上原生构建与运行，执行完整 workspace
质量/恢复测试，以及解压后 binary 的 MQTT/TCP/UDP/优雅关机 smoke。GNU binary 依赖
runner 对应的 Linux loader/glibc 兼容性；其他发行版必须自行验证这些要求、TLS、资源
设置与恢复行为，仓库没有逐一认证。

macOS 用于开发、调试和测试。Windows 保留源码编译和兼容实现，但不保证生产稳定性或
每次 Release 都有官方 binary。macOS/Windows 独立流程失败不阻断 Linux 发布。

## 构建与使用

```sh
cargo +1.88.0 build --locked
cargo run --locked -p netbaiot-cli -- demo --once
cargo xtask package --target x86_64-unknown-linux-gnu
# 在原生 Linux ARM64 主机上：
cargo xtask package --target aarch64-unknown-linux-gnu
```

应使用与主机匹配的架构；交叉编译成功不是实际运行证据。macOS 可使用
`cargo xtask package` 打包探测到的主机，或在配置对应 Rust target/linker 后构建某个
Darwin target。Windows PowerShell 可运行 `cargo build --locked`，再执行
`.\target\debug\netbaiot.exe demo --once`；源码与 release tool 工作需要正常 MSVC build
tools 和 Python。这里没有人为加入全局平台阻断。既有 Windows recovery/reparse-point
保护、原子替换和自动端口的有界 WSAEACCES 处理仍保留。

## 发布与独立验证

`scripts/release_targets.json` 是 target/runner/architecture 的单一来源。
`release-verify.yml` 在 Linux 上保留 MSRV/stable fmt、Clippy、workspace tests、audit
和完整 MQTT 互操作。CI 与 tag release 都调用 `linux-packages.yml`：两个原生 Linux
target 都必须构建、校验、执行其压缩包，并通过精确集合 SHA256 验证。发布只依赖这些
required Linux jobs。手动 release rehearsal 不创建 tag 或 Release，也不修改历史资产。

正式资产集合包含：

- `netbaiot-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz`；
- `netbaiot-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz`；
- `SHA256SUMS`，精确覆盖当前已验证压缩包。

`checksums` 要求两个 Linux 包，并验证出现的每个可选包。未知、不完整、架构错误、路径
不安全或损坏的资产都会失败。`verify-checksums` 检查摘要与文件名/行完整性，不覆盖文件。
包名和内容保持兼容；Windows/macOS 为可选。

```sh
python3 scripts/release_package.py checksums --tag vX.Y.Z --dist dist
python3 scripts/release_package.py verify-checksums --tag vX.Y.Z --dist dist
(cd dist && sha256sum -c SHA256SUMS)
```

`optional-platforms.yml` 是手动、只读、独立流程，可构建两个 Darwin target 和/或
Windows，执行真实检查并保存带独立 SHA256SUMS 的 Actions artifact。`--optional-only`
只验证开发产物，不能豁免正式 Linux 要求。Windows 诊断 workflow 继续手动执行；失败会
正常报告，不用忽略断言或 `continue-on-error` 掩盖。

## 仓库管理员检查

YAML 不会自动修改 branch protection、ruleset、tag 限制或 environment approval。
管理员应要求 Linux MSRV/stable/MQTT/audit/preflight、两个
`Build and run <Linux target>` job 与 `Verify required Linux archives and SHA256`；
从生产/tag 的 Required Status Checks 和发布环境依赖中移除 macOS 开发与 Windows 实验
检查名称，同时保留其独立可见性。修改规则前应以实际生成的 check name 为准。
