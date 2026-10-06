# 审计执行证据

最终代码提交 c235f48e95071596d0caeb526406e9e477dcd7df，起点 f68bfd4。

- `validation-final.json`：在干净最终代码提交上实际运行的命令、退出码和耗时。
- `reproductions.log`：当前源码 red/green、相关基线、ownership、storage repair、soak、AuthCache 原始运行输出拼接；red 是有意捕捉原问题的 FAIL，不是最终门禁失败。
- `stable-tests-final.log` / `msrv-tests-final.log`：最终两套完整 workspace 输出，各418 passed、0 failed、16 ignored。
- `external-and-gates.log`、protocol/gate JSON：最终 MQTT/SDK/格式和 clippy；protocol JSON 包含 commit、空 tracked diff 和 binary SHA256。
- `fuzz-smoke.log`：四个10,000-run ASan smoke 的末尾；完整原始日志在本地 target/strict-audit。
- `first-msrv-jwks-failure.log`：未隐藏首轮既有 JWKS 超时；同代码单项及整套复跑通过。
- `cargo-audit.json`：包括0 known vulnerabilities和现存 unmaintained warning。
- `manifest.json`：完整本地原始日志的 SHA256、代码提交和平台状态。

仓库归档副本只脱敏机器本地路径，不改变测试结果、错误或断言。原始日志未改写。Windows/Linux 原生未运行；CI 配置不能算通过。完整问题分类、兼容性和发布条件见 [审计报告](../../../strict-reliability-audit.md)。
