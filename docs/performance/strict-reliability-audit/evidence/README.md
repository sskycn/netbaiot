# 审计执行证据

起点f68bfd4，最终生产修复f3a5d7c；之后仅测试模块位置、official-client端口准备/启动失败诊断和报告调整。原c235f48验证摘要仍保留为阶段证据，不能当作后续stream修复的门禁。完整日志已从Git移除，原路径、大小和SHA-256见[archive manifest](../../archive-manifest.json)，本机原件保存在repo-root `local-performance-archive/`。

- `validation-final.json`, `validation-stream-final.json`, `validation-ci-diagnostics.json`：保留各阶段命令、通过计数和源码 provenance；完整 `*-tests-*.log` 文件在archive manifest中登记。
- `reproductions.log`：原问题red/green、相关基线、ownership、storage repair、soak、AuthCache实际输出。red是有意捕捉原问题的FAIL；原日志仅保留在本地副本。
- `native-summary.json`, `native-fourth-summary.json`：保留三平台实际job/step、完整suite计数、失败边界和GitHub artifact摘要；完整本地日志已归档。
- `official-client-port-collision-red.log`、`windows-first-failure.log` 等原始日志：原始SHA和路径见archive manifest；本机完整文件仍在本地副本。
- `external-and-gates.log`、protocol/gate JSON：MQTT/SDK/格式/clippy。protocol JSON含c235f48、空tracked diff和binary SHA256；之后MQTT生产路径未改，常规CI也重跑MQTT gate。
- `fuzz-smoke.log`：四个10,000-run ASan smoke末尾；完整原始日志见archive manifest及本机副本。
- `first-msrv-jwks-failure.log`：未隐藏本地首轮既有JWKS等待超时；同代码单项及整套复跑通过。
- `cargo-audit.json`：包括0 known vulnerabilities和现存unmaintained warning。
- `manifest.json`：保留初次完整本地原始日志SHA256；当前GitHub run/Artifact信息仅按源JSON中可验证的元数据记录，不补造缺失ID。

此前提交的副本只脱敏本地路径/去除ANSI颜色；不修改结果或断言。原始日志从Git移除，local-performance-archive保留了本地副本。native ZIP从公开代理取得，与GitHub API artifact SHA256一致，未执行下载内容。最后分支提交的CI仍须按exact SHA核对；生产代码native proof与后续纯测试/报告修改清楚区分。完整分类、兼容性和发布条件见 [审计报告](../../../strict-reliability-audit.md)。
