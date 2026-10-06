# 审计执行证据

起点f68bfd4，最终生产修复f3a5d7c；之后仅测试模块位置和报告调整。原c235f48验证文件仍保留为阶段证据，不能当作后续stream修复的门禁。

- `validation-final.json` / `*-tests-final.log`：c235f48干净提交的初次完整门禁，各418 passed、0 failed、16 ignored，以及MQTT/SDK命令和二进制/source manifest。
- `validation-stream-final.json` / `*-tests-stream-final.log`：最终stream修复+测试位置整理的末轮本地门禁，含tracked diff hash；stable/MSRV各420/0/16。
- `reproductions.log`：原问题red/green、相关基线、ownership、storage repair、soak、AuthCache实际输出。red是有意捕捉原问题的FAIL。
- `native-summary.json` / `native-*-selected.log`：最终生产修复的三平台实际job/step、selected路径、完整suite计数和GitHub artifact摘要；专项重复运行不重复计入full workspace计数。
- `windows-first-failure.log` / `windows-second-failure.log`：真实Windows失败，随后同一原断言通过。常规CI另曾因新增test模块位置clippy FAIL，已移动模块而未加allow。
- `external-and-gates.log`、protocol/gate JSON：MQTT/SDK/格式/clippy。protocol JSON含c235f48、空tracked diff和binary SHA256；之后MQTT生产路径未改，常规CI也重跑MQTT gate。
- `fuzz-smoke.log`：四个10,000-run ASan smoke末尾；完整原始日志在本地target/strict-audit。
- `first-msrv-jwks-failure.log`：未隐藏本地首轮既有JWKS等待超时；同代码单项及整套复跑通过。
- `cargo-audit.json`：包括0 known vulnerabilities和现存unmaintained warning。
- `manifest.json`：初次完整本地原始日志SHA256；后续原生ZIP/完整log与GitHub run元数据在target/strict-audit，native-summary另存摘要。

副本只脱敏本地路径/去除ANSI颜色；不修改结果或断言。原始日志保留。native ZIP从公开代理取得，与GitHub API artifact SHA256一致，未执行下载内容。最后分支提交的CI仍须按exact SHA核对；生产代码native proof与后续纯测试/报告修改清楚区分。完整分类、兼容性和发布条件见 [审计报告](../../../strict-reliability-audit.md)。
