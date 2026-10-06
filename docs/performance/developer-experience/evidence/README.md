# Developer Experience 执行证据

第一阶段：实际提交4cb93af的原生/常规CI全部PASS。phase1 verified JSON包含artifact官方摘要、日志摘要和平台计数。原始GitHub日志曾重新下载并校验SHA256；未提交的原始下载后来已不存在，提交过的日志副本已从Git移除，其路径和SHA-256见archive manifest及本机预清理副本。不重造或伪称当前存在Actions Artifact。

第二阶段：daily/release日志为实际cargo xtask执行，validation JSON记录命令、退出码、耗时与原始日志SHA256。已提交日志副本只去ANSI、归一化机器路径和行尾空白，不修改结果；这些日志当前已从Git移除，并登记在archive manifest、本机预清理副本中。actual-cli JSON包含真实init/check/doctor/schema输出；source manifest包含新增未跟踪模块在内的最终源码摘要。

审计失败与V3超时的结构化摘要保留；原始日志已从Git移除，完整索引在archive manifest。DNS依赖升级至修复版后audit和完整gate通过，V3fixture配对TCP/UDP端口并保留原超时/业务断言。没有通过忽略公告或跳过测试取得绿色结果。ignored、SKIP和未运行项按报告说明，不算PASS。

最终代码提交f9e1047的三平台原生与常规CI全部PASS。[最终CI证据](phase2-final-ci-verification.json)记录确切SHA、job结果、平台计数、官方artifact ZIP摘要和原始日志摘要；各ZIP均已核对官方SHA256，每个平台实际CLI及schema/reference drift检查均PASS。Linux/macOS各433 passed、Windows430 passed，均0 failed/16 ignored。历史失败保留，未用第一阶段绿色替代本阶段结果。详见两个阶段验收报告。

首次合并main后的macOS恢复workflow出现V3连接清理超时，另三个workflow通过；[失败与修复证据](post-merge-v3-close-evidence.json)保留确切main SHA、CI状态、官方artifact摘要及修复前后确定性测试结果。两个取消回归修复前实际FAIL、修复后PASS，原7项真实V3测试全部PASS；不把20次本地原测试通过当成失败CI通过或精确因果证明。post-merge源码指纹单独保存，不重写f9e1047的历史证据。

V3修复合并后的74824ba通过Rust/release gate与三平台DX，恢复workflow的macOS/Linux通过，Windows两项在readiness阶段失败；[证据](post-merge-windows-restart-evidence.json)保留官方artifact摘要和具体失败。重启fixture增加配对TCP/UDP探测、低于管理限流的探测周期及有界child日志诊断，原5秒期限与恢复断言不变；本地专项和实际60秒多代soak通过。未捕获原child stderr，不能虚称已证明当次失败listener。
