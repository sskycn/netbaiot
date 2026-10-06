# Developer Experience 执行证据

第一阶段：实际提交4cb93af的原生/常规CI全部PASS。phase1 verified JSON包含artifact官方摘要、日志摘要和平台计数，selected log为实际执行摘录。原始GitHub日志已重新下载并校验SHA256。第一阶段本地target目录后来消失，原本本地日志不再存在；不重造或伪称已保留这些原始日志。

第二阶段：daily/release日志为实际cargo xtask执行，validation JSON记录命令、退出码、耗时与原始日志SHA256。日志副本只去ANSI、归一化机器路径和行尾空白，不修改结果。原始日志保存在独立临时证据目录。actual-cli JSON包含真实init/check/doctor/schema输出；source manifest包含新增未跟踪模块在内的最终源码摘要。

审计失败与V3超时日志保留；DNS依赖升级至修复版后audit和完整gate通过，V3fixture配对TCP/UDP端口并保留原超时/业务断言。没有通过忽略公告或跳过测试取得绿色结果。ignored、SKIP和未运行项按报告说明，不算PASS。

最终代码提交f9e1047的三平台原生与常规CI全部PASS。[最终CI证据](phase2-final-ci-verification.json)记录确切SHA、job结果、平台计数、官方artifact ZIP摘要和原始日志摘要；各ZIP均已核对官方SHA256，每个平台实际CLI及schema/reference drift检查均PASS。Linux/macOS各433 passed、Windows430 passed，均0 failed/16 ignored。历史失败保留，未用第一阶段绿色替代本阶段结果。详见两个阶段验收报告。
