# 基准与测量

[English](benchmarks.md)

[2026-10-08 工程优化报告](engineering-optimization.md)记录了 EventBus responsibility
增量记账、借用 HTTP JSON 序列化、有界 HTTP outage 恢复和 MQTT session 矩阵；整理后
的测量位于 `docs/performance/engineering-optimization/`。

仓库保留性能、资源和可靠性实验。这些数据只证明其标注的环境与配置；部分来自历史
commit，不能当作当前 revision 或生产部署容量。

## 工程热点专项

[工程热点报告](performance/engineering-hotspots/report.md)记录路由正确性、presence/rate
table 复杂度、MQTT borrowing/shared payload、EventBus ready/deadline queue、session scan
评估和 broker 物理模块拆分。报告包含 release A/B 矩阵、allocation/lock/RSS、MQTT/UDP、
timeout/recovery 检查与明确未运行清单。固定负载结果只是在该环境下的回归证据，不是
生产容量。评估后 session usage 和 expiry scan 没有改变。

## 已发布基线背景

主要历史 MQTT 基线使用以下环境：

| 项目 | 记录环境 |
| --- | --- |
| 硬件 | Mac mini `Mac16,10`，Apple M4 10-core CPU，16 GiB RAM |
| OS | macOS 26.6.2，Darwin 25.6.0，arm64 |
| 构建 | Cargo locked release build；Rust 1.88.0 基线工具链 |
| 网络 | 单主机 IPv4 loopback；server 与 load generator 共用主机 |
| MQTT publisher | 主要 QoS throughput 点使用 64 个 publisher |
| 载荷 | 256 字节生成 JSON |
| Sink | 主要 MQTT 表使用进程内 required audit sink；没有外部 HTTP sink |
| 时长 | 5 秒 warm-up、20 秒测量、2 秒 cooldown；重复三次 |
| TLS | 主要 QoS 表未标注 TLS profile；TLS 连接内存和独立 throughput 实验在详细报告中单独标注 |
| 延迟 | 分别报告 QoS1 PUBACK 与 QoS2 PUBCOMP 分位数；QoS0 没有协议 ACK latency |

主要表在其基线 commit 上报告中位完成率：QoS0 为 24,159.9/s，QoS1 为
19,956.4 PUBACK/s，QoS2 为 19,992.8 PUBCOMP/s。这些是历史同机结果，不是 SLO 或
生产上限。原报告记录了 commit、重复样本、tail latency、CPU、RSS、pending work 和
后续实验。

## 这些数字不代表什么

- 不是生产容量保证，也不承诺适用于其他 revision；
- load generator 与 server 共用 CPU 和 loopback，不能隔离远程 server ceiling；
- localhost 不能代表物理网络、WAN、丢包、NAT 或多主机部署；
- TLS、payload、publisher 数量、QoS、retain 路由、fanout 和业务 sink latency 都会改变
  CPU、内存与 tail latency；
- 进程内 audit sink 不能代表客户数据库、webhook 或远程 RPC；单独测量的 Python HTTP
  sink 在其配置中可能先成为瓶颈；
- burst、microbenchmark、配置最大值或历史数据库时期结果都不是当前生产容量声明。

## 已知测量限制与热点

基线文档中最近的 separate-host audit 只完成准备和单机 control，没有建立新的 dual-host
容量结果。历史 shared-host load generator 在更高 offered rate 下也接近自身 CPU 上限。
测量的 Python webhook 路径在其测试配置中早于 gateway 成为 sink bottleneck。MQTT
wildcard retained replay 会扫描有界 retained store。详细报告列出了未测项目，包括更多
大 fanout、dual-host 与部署特定组合。

## 详细记录

- [性能基线与历史结果](performance-baseline.md)
- [Separate-host audit 准备与 control](performance-separate-host-audit.md)
- [混合入口容量审计](mixed-ingress-capacity-audit.md)
- [复现实验脚本](../scripts/perf/README.md)

发布或引用任何数字前，先阅读对应报告的 measurement SHA、transport/TLS 模式、负载、
sink、时间窗口和限制。生产选型应在目标硬件与网络上，使用计划中的 TLS、认证 provider、
事件 fanout 和业务 sink 重新测量。
