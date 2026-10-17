# 基准与测量

[English](benchmarks.md)

当前网关尚未建立生产容量结论。现有数据覆盖短期 loopback 负载和 Codec 对照，
不足以给出 1000、5000 或 10000 台设备的生产部署规格。

## 保留的测量记录

- [容量与稳定性测试](production-readiness.zh-CN.md)：独立负载工具、报告口径和未完成的生产验证。
- [Codec 对照结果](performance/codec-audit/README.md)：精简结果及源码、二进制和环境来源。
  Codec 微基准与真实 socket 测量分别记录，延迟口径不同。
- [复现工具](../scripts/perf/README.md)：连接内存、MQTT、EventBus 和 Codec 探针。

每项结果只适用于其记录的 revision、构建、负载和时间窗口。短测试、配置上限和
历史结果不能证明最大稳定吞吐。同机 server 与 generator 共用 CPU 和 loopback。

## 部署前的测量要求

在目标硬件和实际网络上测量计划使用的 Codec、MQTT 版本/QoS、payload、TLS、
认证 provider、路由、fanout 和需确认业务 Sink。分别记录计划发送、实际发送、
EventAccepted、Sink ACK、拒绝、丢弃和错过的发送时间槽。

记录连接成功率、TLS/认证时间、端到端 P50/P95/P99、CPU、RSS/增长、FD、task、
session 和队列 count/bytes。Codec 耗时与端到端耗时分开；阶段分位数不能相加。
socket 写入成功不证明接纳或业务处理。

稳定容量需要预热后资源与队列稳定、完整长期运行，以及故障/重启验证。
CPU/RSS 包含 runtime、TLS、allocator 和 kernel 成本，不能用逻辑 payload 记账代替。

## 证据类别

CI Smoke 验证固定 revision 的有界场景；真实负载使用实际 socket 和 ACK；Codec
微基准只测转换成本；profiling 记录采样 CPU/heap。Soak 必须完整运行声明的真实时长，
预热后资源稳定；生产容量还需目标硬件/网络/业务 Sink 及故障和重启覆盖。
[Linux 原生 profiling 已通过](production-readiness.zh-CN.md)，不等于长期稳定性或当前
`main` 的生产容量认证。
