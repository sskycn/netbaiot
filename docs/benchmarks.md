# Benchmarks and measurements

[中文](benchmarks.zh-CN.md)

Production capacity for the current gateway has not been established. The available
measurements cover short loopback workloads and codec comparisons; they do not
provide deployment specifications for 1000, 5000 or 10000 devices.

## Available evidence

- [Capacity and stability testing](production-readiness.zh-CN.md): the independent
  load tool, report definitions and incomplete production validation.
- [Codec comparison results](performance/codec-audit/README.md): compact results
  with source, executable and environment provenance. Codec microbenchmarks and
  real socket measurements have separate latency definitions.
- [Reproduction tools](../scripts/perf/README.md): connection memory, MQTT,
  EventBus and codec probes.

Every result applies to its recorded revision, build, workload and time window.
A short test, configured ceiling or historical result does not establish a maximum
stable rate. Server and generator sharing a host also share CPU and loopback.

## Deployment measurements

Measure the intended codecs, MQTT version/QoS, payload size, TLS, authentication
provider, routes, fanout and confirmed business sinks on the target hardware and
network. Record offered, submitted, EventAccepted and Sink ACK counts separately,
including rejections, drops and missed schedule slots.

Report connection success, TLS/authentication timing, end-to-end P50/P95/P99,
CPU, RSS/growth, file descriptors, tasks, sessions and queue count/bytes. Codec
latency is separate from end-to-end latency; stage percentiles cannot be added.
Successful socket writes do not prove acceptance or business processing.

Sustained capacity requires stable resources and queues after warmup, completed
long-duration runs, and verified fault/restart behavior. CPU/RSS readings include
runtime, TLS, allocator and kernel effects beyond logical payload accounting.
