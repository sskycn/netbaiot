# Capacity and stability testing

[中文](production-readiness.zh-CN.md)

`netbaiot-loadgen` is an independent Rust process using real MQTT 3.1.1/5.0,
TCP and signed UDP sockets with all four built-in V1 codecs. It does not replace
network paths with server-internal calls or change protocol/acceptance semantics.

## Running load

```bash
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bin netbaiot-server --bin netbaiot-loadgen
python3 scripts/perf/production_readiness.py --devices 100 1000 5000 10000 \
  --scenario sensor telemetry16 telemetry64 --duration 30 --sample-interval 5 --tls \
  --output target/performance/production-readiness/capacity
```

The default matrix covers four codecs and four transports. Filter with
`--codec json cbor msgpack protobuf` or `--transport mqtt311 mqtt5 tcp udp`.
`sensor` sends temperature/humidity/battery every ten seconds per device;
`telemetry16/telemetry64` send 16/64 fields every second. `--rate` sets the total
offered rate; missed schedule slots are recorded separately. TLS uses localhost
test certificates. UDP retains HMAC and its 1200-byte ceiling.

Faults are `wrong_hmac`, `wrong_version`, `wrong_codec_version`, `oversize`,
`repeat_sequence`, `reconnect` and `half_connection`. Unsupported static codec
profiles may fail before startup; that is not evidence of an exercised network fault.

For soak, use a fresh, unoccupied directory:

```bash
python3 scripts/perf/production_soak.py --devices 1000 \
  --output target/performance/production-readiness/soak
```

The controller freezes executables and owns eight finite runs: four codec/transport
pairs at 6 and 24 hours, sampling every minute. Each has its own gateway; eight
1000-device runs are not one 8000-device gateway. Ctrl-C stops owned gateway/load/
sink children. Soak uses actual elapsed time.

## Report definitions and limits

Each case saves summary, configuration, environment, executable SHA256, before/after
metrics, final status, resource samples and load samples. Raw captures remain in
ignored `target/performance/`; Git retains compact results and provenance.

Connection, TLS, authentication, reconnect, send, transport receive, EventAccepted,
verified device receipt, application ACK and gateway-observed Sink ACK are separate
counts. Device-send to test-business-ACK latency uses the load process's monotonic
clock; it does not prove business persistence or device execution. Histogram stage
percentiles are bucket upper bounds and cannot be summed into end-to-end percentiles.

The generator owns at most 10000 per-device tasks; its expiring business observation
window holds at most 320000 records. The ACK probe allows 64 connections, 8 KiB
headers, 64 KiB bodies and five-second deadlines. Cancellation joins tasks; metrics
never label devices or record payloads. Growth analysis excludes warmup and emits
candidate reproducers that still need minimization, cause analysis and actual reruns.
`--profiling` distinguishes diagnostic admission pressure from a healthy capacity PASS.

## Verified evidence and remaining work

The [local preflight](performance/production-readiness/local-preflight.json),
[manifest](performance/production-readiness/local-manifest.json) and
[stopped run](performance/production-readiness/stopped-summary.json) preserve macOS
ARM64 results: four codec/MQTT3.1.1 TLS 1000-device 12-second checks each completed
1200 sends/acceptances/confirmations. A JSON MQTT5/TLS telemetry64 35-second check
confirmed 34999 events with P95 0.41 ms, P99 3.23 ms and peak sampled RSS 55040 KiB.
These short colocated checks do not establish maximum stable throughput.

[Native Linux CI run 38886325364](https://github.com/sskycn/netbaiot/actions/runs/38886325364),
revision `8c41c55` on 2026-10-16, passed x86_64 and ARM64 short load, abnormal-device,
CPU stacks/folded stacks/flamegraph and heaptrack steps. The workflow checks nonempty
CPU outputs and runs `heaptrack_print`. Earlier [independent artifact inspection](performance/strict-review-20261013.md#native-linux-ci-profiling)
also records nonempty profiling evidence on both architectures. This evidence does
not cover every subsequent `main` change.

The default workflow uses 100 devices and ten-second cases across four codecs/
transports plus seven fault scripts. CPU profiling uses 1000 MQTT5/TLS telemetry64
devices for 60 seconds, with 20 seconds of sampling; heaptrack load is 30 seconds.
The optional capacity mode defines 192 cases per architecture up to 10000 devices;
default CI success does not establish that mode or a completed 6/24-hour soak.
The local soak stopped after about 14 minutes. Full target-hardware capacity/fault
coverage, dedicated NIC/WAN and long-term leak validation remain incomplete.
No production CPU/memory/network specification for 1000/5000/10000 devices is given.
See [operations](operations-guide.md) and [measurement requirements](benchmarks.md).
