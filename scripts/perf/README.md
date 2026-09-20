# Event-gateway load tools

The Rust `netbaiot-loadgen` is the supported load generator. It targets the
database-free device listeners and reports accepted-event and transport latency.
Do not use the historical PostgreSQL-era JSON results in `docs/performance` as
evidence for the current architecture.

Build release binaries:

```bash
cargo build --release -p netbaiot-server -p netbaiot-loadgen
```

Start `netbaiot-server` with a confirmed local webhook or TCP/RPC sink, then run:

```bash
target/release/netbaiot-loadgen --help
```

Measure at minimum event rate/bytes, device-to-EventAccepted and sink-ACK
P50/P95/P99, queue count/bytes, RSS, CPU, tasks, FDs, and rejection/drop counts.
A sustainable rate requires stable queues and RSS after warmup. Connection memory
experiments must distinguish plaintext, TLS, generic TCP, application logical
bytes, Tokio/TLS/allocator overhead, and kernel buffers.

Reproducible MQTT probes:

```bash
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --tls
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --persistent
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --persistent --subscribe
python3 scripts/perf/connection_memory.py --transport mqtt --connections 10000 --persistent --disconnected
python3 scripts/perf/event_load.py --rate 10000 --duration 20 --connections 64
python3 scripts/perf/event_load.py --rate 1000 --duration 15 --connections 32 --sink-delay-ms 10
cargo bench --bench foundation
```

`--disconnected` measures RSS after every socket/task/FD has gone while persistent
MQTT and auth-cache state remains. Broker logical session bytes are printed by the
foundation benchmark; RSS additionally includes hash tables, allocator capacity,
credentials/auth cache, metrics, runtime, and process overhead.
