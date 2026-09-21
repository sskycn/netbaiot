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
python3 scripts/perf/connection_memory.py --transport tcp --connections 1000
python3 scripts/perf/connection_memory.py --transport tcp --connections 3000
python3 scripts/perf/event_load.py --rate 10000 --duration 20 --warmup 5 --connections 64 --sink-mode none --qos 1
python3 scripts/perf/event_load.py --rate 1000 --duration 15 --connections 32 --sink-mode webhook --sink-delay-ms 10
python3 scripts/perf/event_load.py --rate 10000 --duration 15 --warmup 5 --connections 64 --sink-mode none --qos 1 --tls
python3 scripts/perf/mixed_load.py --scenario route-fairness --duration 15 --warmup 5 --cooldown 2 --sample-every 1
python3 scripts/perf/mixed_load.py --duration 1800 --warmup 30 --cooldown 10 --sample-every 5
cargo bench --bench foundation
cargo test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

`--disconnected` measures RSS after every socket/task/FD has gone while persistent
MQTT and auth-cache state remains. Broker logical session bytes are printed by the
foundation benchmark; RSS additionally includes hash tables, allocator capacity,
credentials/auth cache, metrics, runtime, and process overhead.

`mixed_load.py` uses 1,000 MQTT devices (70% idle, 20% at 1 msg/s, 9% at
10 msg/s, and 1% bursty), an approximately 60/30/10 QoS0/QoS1/QoS2 event mix,
persistent reconnects, low-frequency command/downlink traffic, and the confirmed
HTTP webhook. It writes one JSON result to stdout; redirect it under
`target/perf-audit/`. Run the server and load generator on separate hosts before
treating throughput as a production hardware ceiling.

The `route-fairness` scenario instead runs one saturated QoS1 publisher beside
100 publishers at 10 msg/s each, without an external business sink. Compare the
`low-rate-q1` PUBACK P99 and completion count between builds. Both event and mixed
drivers accept `--server-bin` and `--loadgen-bin`, which permits an exact-SHA
comparison binary from an isolated worktree without moving the current checkout.

The event and mixed drivers set `NETBAIOT_PERF_LOCK_METRICS=1` for the child
server. Broker/EventBus lock timing is disabled by default outside these drivers.

The ignored restart soak runs 12 healthy process generations with a five-second
dwell per generation after the initial forced-spool/recovery pair. Each generation
waits for the accepted `event_id` at the confirmed webhook, drains, exits, and
verifies that no committed EventBus spool segment remains.
