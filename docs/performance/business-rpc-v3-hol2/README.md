# Business RPC V3 HOL2 raw evidence

See [Chinese report](../../business-rpc-v3-hol2.zh-CN.md). All paths below are relative to this directory. The benchmark binaries came from a dirty task worktree based on `4d3583b1e6e4d57e5cfa8f8a94e3ee7b0aa43ea2`; each `*-experiment.json` stores config, OS, architecture and build SHA. Each raw directory also stores loadgen JSON, gateway/proxy logs and metrics. Frame capture JSONL stores headers/timestamps/read chunk sizes, never application payloads. Debug tracing perturbs LAN latency.

- `formal-60s/summary.json`: 3 fresh runs each of V2 multiplexed, V2 dual, V3 baseline, V3 8/128 KiB and V3 16/128 KiB. All individual runs are retained below it.
- `repeated-backlog-15s/summary.json`: 3 fresh runs each of V3 baseline and 16/128 KiB at 256 KiB/s, 2 Event/s, 16 Auth concurrency.
- `repeated-lan-15s-no-trace/summary.json`: 3 fresh loopback runs each, without per-frame debug logging.
- `matrix-32k-v2/`, `matrix-32k-v3/`, `matrix-256k/`: one-run discovery grids. `matrix-32k/` contains initial failed attempts with a gateway frame limit mismatch. The 256 KiB send-ahead case needs a 256 KiB connection limit; earlier invalid configuration remains preserved.
- `socket-baseline/`, `socket-baseline-small/`, `socket-unique-default/`, `socket-unique-small/`: default versus 4096 B SO_SNDBUF, without and with 16/128 KiB send-ahead.
- `socket-option-probe.json`: standalone connected socket probe for SO_SNDBUF and macOS TCP_NOTSENT_LOWAT. NOTSENT was not integrated into V3.
- `backlog2-*`, `unique-*`, `lan-*`, `discovery-*`, `medium-baseline/`: early diagnosis, noisy or one-run evidence. One early backlog candidate lost 2 Event ACKs. A rate=10/s overload case did not reach offered throughput; its data must not be used as a successful capacity result.
- `scheduler-microbench.csv`: synthetic scheduler timings and the selected Event bytes before a late RPC. `frames_per_second` is CPU microbenchmark throughput, not network capacity.

Reproduce after building release binaries:

```sh
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bins
python3 tools/netbaiot-loadgen/run_business_rpc_v3_hol_formal.py --output /tmp/netbaiot-hol2-formal
python3 tools/netbaiot-loadgen/run_business_rpc_v3_hol_matrix.py --output /tmp/netbaiot-hol2-matrix --trace-gateway
cargo bench --locked -p netbaiot-v3-mux --bench send_ahead
```

The formal script's defaults are 60 s, 3 runs, 25 ms per user-space proxy read per direction, 32 KiB/s per direction, fresh identities and 1 Event/s. The proxy reads up to 16 KiB into user space **before** applying its delay/bandwidth limit. This is not a 50 ms network RTT model. These commands create new output directories; do not overwrite the saved evidence.
