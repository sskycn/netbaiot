# Business RPC V3 HOL2 evidence summary

See [Chinese report](../../business-rpc-v3-hol2.zh-CN.md). The compact `summary.json`
files and each `*-experiment.json` remain; the experiment files store config, OS,
architecture and build SHA. Per-run loadgen JSON, gateway/proxy logs, metrics, and
frame-capture JSONL were removed from Git. Their original paths, sizes, and hashes
are indexed in the [archive manifest](../archive-manifest.json), and the local
pre-cleanup copy retains the bytes. No hosted artifact is known. Debug tracing
perturbs LAN latency.

- `formal-60s/summary.json`: 3 fresh runs each of V2 multiplexed, V2 dual, V3 baseline, V3 8/128 KiB and V3 16/128 KiB. Aggregate results remain; the individual raw records are indexed by the archive manifest.
- `repeated-backlog-15s/summary.json`: 3 fresh runs each of V3 baseline and 16/128 KiB at 256 KiB/s, 2 Event/s, 16 Auth concurrency.
- `repeated-lan-15s-no-trace/summary.json`: 3 fresh loopback runs each, without per-frame debug logging.
- `matrix-32k-v2/`, `matrix-32k-v3/`, `matrix-256k/`: retained experiment metadata and aggregate discovery summaries. `matrix-32k/` included initial failed attempts with a gateway frame limit mismatch. The 256 KiB send-ahead case needs a 256 KiB connection limit.
- `socket-baseline/`, `socket-baseline-small/`, `socket-unique-default/`, `socket-unique-small/`: default versus 4096 B SO_SNDBUF, without and with 16/128 KiB send-ahead.
- `socket-option-probe.json`: standalone connected socket probe for SO_SNDBUF and macOS TCP_NOTSENT_LOWAT. NOTSENT was not integrated into V3.
- `backlog2-*`, `unique-*`, `lan-*`, `discovery-*`, `medium-baseline/`: retained experiment metadata and aggregate results for early diagnosis, noisy or one-run evidence. One early backlog candidate lost 2 Event ACKs. A rate=10/s overload case did not reach offered throughput; its data must not be used as a successful capacity result.
- Scheduler microbenchmark conclusions remain in the report. The raw timing CSV is indexed in the archive manifest. `frames_per_second` is CPU microbenchmark throughput, not network capacity.

Reproduce after building release binaries:

```sh
cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bins
python3 tools/netbaiot-loadgen/run_business_rpc_v3_hol_formal.py --output target/performance/business-rpc-v3-hol2/formal
python3 tools/netbaiot-loadgen/run_business_rpc_v3_hol_matrix.py --output target/performance/business-rpc-v3-hol2/matrix --trace-gateway
cargo bench --locked -p netbaiot-v3-mux --bench send_ahead
```

The formal script's defaults are 60 s, 3 runs, 25 ms per user-space proxy read per direction, 32 KiB/s per direction, fresh identities and 1 Event/s. The proxy reads up to 16 KiB into user space **before** applying its delay/bandwidth limit. This is not a 50 ms network RTT model. These commands create new output directories; do not overwrite the saved evidence.
