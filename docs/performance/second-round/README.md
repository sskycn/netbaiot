# Second-round experiment

Baseline production source: `c359a868a38bfd83816e855795456bbdf097893a`. All four main workflows and native/DX Windows, Ubuntu and macOS jobs passed before changes. All six requested baseline checks passed.

Production binaries were frozen before adding measurement tools. Test-only overlays extend the existing release-mode `stats_alloc` probes; production state and limits are unchanged. Each case has three repetitions; setup, cleanup and sample storage are outside the timed allocation region. Allocation requests include reallocations. Instrumented allocator timings are subsystem comparisons, not production capacity.

Raw output and frozen executables stay in `target/second-round/`. This directory stores compact summaries, source/command provenance and the final report. Use `scripts/perf/second_round.py micro` with frozen release test executables and `network` with two frozen server binaries and one identical frozen load generator. Never build or test during a timed run.

Public ingress permits device-owned subscriptions and one live session per device. Network comparisons use legal self-uplink subscribers; cross-device high live fanout is measured through the broker subsystem without changing ingress authorization. The network fixture explicitly scales connection/subscription test ceilings for 256 devices. An initial fixture failed at the default 128 tenant-subscription ceiling; it is retained as setup failure evidence, not a server regression.


For default production behavior pass `--no-lock-metrics` to `second_round.py network`; default instrumentation is retained only for earlier campaign reproduction. Opt-in histograms use fixed operation names. All-site J broker totals cannot be compared to historical route-only totals; percentile values are histogram bucket upper bounds.

Additional reproducible commands (run from repository root, strictly serial):

```sh
python3 scripts/perf/second_round_offline.py --before BEFORE_SERVER --after AFTER_SERVER --output target/replay-abba
python3 scripts/perf/connection_memory.py --transport mqtt --connections 128 --hold-seconds 6 --server FROZEN_SERVER --loadgen FROZEN_GENERATOR
python3 scripts/perf/second_round_profile.py build --output target/profile-new
python3 scripts/perf/second_round_profile.py startup --output target/profile-new
python3 scripts/perf/second_round_profile.py rpc --output target/profile-new
```

The profile tool needs the campaign's frozen `target/second-round/baseline/bin/business_rpc`; supply the same load generator build for both sides. Its custom Cargo profile is passed only via CLI configuration. Existing cold target directories are refused. RPC uses an explicit test rate-ceiling overlay, 500 events/s, bounded queues and four bounded auth workers. All manifests identify tested source/binary hashes; raw pressure/setup failures remain available under `target/second-round/` and are excluded from successful workload comparisons.
