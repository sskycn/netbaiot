# Second-round experiment

Baseline production source: `c359a868a38bfd83816e855795456bbdf097893a`. All four main workflows and native/DX Windows, Ubuntu and macOS jobs passed before changes. All six requested baseline checks passed.

Production binaries were frozen before adding measurement tools. Test-only overlays extend the existing release-mode `stats_alloc` probes; production state and limits are unchanged. Each case has three repetitions; setup, cleanup and sample storage are outside the timed allocation region. Allocation requests include reallocations. Instrumented allocator timings are subsystem comparisons, not production capacity.

Raw output and frozen executables stay in `target/second-round/`. This directory stores compact summaries, source/command provenance and the final report. Use `scripts/perf/second_round.py micro` with frozen release test executables and `network` with two frozen server binaries and one identical frozen load generator. Never build or test during a timed run.

Public ingress permits device-owned subscriptions and one live session per device. Network comparisons use legal self-uplink subscribers; cross-device high live fanout is measured through the broker subsystem without changing ingress authorization. The network fixture explicitly scales connection/subscription test ceilings for 256 devices. An initial fixture failed at the default 128 tenant-subscription ceiling; it is retained as setup failure evidence, not a server regression.
