# Mixed ingress audit evidence

The human report is [mixed-ingress-capacity-audit.md](../../mixed-ingress-capacity-audit.md).
The generated index is [mixed-ingress-capacity-results.json](../../mixed-ingress-capacity-results.json).
`plan.json` identifies the formal matrix; other root JSON files retain calibration,
initial starvation reproductions, and isolated experiments. Do not pool different
scenario names or binary hashes into a single capacity estimate.

Each run preserves the exact server configuration, workload, source revision,
binary SHA256 hashes, UTC epoch timestamp, duration, client receipt histograms and
error counters, one-second management/process samples, cooldown ownership, kernel
counter snapshots, and owned child exit status. Public test credentials are fixtures.
`baseline-environment.json` describes the host and toolchain. Its checkout revision
is the audit-tool revision, while each run's `production_revision` and
`server_sha256` identify the measured server.

`accepted` means the driver observed a valid acceptance receipt. `attempted` counts
transmission attempts, including failed writes. `success_pct` is
`accepted / attempted`; `offered_coverage_pct` is `accepted / (rate * duration)`.
Connection success percentages use all owned attempts, including warmup, to keep
attempt and authentication completion cohorts consistent across the start boundary.
The legacy summary field `sent_per_second` uses attempts. Scheduling misses and
full client windows must be assessed separately from server rejection. Latency
histograms contain successful receipts only. Error counters overlap; a disconnect
and its cause must not be added. `unconfirmed` is diagnostic and does not count
every pending receipt abandoned by a failed write.

Server counters span the first and last successful sample, not the exact client
measurement boundary. Missing management samples under shared connection pressure
are not zero queue depth. CPU is measured in percent of one core; host CPU snapshots
are separate. RSS is resident memory, not the configured logical reservation.
One-second maxima are sampled peaks and can miss shorter excursions. Kernel counters are host-wide. macOS TCP counters returned all zeros in this run;
this does not prove that listen-queue overflow was absent.

`harness_version: 2` closes the management HTTP client after every request/failure.
Six earlier default-IP observer failures are retained under `diagnostics/` and
excluded from the formal matrix. Older calibration and pending-cap comparisons
also retain any `CannotSendRequest` observer state errors; their independent client
and process measurements remain inspectable. Use the corrected formal runs for
management recovery and cleanup conclusions.

Regenerate summaries after the campaign has finished:

```sh
python3 scripts/perf/mixed_ingress_report.py --output docs/mixed-ingress-capacity-results.json
python3 scripts/perf/mixed_ingress_tables.py docs/mixed-ingress-capacity-results.json /tmp/mixed-ingress-tables.md
python3 scripts/perf/mixed_ingress_verify.py --directory docs/performance/mixed-ingress --plan docs/performance/mixed-ingress/plan.json --output docs/performance/mixed-ingress/matrix-verification.json
```

Run these commands from the repository root. `matrix-verification.json` records
missing cases and invariant failures explicitly; a partial matrix is not a pass.

To reproduce the formal comparison, build `netbaiot-server` with Rust 1.88.0
`--release --locked` in separate checkouts of the two production revisions named
in the report. Build `netbaiot-loadgen` at audit-tool revision
`adf3383` for the original formal-driver behavior. Copy the three executables to
`target/mixed-audit/baseline-server`, `final-server` and `final-loadgen` in this
checkout, then run:

```sh
python3 scripts/perf/mixed_ingress_audit.py --candidate target/mixed-audit/final-server --loadgen target/mixed-audit/final-loadgen --plan docs/performance/mixed-ingress/plan.json --output /tmp/netbaiot-mixed-evidence
```

Use a fresh checkout/output directory for a new campaign; the harness deliberately
refuses to overwrite an existing run directory. `--resume` accepts only exact
matching, successful results. Do not run a compiler, tests or another load generator
concurrently. Server and client executable hashes, not the working checkout's HEAD,
identify the measured implementation. Dynamic ports and public fixture credentials
are generated per run. The final harness adds optional bounded settling time and
generator worker count for the separately labeled diagnostic plan; default formal
settings remain unchanged.
