# Maintenance commands

The Cargo alias is defined in .cargo/config.toml. Xtask is a small clap/serde_json
orchestrator with no server/runtime dependency; it invokes the existing tools with
structured process arguments, prints each command and propagates failures.

```bash
cargo xtask check
cargo xtask check --audit
cargo xtask check mqtt
cargo xtask check release
cargo xtask schema
cargo xtask schema --check
cargo xtask config-reference
cargo xtask config-reference --check
cargo xtask package
cargo xtask package --target x86_64-unknown-linux-gnu
```

| Task | Underlying work |
| --- | --- |
| check | fmt --check, locked workspace all-targets/all-features clippy -D warnings, locked all-features tests |
| check mqtt | server build, protocol regressions, Python conformance units, 3.1.1 release gate, MQTT5 raw/Mosquitto, SDK build/interop/measurement |
| check release | MSRV from Cargo + stable check, cargo audit, MQTT suite, schema/reference drift, release preflight/tooling tests, host package/archive smoke |
| schema/reference | Feature-gated Rust Config generator, deterministic committed JSON/field tables; --check normalizes CRLF/LF only and fails on content drift |
| package | Locked release build for host or specified target, existing archive layout validator/packager; no tag or publishing |

Required missing Mosquitto/Python/OpenSSL/cargo-audit tools report BLOCKED and exit
nonzero; interop is not silently skipped. Install them as documented in CONTRIBUTING.
Windows supports check/schema/init/doctor. The preserved Bash archive smoke requires
Linux/macOS and reports BLOCKED on Windows; native DX tests cover the Windows binary.
Cross-target builds require the corresponding installed toolchain/linker, just as
before; xtask does not install them.

CI prepares tools, then uses `check release --part rust|audit|mqtt|preflight|archive`
to preserve independent jobs while calling the same functions as local release
verification. `--archive PATH` makes the archive part inspect the actual downloaded
release artifact. `package --no-build --target TARGET` packages a previously built
Cargo/cross target without rebuilding it. RELEASE_TAG, when supplied, must match
the workspace version. Verify/build/checksums/smoke/publish topology and publication
permissions remain in GitHub Actions; xtask never calls GitHub or publishes.

Bottom-level tools remain available: cargo fmt/clippy/test/audit,
scripts/release_preflight.py, scripts/release_package.py,
tests/mqtt_protocol_regressions.py, tests/mqtt_conformance/run.py,
v5_smoke.py, v5_mosquitto.py and run_device_profile_mosquitto.py.
