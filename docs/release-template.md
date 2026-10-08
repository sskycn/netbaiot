# Release notes template

Copy this drafting aid to `docs/releases/vX.Y.Z.md` before tagging. The release
workflow requires that exact tag's nonempty notes file and uses it as the GitHub
Release body. A missing file or tag/workspace/lockfile mismatch fails preflight.

## Maintainer preflight

```bash
python3 scripts/release_preflight.py --tag vX.Y.Z
python3 -m unittest discover -s tests -p 'test_release_*.py' -v
```

CI and tagged releases share `release-verify.yml`: Rust 1.88.0/stable fmt, clippy,
workspace tests, cargo audit, MQTT protocol regressions, the full release gate,
MQTT 5 raw/Mosquitto tests, and Device Profile SDK interoperability. All must pass
before building. Publishing requires both Linux x86_64/ARM64 archives, native
quality/recovery and actual archive smoke on both architectures, plus exact-set
SHA256 verification. Optional macOS/Windows workflows cannot block publishing.
The target list is owned by `scripts/release_targets.json`; see [platform support](platform-support.md).

Before a formal tag, dispatch and confirm **MQTT Device Profile decoder fuzz
smoke** for the exact candidate commit. Review Actions results and do not tag a
candidate with a failing correctness gate. `scripts/release.sh` runs preflight
before creating/pushing a tag; running it is an explicit release action.

The **GitHub Release** workflow also accepts a manual `release-tag` input. Dispatch
it on the candidate ref with `vX.Y.Z` to run verify, both Linux builds, and native
archive smoke. The publish job is skipped on manual dispatch, so this rehearsal
creates neither a tag nor a GitHub Release. Only an actual tag push publishes.
macOS/Windows artifacts use the separate manual **Optional macOS and experimental
Windows builds** workflow. It reports failures normally and does not publish Releases.

To test packaging locally after building release server/CLI binaries:

```bash
python3 scripts/release_package.py package --tag vX.Y.Z \
  --target aarch64-apple-darwin --build-dir target/release --dist target/release-dist
python3 tests/release_archive_smoke.py target/release-dist/netbaiot-vX.Y.Z-aarch64-apple-darwin.tar.gz
```

Use your actual host target. The smoke needs Bash, Python 3.9+, and
`mosquitto_pub`, has bounded waits, and cleans up child processes. It exercises
the extracted demo, receipt, webhook event, and graceful shutdown. Do not claim
that an archive for another architecture was executed locally.

## What's new

-

## Reliability

-

## Protocol changes

-

## Compatibility

-

## Performance

- Include the measured revision and test setup. Do not describe a configured limit
  or a historical localhost result as production capacity.

## Bug fixes

-

## Known limitations

-

## Upgrade notes

-
