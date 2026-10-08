# Engineering optimization (0.2.3 baseline)

This staged change preserves EventAccepted, required delivery ownership, MQTT wire
behavior, NBSP v3 and NBMQ v6 recovery, and the database-free bounded runtime.

## Phase 1: portable, safe release archives

The POSIX manifest/link fix in `7df10ba` is retained. Native developer CI now builds
release binaries and executes `cargo xtask package --no-build --target <host>` on
Windows, Linux and macOS before a tag is created. Validation rejects backslashes
and noncanonical member names, so Windows extraction cannot reinterpret a member
as traversal and aliases cannot evade duplicate-member checks.

Tests cover nested/percent-encoded links, relative parents, POSIX and Windows
manifest paths, absolute members, traversal, symlinks and duplicate members.
PASS: 13 release-tool tests, `cargo xtask check` (format, Clippy, workspace tests,
evidence check), and native macOS `package --no-build`. Native Windows/Linux
full package jobs will be verified after pushing the staged changes.
