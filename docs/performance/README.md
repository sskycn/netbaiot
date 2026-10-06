# Performance evidence policy

This directory keeps the evidence needed to understand and reproduce performance
decisions: methods, source and commands, environment metadata, compact summaries,
provenance manifests, and small regression fixtures. Raw per-request samples,
profiling output, generated traces, and full run logs do not belong in Git.

Write new raw output under `target/performance/` or `target/evidence/`. These paths
are ignored by Git. CI uploads available output as GitHub Actions Artifacts with a
14-day retention period; normal CI retention is intentionally temporary. Artifacts
may expire, so do not describe an artifact as permanent evidence. For a formal,
long-term release record, a maintainer should attach the reviewed summary and any
required raw evidence to the corresponding GitHub Release Asset. This policy does
not publish or create releases automatically.

Keep an experiment's `README.md`, run plan, environment, aggregate `summary.json`,
and checksum manifest in Git. Keep ordinary summary JSON at or below 256 KiB,
curated `*.excerpt.log` at or below 64 KiB. A larger historical aggregate JSON must
have an exact path, maximum size, and reason in `evidence-allowlist.json`; raw sample
arrays remain prohibited. The archive manifest and all other files stay below the
1 MiB per-file limit. The repository gate permits at most 20 MiB total under this
directory.

Run `cargo xtask check evidence` before committing. The standard
`cargo xtask check` and release Rust gate run the same check. It rejects oversized
files, raw file patterns, full logs, large JSON sample arrays, and oversized text
embedded in JSON. Error messages show the path, observed size, limit, and the expected
artifact location.

`archive-manifest.json` records the original path, exact size, SHA-256, category,
source commit, and availability for files removed from the tracked tree; embedded
raw JSON fields use a JSON pointer. No Actions run or artifact identifier is recorded
unless it is known. Historical cleanup rewrites Git refs; the local
`local-performance-archive/` copy and bundle are deliberately ignored by Git and
preserve the pre-cleanup files on this computer.
