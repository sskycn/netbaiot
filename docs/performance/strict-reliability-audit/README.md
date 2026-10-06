# AuthCache verifier hit measurement

This measures the existing hot path; no cache optimization is included. The test-only
allocator and lock clock do not enter production builds. The lock clock starts after
acquisition and records after mutex release. The host was also doing validation, so
these are operation measurements with instrumentation, not network capacity or a
calibrated throughput benchmark. Three runs per size, 1,000 hits and 100 locked prunes
per run, one current-thread runtime, 16-byte HMAC message, stable Rust 1.99 on macOS arm64.

| Entries | Hit median µs | Held mutex median µs | Locked prune median µs | Allocations/hit | Allocated bytes/hit | Expire-all µs |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1.648 | 1.521 | 0.206 | 2 | 260 | 1.834 |
| 64 | 6.863 | 6.751 | 5.485 | 2 | 7328 | 2.833 |
| 512 | 25.272 | 25.213 | 23.292 | 2 | 58400 | 7.459 |
| 4096 | 203.042 | 202.971 | 203.302 | 2 | 466976 | 62.250 |

Each live-entry prune creates one temporary HashSet allocation. At 4,096 entries,
a hit allocates 466,976 bytes in total, despite all entries remaining valid. Expiring
all entries performs no new allocation in this fixture. The provider call count
equals the initial population size and does not increase during measured hits.

This supports a follow-up bounded expiry/order maintenance design. It does not justify
mixing an expiry-index rewrite with responsibility and recovery fixes. Any follow-up
must preserve TTL, negative caching, single-flight, invalidation epochs, waiter bounds,
count/byte accounting and UDP ACK fencing; stale index nodes must remain bounded.

Run the command in [auth-cache.json](auth-cache.json). The benchmark is ignored in
normal workspace tests and was explicitly selected and executed here. The safe
development-only [stats_alloc API](https://docs.rs/stats_alloc/0.1.10/stats_alloc/) wraps
the System allocator; its atomic counter implementation was reviewed and Rust 1.88
compatibility is checked by the workspace gates.
