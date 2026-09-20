# Runtime restart recovery

The recovery directory has two independent atomic responsibility domains during a
planned restart: EventBus pending delivery segments (`*.spool`) and the MQTT broker
snapshot (`mqtt-runtime.state`). Normal traffic never writes either. Auth/config
caches and business offline commands are never spooled.

Each committed segment contains:

```text
magic "NBSP" | version u32
repeat {
  record length u32 | JSON SpoolRecord | SHA-256 checksum
}
```

`SpoolRecord` contains the complete normalized event, stable `event_id`, pending
required sink IDs, routing revision, accepted time, and attempt metadata. All file
and record lengths are checked against configured record, segment, total-byte, and
record-count limits before allocation/decoding. Unknown version, partial tail,
checksum mismatch, random bytes, and oversized length fail loudly.

Commit writes a private `0600` temporary file inside a `0700` directory, syncs the
file, atomically renames it to `.spool`, then syncs the directory. Only after those
steps may planned shutdown succeed. Startup discovers only committed `.spool`
files; abandoned `.tmp` files are not replayed. Acknowledged segment files are
removed followed by directory sync.

If business processed an event but its ACK was lost, the pending record is replayed
with the same `event_id`. This can duplicate processing and is why consumers must be
idempotent.

The MQTT snapshot is `NBMQ | version u32 | generation u64 | payload length u32 |
JSON snapshot | SHA-256`. One JSON image contains mutually consistent sessions,
subscriptions, offline QoS messages, inbound/outbound QoS state, packet allocator,
and retained state. It uses the same private-directory, temp-file, fsync, rename,
and directory-fsync rules. Unknown versions, mismatched generation, checksum/length
failure, or configured bound violations fail startup. See
[mqtt-session-recovery.md](mqtt-session-recovery.md).

The two files do not claim a cross-domain database transaction. Each responsibility
is complete and independently replay-safe before successful shutdown; failure of
either commit makes shutdown fail. SIGKILL, OS crash, or power loss may discard
recent in-memory changes and must not be described as crash durability.
