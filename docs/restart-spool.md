# Runtime restart recovery

The recovery directory has two independent atomic responsibility domains during a
planned restart: the EventBus authoritative snapshot (`eventbus-recovery.spool`) and the MQTT broker
snapshot (`mqtt-runtime.state`). Normal traffic never writes either. Auth/config
caches and business offline commands are never spooled.

The current EventBus snapshot contains:

```text
magic "NBSP" | version u32 | generation u64
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
file, atomically replaces `eventbus-recovery.spool`, then syncs the directory. Only
after those steps may planned shutdown succeed. The generation increases on every
replacement. A crash after the new rename but before old cleanup therefore selects
only the new generation. Cleanup checks the generation before unlinking, so a stale
cleanup handle cannot delete a newer image at the same path. Abandoned `.tmp` files
are ignored. Version-1 append-only segments remain readable for migration and are
coalesced by stable `event_id`; once a version-2 snapshot exists, it is authoritative
and legacy files are ignored until safe cleanup.

If business processed an event but its ACK was lost, the pending record is replayed
with the same `event_id`. This can duplicate processing and is why consumers must be
idempotent.

The MQTT snapshot is `NBMQ | version u32 | generation u64 | payload length u32 |
JSON snapshot | SHA-256`. One JSON image contains mutually consistent sessions,
subscriptions, offline QoS messages, inbound/outbound QoS state, packet allocator,
and retained state. It uses the same private-directory, temp-file, fsync, rename,
and directory-fsync rules. Its independent `mqtt_recovery_max_bytes` bound is large
enough for the configured global session plus retained-state ceilings; it does not
incorrectly inherit the 1 MiB EventBus record limit. Unknown versions, mismatched
generation, checksum/length failure, or configured bound violations fail startup. See
[mqtt-session-recovery.md](mqtt-session-recovery.md).

The two files do not claim a cross-domain database transaction. Each responsibility
is complete and independently replay-safe before successful shutdown. Failure of
either commit keeps the process alive and unready with bounded retry; failed EventBus
attempts remove their private temporary file. SIGKILL, OS crash, or power loss may
discard recent in-memory changes and must not be described as crash durability.
