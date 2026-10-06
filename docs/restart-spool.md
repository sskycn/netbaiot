# Runtime restart recovery

The recovery directory has two independent atomic responsibility domains during a
planned restart: the EventBus authoritative snapshot (`eventbus-recovery.spool`) and the MQTT broker
snapshot (`mqtt-runtime.state`). Normal traffic never writes either. The auth cache, gateway control
snapshots and business offline commands are never spooled.

The current EventBus writer emits NBSP v3:

```text
magic "NBSP" | version u32 | generation u64
repeat {
  record length u32 | JSON SpoolRecord | SHA-256 checksum
}
trailer "SEND" | record count u64 | protected byte count u64 | SHA-256
```

The final digest covers every byte from the magic through the trailer's byte count,
including version, generation, record lengths, payloads, per-record checksums and
order. The protected byte count is the header plus all record framing/content;
the fixed 52-byte trailer is included in segment/total capacity. The reader requires
this definitive trailer, exact count/length and digest, and rejects trailing bytes.
Only one bounded record is serialized at a time by the writer.

`SpoolRecord` contains the complete normalized event, stable `event_id`, pending
required sink IDs, routing revision, accepted time, and attempt metadata. All file
and record lengths are checked against configured record, segment, total-byte, and
record-count limits before allocation/decoding. Unknown version, partial tail,
checksum mismatch, random bytes, and oversized length fail loudly.

Commit writes a private temporary file (`0600` inside a `0700` directory on Unix),
syncs and closes the file, then atomically replaces `eventbus-recovery.spool` and
completes the platform commit steps described below. Only after those steps may
planned shutdown succeed. The generation increases on every
replacement. A crash after the new rename but before old cleanup therefore selects
only the new generation. Cleanup checks the generation before unlinking, so a stale
cleanup handle cannot delete a newer image at the same path. Abandoned `.tmp` files
are ignored. Version-1 append-only segments and version-2 snapshots remain readable for migration and are
coalesced by stable `event_id`; once an authoritative snapshot exists, it is authoritative
and legacy files are ignored until safe cleanup. Corrupt authoritative images never
fall back to older files. NBSP v1/v2 have only record checksums: a complete-record
boundary truncation (or header-only prefix) cannot be detected retrospectively.
Older releases cannot read v3. Drain and acknowledge v3 work with this version
before rollback; preserve the entire directory before an upgrade or rollback.

If business processed an event but its ACK was lost, the pending record is replayed
with the same `event_id`. This can duplicate processing and is why consumers must be
idempotent.

The MQTT snapshot uses compact NBMQ v6 typed records: a version/generation header
and checksum, bounded binary records with lengths and checksums, and a final
record-count, byte-count and whole-stream SHA-256 trailer. Sessions, subscriptions,
QoS state, retained data and pending Wills are encoded from one coherent view without
a whole-state clone. Readers accept v1–v5 for migration under version-specific bounds.
The independent `mqtt_recovery_max_bytes` ceiling covers configured broker state;
it does not inherit the EventBus record limit. Both formats retain private-directory,
file synchronization and platform replacement rules. See
[mqtt-session-recovery.md](mqtt-session-recovery.md) for compatibility and rollback.

The two files do not claim a cross-domain database transaction. Each responsibility
is complete and independently replay-safe before successful shutdown. Failure of
either commit keeps the process alive and unready with bounded retry; failed EventBus
attempts remove their private temporary file. SIGKILL, OS crash, or power loss may
discard recent in-memory changes and must not be described as crash durability.

## Legacy ConfigAck restart spool compatibility

NBSP container versions 1 and 2 remain readable for supported event records:
telemetry, device event, heartbeat, and command acknowledgement. These container
versions do not independently version the embedded `DeviceEvent` JSON schema.
The v3 writer changes the container integrity format only; supported-record JSON
and the independent MQTT recovery format remain unchanged.

A checksummed record whose exact `event.kind.kind` discriminator is `config_ack`
cannot be delivered by this release. Recovery returns `Error::IncompatibleSpool`
and logs:

```text
EventBus restart recovery failed; startup blocked error=restart spool contains legacy ConfigAck records created by an older NetbaIoT version; drain or complete the old spool with the previous release before upgrading; committed files are preserved
```

Startup fails before listeners bind or readiness becomes true. The new process
neither skips nor converts the event, deletes the file, nor overwrites the pending
responsibility. Unknown kinds, corrupt framing/checksums, malformed JSON, and
excessively nested diagnostics remain invalid input rather than being mislabeled
as ConfigAck. Inspection runs only after bounded record/checksum validation and
current deserialization failure, without materializing a full JSON tree.

If this error occurs, preserve the entire recovery directory and:

1. Run the previous release with the original configuration/recovery directory and
   compatible business consumers. Keep device traffic stopped externally so new
   legacy records cannot arrive while existing required deliveries recover.
2. Allow the required consumers to acknowledge replayed work. With the previous
   release's CLI and existing admin credentials, inspect `netbaiot server status`
   (using the deployment's `--endpoint` / `NETBAIOT_ENDPOINT` and `NETBAIOT_TOKEN`).
   Wait for `pending_required` to reach zero and committed EventBus `.spool` files
   to be removed by the gateway. Do not remove them yourself.
3. Request planned shutdown with `netbaiot server drain --yes`, or send SIGTERM.
   Verify successful exit and no remaining pending EventBus `.spool` records.
   A successful drain can spool undelivered work, so exit alone is insufficient.
4. Upgrade and restart with the same recovery directory, then restore device
   traffic after readiness succeeds. Preserve `mqtt-runtime.state` for its
   independent planned-restart responsibilities.

The new release refuses to silently discard old required work. If the old consumer
cannot acknowledge it, resolve that responsibility with the previous release before
upgrading. Do not delete committed records to bypass the check. See the
[configuration ownership migration](remove-device-config.md) and
[connection event compatibility review](connection-events-spool-upgrade-cleanup.md).

## Directory ownership and platform I/O

The gateway acquires `.netbaiot.lock` before reading either recovery domain. An OS
advisory exclusive lock is held by shared owners, including blocking I/O jobs, until
all owners exit. Another cooperating gateway fails with `Conflict` even on different
ports. The inode is never unlinked on release; process termination releases its lock.
Use a dedicated, trusted local filesystem directory (APFS/ext4/NTFS); network filesystems
and adversarial replacement of parent/directory components are outside this contract.
Library composition roots using raw broker/spool APIs must bind the same directory
owner; standalone decoder calls require no lock. File links, reparse points and special
files are rejected; Unix reads also use no-follow/nonblocking opens. Missing files
are accepted only in an accessible directory. I/O failures block startup, and actual
read lengths and directory enumeration are capped, including ignored temporary entries.
Before either writer creates a temporary file, it reserves room within the shared
`spool_max_records + 16` directory-entry budget. Failed unlink attempts therefore
cannot accumulate temporary files indefinitely through normal sequential shutdown
retries. Excess entries block further commits until the directory is repaired;
committed responsibility is preserved.

On Unix the commit sequence is private temporary creation, bounded streaming write,
`sync_all`, close, same-directory rename and directory `sync_all`. On Windows the
synced, closed file is replaced through `atomicwrites` 0.4.4 using
`MoveFileExW(REPLACE_EXISTING | WRITE_THROUGH)`. This removes the invalid read-only
directory flush. The wrapper propagates replacement failures, including sharing/ACL
errors. It does not add a separately proven NTFS directory flush or power-loss
transaction. ACK cleanup retains a synced empty NBSP v3 successor on Windows;
this is distinct from `commit([])`, which is a no-op and preserves old work. Unix
cleanup unlinks after checking identity/generation and validating all stale files.
Unknown/unreadable stale files block cleanup before authoritative deletion.

`fs2` 0.4.3 supplies safe Rust 1.88-compatible `flock`/Windows `LockFileEx` ownership;
its unsafe platform internals and atomicwrites' small Win32 wrapper were reviewed.
NetbaIoT adds no unsafe code. Native recovery/lifecycle CI is in
`.github/workflows/recovery-platform.yml`; configuration or compilation alone is
not native execution evidence. See the audit report for actual platform results.

Platform references: [Rust 1.88 Windows filesystem implementation](https://github.com/rust-lang/rust/blob/1.88.0/library/std/src/sys/fs/windows.rs),
[MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw),
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers),
[atomicwrites implementation](https://docs.rs/crate/atomicwrites/0.4.4/source/src/lib.rs),
[fs2 lock contract](https://docs.rs/fs2/0.4.3/fs2/trait.FileExt.html).
