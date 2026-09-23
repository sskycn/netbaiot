# Runtime restart recovery

The recovery directory has two independent atomic responsibility domains during a
planned restart: the EventBus authoritative snapshot (`eventbus-recovery.spool`) and the MQTT broker
snapshot (`mqtt-runtime.state`). Normal traffic never writes either. The auth cache, gateway control
snapshots and business offline commands are never spooled.

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

## Legacy ConfigAck restart spool compatibility

NBSP container versions 1 and 2 remain readable for supported event records:
telemetry, device event, heartbeat, and command acknowledgement. These container
versions do not independently version the embedded `DeviceEvent` JSON schema.
This cleanup leaves framing, checksum, generation, and supported-record encoding
unchanged; it does not modify the independent MQTT recovery format.

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
