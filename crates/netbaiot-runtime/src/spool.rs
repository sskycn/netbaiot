use crate::{Error, Limits, Result, SpoolRecord, recovery_io};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"NBSP";
const LEGACY_VERSION: u32 = 1;
const GENERATION_VERSION: u32 = 2;
const VERSION: u32 = 3;
const TRAILER_MAGIC: &[u8; 4] = b"SEND";
const TRAILER_BYTES: usize = 4 + 8 + 8 + 32;
const SNAPSHOT_NAME: &str = "eventbus-recovery.spool";

#[derive(Clone, Debug)]
pub struct CommittedSpool {
    pub path: PathBuf,
    pub generation: u64,
}

#[derive(Debug)]
pub struct RecoveryBatch {
    pub records: Vec<SpoolRecord>,
    pub committed_files: Vec<CommittedSpool>,
    pub generation: u64,
}

#[derive(Clone)]
pub struct RestartSpool {
    directory: Arc<PathBuf>,
    limits: Arc<Limits>,
    owner: Option<Arc<recovery_io::RecoveryDirectory>>,
}

impl RestartSpool {
    pub fn new(directory: PathBuf, limits: Arc<Limits>) -> Self {
        Self {
            directory: Arc::new(directory),
            limits,
            owner: None,
        }
    }

    /// Composition roots must acquire one directory owner before any recovery I/O.
    pub fn with_owner(
        directory: PathBuf,
        limits: Arc<Limits>,
        owner: Arc<recovery_io::RecoveryDirectory>,
    ) -> Self {
        Self {
            directory: Arc::new(directory),
            limits,
            owner: Some(owner),
        }
    }

    pub async fn commit(&self, records: Vec<SpoolRecord>) -> Result<Option<PathBuf>> {
        if records.is_empty() {
            return Ok(None);
        }
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            commit_sync(&directory, &limits, &records)
        })
        .await
        .map_err(|_| Error::Internal)?
        .map(Some)
    }

    pub async fn recover(&self) -> Result<RecoveryBatch> {
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            recover_sync(&directory, &limits)
        })
        .await
        .map_err(|_| Error::Internal)?
    }

    pub async fn remove_committed(&self, files: Vec<CommittedSpool>) -> Result<()> {
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            for committed in files {
                let path = committed.path;
                if path.parent() != Some(directory.as_path())
                    || path.extension().and_then(|value| value.to_str()) != Some("spool")
                {
                    return Err(Error::Invalid);
                }
                if !recovery_io::directory_present(&directory)? {
                    return Err(Error::Storage);
                }
                let file = recovery_io::open_snapshot(&path)?.ok_or(Error::Storage)?;
                let bytes = recovery_io::read_bounded(
                    file,
                    limits.spool_segment_max_bytes.min(limits.spool_max_bytes),
                )?;
                decode_spool_records(&bytes, &limits)?;
                let generation = segment_generation(&bytes)?;
                // Never let cleanup for an older recovery batch delete a newer
                // atomically replaced snapshot at the same path.
                if generation == committed.generation {
                    if path.file_name().and_then(|value| value.to_str()) == Some(SNAPSHOT_NAME) {
                        // Remove ignored legacy generations first. If cleanup
                        // fails, the authoritative snapshot remains intact.
                        let stale =
                            recovery_io::spool_paths(&directory, directory_entry_limit(&limits)?)?;
                        // Validate the complete cleanup set before removing anything. Unknown
                        // or unreadable responsibility is never silently unlinked.
                        for candidate in &stale {
                            if candidate != &path {
                                let file =
                                    recovery_io::open_snapshot(candidate)?.ok_or(Error::Storage)?;
                                let bytes = recovery_io::read_bounded(
                                    file,
                                    limits.spool_segment_max_bytes.min(limits.spool_max_bytes),
                                )?;
                                decode_spool_records(&bytes, &limits)?;
                            }
                        }
                        for candidate in stale {
                            if candidate != path {
                                fs::remove_file(candidate).map_err(|_| Error::Storage)?;
                            }
                        }
                    }
                    #[cfg(not(windows))]
                    fs::remove_file(path).map_err(|_| Error::Storage)?;
                    #[cfg(windows)]
                    {
                        // A synced empty successor is the cleanup commit on Windows;
                        // it avoids pretending that unlink has a directory-fsync guarantee.
                        commit_sync(&directory, &limits, &[])?;
                        if path.file_name().and_then(|v| v.to_str()) != Some(SNAPSHOT_NAME) {
                            fs::remove_file(path).map_err(|_| Error::Storage)?;
                        }
                    }
                }
            }
            #[cfg(not(windows))]
            recovery_io::sync_directory(&directory)?;
            Ok(())
        })
        .await
        .map_err(|_| Error::Internal)?
    }

    pub fn directory(&self) -> &Path {
        self.directory.as_path()
    }
}

fn commit_sync(directory: &Path, limits: &Limits, records: &[SpoolRecord]) -> Result<PathBuf> {
    if records.len() > limits.spool_max_records {
        return Err(Error::Overloaded);
    }
    recovery_io::prepare_directory(directory)?;
    let existing = recover_sync(directory, limits)?;
    let generation = existing
        .generation
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    let id = Uuid::new_v4();
    let temporary = directory.join(format!(".{id}.tmp"));
    let committed = directory.join(SNAPSHOT_NAME);
    let result = (|| {
        let mut file = recovery_io::create_private(&temporary)?;
        file.write_all(MAGIC).map_err(|_| Error::Storage)?;
        file.write_all(&VERSION.to_be_bytes())
            .map_err(|_| Error::Storage)?;
        file.write_all(&generation.to_be_bytes())
            .map_err(|_| Error::Storage)?;
        let mut hash = Sha256::new();
        hash.update(MAGIC);
        hash.update(VERSION.to_be_bytes());
        hash.update(generation.to_be_bytes());
        let mut total = 16usize;
        if total + TRAILER_BYTES > limits.spool_segment_max_bytes
            || total + TRAILER_BYTES > limits.spool_max_bytes
        {
            return Err(Error::Overloaded);
        }
        for record in records {
            let payload = encode_record(record, limits.spool_record_max_bytes)?;
            let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
            total = total
                .checked_add(4 + payload.len() + 32)
                .ok_or(Error::Overloaded)?;
            if total.checked_add(TRAILER_BYTES).is_none_or(|size| {
                size > limits.spool_segment_max_bytes || size > limits.spool_max_bytes
            }) {
                return Err(Error::Overloaded);
            }
            let checksum = Sha256::digest(&payload);
            hash.update(length.to_be_bytes());
            hash.update(&payload);
            hash.update(checksum);
            file.write_all(&length.to_be_bytes())
                .and_then(|_| file.write_all(&payload))
                .and_then(|_| file.write_all(&checksum))
                .map_err(|_| Error::Storage)?;
        }
        let mut trailer = [0u8; TRAILER_BYTES];
        trailer[..4].copy_from_slice(TRAILER_MAGIC);
        trailer[4..12].copy_from_slice(
            &u64::try_from(records.len())
                .map_err(|_| Error::Overloaded)?
                .to_be_bytes(),
        );
        trailer[12..20].copy_from_slice(
            &u64::try_from(total)
                .map_err(|_| Error::Overloaded)?
                .to_be_bytes(),
        );
        hash.update(&trailer[..20]);
        trailer[20..].copy_from_slice(&hash.finalize());
        file.write_all(&trailer).map_err(|_| Error::Storage)?;
        file.sync_all().map_err(|_| Error::Storage)?;
        drop(file);
        recovery_io::replace_synced(&temporary, &committed)?;
        Ok(committed)
    })();
    if result.is_err() {
        // Shutdown may retry after an operator repairs the directory. Failed attempts must not
        // accumulate hidden temporary files and turn a bounded snapshot into unbounded disk use.
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn recover_sync(directory: &Path, limits: &Limits) -> Result<RecoveryBatch> {
    if !recovery_io::directory_present(directory)? {
        return Ok(RecoveryBatch {
            records: Vec::new(),
            committed_files: Vec::new(),
            generation: 0,
        });
    }
    let authoritative = directory.join(SNAPSHOT_NAME);
    if let Some(file) = recovery_io::open_snapshot(&authoritative)? {
        let bytes = recovery_io::read_bounded(
            file,
            limits.spool_segment_max_bytes.min(limits.spool_max_bytes),
        )?;
        let generation = segment_generation(&bytes)?;
        let mut records = Vec::new();
        decode_segment(&bytes, limits, &mut records)?;
        return Ok(RecoveryBatch {
            records,
            committed_files: vec![CommittedSpool {
                path: authoritative,
                generation,
            }],
            generation,
        });
    }
    let files = recovery_io::spool_paths(directory, directory_entry_limit(limits)?)?;
    let mut records = Vec::new();
    let mut committed_files = Vec::new();
    let mut generation = 0;
    let mut total = 0usize;
    for path in &files {
        let file = recovery_io::open_snapshot(path)?.ok_or(Error::Storage)?;
        let bytes = recovery_io::read_bounded(
            file,
            limits
                .spool_segment_max_bytes
                .min(limits.spool_max_bytes.saturating_sub(total)),
        )?;
        total = total.checked_add(bytes.len()).ok_or(Error::Overloaded)?;
        decode_segment(&bytes, limits, &mut records)?;
        let file_generation = segment_generation(&bytes)?;
        generation = generation.max(file_generation);
        committed_files.push(CommittedSpool {
            path: path.clone(),
            generation: file_generation,
        });
    }
    if records.len() > limits.spool_max_records {
        return Err(Error::Overloaded);
    }
    // Legacy append-only segments may contain the same event after a failed
    // repeated restart. Coalesce identical responsibility by stable EventId.
    let mut unique = std::collections::BTreeMap::<netbaiot_core::EventId, SpoolRecord>::new();
    for mut record in records {
        if let Some(existing) = unique.get_mut(&record.event.event_id) {
            if serde_json::to_vec(&existing.event).map_err(|_| Error::Invalid)?
                != serde_json::to_vec(&record.event).map_err(|_| Error::Invalid)?
            {
                return Err(Error::Conflict);
            }
            for sink in record.pending_sinks.drain(..) {
                if !existing.pending_sinks.contains(&sink) {
                    existing.pending_sinks.push(sink);
                }
            }
            for (sink, attempt) in record.attempts {
                existing
                    .attempts
                    .entry(sink)
                    .and_modify(|value| *value = (*value).max(attempt))
                    .or_insert(attempt);
            }
        } else {
            unique.insert(record.event.event_id, record);
        }
    }
    Ok(RecoveryBatch {
        records: unique.into_values().collect(),
        committed_files,
        generation,
    })
}

fn decode_segment(input: &[u8], limits: &Limits, output: &mut Vec<SpoolRecord>) -> Result<()> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != VERSION && version != GENERATION_VERSION && version != LEGACY_VERSION {
        return Err(Error::Invalid);
    }
    let (records_end, expected_count) = if version == VERSION {
        let end = input
            .len()
            .checked_sub(TRAILER_BYTES)
            .filter(|end| *end >= 16)
            .ok_or(Error::Invalid)?;
        let trailer = &input[end..];
        if &trailer[..4] != TRAILER_MAGIC
            || u64::from_be_bytes(trailer[12..20].try_into().map_err(|_| Error::Invalid)?)
                != u64::try_from(end).map_err(|_| Error::Invalid)?
            || Sha256::digest(&input[..end + 20]).as_slice() != &trailer[20..]
        {
            return Err(Error::Invalid);
        }
        let count = usize::try_from(u64::from_be_bytes(
            trailer[4..12].try_into().map_err(|_| Error::Invalid)?,
        ))
        .map_err(|_| Error::Overloaded)?;
        if count > limits.spool_max_records {
            return Err(Error::Overloaded);
        }
        (end, Some(count))
    } else {
        (input.len(), None)
    };
    let start_count = output.len();
    let mut at = if version >= GENERATION_VERSION {
        if input.len() < 16 {
            return Err(Error::Invalid);
        }
        16usize
    } else {
        8usize
    };
    while at < records_end {
        let length_end = at.checked_add(4).ok_or(Error::Invalid)?;
        let length_bytes = input.get(at..length_end).ok_or(Error::Invalid)?;
        let length = usize::try_from(u32::from_be_bytes(
            length_bytes.try_into().map_err(|_| Error::Invalid)?,
        ))
        .map_err(|_| Error::Invalid)?;
        if length == 0 || length > limits.spool_record_max_bytes {
            return Err(Error::Invalid);
        }
        let payload_end = length_end.checked_add(length).ok_or(Error::Invalid)?;
        let checksum_end = payload_end.checked_add(32).ok_or(Error::Invalid)?;
        if checksum_end > records_end {
            return Err(Error::Invalid);
        }
        let payload = input.get(length_end..payload_end).ok_or(Error::Invalid)?;
        let checksum = input.get(payload_end..checksum_end).ok_or(Error::Invalid)?;
        if Sha256::digest(payload).as_slice() != checksum {
            return Err(Error::Invalid);
        }
        if output.len() >= limits.spool_max_records {
            return Err(Error::Overloaded);
        }
        output.push(serde_json::from_slice(payload).map_err(|_| {
            if contains_legacy_config_ack(payload) {
                Error::IncompatibleSpool
            } else {
                Error::Invalid
            }
        })?);
        at = checksum_end;
    }
    if expected_count.is_some_and(|count| output.len() - start_count != count) {
        return Err(Error::Invalid);
    }
    Ok(())
}

/// Failure-path diagnostic only, after record length and checksum validation.
/// Project just the exact event discriminator; unknown fields are skipped without
/// building a Value tree or a legacy domain object. Cap nesting explicitly because
/// serde's ignored-field traversal need not enforce its normal recursion limit.
fn contains_legacy_config_ack(payload: &[u8]) -> bool {
    let mut depth = 0usize;
    let mut string = false;
    let mut escaped = false;
    for &byte in payload {
        if string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                string = false;
            }
        } else {
            match byte {
                b'"' => string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > 64 {
                        return false;
                    }
                }
                b'}' | b']' => {
                    let Some(next) = depth.checked_sub(1) else {
                        return false;
                    };
                    depth = next;
                }
                _ => {}
            }
        }
    }
    if depth != 0 || string {
        return false;
    }
    #[derive(serde::Deserialize)]
    struct RecordTag {
        event: EventTag,
    }
    #[derive(serde::Deserialize)]
    struct EventTag {
        kind: KindTag,
    }
    #[derive(serde::Deserialize)]
    struct KindTag {
        kind: RemovedKind,
    }
    #[derive(serde::Deserialize)]
    enum RemovedKind {
        #[serde(rename = "config_ack")]
        ConfigAck,
    }
    serde_json::from_slice::<RecordTag>(payload)
        .is_ok_and(|record| matches!(record.event.kind.kind, RemovedKind::ConfigAck))
}

fn segment_generation(input: &[u8]) -> Result<u64> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    match version {
        LEGACY_VERSION => Ok(0),
        GENERATION_VERSION | VERSION if input.len() >= 16 => Ok(u64::from_be_bytes(
            input[8..16].try_into().map_err(|_| Error::Invalid)?,
        )),
        _ => Err(Error::Invalid),
    }
}

/// Fuzzable bounded decoder for one committed segment image.
pub fn decode_spool_records(input: &[u8], limits: &Limits) -> Result<Vec<SpoolRecord>> {
    if input.len() > limits.spool_segment_max_bytes {
        return Err(Error::Overloaded);
    }
    let mut records = Vec::new();
    decode_segment(input, limits, &mut records)?;
    Ok(records)
}

fn directory_entry_limit(limits: &Limits) -> Result<usize> {
    limits
        .spool_max_records
        .checked_add(16)
        .ok_or(Error::Overloaded)
}

fn encode_record(record: &SpoolRecord, maximum: usize) -> Result<Vec<u8>> {
    struct Bounded {
        bytes: Vec<u8>,
        maximum: usize,
        overloaded: bool,
    }
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let Some(size) = self
                .bytes
                .len()
                .checked_add(bytes.len())
                .filter(|size| *size <= self.maximum)
            else {
                self.overloaded = true;
                return Err(std::io::ErrorKind::OutOfMemory.into());
            };
            if size > self.bytes.capacity() {
                let capacity = self
                    .bytes
                    .capacity()
                    .saturating_mul(2)
                    .max(size)
                    .min(self.maximum);
                self.bytes.reserve_exact(capacity - self.bytes.len());
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded {
        bytes: Vec::new(),
        maximum,
        overloaded: false,
    };
    serde_json::to_writer(&mut output, record).map_err(|_| {
        if output.overloaded {
            Error::Overloaded
        } else {
            Error::Invalid
        }
    })?;
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use netbaiot_core::*;
    use std::collections::BTreeMap;

    fn record() -> SpoolRecord {
        SpoolRecord {
            event: DeviceEvent {
                event_id: EventId::generate(),
                source_message_id: SourceMessageId::new("source").unwrap(),
                device: DeviceKey {
                    tenant_id: TenantId::new("t").unwrap(),
                    product_id: ProductId::new("p").unwrap(),
                    device_id: DeviceId::new("d").unwrap(),
                },
                received_at: 1,
                occurred_at: None,
                kind: DeviceEventKind::Heartbeat(Heartbeat { sequence: 1 }),
            },
            pending_sinks: vec![SinkId::new("required").unwrap()],
            routing_revision: 1,
            accepted_at: 1,
            attempts: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn whole_snapshot_integrity() {
        let directory = std::env::temp_dir().join(format!("netbaiot-integrity-{}", Uuid::new_v4()));
        let limits = Arc::new(Limits::default());
        let spool = RestartSpool::new(directory.clone(), limits.clone());
        let path = spool
            .commit(vec![record(), record(), record()])
            .await
            .unwrap()
            .unwrap();
        let image = fs::read(&path).unwrap();
        assert_eq!(spool.recover().await.unwrap().records.len(), 3);
        let mut ends = vec![16];
        for _ in 0..3 {
            let at = *ends.last().unwrap();
            let size = u32::from_be_bytes(image[at..at + 4].try_into().unwrap()) as usize;
            ends.push(at + 4 + size + 32);
        }
        let mut changed_header = image.clone();
        changed_header[15] ^= 1;
        let mut deleted = image.clone();
        deleted.drain(ends[1]..ends[2]);
        let mut duplicated = image.clone();
        duplicated.splice(ends[1]..ends[1], image[ends[0]..ends[1]].iter().copied());
        let mut reordered = image.clone();
        reordered.splice(
            ends[0]..ends[2],
            image[ends[1]..ends[2]]
                .iter()
                .chain(&image[ends[0]..ends[1]])
                .copied(),
        );
        let mut garbage = image.clone();
        garbage.push(42);
        let cases = [
            ("header only", image[..ends[0]].to_vec()),
            ("first record", image[..ends[1]].to_vec()),
            ("second record", image[..ends[2]].to_vec()),
            ("mid record", image[..ends[1] - 2].to_vec()),
            ("header mutation", changed_header),
            ("record deletion", deleted),
            ("record duplication", duplicated),
            ("record reordering", reordered),
            ("trailing garbage", garbage),
        ];
        let mut accepted = Vec::new();
        for (name, bytes) in cases {
            fs::write(&path, bytes).unwrap();
            let result = spool.recover().await;
            eprintln!(
                "{name}: {}",
                if result.is_ok() {
                    "ACCEPTED"
                } else {
                    "REJECTED"
                }
            );
            if result.is_ok() {
                accepted.push(name);
            }
        }
        fs::remove_dir_all(directory).unwrap();
        assert!(
            accepted.is_empty(),
            "incomplete or mutated snapshots accepted: {accepted:?}"
        );
    }

    #[tokio::test]
    async fn committed_record_round_trips_with_same_event_id() {
        let directory = std::env::temp_dir().join(format!("netbaiot-spool-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let original = record();
        spool.commit(vec![original.clone()]).await.unwrap();
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.records[0].event.event_id, original.event.event_id);
        spool
            .remove_committed(recovered.committed_files)
            .await
            .unwrap();
        let _ = fs::remove_dir(directory);
    }

    const LEGACY_ACK_V1: &[u8] =
        include_bytes!("../../../tests/fixtures/restart-spool/config-ack-v1.spool");
    const LEGACY_ACK_V2: &[u8] =
        include_bytes!("../../../tests/fixtures/restart-spool/config-ack-v2.spool");
    const SUPPORTED_V1: &[u8] =
        include_bytes!("../../../tests/fixtures/restart-spool/supported-v1.spool");
    const SUPPORTED_V2: &[u8] =
        include_bytes!("../../../tests/fixtures/restart-spool/supported-v2.spool");

    fn segment(payload: &[u8]) -> Vec<u8> {
        let mut bytes = b"NBSP\0\0\0\x01".to_vec();
        bytes.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes.extend_from_slice(&Sha256::digest(payload));
        bytes
    }

    #[tokio::test]
    async fn legacy_config_ack_blocks_recovery_and_replacement_without_mutation() {
        for (name, bytes) in [
            ("legacy.spool", LEGACY_ACK_V1),
            (SNAPSHOT_NAME, LEGACY_ACK_V2),
        ] {
            let directory =
                std::env::temp_dir().join(format!("netbaiot-spool-legacy-{}", Uuid::new_v4()));
            fs::create_dir_all(&directory).unwrap();
            let committed = directory.join(name);
            fs::write(&committed, bytes).unwrap();
            let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
            let error = spool.recover().await.unwrap_err();
            assert!(matches!(error, Error::IncompatibleSpool));
            assert_eq!(
                error.to_string(),
                "restart spool contains legacy ConfigAck records created by an older NetbaIoT version; drain or complete the old spool with the previous release before upgrading; committed files are preserved"
            );
            // Neither startup nor a subsequent commit may overwrite old responsibility.
            assert!(matches!(
                spool.commit(vec![record()]).await,
                Err(Error::IncompatibleSpool)
            ));
            assert_eq!(fs::read(&committed).unwrap(), bytes);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn historical_supported_records_recover_with_identity_and_attempts() {
        for (name, bytes, generation) in [
            ("legacy.spool", SUPPORTED_V1, 0),
            (SNAPSHOT_NAME, SUPPORTED_V2, 7),
        ] {
            let directory =
                std::env::temp_dir().join(format!("netbaiot-spool-supported-{}", Uuid::new_v4()));
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join(name), bytes).unwrap();
            let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
            let recovered = spool.recover().await.unwrap();
            assert_eq!(recovered.generation, generation);
            assert_eq!(recovered.records.len(), 3);
            for (index, record) in recovered.records.iter().enumerate() {
                assert_eq!(record.event.event_id.0.as_u128(), index as u128 + 2);
                assert_eq!(record.routing_revision, 1);
                assert_eq!(record.attempts[&record.pending_sinks[0]], 2);
            }
            assert!(matches!(
                recovered.records[0].event.kind,
                DeviceEventKind::Heartbeat(_)
            ));
            assert!(matches!(
                recovered.records[1].event.kind,
                DeviceEventKind::Telemetry(_)
            ));
            assert!(matches!(
                recovered.records[2].event.kind,
                DeviceEventKind::CommandAck(_)
            ));
            assert_eq!(fs::read(directory.join(name)).unwrap(), bytes);
            spool
                .remove_committed(recovered.committed_files)
                .await
                .unwrap();
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn legacy_named_v2_preserves_generation_for_cleanup_and_migration() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-v2-generation-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("legacy-v2.spool"), SUPPORTED_V2).unwrap();
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.generation, 7);
        assert_eq!(recovered.committed_files[0].generation, 7);
        spool.commit(recovered.records).await.unwrap();
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.generation, 8);
        spool
            .remove_committed(recovered.committed_files)
            .await
            .unwrap();
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_diagnostic_requires_exact_path_valid_json_and_intact_framing() {
        let limits = Limits::default();
        let length = u32::from_be_bytes(LEGACY_ACK_V1[8..12].try_into().unwrap()) as usize;
        let payload = &LEGACY_ACK_V1[12..12 + length];
        assert!(contains_legacy_config_ack(payload));
        for kind in ["future_event", "connected", "disconnected"] {
            let changed = String::from_utf8(payload.to_vec())
                .unwrap()
                .replace("config_ack", kind);
            assert!(matches!(
                decode_spool_records(&segment(changed.as_bytes()), &limits),
                Err(Error::Invalid)
            ));
        }
        for payload in [
            br#"{"event":{"kind":{"kind":"future","data":{"kind":"config_ack"}}}}"#.as_slice(),
            br#"{"kind":"config_ack"}"#,
            br#"{"event":{"kind":{"kind":"config_ack","kind":"future"}}}"#,
            br#"{"event":{"kind":{"kind":"config_ack"}}} trailing"#,
            br#"{"event":{"kind":{"kind":"config_ack"}},"extra":[}"#,
        ] {
            assert!(matches!(
                decode_spool_records(&segment(payload), &limits),
                Err(Error::Invalid)
            ));
        }
        let deep = format!(
            r#"{{"event":{{"kind":{{"kind":"config_ack"}}}},"extra":{}0{}}}"#,
            "[".repeat(128),
            "]".repeat(128)
        );
        assert!(!contains_legacy_config_ack(deep.as_bytes()));
        let mut corrupt = LEGACY_ACK_V1.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        let mut unknown_version = LEGACY_ACK_V1.to_vec();
        unknown_version[7] = 99;
        let mut bad_length = LEGACY_ACK_V1.to_vec();
        bad_length[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        for bytes in [
            corrupt,
            unknown_version,
            bad_length,
            LEGACY_ACK_V1[..LEGACY_ACK_V1.len() - 1].to_vec(),
        ] {
            assert!(matches!(
                decode_spool_records(&bytes, &limits),
                Err(Error::Invalid)
            ));
        }
        let limits = Limits {
            spool_record_max_bytes: length - 1,
            ..limits
        };
        assert!(matches!(
            decode_spool_records(LEGACY_ACK_V1, &limits),
            Err(Error::Invalid)
        ));
    }

    #[tokio::test]
    async fn empty_or_absent_spool_recovers_no_work() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-spool-empty-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::create_dir_all(&directory).unwrap();
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::write(directory.join("empty.spool"), b"NBSP\0\0\0\x01").unwrap();
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_authoritative_symlink_is_not_first_start() {
        let directory = std::env::temp_dir().join(format!("netbaiot-symlink-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        std::os::unix::fs::symlink(directory.join("missing"), directory.join(SNAPSHOT_NAME))
            .unwrap();
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let result = spool.recover().await;
        fs::remove_dir_all(directory).unwrap();
        assert!(matches!(result, Err(Error::Storage)));
    }

    #[tokio::test]
    async fn authority_and_cleanup_fail_closed_and_retry_preserves_latest() {
        let directory = std::env::temp_dir().join(format!("netbaiot-storage-{}", Uuid::new_v4()));
        let limits = Arc::new(Limits::default());
        let spool = RestartSpool::new(directory.clone(), limits.clone());
        let original = record();
        let path = spool.commit(vec![original.clone()]).await.unwrap().unwrap();
        let image = fs::read(&path).unwrap();
        let recovery = spool.recover().await.unwrap();
        assert!(spool.commit(vec![]).await.unwrap().is_none());
        assert_eq!(
            fs::read(&path).unwrap(),
            image,
            "empty commit must preserve old responsibility"
        );
        // An unknown legacy entry blocks cleanup before the authority is removed.
        let stale = directory.join("unknown.spool");
        fs::write(&stale, b"unknown responsibility").unwrap();
        assert!(
            spool
                .remove_committed(recovery.committed_files.clone())
                .await
                .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), image);
        fs::remove_file(&stale).unwrap();
        // A corrupt authority cannot fall back to a good legacy file or be overwritten.
        fs::write(&stale, SUPPORTED_V1).unwrap();
        fs::write(&path, b"broken").unwrap();
        assert!(spool.recover().await.is_err());
        assert!(spool.commit(vec![record()]).await.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"broken");
        fs::write(&path, &image).unwrap();
        spool.commit(vec![original.clone()]).await.unwrap();
        assert_eq!(spool.recover().await.unwrap().generation, 2);
        let small = RestartSpool::new(
            directory.clone(),
            Arc::new(Limits {
                spool_segment_max_bytes: image.len() - 1,
                ..(*limits).clone()
            }),
        );
        assert!(matches!(small.recover().await, Err(Error::Overloaded)));
        assert!(matches!(
            small.commit(vec![record()]).await,
            Err(Error::Overloaded)
        ));
        assert_eq!(
            spool.recover().await.unwrap().records[0].event.event_id,
            original.event.event_id
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unreadable_file_parent_and_special_paths_are_storage_errors() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        assert_ne!(
            effective_uid_from_owned_file(),
            0,
            "run permissions test as an unprivileged user"
        );
        let directory =
            std::env::temp_dir().join(format!("netbaiot-permissions-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let path = spool.commit(vec![record()]).await.unwrap().unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o0)).unwrap();
        let unreadable = spool.recover().await;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o0)).unwrap();
        let parent = spool.recover().await;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(unreadable, Err(Error::Storage)));
        assert!(matches!(parent, Err(Error::Storage)));
        let alias = directory.with_extension("alias");
        symlink(directory.join("missing"), &alias).unwrap();
        assert!(matches!(
            RestartSpool::new(alias.clone(), Arc::new(Limits::default()))
                .recover()
                .await,
            Err(Error::Storage)
        ));
        fs::remove_file(alias).unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    fn effective_uid_from_owned_file() -> u32 {
        // The test does not call libc: ownership of the newly created file is the
        // effective uid. This also detects privileged CI without weakening assertions.
        use std::os::unix::fs::MetadataExt;
        let path = std::env::temp_dir().join(format!("netbaiot-uid-{}", Uuid::new_v4()));
        fs::write(&path, b"").unwrap();
        let uid = fs::metadata(&path).unwrap().uid();
        fs::remove_file(path).unwrap();
        uid
    }

    #[tokio::test]
    async fn repeated_failed_restarts_replace_one_generation_without_duplicates() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-spool-generations-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let original = record();
        for generation in 1..=3 {
            spool.commit(vec![original.clone()]).await.unwrap();
            let recovered = spool.recover().await.unwrap();
            assert_eq!(recovered.generation, generation);
            assert_eq!(recovered.records.len(), 1);
            assert_eq!(recovered.records[0].event.event_id, original.event.event_id);
        }
        let committed = fs::read_dir(&directory)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension().and_then(|value| value.to_str()) == Some("spool")
            })
            .count();
        assert_eq!(committed, 1);
        let recovered = spool.recover().await.unwrap();
        spool
            .remove_committed(recovered.committed_files)
            .await
            .unwrap();
        let _ = fs::remove_dir(directory);
    }

    #[tokio::test]
    async fn stale_cleanup_handle_cannot_delete_newer_committed_generation() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-spool-cleanup-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let original = record();
        spool.commit(vec![original.clone()]).await.unwrap();
        let stale = spool.recover().await.unwrap();
        spool.commit(vec![original.clone()]).await.unwrap();
        spool.remove_committed(stale.committed_files).await.unwrap();
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.generation, 2);
        assert_eq!(recovered.records.len(), 1);
        spool
            .remove_committed(recovered.committed_files)
            .await
            .unwrap();
        let _ = fs::remove_dir(directory);
    }

    #[tokio::test]
    async fn capacity_and_unusable_directory_fail_without_a_commit() {
        let root = std::env::temp_dir().join(format!("netbaiot-spool-failure-{}", Uuid::new_v4()));
        let limits = Arc::new(Limits {
            spool_max_records: 1,
            ..Limits::default()
        });
        let spool = RestartSpool::new(root.join("spool"), limits);
        assert!(matches!(
            spool.commit(vec![record(), record()]).await,
            Err(Error::Overloaded)
        ));
        fs::create_dir_all(&root).unwrap();
        let file_path = root.join("not-a-directory");
        fs::write(&file_path, b"x").unwrap();
        let unusable = RestartSpool::new(file_path, Arc::new(Limits::default()));
        assert!(matches!(
            unusable.commit(vec![record()]).await,
            Err(Error::Storage)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_truncated_oversized_and_unknown_versions_fail() {
        let limits = Limits::default();
        for bytes in [
            b"".as_slice(),
            b"NBSP\0\0\0\x02",
            b"NBSP\0\0\0\x01\0\0\0\x10",
        ] {
            assert!(decode_segment(bytes, &limits, &mut Vec::new()).is_err());
        }
        let mut oversized = b"NBSP\0\0\0\x01".to_vec();
        oversized.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_segment(&oversized, &limits, &mut Vec::new()).is_err());
    }

    #[test]
    fn serialization_has_a_hard_record_limit_before_allocation_growth() {
        let record = record();
        let bytes = serde_json::to_vec(&record).unwrap();
        assert_eq!(encode_record(&record, bytes.len()).unwrap(), bytes);
        assert!(matches!(
            encode_record(&record, bytes.len() - 1),
            Err(Error::Overloaded)
        ));
        assert!(matches!(encode_record(&record, 0), Err(Error::Overloaded)));
    }
}
