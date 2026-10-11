use crate::{Error, Limits, Result, SpoolRecord, event::SpoolSnapshot, recovery_io};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"NBSP";
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

    pub(crate) async fn commit_snapshot(
        &self,
        records: Vec<SpoolSnapshot>,
    ) -> Result<Option<PathBuf>> {
        if records.is_empty() {
            return Ok(None);
        }
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        let owner = self.owner.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            commit_serializable_sync(&directory, &limits, &records)
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
                let (generation, _) = read_segment_stream(file, &limits, false)?;
                // Never let cleanup for an older recovery batch delete a newer
                // atomically replaced snapshot at the same path.
                if generation == committed.generation {
                    if path.file_name().and_then(|value| value.to_str()) == Some(SNAPSHOT_NAME) {
                        // Remove superseded current snapshots first. If cleanup
                        // fails, the authoritative snapshot remains intact.
                        let stale =
                            recovery_io::spool_paths(&directory, directory_entry_limit(&limits)?)?;
                        // Validate the complete cleanup set before removing anything. Unknown
                        // or unreadable responsibility is never silently unlinked.
                        for candidate in &stale {
                            if candidate != &path {
                                let file =
                                    recovery_io::open_snapshot(candidate)?.ok_or(Error::Storage)?;
                                read_segment_stream(file, &limits, false)?;
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
    commit_serializable_sync(directory, limits, records)
}

fn commit_serializable_sync<T: Serialize>(
    directory: &Path,
    limits: &Limits,
    records: &[T],
) -> Result<PathBuf> {
    if records.len() > limits.spool_max_records {
        return Err(Error::Overloaded);
    }
    recovery_io::prepare_directory(directory)?;
    recovery_io::ensure_temporary_capacity(directory, limits.spool_max_records)?;
    let previous_generation = match recovery_io::open_snapshot(&directory.join(SNAPSHOT_NAME))? {
        Some(file) => read_segment_stream(file, limits, false)?.0,
        None => recover_sync(directory, limits)?.generation,
    };
    let generation = previous_generation
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
        let (generation, records) = read_segment_stream(file, limits, true)?;
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
    // Only the named, atomically committed current snapshot is authoritative.
    // A leftover file must fail closed; it may still own accepted work.
    if let Some(path) = files.first() {
        let file = recovery_io::open_snapshot(path)?.ok_or(Error::Storage)?;
        read_segment_stream(file, limits, false)?;
        return Err(Error::Invalid);
    }
    Ok(RecoveryBatch {
        records: Vec::new(),
        committed_files: Vec::new(),
        generation: 0,
    })
}

fn read_exact_segment(reader: &mut impl Read, bytes: &mut [u8]) -> Result<()> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Invalid
        } else {
            Error::Storage
        }
    })
}

/// Validate the complete NBSP v3 image while retaining at most one wire record.
/// Commit validation discards decoded records; startup recovery returns collected
/// records only after the whole-stream digest and every record have passed validation.
fn read_segment_stream(
    file: fs::File,
    limits: &Limits,
    collect: bool,
) -> Result<(u64, Vec<SpoolRecord>)> {
    let length = usize::try_from(file.metadata().map_err(|_| Error::Storage)?.len())
        .map_err(|_| Error::Overloaded)?;
    if length > limits.spool_segment_max_bytes.min(limits.spool_max_bytes) {
        return Err(Error::Overloaded);
    }
    if length < 8 {
        return Err(Error::Invalid);
    }
    let mut reader = BufReader::new(file);
    let mut header = [0u8; 16];
    read_exact_segment(&mut reader, &mut header[..8])?;
    if header[..4] != *MAGIC {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != VERSION {
        return Err(Error::UnsupportedRecoveryVersion(version));
    }
    let records_end = length
        .checked_sub(TRAILER_BYTES)
        .filter(|end| *end >= 16)
        .ok_or(Error::Invalid)?;
    read_exact_segment(&mut reader, &mut header[8..])?;
    let generation = u64::from_be_bytes(header[8..16].try_into().map_err(|_| Error::Invalid)?);
    let mut digest = Sha256::new();
    digest.update(header);
    let mut at = 16usize;
    let mut count = 0usize;
    let mut payload = Vec::new();
    let mut records = Vec::new();
    while at < records_end {
        let mut length_bytes = [0u8; 4];
        read_exact_segment(&mut reader, &mut length_bytes)?;
        let record_len =
            usize::try_from(u32::from_be_bytes(length_bytes)).map_err(|_| Error::Invalid)?;
        if record_len == 0 || record_len > limits.spool_record_max_bytes {
            return Err(Error::Invalid);
        }
        at = at
            .checked_add(4)
            .and_then(|value| value.checked_add(record_len))
            .and_then(|value| value.checked_add(32))
            .filter(|value| *value <= records_end)
            .ok_or(Error::Invalid)?;
        if count >= limits.spool_max_records {
            return Err(Error::Overloaded);
        }
        payload.resize(record_len, 0);
        read_exact_segment(&mut reader, &mut payload)?;
        let mut checksum = [0u8; 32];
        read_exact_segment(&mut reader, &mut checksum)?;
        if Sha256::digest(&payload).as_slice() != checksum {
            return Err(Error::Invalid);
        }
        let record: SpoolRecord = serde_json::from_slice(&payload).map_err(|_| Error::Invalid)?;
        if collect {
            records.push(record);
        }
        digest.update(length_bytes);
        digest.update(&payload);
        digest.update(checksum);
        count += 1;
    }
    let mut trailer = [0u8; TRAILER_BYTES];
    read_exact_segment(&mut reader, &mut trailer)?;
    let trailer_count = u64::from_be_bytes(trailer[4..12].try_into().map_err(|_| Error::Invalid)?);
    let trailer_bytes = u64::from_be_bytes(trailer[12..20].try_into().map_err(|_| Error::Invalid)?);
    digest.update(&trailer[..20]);
    if trailer[..4] != *TRAILER_MAGIC
        || trailer_count != u64::try_from(count).map_err(|_| Error::Overloaded)?
        || trailer_bytes != u64::try_from(at).map_err(|_| Error::Overloaded)?
        || digest.finalize().as_slice() != &trailer[20..]
        || reader.read(&mut [0; 1]).map_err(|_| Error::Storage)? != 0
    {
        return Err(Error::Invalid);
    }
    Ok((generation, records))
}

fn decode_segment(input: &[u8], limits: &Limits, output: &mut Vec<SpoolRecord>) -> Result<()> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != VERSION {
        return Err(Error::UnsupportedRecoveryVersion(version));
    }
    let (records_end, expected_count) = {
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
        (end, count)
    };
    let start_count = output.len();
    let mut at = 16usize;
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
        output.push(serde_json::from_slice(payload).map_err(|_| Error::Invalid)?);
        at = checksum_end;
    }
    if output.len() - start_count != expected_count {
        return Err(Error::Invalid);
    }
    Ok(())
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

fn encode_record(record: &impl Serialize, maximum: usize) -> Result<Vec<u8>> {
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

    #[tokio::test]
    async fn unsupported_versions_block_recovery_commit_and_preserve_files() {
        for version in [0, 1, 2, VERSION + 1, u32::MAX] {
            let mut bytes = MAGIC.to_vec();
            bytes.extend_from_slice(&version.to_be_bytes());
            assert!(
                matches!(decode_spool_records(&bytes, &Limits::default()), Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
            );
            let directory =
                std::env::temp_dir().join(format!("netbaiot-rejected-spool-{}", Uuid::new_v4()));
            fs::create_dir_all(&directory).unwrap();
            let path = directory.join(SNAPSHOT_NAME);
            fs::write(&path, &bytes).unwrap();
            let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
            assert!(
                matches!(spool.recover().await, Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
            );
            assert!(
                matches!(spool.commit(vec![record()]).await, Err(Error::UnsupportedRecoveryVersion(found)) if found == version)
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn current_records_preserve_identity_attempts_telemetry_and_command_ack() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-current-spool-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        let mut records = Vec::new();
        for (index, kind) in [
            serde_json::json!({"kind":"heartbeat","data":{"sequence":1}}),
            serde_json::json!({"kind":"telemetry","data":{"temperature":22}}),
            serde_json::json!({"kind":"command_ack","data":{"command_id":Uuid::from_u128(9),"execution":"succeeded"}}),
        ].into_iter().enumerate() {
            let mut value = record();
            value.event.event_id = EventId(Uuid::from_u128(index as u128 + 2));
            value.event.kind = serde_json::from_value(kind).unwrap();
            value.attempts.insert(value.pending_sinks[0].clone(), 2);
            records.push(value);
        }
        spool.commit(records.clone()).await.unwrap();
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.generation, 1);
        assert_eq!(recovered.records.len(), 3);
        for (got, original) in recovered.records.iter().zip(records) {
            assert_eq!(
                serde_json::to_value(got).unwrap(),
                serde_json::to_value(original).unwrap()
            );
        }
        spool
            .remove_committed(recovered.committed_files)
            .await
            .unwrap();
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn large_streamed_snapshot_validates_before_generation_update() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-streamed-spool-{}", Uuid::new_v4()));
        let limits = Arc::new(Limits::default());
        let spool = RestartSpool::new(directory.clone(), limits.clone());
        let second_sink = SinkId::new("second").unwrap();
        let records = (0..2_048)
            .map(|index| {
                let mut item = record();
                item.event.event_id = EventId(Uuid::from_u128(index + 1));
                item.pending_sinks.push(second_sink.clone());
                item.attempts.insert(second_sink.clone(), 2);
                item
            })
            .collect::<Vec<_>>();
        let path = spool.commit(records.clone()).await.unwrap().unwrap();
        let (generation, discarded) = read_segment_stream(
            recovery_io::open_snapshot(&path).unwrap().unwrap(),
            &limits,
            false,
        )
        .unwrap();
        assert_eq!(generation, 1);
        assert!(discarded.is_empty());
        let old_bytes = fs::read(&path).unwrap();
        let decoded = decode_spool_records(&old_bytes, &limits).unwrap();
        let (_, streamed) = read_segment_stream(
            recovery_io::open_snapshot(&path).unwrap().unwrap(),
            &limits,
            true,
        )
        .unwrap();
        assert_eq!(streamed.len(), records.len());
        assert_eq!(
            serde_json::to_value(&streamed).unwrap(),
            serde_json::to_value(&decoded).unwrap()
        );
        spool.commit(records).await.unwrap();
        let recovered = spool.recover().await.unwrap();
        assert_eq!(recovered.generation, 2);
        assert_eq!(recovered.records.len(), 2_048);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn streamed_and_fuzzable_decoders_agree_on_truncation_and_bit_flips() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-stream-parity-{}", Uuid::new_v4()));
        let limits = Limits::default();
        let path = commit_sync(&directory, &limits, &[record()]).unwrap();
        let image = fs::read(&path).unwrap();
        let check = |bytes: &[u8]| {
            fs::write(&path, bytes).unwrap();
            let streamed = read_segment_stream(fs::File::open(&path).unwrap(), &limits, true);
            let buffered = decode_spool_records(bytes, &limits);
            assert_eq!(streamed.is_ok(), buffered.is_ok(), "length={}", bytes.len());
        };
        check(&image);
        for length in 0..image.len() {
            check(&image[..length]);
        }
        for offset in 0..image.len() {
            let mut mutated = image.clone();
            mutated[offset] ^= 0x5a;
            check(&mutated);
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn empty_or_absent_spool_recovers_no_work() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-spool-empty-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
        assert!(spool.recover().await.unwrap().records.is_empty());
        fs::create_dir_all(&directory).unwrap();
        assert!(spool.recover().await.unwrap().records.is_empty());
        commit_sync(&directory, &Limits::default(), &[]).unwrap();
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
        // An unknown snapshot entry blocks cleanup before the authority is removed.
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
        // A corrupt authority cannot fall back to a good non-authoritative current file or be overwritten.
        fs::write(&stale, &image).unwrap();
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
    async fn temporary_file_budget_blocks_commit_without_replacing_authority() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-temp-budget-{}", Uuid::new_v4()));
        let spool = RestartSpool::new(
            directory.clone(),
            Arc::new(Limits {
                spool_max_records: 1,
                ..Limits::default()
            }),
        );
        let path = spool.commit(vec![record()]).await.unwrap().unwrap();
        let image = fs::read(&path).unwrap();
        for index in 0..17 {
            fs::write(directory.join(format!("abandoned-{index}.tmp")), b"").unwrap();
        }
        let result = spool.commit(vec![record()]).await;
        assert!(matches!(result, Err(Error::Overloaded)));
        assert_eq!(fs::read(&path).unwrap(), image);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 18);
        for index in 0..17 {
            fs::remove_file(directory.join(format!("abandoned-{index}.tmp"))).unwrap();
        }
        spool.commit(vec![record()]).await.unwrap();
        assert_eq!(spool.recover().await.unwrap().generation, 2);
        fs::remove_dir_all(directory).unwrap();
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

    #[tokio::test]
    async fn corrupt_truncated_oversized_and_unknown_versions_fail() {
        let directory =
            std::env::temp_dir().join(format!("netbaiot-bounded-spool-{}", Uuid::new_v4()));
        let limits = Limits::default();
        let path = commit_sync(&directory, &limits, &[record()]).unwrap();
        let image = fs::read(&path).unwrap();
        for end in [0, 4, 8, 16, image.len() - 1] {
            assert!(decode_spool_records(&image[..end], &limits).is_err());
        }
        let mut oversized = image.clone();
        oversized[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
        let trailer = oversized.len() - TRAILER_BYTES;
        let digest = Sha256::digest(&oversized[..trailer + 20]);
        oversized[trailer + 20..].copy_from_slice(&digest);
        assert!(matches!(
            decode_spool_records(&oversized, &limits),
            Err(Error::Invalid)
        ));
        let mut unknown = image.clone();
        unknown[4..8].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            decode_spool_records(&unknown, &limits),
            Err(Error::UnsupportedRecoveryVersion(u32::MAX))
        ));
        assert_eq!(fs::read(path).unwrap(), image);
        fs::remove_dir_all(directory).unwrap();
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
    #[tokio::test]
    #[ignore = "serial cleanup recovery measurement"]
    async fn legacy_cleanup_recovery_measurement() {
        if std::env::var_os("NETBAIOT_LEGACY_BENCH").is_none() {
            return;
        }
        if cfg!(debug_assertions) {
            panic!("release measurements only");
        }
        for count in [1, 128, 512] {
            let directory =
                std::env::temp_dir().join(format!("netbaiot-cleanup-spool-{}", Uuid::new_v4()));
            let spool = RestartSpool::new(directory.clone(), Arc::new(Limits::default()));
            let records: Vec<_> = (0..count).map(|_| record()).collect();
            let mut saves = Vec::new();
            let mut loads = Vec::new();
            let mut bytes = 0;
            for iteration in 0..23 {
                let input = records.clone();
                let started = std::time::Instant::now();
                let path = spool.commit(input).await.unwrap().unwrap();
                let save = started.elapsed().as_nanos();
                bytes = fs::metadata(path).unwrap().len();
                let started = std::time::Instant::now();
                let recovered = spool.recover().await.unwrap();
                let load = started.elapsed().as_nanos();
                assert_eq!(recovered.records.len(), count);
                if iteration >= 3 {
                    saves.push(save);
                    loads.push(load);
                }
            }
            for (operation, mut samples) in [("save", saves), ("load", loads)] {
                samples.sort_unstable();
                println!(
                    "CLEANUP_RECOVERY,spool,{count},{operation},{},{},{},{},{bytes}",
                    samples[10], samples[18], samples[19], samples[19]
                );
            }
            fs::remove_dir_all(directory).unwrap();
        }
    }
}
