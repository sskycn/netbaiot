use crate::{Error, Limits, Result, SpoolRecord};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

const MAGIC: &[u8; 4] = b"NBSP";
const LEGACY_VERSION: u32 = 1;
const VERSION: u32 = 2;
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
}

impl RestartSpool {
    pub fn new(directory: PathBuf, limits: Arc<Limits>) -> Self {
        Self {
            directory: Arc::new(directory),
            limits,
        }
    }

    pub async fn commit(&self, records: Vec<SpoolRecord>) -> Result<Option<PathBuf>> {
        if records.is_empty() {
            return Ok(None);
        }
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        tokio::task::spawn_blocking(move || commit_sync(&directory, &limits, &records))
            .await
            .map_err(|_| Error::Internal)?
            .map(Some)
    }

    pub async fn recover(&self) -> Result<RecoveryBatch> {
        let directory = self.directory.clone();
        let limits = self.limits.clone();
        tokio::task::spawn_blocking(move || recover_sync(&directory, &limits))
            .await
            .map_err(|_| Error::Internal)?
    }

    pub async fn remove_committed(&self, files: Vec<CommittedSpool>) -> Result<()> {
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || {
            for committed in files {
                let path = committed.path;
                if path.parent() != Some(directory.as_path())
                    || path.extension().and_then(|value| value.to_str()) != Some("spool")
                {
                    return Err(Error::Invalid);
                }
                let bytes = fs::read(&path).map_err(|_| Error::Storage)?;
                let generation = segment_generation(&bytes)?;
                // Never let cleanup for an older recovery batch delete a newer
                // atomically replaced snapshot at the same path.
                if generation == committed.generation {
                    if path.file_name().and_then(|value| value.to_str()) == Some(SNAPSHOT_NAME) {
                        // Remove ignored legacy generations first. If cleanup
                        // fails, the authoritative snapshot remains intact.
                        for entry in
                            fs::read_dir(directory.as_path()).map_err(|_| Error::Storage)?
                        {
                            let stale = entry.map_err(|_| Error::Storage)?.path();
                            if stale != path
                                && stale.extension().and_then(|value| value.to_str())
                                    == Some("spool")
                            {
                                fs::remove_file(stale).map_err(|_| Error::Storage)?;
                            }
                        }
                    }
                    fs::remove_file(path).map_err(|_| Error::Storage)?;
                }
            }
            sync_directory(&directory)
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
    fs::create_dir_all(directory).map_err(|_| Error::Storage)?;
    set_directory_permissions(directory)?;
    let existing = recover_sync(directory, limits)?;
    let generation = existing
        .generation
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    let id = Uuid::new_v4();
    let temporary = directory.join(format!(".{id}.tmp"));
    let committed = directory.join(SNAPSHOT_NAME);
    let result = (|| {
        let mut file = open_private(&temporary)?;
        file.write_all(MAGIC).map_err(|_| Error::Storage)?;
        file.write_all(&VERSION.to_be_bytes())
            .map_err(|_| Error::Storage)?;
        file.write_all(&generation.to_be_bytes())
            .map_err(|_| Error::Storage)?;
        let mut total = 16usize;
        for record in records {
            let payload = serde_json::to_vec(record).map_err(|_| Error::Invalid)?;
            if payload.len() > limits.spool_record_max_bytes {
                return Err(Error::Overloaded);
            }
            let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
            total = total
                .checked_add(4 + payload.len() + 32)
                .ok_or(Error::Overloaded)?;
            if total > limits.spool_segment_max_bytes || total > limits.spool_max_bytes {
                return Err(Error::Overloaded);
            }
            file.write_all(&length.to_be_bytes())
                .and_then(|_| file.write_all(&payload))
                .and_then(|_| file.write_all(&Sha256::digest(&payload)))
                .map_err(|_| Error::Storage)?;
        }
        file.sync_all().map_err(|_| Error::Storage)?;
        fs::rename(&temporary, &committed).map_err(|_| Error::Storage)?;
        sync_directory(directory)?;
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
    if !directory.exists() {
        return Ok(RecoveryBatch {
            records: Vec::new(),
            committed_files: Vec::new(),
            generation: 0,
        });
    }
    let authoritative = directory.join(SNAPSHOT_NAME);
    if authoritative.exists() {
        let size = usize::try_from(
            fs::metadata(&authoritative)
                .map_err(|_| Error::Storage)?
                .len(),
        )
        .map_err(|_| Error::Overloaded)?;
        if size > limits.spool_segment_max_bytes || size > limits.spool_max_bytes {
            return Err(Error::Overloaded);
        }
        let bytes = fs::read(&authoritative).map_err(|_| Error::Storage)?;
        if bytes.len() != size {
            return Err(Error::Storage);
        }
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
    let mut files = fs::read_dir(directory)
        .map_err(|_| Error::Storage)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("spool"))
        .collect::<Vec<_>>();
    files.sort();
    let mut records = Vec::new();
    let mut total = 0usize;
    for path in &files {
        let metadata = fs::metadata(path).map_err(|_| Error::Storage)?;
        let size = usize::try_from(metadata.len()).map_err(|_| Error::Overloaded)?;
        if size > limits.spool_segment_max_bytes {
            return Err(Error::Invalid);
        }
        total = total.checked_add(size).ok_or(Error::Overloaded)?;
        if total > limits.spool_max_bytes {
            return Err(Error::Overloaded);
        }
        let mut bytes = Vec::with_capacity(size);
        fs::File::open(path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(|_| Error::Storage)?;
        if bytes.len() != size {
            return Err(Error::Storage);
        }
        decode_segment(&bytes, limits, &mut records)?;
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
        committed_files: files
            .into_iter()
            .map(|path| CommittedSpool {
                path,
                generation: 0,
            })
            .collect(),
        generation: 0,
    })
}

fn decode_segment(input: &[u8], limits: &Limits, output: &mut Vec<SpoolRecord>) -> Result<()> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != VERSION && version != LEGACY_VERSION {
        return Err(Error::Invalid);
    }
    let mut at = if version == VERSION {
        if input.len() < 16 {
            return Err(Error::Invalid);
        }
        16usize
    } else {
        8usize
    };
    while at < input.len() {
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
    Ok(())
}

fn segment_generation(input: &[u8]) -> Result<u64> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    match version {
        LEGACY_VERSION => Ok(0),
        VERSION if input.len() >= 16 => Ok(u64::from_be_bytes(
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

#[cfg(unix)]
fn open_private(path: &Path) -> Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| Error::Storage)
}

#[cfg(not(unix))]
fn open_private(path: &Path) -> Result<fs::File> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| Error::Storage)
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| Error::Storage)
}

#[cfg(not(unix))]
fn set_directory_permissions(_: &Path) -> Result<()> {
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Storage)
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
}
