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
const VERSION: u32 = 1;

#[derive(Debug)]
pub struct RecoveryBatch {
    pub records: Vec<SpoolRecord>,
    pub committed_files: Vec<PathBuf>,
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

    pub async fn remove_committed(&self, paths: Vec<PathBuf>) -> Result<()> {
        let directory = self.directory.clone();
        tokio::task::spawn_blocking(move || {
            for path in paths {
                if path.parent() != Some(directory.as_path())
                    || path.extension().and_then(|value| value.to_str()) != Some("spool")
                {
                    return Err(Error::Invalid);
                }
                fs::remove_file(path).map_err(|_| Error::Storage)?;
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
    // Include already committed segments in both configured spool budgets. This prevents
    // repeated failed restarts from accumulating more durable state than the global cap.
    let existing = recover_sync(directory, limits)?;
    if existing
        .records
        .len()
        .checked_add(records.len())
        .ok_or(Error::Overloaded)?
        > limits.spool_max_records
    {
        return Err(Error::Overloaded);
    }
    let existing_bytes = existing
        .committed_files
        .iter()
        .try_fold(0usize, |total, path| {
            let bytes = usize::try_from(fs::metadata(path).map_err(|_| Error::Storage)?.len())
                .map_err(|_| Error::Overloaded)?;
            total.checked_add(bytes).ok_or(Error::Overloaded)
        })?;
    let id = Uuid::new_v4();
    let temporary = directory.join(format!(".{id}.tmp"));
    let committed = directory.join(format!("{id}.spool"));
    let mut file = open_private(&temporary)?;
    file.write_all(MAGIC).map_err(|_| Error::Storage)?;
    file.write_all(&VERSION.to_be_bytes())
        .map_err(|_| Error::Storage)?;
    let mut total = 8usize;
    for record in records {
        let payload = serde_json::to_vec(record).map_err(|_| Error::Invalid)?;
        if payload.len() > limits.spool_record_max_bytes {
            return Err(Error::Overloaded);
        }
        let length = u32::try_from(payload.len()).map_err(|_| Error::Overloaded)?;
        total = total
            .checked_add(4 + payload.len() + 32)
            .ok_or(Error::Overloaded)?;
        if total > limits.spool_segment_max_bytes
            || existing_bytes.saturating_add(total) > limits.spool_max_bytes
        {
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
}

fn recover_sync(directory: &Path, limits: &Limits) -> Result<RecoveryBatch> {
    if !directory.exists() {
        return Ok(RecoveryBatch {
            records: Vec::new(),
            committed_files: Vec::new(),
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
    Ok(RecoveryBatch {
        records,
        committed_files: files,
    })
}

fn decode_segment(input: &[u8], limits: &Limits, output: &mut Vec<SpoolRecord>) -> Result<()> {
    if input.len() < 8 || input.get(..4) != Some(MAGIC) {
        return Err(Error::Invalid);
    }
    let version = u32::from_be_bytes(input[4..8].try_into().map_err(|_| Error::Invalid)?);
    if version != VERSION {
        return Err(Error::Invalid);
    }
    let mut at = 8usize;
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
