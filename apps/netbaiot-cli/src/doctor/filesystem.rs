use super::*;
use std::{fs, io::Write};
struct Probe(std::path::PathBuf);
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
pub(super) async fn inspect(config: &Config) -> Check {
    let path = config.spool_directory.clone();
    let probe_path = path.clone();
    let result = tokio::task::spawn_blocking(move || {
        match fs::symlink_metadata(&probe_path) {
            Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                return Err("Recovery path must be a real directory");
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err("Cannot inspect recovery directory");
            }
            _ => (),
        }
        fs::create_dir_all(&probe_path).map_err(|_| "Cannot create recovery directory")?;
        let owner =
            netbaiot_runtime::recovery_io::RecoveryDirectory::acquire_for_inspection(&probe_path)
                .map_err(|_| "Recovery directory is inaccessible or owned by another gateway")?;
        let temporary =
            Probe(probe_path.join(format!(".netbaiot-doctor-{}", uuid::Uuid::new_v4())));
        let mut file = netbaiot_runtime::recovery_io::create_private(&temporary.0)
            .map_err(|_| "Cannot create private recovery probe")?;
        file.write_all(b"NetbaIoT local permission probe\n")
            .and_then(|_| file.sync_all())
            .map_err(|_| "Cannot write/sync recovery probe")?;
        drop(file);
        fs::remove_file(&temporary.0).map_err(|_| "Cannot remove recovery probe")?;
        Ok::<_, &str>(owner)
    })
    .await;
    match result {
        Ok(Ok(owner)) => {
            // Keep exclusive ownership across both asynchronous decoder reads.
            let owner = std::sync::Arc::new(owner);
            let result =
                netbaiot_server::inspect_recovery_owned(path, config.limits.clone(), owner).await;
            match result {
                Ok(count) => Check::new(
                    "recovery",
                    Status::Pass,
                    "NBI-DOC-001",
                    format!(
                        "Local probe created/synced/deleted; snapshots checked without mutation; {count} pending spool records"
                    ),
                ),
                Err(e) => Check::new(
                    "recovery",
                    Status::Fail,
                    "NBI-DOC-001",
                    format!("Recovery inspection failed: {e}; snapshots preserved"),
                ),
            }
        }
        Ok(Err(message)) => Check::new("recovery", Status::Fail, "NBI-DOC-001", message),
        Err(_) => Check::new(
            "recovery",
            Status::Fail,
            "NBI-DOC-001",
            "Recovery inspection worker failed; existing snapshots preserved",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn recovery_failures_preserve_existing_files_and_leave_no_probe() {
        let root = tempfile::tempdir().unwrap();
        let mut config: Config =
            serde_json::from_str(include_str!("../../../../configs/development.json")).unwrap();
        let file = root.path().join("file");
        std::fs::write(&file, b"unchanged").unwrap();
        config.spool_directory = file.clone();
        assert!(inspect(&config).await.status == Status::Fail);
        assert_eq!(std::fs::read(&file).unwrap(), b"unchanged");
        config.spool_directory = root.path().join("recovery");
        std::fs::create_dir(&config.spool_directory).unwrap();
        let snapshot = config.spool_directory.join("eventbus-recovery.spool");
        std::fs::write(&snapshot, b"corrupt unchanged snapshot").unwrap();
        assert!(inspect(&config).await.status == Status::Fail);
        assert_eq!(
            std::fs::read(&snapshot).unwrap(),
            b"corrupt unchanged snapshot"
        );
        assert!(
            !std::fs::read_dir(&config.spool_directory)
                .unwrap()
                .any(|e| e
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".netbaiot-doctor-"))
        );
    }
}
