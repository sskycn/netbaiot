//! Recovery I/O for an exclusively owned local directory. Parents must be trusted;
//! live operators must not replace directory components while the gateway owns it.
use crate::{Error, Result};
use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

pub fn directory_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(Error::Storage),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Error::Storage),
    }
}

fn regular(meta: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x400 != 0 {
            return false;
        } // reparse point
    }
    meta.is_file() && !meta.file_type().is_symlink()
}

/// Missing is permitted only after the caller has verified the owned directory.
/// Nonblocking/no-follow open prevents a substituted FIFO from blocking a worker.
pub fn open_snapshot(path: &Path) -> Result<Option<fs::File>> {
    match fs::symlink_metadata(path) {
        Ok(meta) if regular(&meta) => (),
        Ok(_) => return Err(Error::Storage),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(Error::Storage),
    }
    let mut options = fs::OpenOptions::new();
    options.read(true);
    harden_open(&mut options);
    let file = options.open(path).map_err(|_| Error::Storage)?;
    if !regular(&file.metadata().map_err(|_| Error::Storage)?) {
        return Err(Error::Storage);
    }
    Ok(Some(file))
}

fn harden_open(options: &mut fs::OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000); // FILE_FLAG_OPEN_REPARSE_POINT
    }
    #[cfg(not(any(unix, windows)))]
    let _ = options;
}

pub fn read_bounded(reader: impl Read, maximum: usize) -> Result<Vec<u8>> {
    let ceiling = u64::try_from(maximum)
        .map_err(|_| Error::Overloaded)?
        .checked_add(1)
        .ok_or(Error::Overloaded)?;
    let mut bytes = Vec::new();
    reader
        .take(ceiling)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Storage)?;
    if bytes.len() > maximum {
        return Err(Error::Overloaded);
    }
    Ok(bytes)
}

/// Preserve the distinction between corrupt/truncated bytes and underlying I/O
/// errors when a pure decoder maps short reads to Invalid.
pub struct StorageReader<R> {
    inner: R,
    pub failed: bool,
}
impl<R> StorageReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            failed: false,
        }
    }
}
impl<R: Read> Read for StorageReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let result = self.inner.read(bytes);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() != io::ErrorKind::Interrupted)
        {
            self.failed = true;
        }
        result
    }
}

/// Bounds *all* directory entries, including ignored temporary files, before collection.
pub fn spool_paths(directory: &Path, maximum: usize) -> Result<Vec<PathBuf>> {
    collect_spool_paths(
        fs::read_dir(directory)
            .map_err(|_| Error::Storage)?
            .map(|entry| entry.map(|entry| entry.path())),
        maximum,
    )
}

fn collect_spool_paths(
    entries: impl Iterator<Item = io::Result<PathBuf>>,
    maximum: usize,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= maximum {
            return Err(Error::Overloaded);
        }
        let path = entry.map_err(|_| Error::Storage)?;
        if path.extension().and_then(|value| value.to_str()) == Some("spool") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub fn prepare_directory(path: &Path) -> Result<()> {
    if !directory_present(path)? {
        fs::create_dir_all(path).map_err(|_| Error::Storage)?;
    }
    if !directory_present(path)? {
        return Err(Error::Storage);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| Error::Storage)?;
    }
    Ok(())
}

pub fn create_private(path: &Path) -> Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|_| Error::Storage)
}

/// Caller has synced and closed the private temporary file in the same directory.
/// Unix: rename + directory fsync. Windows: MoveFileExW(REPLACE_EXISTING |
/// WRITE_THROUGH), through a reviewed safe wrapper. No directory handle flush is
/// implied on Windows and no general power-loss/crash durability is promised.
pub fn replace_synced(temporary: &Path, committed: &Path) -> Result<()> {
    if temporary.parent() != committed.parent() {
        return Err(Error::Invalid);
    }
    #[cfg(windows)]
    atomicwrites::replace_atomic(temporary, committed).map_err(|_| Error::Storage)?;
    #[cfg(not(windows))]
    {
        fs::rename(temporary, committed).map_err(|_| Error::Storage)?;
        sync_directory(committed.parent().ok_or(Error::Invalid)?)?;
    }
    Ok(())
}

#[cfg(not(windows))]
pub fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| Error::Storage)
}

/// OS advisory lock; released by the kernel on process exit. Never unlink its inode.
/// Only cooperating processes on a local filesystem are supported.
pub struct RecoveryDirectory {
    _file: fs::File,
}
impl RecoveryDirectory {
    pub fn acquire(directory: &Path) -> Result<Self> {
        prepare_directory(directory)?;
        let path = directory.join(".netbaiot.lock");
        match fs::symlink_metadata(&path) {
            Ok(meta) if regular(&meta) => (),
            Ok(_) => return Err(Error::Storage),
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(_) => return Err(Error::Storage),
        }
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        harden_open(&mut options);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path).map_err(|_| Error::Storage)?;
        if !regular(&file.metadata().map_err(|_| Error::Storage)?) {
            return Err(Error::Storage);
        }
        fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
            if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() {
                Error::Conflict
            } else {
                Error::Storage
            }
        })?;
        Ok(Self { _file: file })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_read_and_directory_enumeration_are_bounded_and_propagate_errors() {
        assert!(matches!(
            read_bounded(io::Cursor::new(vec![1; 65]), 64),
            Err(Error::Overloaded)
        ));
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::ErrorKind::PermissionDenied.into())
            }
        }
        assert!(matches!(read_bounded(Broken, 64), Err(Error::Storage)));
        let entries = vec![
            Ok(PathBuf::from("a.spool")),
            Err(io::ErrorKind::PermissionDenied.into()),
        ];
        assert!(matches!(
            collect_spool_paths(entries.into_iter(), 8),
            Err(Error::Storage)
        ));
        assert!(matches!(
            collect_spool_paths((0..9).map(|_| Ok(PathBuf::from("ignored.tmp"))), 8),
            Err(Error::Overloaded)
        ));
    }
}
