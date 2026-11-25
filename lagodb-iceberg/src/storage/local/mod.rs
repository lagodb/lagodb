//! PostgreSQL-native local file storage implementation.
//!
//! This module provides a local file storage implementation that uses PostgreSQL's
//! internal Virtual File Descriptor (VFD) system via `PathNameOpenFile`, `FileRead`,
//! `FileWrite`, and `FileClose` functions. This integration provides:
//!
//! - **Rust-owned VFD lifetime**: Reader and writer owners call `FileClose` from
//!   `Drop`. Their enclosing executor and transaction state provide the
//!   error-unwind cleanup boundary; `PathNameOpenFile` does not itself register a
//!   VFD with PostgreSQL's `ResourceOwner`.
//! - **VFD Pool Management**: PostgreSQL's VFD system manages file descriptor limits
//!   transparently, automatically closing/reopening files as needed.
//! - **Consistent Error Handling**: Uses PostgreSQL's error reporting mechanisms.
//! - **WAL Support**: Optional Iceberg file WAL for standby WAL replay or
//!   archive recovery of local files.
//!
//! # WAL invariants
//!
//! - Object storage does not use this WAL path. Object stores provide their own
//!   durability after successful writes, and orphan cleanup is handled outside
//!   PostgreSQL redo.
//! - `LocalStorage::default()` disables Iceberg file WAL. Callers must opt in
//!   with [`LocalStorage::with_wal`] when
//!   PostgreSQL says the owning relation needs WAL.
//! - When WAL is enabled, `WRITE_FILE` records are for best-effort, lossy
//!   reconstruction during standby WAL replay or archive recovery. Local crash
//!   recovery intentionally skips replaying them because successful explicit
//!   close performs `FileSync`.
//! - Dropping [`writer::LocalFileWriter`] only closes the VFD. Callers must drive
//!   the normal `FileWrite::close`/`OutputFileWriter::finish` path to observe
//!   `FileSync` errors.
//!
//! # Local durability limitation
//!
//! This adapter syncs files without syncing their directories. FileSync alone
//! does not make newly-created file or directory entries durable: an OS crash
//! or power loss can leave committed metadata referencing missing paths. Primary
//! crash recovery skips WRITE_FILE redo and does not repair this gap.

mod reader;
mod writer;

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use iceberg_lite::io::{FileMetadata, FileWrite, OpenedFile, Storage};
use iceberg_lite::{Error, ErrorKind, Result};
use lagodb_core::backend::ExternalFdReservation;
use lagodb_storage::StorageError;
use pgrx::check_for_interrupts;

use self::reader::LocalFileReader;
use self::writer::LocalFileWriter;
use crate::error::storage_error_to_iceberg_lite;
use crate::storage::local_file_wal::log_truncate_directory;
use crate::storage::transaction_resources::register_local_file_created;

/// Local file storage implementation using PostgreSQL's VFD system.
///
/// This implementation wraps PostgreSQL's internal file I/O functions to provide
/// seamless integration with PostgreSQL's resource management infrastructure.
///
/// # WAL Support
///
/// By default, `LocalStorage` does not enable Iceberg file WAL. Use
/// [`LocalStorage::with_wal`] to create a storage instance that logs writes for
/// best-effort reconstruction during standby WAL replay or archive recovery of
/// local Iceberg files. This WAL is not needed for object storage, and local
/// crash recovery relies on explicit close-time `FileSync` instead of replaying
/// `WRITE_FILE` records.
#[derive(Debug, Default)]
pub(crate) struct LocalStorage {
    /// Whether Iceberg file WAL is enabled for local write operations.
    needs_wal: bool,
    require_absolute_paths: bool,
    backend_thread: PhantomData<Rc<()>>,
}

// SAFETY: `LocalStorage` is a private host adapter for iceberg-lite's
// `Storage: Send + Sync` contract. Every method, returned VFD owner, and Drop
// remains inside a PostgreSQL-owned scan/mutation lifecycle on one backend
// thread; the adapter is never handed to a parallel Iceberg executor.
// `backend_thread` intentionally opts out of compiler-derived `Send + Sync`
// from the `bool` fields so this invariant stays explicit.
unsafe impl Send for LocalStorage {}
// SAFETY: the same closed execution boundary prevents concurrent calls; the
// shared reference capability exists only to satisfy the upstream trait.
unsafe impl Sync for LocalStorage {}

impl LocalStorage {
    /// Require absolute paths when resolving file locations.
    ///
    /// `file://` URIs must also be unescaped and contain no query or fragment;
    /// resolution strips the prefix without decoding the URI. Bare paths are
    /// treated literally. This preserves the configured WAL policy.
    #[must_use]
    pub(crate) fn with_absolute_paths(mut self) -> Self {
        self.require_absolute_paths = true;
        self
    }

    /// Create a new LocalStorage with WAL support configured.
    pub fn with_wal(needs_wal: bool) -> Self {
        Self {
            needs_wal,
            require_absolute_paths: false,
            backend_thread: PhantomData,
        }
    }

    /// Check if Iceberg file WAL logging is enabled.
    #[inline]
    pub fn needs_wal(&self) -> bool {
        self.needs_wal
    }

    /// Clear a transaction-local generation selected by PostgreSQL's
    /// nontransactional-truncate callback. Abort discards this whole generation.
    pub(crate) fn truncate_directory(&self, path: &str) -> Result<()> {
        self.remove_dir_all(path)?;
        if self.needs_wal {
            log_truncate_directory(path);
        }
        Ok(())
    }

    pub(crate) fn list_older_than(
        &self,
        table_location: &str,
        cutoff_ms: i64,
    ) -> Result<HashSet<String>> {
        let (prefix, root) = match table_location.strip_prefix("file://") {
            Some(path) => ("file://", Path::new(path)),
            None => ("", Path::new(table_location)),
        };
        let cutoff_ms = i128::from(cutoff_ms);
        let mut pending = vec![root.to_path_buf()];
        let mut paths = HashSet::new();
        while let Some(directory) = pending.pop() {
            let _fd_reservation = acquire_dir_fd_reservation()?;
            let entries = fs::read_dir(&directory)?;
            for entry in entries {
                check_for_interrupts();
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_symlink() {
                    continue;
                }
                if file_type.is_dir() {
                    pending.push(entry.path());
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let metadata = entry.metadata()?;
                let modified_ms = system_time_epoch_ms(metadata.modified()?);
                if modified_ms >= cutoff_ms {
                    continue;
                }
                paths.insert(format!("{prefix}{}", entry.path().display()));
            }
        }
        Ok(paths)
    }

    pub(crate) fn location_is_empty(&self, location: &str) -> Result<bool> {
        let _fd_reservation = acquire_dir_fd_reservation()?;
        let mut entries = match fs::read_dir(location) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(true);
            }
            Err(error) if error.kind() == io::ErrorKind::NotADirectory => {
                return Ok(false);
            }
            Err(error) => return Err(error.into()),
        };
        Ok(entries.next().transpose()?.is_none())
    }
}

fn acquire_dir_fd_reservation() -> Result<ExternalFdReservation> {
    ExternalFdReservation::try_acquire().ok_or_else(|| {
        storage_error_to_iceberg_lite(StorageError::resource_exhausted(
            "PostgreSQL external file descriptor budget exhausted",
        ))
    })
}

fn system_time_epoch_ms(time: SystemTime) -> i128 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as i128,
        Err(before_epoch) => {
            let duration = before_epoch.duration();
            let millis = duration.as_millis() as i128;
            if duration.subsec_nanos() % 1_000_000 == 0 {
                -millis
            } else {
                -(millis + 1)
            }
        }
    }
}

impl Storage for LocalStorage {
    fn resolve_uri(&self, uri: &str) -> Result<usize> {
        let offset = if uri.starts_with("file://") {
            "file://".len()
        } else {
            0
        };
        if self.require_absolute_paths {
            let path = &uri[offset..];
            if !Path::new(path).is_absolute()
                || (offset != 0 && uri.contains(['%', '?', '#']))
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Local storage requires an absolute path or an unescaped file:/// URI",
                ));
            }
        }
        Ok(offset)
    }

    fn delete(&self, path: &str) -> Result<()> {
        match fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let p = Path::new(path);
        let metadata = match fs::symlink_metadata(p) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let result = if metadata.is_dir() {
            fs::remove_dir_all(p)
        } else {
            fs::remove_file(p)
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Local filesystem deletion has no remote wait to bound.
    fn delete_cleanup(&self, path: &str) -> Result<()> {
        self.delete(path)
    }

    /// Local filesystem removal has no remote wait to bound.
    fn remove_dir_all_cleanup(&self, path: &str) -> Result<()> {
        self.remove_dir_all(path)
    }

    fn status(&self, path: &str) -> Result<Option<FileMetadata>> {
        match fs::metadata(path) {
            Ok(metadata) => Ok(Some(FileMetadata {
                size: metadata.len(),
            })),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn open_reader(&self, path: &str) -> Result<OpenedFile> {
        let reader = LocalFileReader::open(path)?;
        let metadata = FileMetadata {
            size: reader.size(),
        };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(reader),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        if let Some(parent) = Path::new(path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent)?;
        }
        let writer = LocalFileWriter::open_with_wal(path, self.needs_wal)?;

        register_local_file_created(PathBuf::from(path));

        Ok(Box::new(writer))
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        "file"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_disables_wal() {
        assert!(!LocalStorage::default().needs_wal());
    }

    #[test]
    fn with_wal_is_the_explicit_wal_opt_in() {
        assert!(LocalStorage::with_wal(true).needs_wal());
        assert!(!LocalStorage::with_wal(false).needs_wal());
    }
}
