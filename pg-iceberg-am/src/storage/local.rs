//! PostgreSQL-native local file storage implementation.
//!
//! This module provides a local file storage implementation that uses PostgreSQL's
//! internal Virtual File Descriptor (VFD) system via `PathNameOpenFile`, `FileRead`,
//! `FileWrite`, and `FileClose` functions. This integration provides:
//!
//! - **Resource Owner Integration**: File handles are automatically registered with
//!   PostgreSQL's ResourceOwner system, ensuring automatic cleanup on transaction
//!   abort or backend exit.
//! - **VFD Pool Management**: PostgreSQL's VFD system manages file descriptor limits
//!   transparently, automatically closing/reopening files as needed.
//! - **Consistent Error Handling**: Uses PostgreSQL's error reporting mechanisms.
//! - **WAL Support**: Optional Iceberg file WAL for standby/archive/PITR
//!   recovery of local files.
//!
//! # WAL invariants
//!
//! - Object storage does not use this WAL path. Object stores provide their own
//!   durability after successful writes, and orphan cleanup is handled outside
//!   PostgreSQL redo.
//! - `LocalStorage::default()` and [`LocalStorage::new`] both disable Iceberg
//!   file WAL. Callers must opt in with [`LocalStorage::with_wal`] when
//!   PostgreSQL says the owning relation needs WAL.
//! - When WAL is enabled, `WRITE_FILE` records are for standby/archive/PITR
//!   recovery. Local crash recovery intentionally skips replaying them because
//!   successful explicit close performs `FileSync`.
//! - Dropping [`PgFileWrite`] only closes the VFD. Callers must drive the normal
//!   `FileWrite::close`/`OutputFileWriter::finish` path to observe `FileSync`
//!   errors.

use std::any::Any;
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use pgrx::pg_sys;

use iceberg_lite::Result;
use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, OpenedFile, Storage};
use pg_lakebase_core::diag;

use crate::wal::log_write_file;

/// Local file storage implementation using PostgreSQL's VFD system.
///
/// This implementation wraps PostgreSQL's internal file I/O functions to provide
/// seamless integration with PostgreSQL's resource management infrastructure.
///
/// # WAL Support
///
/// By default, `LocalStorage` does not enable Iceberg file WAL. Use
/// [`LocalStorage::with_wal`] to create a storage instance that logs writes for
/// standby/archive/PITR recovery of local Iceberg files. This WAL is not needed
/// for object storage, and local crash recovery relies on explicit close-time
/// `FileSync` instead of replaying `WRITE_FILE` records.
///
/// ```ignore
/// // Without WAL (default)
/// let storage = LocalStorage::default();
///
/// // With WAL enabled
/// let storage = LocalStorage::with_wal(true);
/// ```
#[derive(Debug, Default)]
pub struct LocalStorage {
    /// Whether Iceberg file WAL is enabled for local write operations.
    needs_wal: bool,
}

impl LocalStorage {
    /// Create a new LocalStorage with default settings (no WAL).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new LocalStorage with WAL support configured.
    ///
    /// # Arguments
    /// * `needs_wal` - Whether to emit Iceberg file WAL for local writes
    ///
    /// # Example
    /// ```ignore
    /// use pg_iceberg_am::storage::LocalStorage;
    ///
    /// // Enable WAL for standby/archive/PITR recovery
    /// let storage = LocalStorage::with_wal(true);
    /// ```
    pub fn with_wal(needs_wal: bool) -> Self {
        Self { needs_wal }
    }

    /// Check if Iceberg file WAL logging is enabled.
    #[inline]
    pub fn needs_wal(&self) -> bool {
        self.needs_wal
    }
}

impl Storage for LocalStorage {
    fn delete(&self, path: &str) -> Result<()> {
        match fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let p = Path::new(path);
        if p.exists() {
            if p.is_dir() {
                fs::remove_dir_all(p)?;
            } else {
                fs::remove_file(p)?;
            }
        }
        Ok(())
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
        let reader = PgFileRead::open(path)?;
        let metadata = FileMetadata {
            size: reader.size as u64,
        };
        Ok(OpenedFile {
            metadata,
            reader: Box::new(reader),
        })
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        // Create parent directories if they don't exist
        if let Some(parent) = Path::new(path).parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent)?;
        }
        // Create writer with Iceberg file WAL support if enabled.
        let writer = PgFileWrite::open_with_wal(path, self.needs_wal)?;

        crate::storage::transactional_artifacts::register_local_file_created(
            std::path::PathBuf::from(path),
        );

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

#[derive(Debug)]
struct VfdOwner(pg_sys::File);

impl Drop for VfdOwner {
    fn drop(&mut self) {
        // SAFETY: fd is valid and FileClose handles cleanup properly
        unsafe {
            pg_sys::FileClose(self.0);
        }
    }
}

/// PostgreSQL VFD-backed file reader.
///
/// Wraps a PostgreSQL virtual file descriptor for reading operations. The file
/// handle is automatically registered with the current ResourceOwner and will
/// be closed when the ResourceOwner is released (e.g., at transaction end).
#[derive(Debug)]
pub struct PgFileRead {
    /// Path to the file (for error reporting)
    path: String,
    /// PostgreSQL virtual file descriptor owner
    file: Arc<VfdOwner>,
    /// Total file size in bytes
    size: i64,
}

// Note: PgFileRead is intentionally NOT Send/Sync.
// PostgreSQL's VFD system is thread-local and bound to the current backend.
// Using VFD handles across threads is undefined behavior.

impl PgFileRead {
    /// Open a file for reading using PostgreSQL's VFD system.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open
    ///
    /// # Returns
    /// A new `PgFileRead` instance on success, or an error if the file cannot be opened.
    pub fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let file =
            unsafe { pg_sys::PathNameOpenFile(c_path.as_ptr(), libc::O_RDONLY) };
        if file < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}': {}", path, err),
            ));
        }

        let size = unsafe { pg_sys::FileSize(file) };
        if size < 0 {
            unsafe { pg_sys::FileClose(file) };
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to get size of file '{}': {}", path, err),
            ));
        }

        Ok(Self {
            path: path.to_string(),
            file: Arc::new(VfdOwner(file)),
            size: size as i64,
        })
    }

    /// Read bytes from a specific offset in the file.
    fn read_at(&self, offset: i64, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];

        let result = unsafe {
            pg_sys::FileRead(
                self.file.0,
                buffer.as_mut_ptr() as *mut std::ffi::c_void,
                len,
                offset as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_READ,
            )
        };

        if result < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "failed to read from file '{}' at offset {}: {}",
                    self.path, offset, err
                ),
            ));
        }

        buffer.truncate(result as usize);
        Ok(buffer)
    }
}

impl FileRead for PgFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        // Validate range boundaries
        if range.start > range.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid range: start {} > end {}", range.start, range.end),
            )
            .into());
        }
        if range.end > self.size as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "range end {} exceeds file size {} for '{}'",
                    range.end, self.size, self.path
                ),
            )
            .into());
        }

        let start = range.start as i64;
        let len = (range.end - range.start) as usize;
        let buffer = self.read_at(start, len)?;
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range(0..self.size as u64)
    }

    fn try_clone(&self) -> io::Result<Box<dyn FileRead>> {
        Ok(Box::new(Self {
            path: self.path.clone(),
            file: self.file.clone(),
            size: self.size,
        }))
    }
}

impl Read for PgFileRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let fd = unsafe { pg_sys::FileGetRawDesc(self.file.0) };
        if fd < 0 {
            return Err(io::Error::other(format!(
                "invalid file descriptor for file '{}'",
                self.path
            )));
        }

        let ret = unsafe {
            libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len() as _)
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to read from file '{}': {}", self.path, err),
            ));
        }

        Ok(ret as usize)
    }
}

impl Seek for PgFileRead {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let fd = unsafe { pg_sys::FileGetRawDesc(self.file.0) };
        if fd < 0 {
            return Err(io::Error::other(format!(
                "invalid file descriptor for file '{}'",
                self.path
            )));
        }

        let (whence, offset) = match pos {
            SeekFrom::Start(off) => (libc::SEEK_SET, off as i64),
            SeekFrom::End(off) => (libc::SEEK_END, off),
            SeekFrom::Current(off) => (libc::SEEK_CUR, off),
        };

        let new_pos = unsafe { libc::lseek(fd, offset as libc::off_t, whence) };
        if new_pos < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to seek in file '{}': {}", self.path, err),
            ));
        }

        Ok(new_pos as u64)
    }
}

/// PostgreSQL VFD-backed file writer.
///
/// Wraps a PostgreSQL virtual file descriptor for writing operations. The file
/// handle is automatically registered with the current ResourceOwner and will
/// be closed when the ResourceOwner is released.
///
/// # WAL Support
///
/// When `needs_wal` is true, the writer logs local Iceberg file writes to
/// PostgreSQL WAL for standby/archive/PITR recovery. Following PostgreSQL's
/// convention, the write is performed first, and then the WAL record is
/// written. This order ensures that:
/// - If the write fails (e.g., disk full), no WAL record is created
/// - During standby/archive/PITR recovery, the WAL record can be replayed to
///   restore the file
///
/// Local crash recovery does not replay `WRITE_FILE` records; successful
/// explicit close calls `FileSync`. Distributed storage (S3, etc.) provides its
/// own durability guarantees and does not use this WAL path.
pub struct PgFileWrite {
    /// Path to the file (for error reporting and WAL logging)
    path: String,
    /// PostgreSQL virtual file descriptor
    file: pg_sys::File,
    /// Current write position in the file
    position: i64,
    /// Whether Iceberg file WAL logging is enabled for this writer.
    needs_wal: bool,
}

// Note: PgFileWrite is intentionally NOT Send/Sync.
// PostgreSQL's VFD system is thread-local and bound to the current backend.
// Using VFD handles across threads is undefined behavior.

impl PgFileWrite {
    /// Open a file for writing using PostgreSQL's VFD system.
    ///
    /// Creates the file if it doesn't exist and truncates it if it does.
    /// WAL logging is disabled by default.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open or create
    ///
    /// # Returns
    /// A new `PgFileWrite` instance on success, or an error if the file cannot be opened.
    pub fn open(path: &str) -> io::Result<Self> {
        Self::open_with_wal(path, false)
    }

    /// Open a file for writing with optional Iceberg file WAL support.
    ///
    /// Creates the file if it doesn't exist and truncates it if it does.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open or create
    /// * `needs_wal` - Whether to log writes for standby/archive/PITR recovery
    ///
    /// # Returns
    /// A new `PgFileWrite` instance on success, or an error if the file cannot be opened.
    pub fn open_with_wal(path: &str, needs_wal: bool) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let file = unsafe {
            pg_sys::PathNameOpenFile(
                c_path.as_ptr(),
                libc::O_WRONLY
                    | libc::O_CREAT
                    | libc::O_TRUNC
                    | pg_sys::PG_BINARY as i32,
            )
        };
        if file < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}' for writing: {}", path, err),
            ));
        }

        Ok(Self {
            path: path.to_string(),
            file,
            position: 0,
            needs_wal,
        })
    }

    /// Check if Iceberg file WAL logging is enabled for this writer.
    #[inline]
    pub fn needs_wal(&self) -> bool {
        self.needs_wal
    }

    /// Enable or disable Iceberg file WAL logging for this writer.
    #[inline]
    pub fn set_needs_wal(&mut self, needs_wal: bool) {
        self.needs_wal = needs_wal;
    }
}

impl Write for PgFileWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Record the position before writing (needed for WAL)
        let write_position = self.position;

        // Step 1: Write to the file first
        // Following PostgreSQL's convention: write to file first, then WAL record.
        // This avoids issues if disk is full - we won't have orphaned WAL records.
        let result = unsafe {
            pg_sys::FileWrite(
                self.file,
                buf.as_ptr() as *const std::ffi::c_void,
                buf.len(),
                write_position as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE,
            )
        };

        if result < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to write to file '{}': {}", self.path, err),
            ));
        }

        let bytes_written = result as usize;
        self.position += bytes_written as i64;

        // Step 2: Log to WAL after successful write. The WAL record contains
        // the file path, offset, and data written. Standby/archive/PITR redo
        // can replay it to restore the file; local crash recovery skips
        // WRITE_FILE redo and relies on close-time FileSync.
        if self.needs_wal && bytes_written > 0 {
            log_write_file(&self.path, write_position, &buf[..bytes_written]);

            // Injection point for WAL/recovery testing.
            if crate::gucs::injection_point_matches("panic_after_wal_write") {
                diag::report_panic(
                    pgrx::PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    "iceberg: injection point panic_after_wal_write triggered",
                );
            }
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        // SAFETY: file is valid
        let ret = unsafe {
            pg_sys::FileSync(
                self.file,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_SYNC,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to sync file '{}': {}", self.path, err),
            ));
        }

        Ok(())
    }
}

impl FileWrite for PgFileWrite {
    fn close(&mut self) -> Result<()> {
        self.flush()?;
        // Drop only closes the VFD. Keeping FileSync in explicit close lets
        // callers observe and propagate durability errors.
        Ok(())
    }
}

impl Drop for PgFileWrite {
    fn drop(&mut self) {
        // SAFETY: file is valid and FileClose handles cleanup properly.
        // Do not call close() here: fsync failures in Drop could only be logged
        // and would be easy to mistake for a successful write.
        unsafe {
            pg_sys::FileClose(self.file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_matches_default_wal_policy() {
        assert!(!LocalStorage::new().needs_wal());
        assert!(!LocalStorage::default().needs_wal());
    }

    #[test]
    fn with_wal_is_the_explicit_wal_opt_in() {
        assert!(LocalStorage::with_wal(true).needs_wal());
        assert!(!LocalStorage::with_wal(false).needs_wal());
    }
}
