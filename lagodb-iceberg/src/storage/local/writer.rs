//! PostgreSQL VFD-backed file writer.

use std::ffi::CString;
use std::io::{self, Write};

use iceberg_lite::Result;
use iceberg_lite::io::{FileMetadata, FileWrite};
use lagodb_core::backend::OwnedVfd;
use pgrx::pg_sys;

use crate::storage::local_file_wal::log_write_file;

/// PostgreSQL VFD-backed file writer.
///
/// Wraps a PostgreSQL virtual file descriptor for writing operations. The file
/// handle is owned by [`OwnedVfd`] and closed by its `Drop` implementation
/// without calling `FileSync` (fsync errors must be observed explicitly via
/// [`FileWrite::close`]). The surrounding mutation state is responsible for
/// dropping the writer during normal completion and PostgreSQL error unwinding.
///
/// # WAL Support
///
/// When `needs_wal` is true, the writer logs local Iceberg file writes to
/// PostgreSQL WAL for standby WAL replay or archive recovery. Following
/// PostgreSQL's convention, the write is performed first, and then the WAL
/// record is written. This order ensures that:
/// - If the write fails (e.g., disk full), no WAL record is created
/// - During standby WAL replay or archive recovery, the WAL record can be
///   replayed to restore the file
///
/// Local crash recovery does not replay `WRITE_FILE` records; successful
/// explicit close calls `FileSync`. Directory entries are not synced; see the
/// module's local durability limitation. Distributed storage provides its own
/// durability guarantees and does not use this WAL path.
pub(super) struct LocalFileWriter {
    /// Path to the file (for error reporting and WAL logging)
    path: Box<str>,
    /// PostgreSQL virtual file descriptor owner
    vfd: OwnedVfd,
    /// Current write position in the file
    position: i64,
    /// Whether Iceberg file WAL logging is enabled for this writer.
    needs_wal: bool,
}

// SAFETY: this private adapter implements iceberg-lite `FileWrite`, which
// requires `Send + Sync`. The contained `OwnedVfd` remains intentionally
// `!Send + !Sync` so `Send + Sync` is never an accidental compiler-derived
// property; all VFD and WAL operations stay in the owning backend's serial
// mutation lifecycle through close and `Drop`.
unsafe impl Send for LocalFileWriter {}
// SAFETY: no extension path shares or writes one instance concurrently; this
// capability is present only for the upstream trait bound.
unsafe impl Sync for LocalFileWriter {}

impl LocalFileWriter {
    /// Open a file for writing with optional Iceberg file WAL support.
    ///
    /// Creates the file if it doesn't exist and truncates it if it does.
    pub(super) fn open_with_wal(path: &str, needs_wal: bool) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let flags =
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC | pg_sys::PG_BINARY as i32;
        let Some(vfd) = OwnedVfd::open(&c_path, flags) else {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}' for writing: {}", path, err),
            ));
        };

        Ok(Self {
            path: path.into(),
            vfd,
            position: 0,
            needs_wal,
        })
    }
}

impl Write for LocalFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // Record the position before writing (needed for WAL)
        let write_position = self.position;

        // Step 1: Write to the file first.
        // Following PostgreSQL's convention: write to file first, then WAL record.
        // `FileWriteV` sets `errno = ENOSPC` on short writes (including 0).
        let result = self.vfd.write_at(
            buf,
            write_position as pg_sys::off_t,
            pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE,
        );

        if result <= 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to write to file '{}': {}", self.path, err),
            ));
        }

        let bytes_written = result as usize;
        self.position += bytes_written as i64;

        // Step 2: Log to WAL after successful write. The WAL record contains
        // the file path, offset, and data written. Standby WAL replay or
        // archive recovery can replay it to restore the file; local crash
        // recovery skips WRITE_FILE redo and relies on close-time FileSync.
        if self.needs_wal {
            log_write_file(&self.path, write_position, &buf[..bytes_written]);
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        let ret = self
            .vfd
            .sync(pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_SYNC);

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

impl FileWrite for LocalFileWriter {
    fn close(&mut self) -> Result<FileMetadata> {
        self.flush()?;
        // Drop only closes the VFD. Keeping FileSync in explicit close lets
        // callers observe and propagate durability errors.
        Ok(FileMetadata {
            size: self.position as u64,
        })
    }
}
