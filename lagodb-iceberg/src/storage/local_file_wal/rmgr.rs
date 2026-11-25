use std::cell::RefCell;
use std::collections::HashSet;
use std::ffi::CString;
use std::fmt::Write as _;
use std::path::Path;
use std::{fs, io};

use lagodb_core::backend::OwnedVfd;
use lagodb_core::wal::{RmgrId, WalRecord, WalResourceManager, WalRmgrError};
use lagodb_core::{diag, wal};
use pgrx::pg_sys;

use super::record::{
    DeleteFilesHeader, DirectoryHeader, IcebergWalOp, WriteFileHeader,
};

// Paths whose WRITE_FILE replay has entered lossy mode because recovery saw a
// later chunk (offset > 0) but the base file was missing. This is intentionally
// not called "invalid": under the local Iceberg WAL contract, missing files are
// an availability-first replay outcome that will surface later as unreadable
// Iceberg data if committed metadata references them.
thread_local! {
    static LOSSY_SKIPPED_WRITE_PATHS: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
}

/// Iceberg WAL resource manager ID
///
/// Custom resource manager IDs must be >= 128. We use 128 as our base.
pub(super) const ICEBERG_RMGR_ID_U8: u8 = 128;
pub(super) const ICEBERG_RMGR_ID: RmgrId = RmgrId::new(ICEBERG_RMGR_ID_U8);

/// Iceberg WAL Resource Manager implementation
///
/// This resource manager reconstructs local Iceberg files during standby WAL
/// replay or archive recovery by replaying file system operations recorded in
/// the WAL.
///
/// Invariants:
/// - Local crash-only recovery does not replay `WRITE_FILE` records. It requires
///   the write path to make files and publishing directory entries durable
///   before commit; see the module's known design debt.
/// - Standby WAL replay or archive recovery replays `WRITE_FILE` records on a
///   best-effort basis because the target system may not have local Iceberg
///   files. Missing base files are skipped in lossy mode rather than aborting
///   PostgreSQL recovery.
/// - Directory and canceled-file deletes are post-commit cleanup records.
///   PostgreSQL does not let an extension attach arbitrary paths to core
///   transaction commit/abort records, and PostgreSQL's `smgr` is not
///   extension-customizable, so delete WAL is emitted only after commit is
///   known to have succeeded.
/// - In-place truncation clears only a disposable transaction-local generation.
///   Its separate record is skipped during primary crash recovery; replaying
///   it there would delete the already-synced replacement table.
/// - Distributed storage (S3, GCS, Azure) doesn't need WAL-based redo because:
///   1. The storage layer guarantees durability after successful write
///   2. Orphaned files should be cleaned via garbage collection
///      (e.g., remove_orphan_files)
pub(super) struct IcebergRmgr;

impl WalResourceManager for IcebergRmgr {
    fn rmgr_id(&self) -> RmgrId {
        ICEBERG_RMGR_ID
    }

    fn name(&self) -> &'static str {
        "iceberg"
    }

    fn redo(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let op = IcebergWalOp::from_info(record.info()).ok_or_else(|| {
            WalRmgrError::InvalidRecord(format!(
                "Unknown Iceberg WAL op: {:#04x}",
                record.info()
            ))
        })?;

        diag::log_debug1(format_args!(
            "Iceberg WAL redo: {} at LSN {}",
            op.name(),
            record.lsn()
        ));

        match op {
            IcebergWalOp::DeleteDirectory => self.redo_remove_directory(record),
            IcebergWalOp::WriteFile => self.redo_write_file(record),
            IcebergWalOp::DeleteFiles => self.redo_delete_files(record),
            IcebergWalOp::TruncateDirectory => {
                // Replaying a historical in-place truncate on the primary
                // would erase the new files that close has already synced.
                if wal::is_crash_recovery_only() {
                    Ok(())
                } else {
                    self.redo_remove_directory(record)
                }
            }
        }
    }

    fn desc(&self, record: &WalRecord, buf: &mut String) {
        let Some(op) = IcebergWalOp::from_info(record.info()) else {
            let _ = write!(buf, "iceberg UNKNOWN");
            return;
        };
        let _ = write!(buf, "iceberg {}", op.name());

        let Some(data) = record.main_data() else {
            return;
        };
        match op {
            IcebergWalOp::DeleteDirectory | IcebergWalOp::TruncateDirectory => {
                if let Some(path) = DirectoryHeader::decode_path(data) {
                    let _ = write!(buf, " path={path}");
                }
            }
            IcebergWalOp::WriteFile => {
                if let Some(decoded) = WriteFileHeader::decode(data) {
                    let _ = write!(
                        buf,
                        " path={} offset={} len={}",
                        decoded.path,
                        decoded.offset,
                        decoded.file_data.len()
                    );
                }
            }
            IcebergWalOp::DeleteFiles => {
                if let Some(paths) = DeleteFilesHeader::decode_paths(data) {
                    let _ = write!(buf, " paths={}", paths.len());
                }
            }
        }
    }

    fn identify(&self, info: u8) -> Option<&'static str> {
        IcebergWalOp::from_info(info).map(|op| op.name())
    }

    fn startup(&self) -> Result<(), WalRmgrError> {
        diag::log_debug1("Iceberg WAL resource manager starting up");
        Ok(())
    }

    fn cleanup(&self) -> Result<(), WalRmgrError> {
        diag::log_debug1("Iceberg WAL resource manager cleaning up");
        Self::clear_lossy_skipped_write_paths();
        Ok(())
    }
}

impl IcebergRmgr {
    fn mark_lossy_skipped_write_path(path: &str) -> bool {
        LOSSY_SKIPPED_WRITE_PATHS
            .with_borrow_mut(|paths| paths.insert(path.to_owned()))
    }

    fn is_lossy_skipped_write_path(path: &str) -> bool {
        LOSSY_SKIPPED_WRITE_PATHS.with_borrow(|paths| paths.contains(path))
    }

    fn unmark_lossy_skipped_write_path(path: &str) {
        LOSSY_SKIPPED_WRITE_PATHS.with_borrow_mut(|paths| {
            paths.remove(path);
        });
    }

    fn clear_lossy_skipped_write_paths() {
        LOSSY_SKIPPED_WRITE_PATHS.with_borrow_mut(HashSet::clear);
    }

    /// Redo directory retirement or transaction-local truncation.
    ///
    /// Directory deletion is cleanup, not file reconstruction. In lossy replay
    /// mode failures are reported but do not stop PostgreSQL recovery.
    fn redo_remove_directory(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        let path = DirectoryHeader::decode_path(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Failed to extract directory path".into())
        })?;

        diag::log_debug1(format_args!("Iceberg directory removal redo: path={path}"));

        let path_ref = Path::new(path);
        let delete_result = match fs::metadata(path_ref) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path_ref),
            Ok(_) => fs::remove_file(path_ref),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                diag::log_debug1(format_args!("Directory does not exist: {path}"));
                return Ok(());
            }
            Err(e) => {
                diag::report_warning(format_args!(
                    "Failed to stat directory during redo: {path} - {e}"
                ));
                return Ok(());
            }
        };

        match delete_result {
            Ok(()) => {
                diag::log_debug1(format_args!("Deleted path during redo: {path}"))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                diag::log_debug1(format_args!(
                    "Directory disappeared during redo: {path}"
                ));
            }
            Err(e) => {
                diag::report_warning(format_args!(
                    "Failed to delete directory during lossy redo: {path} - {e}"
                ));
            }
        }

        Ok(())
    }

    /// Redo post-commit cleanup of transaction-created files canceled by the
    /// final Iceberg metadata commit. Missing files and unlink failures are
    /// cleanup outcomes, not recovery-fatal table corruption.
    fn redo_delete_files(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;
        let paths = DeleteFilesHeader::decode_paths(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Invalid DELETE_FILES record".into())
        })?;

        for path in paths {
            Self::unmark_lossy_skipped_write_path(path);
            match fs::remove_file(path) {
                Ok(()) => {
                    diag::log_debug1(format_args!(
                        "Deleted canceled Iceberg file during redo: {path}"
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    diag::report_warning(format_args!(
                        "Failed to delete canceled Iceberg file during lossy redo: {path} - {error}"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Redo a WRITE_FILE operation for standby WAL replay or archive recovery.
    ///
    /// Writes data to the file at the specified offset. If offset is 0,
    /// the file is created (or truncated if it exists), and any missing
    /// parent directories are created automatically.
    fn redo_write_file(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        if wal::is_crash_recovery_only() {
            return Ok(());
        }

        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        let decoded = WriteFileHeader::decode(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Invalid WRITE_FILE record".into())
        })?;
        let path = decoded.path;
        let offset = decoded.offset;
        let file_data = decoded.file_data;

        diag::log_debug1(format_args!(
            "Iceberg WRITE_FILE redo: path={path}, offset={offset}, data_len={}",
            file_data.len()
        ));

        if offset > 0 && Self::is_lossy_skipped_write_path(path) {
            diag::log_debug1(format_args!(
                "Skipping WRITE_FILE chunk during lossy redo: path={path}, offset={offset}"
            ));
            return Ok(());
        }

        let file_path = Path::new(path);
        if let Some(parent) = file_path.parent()
            && !parent.exists()
        {
            diag::log_debug1(format_args!(
                "Creating parent directories: {}",
                parent.display()
            ));
            fs::create_dir_all(parent).map_err(|e| {
                WalRmgrError::RedoFailed(format!(
                    "Failed to create parent directories during redo: {} - {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let c_path = CString::new(path.as_bytes()).map_err(|e| {
            WalRmgrError::InvalidRecord(format!("Invalid path string: {e}"))
        })?;

        let mut flags = libc::O_RDWR | pg_sys::PG_BINARY as i32;
        if offset == 0 {
            flags |= libc::O_CREAT | libc::O_TRUNC;
        }

        let Some(vfd) = OwnedVfd::open(&c_path, flags) else {
            let err = io::Error::last_os_error();
            if offset > 0 && err.kind() == io::ErrorKind::NotFound {
                let first_skip = Self::mark_lossy_skipped_write_path(path);
                if first_skip {
                    diag::report_warning(format_args!(
                        "Skipping local Iceberg file during lossy WAL replay because \
                         base file is missing: path={path}, offset={offset}"
                    ));
                } else {
                    diag::log_debug1(format_args!(
                        "Skipping WRITE_FILE chunk for missing local Iceberg file: \
                         path={path}, offset={offset}"
                    ));
                }
                return Ok(());
            }
            return Err(WalRmgrError::RedoFailed(format!(
                "Failed to open file during redo (offset={offset}): {path} - {err}"
            )));
        };

        if offset == 0 {
            Self::unmark_lossy_skipped_write_path(path);
        }

        if !file_data.is_empty() {
            let bytes_written = vfd.write_at(
                file_data,
                offset as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_COPY_FILE_WRITE,
            );

            if bytes_written < 0 || bytes_written as usize != file_data.len() {
                let err = io::Error::last_os_error();
                return Err(WalRmgrError::RedoFailed(format!(
                    "Failed to write {} bytes to file (written={bytes_written}): {path} - {err}",
                    file_data.len(),
                )));
            }
        }

        diag::log_debug1(format_args!(
            "Wrote {} bytes to file: {path} at offset {offset}",
            file_data.len(),
        ));

        Ok(())
    }
}
