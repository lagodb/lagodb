use super::record::{
    DeleteDirectoryHeader, DeleteFileHeader, IcebergWalOp, SIZE_OF_DELETE_DIRECTORY,
    SIZE_OF_DELETE_FILE, SIZE_OF_WRITE_FILE, WriteFileHeader,
};
use pg_lakebase_core::wal::{RmgrId, WalRecord, WalResourceManager, WalRmgrError};
use pg_lakebase_core::{diag, wal};

use pgrx::pg_sys;

use std::collections::HashSet;
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

// Global state for invalid paths
static INVALID_PATHS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Iceberg WAL Resource Manager ID
///
/// Custom resource manager IDs must be >= 128. We use 128 as our base.
pub const ICEBERG_RMGR_ID_U8: u8 = 128;
pub const ICEBERG_RMGR_ID: RmgrId = RmgrId::new(ICEBERG_RMGR_ID_U8);

/// Iceberg WAL Resource Manager implementation
///
/// This resource manager handles crash recovery for Iceberg table operations
/// by replaying file system operations recorded in the WAL.
///
/// Note: This only handles local file system operations. Distributed storage
/// (S3, GCS, Azure) doesn't need WAL-based redo because:
/// 1. The storage layer guarantees durability after successful write
/// 2. Orphaned files should be cleaned via garbage collection (e.g., expire_snapshots)
pub struct IcebergRmgr;

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

        diag::log_debug1(&format!(
            "Iceberg WAL redo: {} at LSN {}",
            op.name(),
            record.lsn()
        ));

        match op {
            IcebergWalOp::DeleteDirectory => self.redo_delete_directory(record),
            IcebergWalOp::WriteFile => self.redo_write_file(record),
            IcebergWalOp::DeleteFile => self.redo_delete_file(record),
        }
    }

    fn desc(&self, record: &WalRecord, buf: &mut String) {
        if let Some(op) = IcebergWalOp::from_info(record.info()) {
            let _ = std::fmt::write(buf, format_args!("iceberg {}", op.name()));

            // Try to extract and display the path from the record
            if let Some(data) = record.main_data() {
                match op {
                    IcebergWalOp::DeleteDirectory => {
                        if let Some(path) = self.extract_delete_directory_path(data) {
                            let _ =
                                std::fmt::write(buf, format_args!(" path={}", path));
                        }
                    }
                    IcebergWalOp::WriteFile => {
                        if let Some((path, offset, data_len)) =
                            self.extract_write_file_info(data)
                        {
                            let _ = std::fmt::write(
                                buf,
                                format_args!(
                                    " path={} offset={} len={}",
                                    path, offset, data_len
                                ),
                            );
                        }
                    }
                    IcebergWalOp::DeleteFile => {
                        if let Some(path) = self.extract_delete_file_path(data) {
                            let _ =
                                std::fmt::write(buf, format_args!(" path={}", path));
                        }
                    }
                }
            }
        } else {
            let _ = std::fmt::write(buf, format_args!("iceberg UNKNOWN"));
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
        Self::check_consistency();
        Ok(())
    }
}

impl IcebergRmgr {
    fn get_invalid_paths() -> &'static Mutex<HashSet<String>> {
        INVALID_PATHS.get_or_init(|| Mutex::new(HashSet::new()))
    }

    fn log_invalid_path(path: String) {
        let mut paths = Self::get_invalid_paths().lock().unwrap();
        if paths.insert(path.clone()) {
            diag::log_debug1(&format!("Marking path as invalid: {}", path));
        }
    }

    fn is_path_usable(path: &str) -> bool {
        let paths = Self::get_invalid_paths().lock().unwrap();
        !paths.contains(path)
    }

    fn check_consistency() {
        if let Some(mutex) = INVALID_PATHS.get() {
            let mut paths = mutex.lock().unwrap();
            if !paths.is_empty() {
                for p in paths.iter() {
                    diag::report_warning(&format!(
                        "Iceberg WAL invalid path detected: {}",
                        p
                    ));
                }
                diag::report_warning(
                    "Iceberg WAL contains references to invalid files",
                );
                paths.clear();
            }
        }
    }

    // ========================================================================
    // Redo Functions (Local Storage Only)
    // ========================================================================

    /// Redo a DELETE_DIRECTORY operation
    ///
    /// Removes the directory and all its contents. If the directory doesn't
    /// exist, this is a no-op (idempotent).
    fn redo_delete_directory(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        let path = self.extract_delete_directory_path(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Failed to extract directory path".into())
        })?;

        diag::log_debug1(&format!("Iceberg DELETE_DIRECTORY redo: path={}", path));

        let dir_path = Path::new(&path);
        if dir_path.exists() {
            if let Err(e) = fs::remove_dir_all(dir_path) {
                diag::report_warning(&format!(
                    "Failed to delete directory during redo: {} - {}",
                    path, e
                ));
            } else {
                diag::log_debug1(&format!("Deleted directory: {}", path));
            }
        } else {
            diag::log_debug1(&format!("Directory does not exist: {}", path));
        }

        Ok(())
    }

    /// Redo a WRITE_FILE operation
    ///
    /// Writes data to the file at the specified offset. If offset is 0,
    /// the file is created (or truncated if it exists), and any missing
    /// parent directories are created automatically.
    fn redo_write_file(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        // Do not perform redo for crash recovery mode. We do not need to
        // replay Iceberg file write operations in this case because fsync
        // (FileSync) is performed on file close (see PgFileWrite::close).
        //
        // This is safe because:
        // 1. If the transaction committed, the file is already synced to disk.
        // 2. If the transaction aborted, we don't care about the file state.
        //
        // Note: Archive recovery (standby/PITR) still needs redo as files may
        // not exist on the target system.
        if wal::is_crash_recovery_only() {
            return Ok(());
        }

        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        // Use helper to extract info (handles parsing and validation)
        let (path, offset, data_len) =
            self.extract_write_file_info(data).ok_or_else(|| {
                WalRmgrError::InvalidRecord("Invalid WRITE_FILE record".into())
            })?;

        // Extract the file data slice
        // data structure: [header][path][file_data]
        // data_len is the length of file_data
        let file_data = &data[data.len() - data_len..];

        diag::log_debug1(&format!(
            "Iceberg WRITE_FILE redo: path={}, offset={}, data_len={}",
            path,
            offset,
            file_data.len()
        ));

        // Block subsequent operations if path is invalid
        if !Self::is_path_usable(&path) {
            diag::log_debug1(&format!("Skipping write to invalid path: {}", path));
            return Ok(());
        }

        let file_path = Path::new(&path);
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                diag::log_debug1(&format!(
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
        }

        let c_path = CString::new(path.clone()).map_err(|e| {
            WalRmgrError::InvalidRecord(format!("Invalid path string: {}", e))
        })?;

        let mut flags = libc::O_RDWR | pg_sys::PG_BINARY as i32;
        if offset == 0 {
            flags |= libc::O_CREAT | libc::O_TRUNC;
        }

        let file = unsafe { pg_sys::PathNameOpenFile(c_path.as_ptr(), flags) };
        if file < 0 {
            let err = std::io::Error::last_os_error();
            // Handle ENOENT specifically: mark file as invalid
            if err.kind() == std::io::ErrorKind::NotFound {
                diag::log_debug1(&format!("File does not exist: {}", path));
                Self::log_invalid_path(path);
                return Ok(());
            }
            return Err(WalRmgrError::RedoFailed(format!(
                "Failed to open file during redo (offset={}): {} - {}",
                offset, path, err
            )));
        }

        // Ensure file is closed when we drop this guard
        struct FileGuard(pg_sys::File);
        impl Drop for FileGuard {
            fn drop(&mut self) {
                unsafe { pg_sys::FileClose(self.0) };
            }
        }
        let _guard = FileGuard(file);

        if !file_data.is_empty() {
            let bytes_written = unsafe {
                pg_sys::FileWrite(
                    file,
                    file_data.as_ptr() as *const std::ffi::c_void,
                    file_data.len(),
                    offset as pg_sys::off_t,
                    pg_sys::WaitEventIO::WAIT_EVENT_COPY_FILE_WRITE as u32,
                )
            };

            // Check if all bytes were written correctly
            if bytes_written < 0 || bytes_written as usize != file_data.len() {
                let err = std::io::Error::last_os_error();
                return Err(WalRmgrError::RedoFailed(format!(
                    "Failed to write {} bytes to file (written={}): {} - {}",
                    file_data.len(),
                    bytes_written,
                    path,
                    err
                )));
            }
        }

        diag::log_debug1(&format!(
            "Wrote {} bytes to file: {} at offset {}",
            file_data.len(),
            path,
            offset
        ));

        Ok(())
    }

    /// Redo a DELETE_FILE operation
    ///
    /// Removes the file. If the file doesn't exist, this is a no-op (idempotent).
    fn redo_delete_file(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        let path = self.extract_delete_file_path(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Failed to extract file path".into())
        })?;

        diag::log_debug1(&format!("Iceberg DELETE_FILE redo: path={}", path));

        let file_path = Path::new(&path);
        if file_path.exists() {
            if let Err(e) = fs::remove_file(file_path) {
                diag::report_warning(&format!(
                    "Failed to delete file during redo: {} - {}",
                    path, e
                ));
            } else {
                diag::log_debug1(&format!("Deleted file: {}", path));
            }
        } else {
            diag::log_debug1(&format!("File does not exist: {}", path));
        }

        Ok(())
    }

    // ========================================================================
    // Helper Functions for Parsing WAL Records
    // ========================================================================

    /// Extract directory path from DeleteDirectoryHeader + data
    fn extract_delete_directory_path(&self, data: &[u8]) -> Option<String> {
        if data.len() < SIZE_OF_DELETE_DIRECTORY {
            return None;
        }

        // Safe unaligned read of the header
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const DeleteDirectoryHeader)
        };

        let path_start = SIZE_OF_DELETE_DIRECTORY;
        let path_end = path_start + header.path_len as usize;

        if data.len() < path_end {
            return None;
        }

        let path_bytes = &data[path_start..path_end];
        std::str::from_utf8(path_bytes).ok().map(|s| s.to_string())
    }

    /// Extract file path from DeleteFileHeader + data
    fn extract_delete_file_path(&self, data: &[u8]) -> Option<String> {
        if data.len() < SIZE_OF_DELETE_FILE {
            return None;
        }

        // Safe unaligned read of the header
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const DeleteFileHeader)
        };

        let path_start = SIZE_OF_DELETE_FILE;
        let path_end = path_start + header.path_len as usize;

        if data.len() < path_end {
            return None;
        }

        let path_bytes = &data[path_start..path_end];
        std::str::from_utf8(path_bytes).ok().map(|s| s.to_string())
    }

    /// Extract file info from WriteFileHeader + path bytes + data
    fn extract_write_file_info(&self, data: &[u8]) -> Option<(String, i64, usize)> {
        if data.len() < SIZE_OF_WRITE_FILE {
            return None;
        }

        // Safe unaligned read of the header
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const WriteFileHeader)
        };

        let path_start = SIZE_OF_WRITE_FILE;
        let path_end = path_start + header.path_len as usize;

        if data.len() < path_end {
            return None;
        }

        let path_bytes = &data[path_start..path_end];
        let path = std::str::from_utf8(path_bytes).ok()?;

        let data_len = data.len() - path_end;

        Some((path.to_string(), header.offset, data_len))
    }
}
