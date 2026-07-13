use super::record::{
    DeleteDirectoryHeader, DeleteFilesHeader, IcebergWalOp,
    MAX_DELETE_FILES_PAYLOAD_BYTES, MAX_DELETE_FILES_PER_RECORD,
    SIZE_OF_DELETE_DIRECTORY, SIZE_OF_DELETE_FILES, SIZE_OF_WRITE_FILE,
    WriteFileHeader,
};
use pg_lakebase_core::wal::{RmgrId, WalRecord, WalResourceManager, WalRmgrError};
use pg_lakebase_core::{diag, wal};

use pgrx::pg_sys;

use std::collections::HashSet;
use std::ffi::CString;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock};

// Paths whose WRITE_FILE replay has entered lossy mode because recovery saw a
// later chunk (offset > 0) but the base file was missing. This is intentionally
// not called "invalid": under the local Iceberg WAL contract, missing files are
// an availability-first replay outcome that will surface later as unreadable
// Iceberg data if committed metadata references them.
static LOSSY_SKIPPED_WRITE_PATHS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Iceberg WAL Resource Manager ID
///
/// Custom resource manager IDs must be >= 128. We use 128 as our base.
pub const ICEBERG_RMGR_ID_U8: u8 = 128;
pub const ICEBERG_RMGR_ID: RmgrId = RmgrId::new(ICEBERG_RMGR_ID_U8);

/// Iceberg WAL Resource Manager implementation
///
/// This resource manager reconstructs local Iceberg files during standby WAL
/// replay or archive recovery by replaying file system operations recorded in
/// the WAL.
///
/// Invariants:
/// - Local crash-only recovery does not replay `WRITE_FILE` records. The normal
///   write path performs `FileSync` during explicit writer close, so committed
///   local files are already durable on the primary.
/// - Standby WAL replay or archive recovery replays `WRITE_FILE` records on a
///   best-effort basis because the target system may not have local Iceberg
///   files. Missing base files are skipped in lossy mode rather than aborting
///   PostgreSQL recovery.
/// - Directory and canceled-file deletes are post-commit cleanup records.
///   PostgreSQL does not let an extension attach arbitrary paths to core
///   transaction commit/abort records, and PG17 `smgr` is not
///   extension-customizable, so delete WAL is emitted only after commit is
///   known to have succeeded.
/// - Distributed storage (S3, GCS, Azure) doesn't need WAL-based redo because:
/// 1. The storage layer guarantees durability after successful write
/// 2. Orphaned files should be cleaned via garbage collection
///    (e.g., remove_orphan_files)
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

        diag::log_debug1(format_args!(
            "Iceberg WAL redo: {} at LSN {}",
            op.name(),
            record.lsn()
        ));

        match op {
            IcebergWalOp::DeleteDirectory => self.redo_delete_directory(record),
            IcebergWalOp::WriteFile => self.redo_write_file(record),
            IcebergWalOp::DeleteFiles => self.redo_delete_files(record),
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
                    IcebergWalOp::DeleteFiles => {
                        if let Some(paths) = self.extract_delete_file_paths(data) {
                            let _ = std::fmt::write(
                                buf,
                                format_args!(" paths={}", paths.len()),
                            );
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
        Self::clear_lossy_skipped_write_paths();
        Ok(())
    }
}

impl IcebergRmgr {
    fn lossy_skipped_write_paths() -> &'static Mutex<HashSet<String>> {
        LOSSY_SKIPPED_WRITE_PATHS.get_or_init(|| Mutex::new(HashSet::new()))
    }

    fn lock_lossy_skipped_write_paths() -> MutexGuard<'static, HashSet<String>> {
        Self::lossy_skipped_write_paths()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn mark_lossy_skipped_write_path(path: &str) -> bool {
        Self::lock_lossy_skipped_write_paths().insert(path.to_string())
    }

    fn is_lossy_skipped_write_path(path: &str) -> bool {
        Self::lock_lossy_skipped_write_paths().contains(path)
    }

    fn unmark_lossy_skipped_write_path(path: &str) {
        Self::lock_lossy_skipped_write_paths().remove(path);
    }

    fn clear_lossy_skipped_write_paths() {
        if let Some(paths) = LOSSY_SKIPPED_WRITE_PATHS.get() {
            paths
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
        }
    }

    // ========================================================================
    // Redo Functions (Local Storage Only)
    // ========================================================================

    /// Redo a DELETE_DIRECTORY operation.
    ///
    /// Directory deletion is cleanup, not file reconstruction. In lossy replay
    /// mode failures are reported but do not stop PostgreSQL recovery.
    fn redo_delete_directory(&self, record: &WalRecord) -> Result<(), WalRmgrError> {
        let data = record
            .main_data()
            .ok_or_else(|| WalRmgrError::InvalidRecord("Missing main data".into()))?;

        let path = self.extract_delete_directory_path(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Failed to extract directory path".into())
        })?;

        diag::log_debug1(format_args!(
            "Iceberg DELETE_DIRECTORY redo: path={}",
            path
        ));

        let path_ref = Path::new(&path);
        let delete_result = match fs::metadata(path_ref) {
            Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path_ref),
            Ok(_) => fs::remove_file(path_ref),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                diag::log_debug1(format_args!("Directory does not exist: {}", path));
                return Ok(());
            }
            Err(e) => {
                diag::report_warning(format_args!(
                    "Failed to stat directory during redo: {} - {}",
                    path, e
                ));
                return Ok(());
            }
        };

        match delete_result {
            Ok(()) => {
                diag::log_debug1(format_args!("Deleted path during redo: {}", path))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                diag::log_debug1(format_args!(
                    "Directory disappeared during redo: {}",
                    path
                ));
            }
            Err(e) => {
                diag::report_warning(format_args!(
                    "Failed to delete directory during lossy redo: {} - {}",
                    path, e
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
        let paths = self.extract_delete_file_paths(data).ok_or_else(|| {
            WalRmgrError::InvalidRecord("Invalid DELETE_FILES record".into())
        })?;

        for path in paths {
            Self::unmark_lossy_skipped_write_path(&path);
            match fs::remove_file(&path) {
                Ok(()) => {
                    diag::log_debug1(format_args!(
                        "Deleted canceled Iceberg file during redo: {}",
                        path
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    diag::report_warning(format_args!(
                        "Failed to delete canceled Iceberg file during lossy redo: {} - {}",
                        path, error
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
        // Do not replay WRITE_FILE records during local crash-only recovery:
        // fsync (FileSync) is performed by the explicit writer close path
        // before a successful file write is reported.
        //
        // This is safe because:
        // 1. If the transaction committed, the file is already synced to disk.
        // 2. If the transaction aborted, we don't care about the file state.
        //
        // Standby WAL replay or archive recovery still needs redo because the
        // target system may not have the local Iceberg files. This is
        // availability-first lossy replay: if recovery starts from a point where
        // the base file is missing, later chunks for that path are skipped and
        // any committed metadata reference will fail at read time.
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

        diag::log_debug1(format_args!(
            "Iceberg WRITE_FILE redo: path={}, offset={}, data_len={}",
            path,
            offset,
            file_data.len()
        ));

        if offset > 0 && Self::is_lossy_skipped_write_path(&path) {
            diag::log_debug1(format_args!(
                "Skipping WRITE_FILE chunk during lossy redo: path={}, offset={}",
                path, offset
            ));
            return Ok(());
        }

        let file_path = Path::new(&path);
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
            WalRmgrError::InvalidRecord(format!("Invalid path string: {}", e))
        })?;

        let mut flags = libc::O_RDWR | pg_sys::PG_BINARY as i32;
        if offset == 0 {
            flags |= libc::O_CREAT | libc::O_TRUNC;
        }

        let file = unsafe { pg_sys::PathNameOpenFile(c_path.as_ptr(), flags) };
        if file < 0 {
            let err = std::io::Error::last_os_error();
            if offset > 0 && err.kind() == std::io::ErrorKind::NotFound {
                let first_skip = Self::mark_lossy_skipped_write_path(&path);
                if first_skip {
                    diag::report_warning(format_args!(
                        "Skipping local Iceberg file during lossy WAL replay because \
                         base file is missing: path={}, offset={}",
                        path, offset
                    ));
                } else {
                    diag::log_debug1(format_args!(
                        "Skipping WRITE_FILE chunk for missing local Iceberg file: \
                         path={}, offset={}",
                        path, offset
                    ));
                }
                return Ok(());
            }
            return Err(WalRmgrError::RedoFailed(format!(
                "Failed to open file during redo (offset={}): {} - {}",
                offset, path, err
            )));
        }
        if offset == 0 {
            Self::unmark_lossy_skipped_write_path(&path);
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
                    pg_sys::WaitEventIO::WAIT_EVENT_COPY_FILE_WRITE,
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

        diag::log_debug1(format_args!(
            "Wrote {} bytes to file: {} at offset {}",
            file_data.len(),
            path,
            offset
        ));

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

        // Safe unaligned read of the header. PostgreSQL WAL is trusted input for
        // this cluster, but record parsing still bounds-checks lengths so corrupt
        // records fail as InvalidRecord instead of reading past the byte slice.
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const DeleteDirectoryHeader)
        };

        let path_start = SIZE_OF_DELETE_DIRECTORY;
        let path_end = path_start.checked_add(header.path_len as usize)?;

        if data.len() < path_end {
            return None;
        }

        let path_bytes = &data[path_start..path_end];
        std::str::from_utf8(path_bytes).ok().map(|s| s.to_string())
    }

    fn extract_delete_file_paths(&self, data: &[u8]) -> Option<Vec<String>> {
        if data.len() < SIZE_OF_DELETE_FILES {
            return None;
        }
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const DeleteFilesHeader)
        };
        if header.path_count == 0
            || header.path_count as usize > MAX_DELETE_FILES_PER_RECORD
            || header.payload_len as usize > MAX_DELETE_FILES_PAYLOAD_BYTES
        {
            return None;
        }
        let payload_end =
            SIZE_OF_DELETE_FILES.checked_add(header.payload_len as usize)?;
        if data.len() != payload_end {
            return None;
        }

        let mut cursor = SIZE_OF_DELETE_FILES;
        let mut paths = Vec::with_capacity(header.path_count as usize);
        for _ in 0..header.path_count {
            let length_end = cursor.checked_add(std::mem::size_of::<u32>())?;
            let length_bytes: [u8; 4] =
                data.get(cursor..length_end)?.try_into().ok()?;
            let path_len = u32::from_ne_bytes(length_bytes) as usize;
            let path_end = length_end.checked_add(path_len)?;
            let path_bytes = data.get(length_end..path_end)?;
            if path_bytes.is_empty() || path_bytes.contains(&0) {
                return None;
            }
            paths.push(std::str::from_utf8(path_bytes).ok()?.to_owned());
            cursor = path_end;
        }
        (cursor == payload_end).then_some(paths)
    }

    /// Extract file info from WriteFileHeader + path bytes + data
    fn extract_write_file_info(&self, data: &[u8]) -> Option<(String, i64, usize)> {
        if data.len() < SIZE_OF_WRITE_FILE {
            return None;
        }

        // Safe unaligned read of the header. PostgreSQL WAL is trusted input for
        // this cluster, but record parsing still bounds-checks lengths so corrupt
        // records fail as InvalidRecord instead of reading past the byte slice.
        let header = unsafe {
            std::ptr::read_unaligned(data.as_ptr() as *const WriteFileHeader)
        };

        let path_start = SIZE_OF_WRITE_FILE;
        let path_end = path_start.checked_add(header.path_len as usize)?;

        if data.len() < path_end {
            return None;
        }

        let path_bytes = &data[path_start..path_end];
        let path = std::str::from_utf8(path_bytes).ok()?;

        let data_len = data.len() - path_end;

        Some((path.to_string(), header.offset, data_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes<T>(header: &T) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                header as *const T as *const u8,
                std::mem::size_of::<T>(),
            )
        }
    }

    fn delete_directory_record(path: &[u8], path_len: u32) -> Vec<u8> {
        let header = DeleteDirectoryHeader { path_len };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(path);
        data
    }

    fn write_file_record(path: &[u8], offset: i64, payload: &[u8]) -> Vec<u8> {
        let header = WriteFileHeader {
            path_len: path.len() as u32,
            _padding: 0,
            offset,
        };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(path);
        data.extend_from_slice(payload);
        data
    }

    fn delete_files_record(paths: &[&[u8]]) -> Vec<u8> {
        let mut payload = Vec::new();
        for path in paths {
            payload.extend_from_slice(&(path.len() as u32).to_ne_bytes());
            payload.extend_from_slice(path);
        }
        let header = DeleteFilesHeader {
            path_count: paths.len() as u32,
            payload_len: payload.len() as u32,
        };
        let mut data = Vec::new();
        data.extend_from_slice(header_bytes(&header));
        data.extend_from_slice(&payload);
        data
    }

    #[test]
    fn wal_op_from_info_masks_postgres_flags() {
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::DeleteDirectory as u8 | 0x0f),
            Some(IcebergWalOp::DeleteDirectory)
        );
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::WriteFile as u8 | 0x0f),
            Some(IcebergWalOp::WriteFile)
        );
        assert_eq!(
            IcebergWalOp::from_info(IcebergWalOp::DeleteFiles as u8 | 0x0f),
            Some(IcebergWalOp::DeleteFiles)
        );
        assert_eq!(IcebergWalOp::from_info(0x30), None);
    }

    #[test]
    fn extracts_delete_directory_path() {
        let rmgr = IcebergRmgr;
        let data = delete_directory_record(b"base/1/2_iceberg", 16);

        assert_eq!(
            rmgr.extract_delete_directory_path(&data),
            Some("base/1/2_iceberg".to_string())
        );
    }

    #[test]
    fn rejects_malformed_delete_directory_records() {
        let rmgr = IcebergRmgr;

        assert_eq!(rmgr.extract_delete_directory_path(&[]), None);
        assert_eq!(
            rmgr.extract_delete_directory_path(&delete_directory_record(b"abc", 4)),
            None
        );
        assert_eq!(
            rmgr.extract_delete_directory_path(&delete_directory_record(
                b"",
                u32::MAX
            )),
            None
        );
        assert_eq!(
            rmgr.extract_delete_directory_path(&delete_directory_record(&[0xff], 1)),
            None
        );
    }

    #[test]
    fn extracts_delete_file_paths() {
        let rmgr = IcebergRmgr;
        let data = delete_files_record(&[
            &b"base/1/data-a.parquet"[..],
            &b"base/1/delete-a.parquet"[..],
        ]);

        assert_eq!(
            rmgr.extract_delete_file_paths(&data),
            Some(vec![
                "base/1/data-a.parquet".to_owned(),
                "base/1/delete-a.parquet".to_owned(),
            ])
        );
    }

    #[test]
    fn rejects_malformed_delete_files_records() {
        let rmgr = IcebergRmgr;
        assert_eq!(rmgr.extract_delete_file_paths(&[]), None);

        let empty_header = DeleteFilesHeader {
            path_count: 0,
            payload_len: 0,
        };
        assert_eq!(
            rmgr.extract_delete_file_paths(header_bytes(&empty_header)),
            None
        );

        let too_many_paths = DeleteFilesHeader {
            path_count: (MAX_DELETE_FILES_PER_RECORD + 1) as u32,
            payload_len: 0,
        };
        assert_eq!(
            rmgr.extract_delete_file_paths(header_bytes(&too_many_paths)),
            None
        );

        let oversized_payload = DeleteFilesHeader {
            path_count: 1,
            payload_len: (MAX_DELETE_FILES_PAYLOAD_BYTES + 1) as u32,
        };
        assert_eq!(
            rmgr.extract_delete_file_paths(header_bytes(&oversized_payload)),
            None
        );

        let mut truncated = delete_files_record(&[&b"abc"[..]]);
        truncated.pop();
        assert_eq!(rmgr.extract_delete_file_paths(&truncated), None);

        let mut trailing = delete_files_record(&[&b"abc"[..]]);
        trailing.push(0);
        assert_eq!(rmgr.extract_delete_file_paths(&trailing), None);

        assert_eq!(
            rmgr.extract_delete_file_paths(&delete_files_record(&[&b"a\0b"[..]])),
            None
        );
        assert_eq!(
            rmgr.extract_delete_file_paths(&delete_files_record(&[&[0xff_u8][..]])),
            None
        );
    }

    #[test]
    fn extracts_write_file_info() {
        let rmgr = IcebergRmgr;
        let data = write_file_record(b"base/1/data.parquet", 128, b"payload");

        assert_eq!(
            rmgr.extract_write_file_info(&data),
            Some(("base/1/data.parquet".to_string(), 128, 7))
        );
    }

    #[test]
    fn rejects_malformed_write_file_records() {
        let rmgr = IcebergRmgr;

        assert_eq!(rmgr.extract_write_file_info(&[]), None);

        let mut short_path = write_file_record(b"abc", 0, b"");
        let header = WriteFileHeader {
            path_len: 4,
            _padding: 0,
            offset: 0,
        };
        short_path[..SIZE_OF_WRITE_FILE].copy_from_slice(header_bytes(&header));
        assert_eq!(rmgr.extract_write_file_info(&short_path), None);

        let long_path_header = WriteFileHeader {
            path_len: u32::MAX,
            _padding: 0,
            offset: 0,
        };
        let mut long_path = Vec::new();
        long_path.extend_from_slice(header_bytes(&long_path_header));
        assert_eq!(rmgr.extract_write_file_info(&long_path), None);

        assert_eq!(
            rmgr.extract_write_file_info(&write_file_record(&[0xff], 0, b"")),
            None
        );
    }
}
