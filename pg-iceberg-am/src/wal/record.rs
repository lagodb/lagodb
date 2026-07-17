use super::rmgr::ICEBERG_RMGR_ID;
use pg_lakebase_core::diag;
use pg_lakebase_core::wal::{WalRecordBuilder, XLogRecPtr};

// ============================================================================
// WAL Operation Types
// ============================================================================

/// WAL record operation types for Iceberg
///
/// This enum defines the file system operations that are logged
/// to the WAL for standby WAL replay or archive recovery of local Iceberg files:
/// - WriteFile: Write data to a file (creates file and parent directories if
///   offset is 0)
/// - DeleteDirectory: Remove a directory and its contents after the PostgreSQL
///   transaction has committed
/// - DeleteFiles: Remove transaction-created files canceled by the final commit
///
/// Invariants:
/// - These WAL records are only for local file systems. Distributed storage
///   (S3, GCS, Azure) guarantees durability after successful writes and does
///   not use WAL-based redo.
/// - `WRITE_FILE` redo is skipped during local crash recovery because
///   successful explicit writer close performs `FileSync`.
/// - Standby WAL replay or archive recovery uses these records for best-effort,
///   lossy reconstruction because local Iceberg files may not exist on the
///   target system.
/// - `DELETE_DIRECTORY` and `DELETE_FILES` are post-commit cleanup. PostgreSQL
///   extensions cannot add arbitrary AM paths to core commit/abort records, and
///   PostgreSQL's `smgr` switch is not extension-customizable, so we must never
///   log a delete record before the transaction outcome is known.
/// - Orphaned files on distributed storage should be cleaned up via a separate
///   garbage collection mechanism (e.g., Iceberg's remove_orphan_files).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergWalOp {
    /// Delete a directory (and all its contents)
    DeleteDirectory = 0x00,
    /// Write data to a file (creates file and parent directories if offset is 0)
    WriteFile = 0x10,
    /// Delete bounded batches of canceled transaction-created files after commit.
    DeleteFiles = 0x20,
}

impl IcebergWalOp {
    /// Parse operation type from WAL info byte
    ///
    /// The info byte contains the operation type in the high 4 bits.
    /// Returns None if the operation type is not recognized.
    pub fn from_info(info: u8) -> Option<Self> {
        // Mask off the high bits (flags) to get the operation type
        let op = info & 0xF0;
        match op {
            0x00 => Some(Self::DeleteDirectory),
            0x10 => Some(Self::WriteFile),
            0x20 => Some(Self::DeleteFiles),
            _ => None,
        }
    }

    /// Get the human-readable name of this operation
    pub fn name(&self) -> &'static str {
        match self {
            Self::DeleteDirectory => "DELETE_DIRECTORY",
            Self::WriteFile => "WRITE_FILE",
            Self::DeleteFiles => "DELETE_FILES",
        }
    }
}

// ============================================================================
// WAL Record Data Structures
// ============================================================================

/// WAL record header for DeleteDirectory operation
///
/// Layout in WAL record:
/// - DeleteDirectoryHeader (this struct)
/// - path bytes (path_len bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DeleteDirectoryHeader {
    /// Length of the directory path (not including null terminator)
    pub path_len: u32,
}

/// Size of DeleteDirectoryHeader in bytes
pub const SIZE_OF_DELETE_DIRECTORY: usize =
    std::mem::size_of::<DeleteDirectoryHeader>();

/// Header for one bounded batch of file paths.
///
/// The payload repeats `[path_len: u32][path bytes]` `path_count` times.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DeleteFilesHeader {
    pub path_count: u32,
    pub payload_len: u32,
}

pub const SIZE_OF_DELETE_FILES: usize = std::mem::size_of::<DeleteFilesHeader>();

/// Project batching policy, not PostgreSQL's WAL record limit. These bounds
/// keep one cleanup record and its redo path vector small; larger transactions
/// are split across records rather than rejected.
pub const MAX_DELETE_FILES_PER_RECORD: usize = 256;
pub const MAX_DELETE_FILES_PAYLOAD_BYTES: usize = 64 * 1024;

/// WAL record header for WriteFile operation
///
/// Layout in WAL record:
/// - WriteFileHeader (this struct)
/// - path bytes (path_len bytes)
/// - file data (remaining bytes)
///
/// When offset is 0, the file will be created (along with any missing parent
/// directories). If the file already exists, it will be truncated.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WriteFileHeader {
    /// Length of the file path (not including null terminator)
    pub path_len: u32,
    /// Explicit padding keeps the WAL header deterministic on all platforms.
    pub _padding: u32,
    /// Offset in the file to write at
    /// If offset is 0, the file (and parent directories) will be created
    pub offset: i64,
}

/// Size of WriteFileHeader in bytes
pub const SIZE_OF_WRITE_FILE: usize = std::mem::size_of::<WriteFileHeader>();

// ============================================================================
// WAL Logging Helper Functions
// ============================================================================

/// Log a directory deletion to WAL
///
/// Call this before deleting a directory and all its contents.
/// This is used for post-commit cleanup of local table directories. It must not
/// be called from pre-commit or abort cleanup paths: unlike PostgreSQL's native
/// relation storage, extension-owned Iceberg paths cannot be embedded in the
/// core transaction commit/abort WAL record, and `smgr` cannot be registered by
/// this AM. Logging the delete before the transaction outcome is known would let
/// standby WAL replay or archive recovery delete data for a transaction that later aborts.
///
/// Note: Only use this for local file systems. Distributed storage should rely
/// on garbage collection mechanisms instead.
///
/// # Arguments
/// * `path` - The path of the directory to delete (absolute, or relative to DataDir)
///
/// # Returns
/// The LSN of the WAL record
pub fn log_delete_directory(path: &str) -> XLogRecPtr {
    let header = DeleteDirectoryHeader {
        path_len: path.len() as u32,
    };

    let mut builder = WalRecordBuilder::begin();

    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(path.as_bytes());

    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::DeleteDirectory as u8)
}

/// Log bounded batches of local canceled-file deletions after commit.
///
/// Returns the last inserted LSN so the caller can flush every preceding batch
/// before deleting the files on the primary. Paths that cannot fit the record
/// representation are skipped with a warning; their primary cleanup may still
/// proceed and standby orphan maintenance can reclaim any replayed copy.
pub fn log_delete_files<'a>(
    paths: impl IntoIterator<Item = &'a str>,
) -> Option<XLogRecPtr> {
    let mut payload = Vec::with_capacity(MAX_DELETE_FILES_PAYLOAD_BYTES);
    let mut path_count = 0_u32;
    let mut last_lsn = None;

    for path in paths {
        if path.is_empty() || path.as_bytes().contains(&0) {
            diag::report_warning(
                "skipping DELETE_FILES WAL for empty path or path containing NUL",
            );
            continue;
        }
        let Ok(path_len) = u32::try_from(path.len()) else {
            diag::report_warning(format_args!(
                "skipping DELETE_FILES WAL for path longer than u32: {} bytes",
                path.len()
            ));
            continue;
        };
        let encoded_len = std::mem::size_of::<u32>() + path.len();
        if encoded_len > MAX_DELETE_FILES_PAYLOAD_BYTES {
            diag::report_warning(format_args!(
                "skipping DELETE_FILES WAL for path exceeding bounded payload: {} bytes",
                path.len()
            ));
            continue;
        }
        if path_count > 0
            && (path_count as usize >= MAX_DELETE_FILES_PER_RECORD
                || payload.len() + encoded_len > MAX_DELETE_FILES_PAYLOAD_BYTES)
        {
            last_lsn = Some(insert_delete_files_record(path_count, &payload));
            payload.clear();
            path_count = 0;
        }

        payload.extend_from_slice(&path_len.to_ne_bytes());
        payload.extend_from_slice(path.as_bytes());
        path_count += 1;
    }

    if path_count > 0 {
        last_lsn = Some(insert_delete_files_record(path_count, &payload));
    }
    last_lsn
}

pub(crate) fn delete_file_fits_wal(path: &str) -> bool {
    !path.is_empty()
        && !path.as_bytes().contains(&0)
        && u32::try_from(path.len()).is_ok()
        && std::mem::size_of::<u32>() + path.len()
            <= MAX_DELETE_FILES_PAYLOAD_BYTES
}

fn insert_delete_files_record(path_count: u32, payload: &[u8]) -> XLogRecPtr {
    let header = DeleteFilesHeader {
        path_count,
        payload_len: u32::try_from(payload.len())
            .expect("bounded DELETE_FILES payload must fit u32"),
    };
    let mut builder = WalRecordBuilder::begin();
    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(payload);
    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::DeleteFiles as u8)
}

/// Log a file write to WAL
///
/// Call this when writing data to a file. If offset is 0, the file will be
/// created (along with any missing parent directories), or truncated if it
/// already exists.
///
/// Note: Only use this for local file systems when the owning relation requires
/// WAL. The record is for standby WAL replay or archive recovery, not local
/// crash-only recovery. Distributed storage guarantees durability after
/// successful write and does not use this WAL path.
///
/// # Arguments
/// * `path` - The path of the file to write (absolute, or relative to DataDir)
/// * `offset` - The offset in the file to write at (0 = create/truncate file)
/// * `data` - The data to write
///
/// # Returns
/// The LSN of the WAL record
pub fn log_write_file(path: &str, offset: i64, data: &[u8]) -> XLogRecPtr {
    let header = WriteFileHeader {
        path_len: path.len() as u32,
        _padding: 0,
        offset,
    };

    let mut builder = WalRecordBuilder::begin();

    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(path.as_bytes());

    // Only register file data if there is any
    if !data.is_empty() {
        builder.register_data(data);
    }

    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::WriteFile as u8)
}
