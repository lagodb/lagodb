use super::rmgr::ICEBERG_RMGR_ID;
use pg_lakehouse_core::wal::{WalRecordBuilder, XLogRecPtr};

// ============================================================================
// WAL Operation Types
// ============================================================================

/// WAL record operation types for Iceberg
///
/// This enum defines the three basic file system operations that are logged
/// to the WAL for crash recovery on local file systems:
/// - DeleteDirectory: Remove a directory and its contents
/// - WriteFile: Write data to a file (creates file and parent directories if offset is 0)
/// - DeleteFile: Remove a file
///
/// Note: These WAL records are only used for local file systems. Distributed
/// storage (S3, GCS, Azure) guarantees durability and doesn't need WAL-based
/// redo. Orphaned files on distributed storage should be cleaned up via a
/// separate garbage collection mechanism (e.g., Iceberg's expire_snapshots).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcebergWalOp {
    /// Delete a directory (and all its contents)
    DeleteDirectory = 0x00,
    /// Write data to a file (creates file and parent directories if offset is 0)
    WriteFile = 0x10,
    /// Delete a file
    DeleteFile = 0x20,
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
            0x20 => Some(Self::DeleteFile),
            _ => None,
        }
    }

    /// Get the human-readable name of this operation
    pub fn name(&self) -> &'static str {
        match self {
            Self::DeleteDirectory => "DELETE_DIRECTORY",
            Self::WriteFile => "WRITE_FILE",
            Self::DeleteFile => "DELETE_FILE",
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
    /// Offset in the file to write at
    /// If offset is 0, the file (and parent directories) will be created
    pub offset: i64,
}

/// Size of WriteFileHeader in bytes
pub const SIZE_OF_WRITE_FILE: usize = std::mem::size_of::<WriteFileHeader>();

/// WAL record header for DeleteFile operation
///
/// Layout in WAL record:
/// - DeleteFileHeader (this struct)
/// - path bytes (path_len bytes)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DeleteFileHeader {
    /// Length of the file path (not including null terminator)
    pub path_len: u32,
}

/// Size of DeleteFileHeader in bytes
pub const SIZE_OF_DELETE_FILE: usize = std::mem::size_of::<DeleteFileHeader>();

// ============================================================================
// WAL Logging Helper Functions
// ============================================================================

/// Log a directory deletion to WAL
///
/// Call this before deleting a directory and all its contents.
/// This is typically used for TRUNCATE TABLE or DROP TABLE operations
/// on local storage.
///
/// Note: Only use this for local file systems. Distributed storage should
/// rely on garbage collection mechanisms instead.
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

/// Log a file write to WAL
///
/// Call this when writing data to a file. If offset is 0, the file will be
/// created (along with any missing parent directories), or truncated if it
/// already exists.
///
/// Note: Only use this for local file systems. Distributed storage guarantees
/// durability after successful write.
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

/// Log a file deletion to WAL
///
/// Call this before deleting a data file on local storage.
///
/// Note: Only use this for local file systems. Distributed storage should
/// rely on garbage collection mechanisms instead.
///
/// # Arguments
/// * `path` - The path of the file to delete (absolute, or relative to DataDir)
///
/// # Returns
/// The LSN of the WAL record
pub fn log_delete_file(path: &str) -> XLogRecPtr {
    let header = DeleteFileHeader {
        path_len: path.len() as u32,
    };

    let mut builder = WalRecordBuilder::begin();

    unsafe {
        builder.register_data_as(&header);
    }
    builder.register_data(path.as_bytes());

    builder.insert(ICEBERG_RMGR_ID.as_u8(), IcebergWalOp::DeleteFile as u8)
}
