//! Iceberg WAL (Write-Ahead Logging) Resource Manager
//!
//! This module implements custom WAL support for Iceberg tables stored on
//! local filesystem. It ensures crash-consistency for local file operations.
//!
//! # Supported Operations
//!
//! The WAL module supports three basic file system operations:
//!
//! 1. **WriteFile** - Write data to a file (creates file and parent directories if offset is 0)
//! 2. **DeleteFile** - Remove a file
//! 3. **DeleteDirectory** - Remove a directory and all its contents
//!
//! # Usage Example
//!
//! ```ignore
//! use pg_iceberg::wal::{log_write_file, log_delete_file, log_delete_directory};
//!
//! // Write a new data file (parent directories will be created automatically)
//! log_write_file("/data/iceberg/table1/data/file.parquet", 0, &data);
//!
//! // Append more data to the file
//! log_write_file("/data/iceberg/table1/data/file.parquet", 1024, &more_data);
//!
//! // Delete a file during cleanup
//! log_delete_file("/data/iceberg/table1/data/old_file.parquet");
//!
//! // Delete entire table directory (for TRUNCATE or DROP TABLE)
//! log_delete_directory("/data/iceberg/table1");
//! ```
//!
//! # Recovery Behavior
//!
//! During crash recovery, the WAL records are replayed to restore file system
//! consistency. All operations are designed to be idempotent:
//!
//! - WriteFile: Creates parent directories and file at offset 0, or writes at offset
//! - DeleteFile: Removes file if it exists
//! - DeleteDirectory: Removes directory if it exists

pub mod record;
pub mod rmgr;

use pg_tam::wal::register_wal_rmgr;

// Re-export commonly used types and functions
pub use record::{
    IcebergWalOp, log_delete_directory, log_delete_file, log_write_file,
};
pub use rmgr::{ICEBERG_RMGR_ID, IcebergRmgr};

/// Initialize the Iceberg WAL resource manager
///
/// This should be called from `_PG_init` to register the custom WAL
/// resource manager with PostgreSQL.
pub fn init_wal_rmgr() {
    register_wal_rmgr(Box::new(IcebergRmgr));
}
