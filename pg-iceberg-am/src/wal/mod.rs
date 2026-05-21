//! Iceberg WAL (Write-Ahead Logging) Resource Manager
//!
//! This module implements custom WAL support for Iceberg tables stored on
//! local filesystem. It reconstructs local Iceberg files during standby WAL
//! replay (including hot standby) or archive recovery, and performs best-effort
//! post-commit cleanup of local table directories. This is an
//! availability-first lossy reconstruction path: local crash recovery does not
//! need `WRITE_FILE` replay because successful writers call `FileSync` before
//! close returns, and standby WAL replay or archive recovery skips later chunks
//! if their base local Iceberg file is missing.
//!
//! PostgreSQL's native relation storage can place relfilenode cleanup directly
//! in transaction commit/abort WAL records. Extensions cannot attach arbitrary
//! AM-owned paths to those core records, and PostgreSQL's `smgr` switch is not
//! an extension registration API. Consequently, Iceberg delete WAL is emitted
//! only after the PostgreSQL transaction outcome is known. This design may leave
//! orphan files if the server crashes before post-commit cleanup WAL is written;
//! it must never delete committed data before the transaction commits.
//! See `src/wal/README.md` for the full contract and known design debt.
//!
//! # Supported Operations
//!
//! The WAL module supports two local file system operations:
//!
//! 1. **WriteFile** - Write data to a file (creates file and parent directories if offset is 0)
//! 2. **DeleteDirectory** - Remove a directory and all its contents after commit
//!
//! # Usage Example
//!
//! ```ignore
//! use pg_iceberg_am::wal::{log_write_file, log_delete_directory};
//!
//! // Write a new data file (parent directories will be created automatically)
//! log_write_file("/data/iceberg/table1/data/file.parquet", 0, &data);
//!
//! // Append more data to the file
//! log_write_file("/data/iceberg/table1/data/file.parquet", 1024, &more_data);
//!
//! // Delete entire table directory after PostgreSQL commit has succeeded
//! log_delete_directory("/data/iceberg/table1");
//! ```
//!
//! # Recovery Behavior
//!
//! During standby WAL replay or archive recovery, the WAL records are replayed
//! to restore local Iceberg files that are not otherwise present on the target
//! system. Local crash-only recovery intentionally skips `WriteFile`: successful
//! writers call `FileSync` on explicit close, so the primary's committed files
//! are already durable.
//!
//! - WriteFile: Creates parent directories and file at offset 0, writes at later
//!   offsets, and skips later chunks if the base file is missing during lossy replay
//! - DeleteDirectory: Best-effort removal; missing paths and delete failures do
//!   not stop recovery

pub mod record;
pub mod rmgr;

use pg_lakebase_core::wal::register_wal_rmgr;

// Re-export commonly used types and functions
pub use record::{IcebergWalOp, log_delete_directory, log_write_file};
pub use rmgr::{ICEBERG_RMGR_ID, ICEBERG_RMGR_ID_U8, IcebergRmgr};

/// Initialize the Iceberg WAL resource manager
///
/// This should be called from `_PG_init` to register the custom WAL
/// resource manager with PostgreSQL.
pub fn init_wal_rmgr() {
    register_wal_rmgr::<ICEBERG_RMGR_ID_U8>(Box::new(IcebergRmgr));
}
