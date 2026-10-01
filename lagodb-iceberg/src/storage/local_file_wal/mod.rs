//! Local-file Iceberg WAL (Write-Ahead Logging) resource manager.
//!
//! This module implements custom WAL support for Iceberg tables stored on
//! local filesystem. It reconstructs local Iceberg files during standby WAL
//! replay (including hot standby) or archive recovery, and performs best-effort
//! post-commit cleanup of local table directories. This is an
//! availability-first lossy reconstruction path: local crash recovery skips
//! `WRITE_FILE` and relies on file sync at writer close. Standby WAL replay
//! or archive recovery skips later chunks if their base local Iceberg file is
//! missing.
//!
//! PostgreSQL's native relation storage can place relfilenode cleanup directly
//! in transaction commit/abort WAL records. Extensions cannot attach arbitrary
//! AM-owned paths to those core records, and PostgreSQL's `smgr` switch is not
//! an extension registration API. Consequently, Iceberg delete WAL is emitted
//! for retired storage only after the PostgreSQL transaction outcome is known.
//! This design may leave orphan files if the server crashes before cleanup WAL
//! is written; it must never delete committed data before the transaction commits.
//!
//! Known design debt: writers sync files but intentionally do not fsync
//! directories. An OS crash or power loss can therefore leave committed catalog
//! metadata referencing missing file or directory entries. Primary crash
//! recovery skips WRITE_FILE redo and provides no repair for this gap.
//! Retired directories are not part of core commit/abort cleanup, leaving the
//! post-commit cleanup gap above. The native main fork reserves a PG locator;
//! it does not give the adjacent Iceberg directory native smgr ownership.
//! Following native storage lifecycle ordering does not give this PostgreSQL
//! extension the same recovery/cleanup protocol as native smgr-owned storage.
//! See `src/storage/local_file_wal/README.md` for the full contract and known design debt.
//!
//! # Supported Operations
//!
//! The WAL module supports four local file system operations:
//!
//! 1. **WriteFile** - Write data to a file (creates file and parent directories if offset is 0)
//! 2. **DeleteDirectory** - Remove a directory and all its contents after commit
//! 3. **DeleteFiles** - Remove canceled transaction-created files after commit
//! 4. **TruncateDirectory** - Clear a disposable transaction-local generation;
//!    replayed only on standby or during archive recovery
//!
//! # Usage Example
//!
//! ```ignore
//! use crate::storage::local_file_wal::{log_write_file, log_delete_directory};
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
//! system. Local crash-only recovery intentionally skips `WriteFile` and relies
//! on writer close to sync files before commit. It does not guarantee durability
//! of their publishing directory entries; see the known design debt above.
//!
//! - WriteFile: Creates parent directories and file at offset 0, writes at later
//!   offsets, and skips later chunks if the base file is missing during lossy replay
//! - DeleteDirectory and DeleteFiles: Best-effort removal; missing paths and
//!   delete failures do not stop recovery
//! - TruncateDirectory: Uses directory removal on standby/archive replay and
//!   is skipped during primary crash recovery, preserving the new synced table

pub mod record;
pub mod rmgr;

use lagodb_core::wal::register_wal_rmgr;

pub(crate) use record::log_write_file;
use rmgr::{ICEBERG_RMGR_ID_U8, IcebergRmgr};

/// Initialize the Iceberg WAL resource manager
///
/// This should be called from `_PG_init` to register the custom WAL
/// resource manager with PostgreSQL.
pub fn init_wal_rmgr() {
    register_wal_rmgr::<ICEBERG_RMGR_ID_U8>(Box::new(IcebergRmgr));
}
