//! Iceberg-specific pending delete implementation.
//!
//! This module provides the pending delete mechanism for Iceberg tables,
//! allowing table directories to be cleaned up when transactions abort (for CREATE)
//! or commit (for DROP).
//!
//! # Background
//!
//! This is an implementation of `pg_lakebase_core::transaction::cleanup::PendingDelete`
//! tailored for Iceberg. It uses `iceberg_lite::io::FileIO` to physically
//! remove table directories from storage (Local, S3, GCS, etc.) when
//! the transaction reaches the appropriate state.
//!
//! # WAL Logging
//!
//! WAL logging for directory deletion is only performed for **local storage**.
//! This ensures that standby nodes and crash recovery can properly replay
//! the deletion.
//!
//! For **distributed storage** (S3, GCS, Azure), WAL logging is not used because:
//! 1. The storage is shared between primary and standby, so only one deletion is needed
//! 2. Orphaned files should be cleaned via garbage collection (e.g., Iceberg's expire_snapshots)

use crate::storage::ObjectStorage;
use crate::wal::record::log_delete_directory;
use pg_lakebase_core::transaction::cleanup::{
    CleanupTiming, PendingDelete, register_pending_delete,
};

use iceberg_lite::io::FileIO;

/// Pending delete entry for Iceberg table directories.
///
/// This entry handles both:
/// - **Abort Cleanup**: Deleting a newly created table directory if `CREATE TABLE` fails.
/// - **Commit Cleanup**: Deleting a table directory after `DROP TABLE` successfully commits.
#[derive(Debug)]
pub struct IcebergPendingDelete {
    /// The table location directory to delete
    location: String,
    /// The FileIO instance for performing the delete
    file_io: FileIO,
    /// The transaction outcome that triggers the delete.
    timing: CleanupTiming,
}

impl IcebergPendingDelete {
    /// Create a new pending delete for abort cleanup.
    ///
    /// This is used when creating a table - if the transaction aborts,
    /// the table directory will be deleted.
    pub fn new_for_abort(location: String, file_io: FileIO) -> Self {
        Self {
            location,
            file_io,
            timing: CleanupTiming::OnAbort,
        }
    }

    /// Create a new pending delete for commit cleanup.
    ///
    /// This is used when dropping a table - after the transaction commits,
    /// the table directory will be deleted.
    pub fn new_for_commit(location: String, file_io: FileIO) -> Self {
        Self {
            location,
            file_io,
            timing: CleanupTiming::OnCommit,
        }
    }

    /// Check if the storage is local (not distributed)
    fn is_local_storage(&self) -> bool {
        // If it's NOT an ObjectStorage, it's local storage
        self.file_io
            .storage()
            .as_any()
            .downcast_ref::<ObjectStorage>()
            .is_none()
    }
}

impl PendingDelete for IcebergPendingDelete {
    fn execute(&self) {
        // If this is a commit cleanup (DROP TABLE) on LOCAL storage,
        // log it to WAL for standby synchronization and crash recovery.
        //
        // For distributed storage, we skip WAL logging because:
        // 1. Storage is shared - primary deletion is sufficient
        // 2. Orphaned files are handled by garbage collection
        if self.timing == CleanupTiming::OnCommit && self.is_local_storage() {
            log_delete_directory(&self.location);
        }

        // Best-effort deletion - log errors but don't fail
        if let Err(e) = self.file_io.remove_dir_all(&self.location) {
            pg_lakebase_core::diag::report_warning(&format!(
                "Failed to delete table directory '{}': {}",
                self.location, e
            ));
        }
    }

    fn timing(&self) -> CleanupTiming {
        self.timing
    }
}

/// Register a pending delete for an Iceberg table directory (abort cleanup).
///
/// This should be called immediately after creating the table directory.
/// If the transaction aborts, the directory will be deleted.
///
/// # Arguments
///
/// * `location` - The table location directory path
/// * `file_io` - The FileIO instance used for deletion
pub fn register_table_pending_delete(location: String, file_io: FileIO) {
    let pending = IcebergPendingDelete::new_for_abort(location, file_io);
    register_pending_delete(Box::new(pending));
}

/// Register a pending delete for DROP TABLE (commit cleanup).
///
/// This should be called when processing DROP TABLE.
/// After the transaction commits, the directory will be deleted.
///
/// # Arguments
///
/// * `location` - The table location directory path
/// * `file_io` - The FileIO instance used for deletion
pub fn register_drop_table_pending_delete(location: String, file_io: FileIO) {
    let pending = IcebergPendingDelete::new_for_commit(location, file_io);
    register_pending_delete(Box::new(pending));
}
