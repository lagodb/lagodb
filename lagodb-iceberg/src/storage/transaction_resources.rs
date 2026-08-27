//! Transaction-scoped lifecycle for storage resources.
//!
//! Manages files and directories created during a transaction: local data files,
//! table directories, and object-storage staging/uploaded files. Transaction-owned
//! resources carry their savepoint nesting level. Metadata materialization
//! resources live in a separate top-level-only registry until their catalog
//! publication succeeds or fails.
//!
//! The module exposes a small set of domain-level registration functions. Internally
//! a single [`StorageTransactionResource`] is lazily registered as a
//! [`pg_lakebase_core::transaction::TransactionResource`] the first time any
//! storage resource is recorded.
//!
//! **Commit behaviour:**
//! - `DroppedLocalTableRoot` → after PostgreSQL commit, WAL-log local deletion only
//!   when the relation WAL policy requires it, then remove the table directory
//!   recursively.
//! - `ObjectFile(Uploaded)` → unlink the staging file (best-effort).
//! - `ObjectFile(Staged)` → warn, then unlink the staging file (best-effort).
//! - unresolved metadata-materialization resources → abort-style cleanup instead
//!   of preserving files that no successful catalog publication references.
//! - final-action-canceled data/delete files → one aggregated post-commit
//!   cleanup resource; local WAL-enabled storage flushes `DELETE_FILES` first.
//! - Everything else → no-op.
//!
//! **Abort behaviour:**
//! - `CreatedLocalFile` → unlink the local data file.
//! - `CreatedTableDir` → remove the table directory.
//! - `ObjectFile(Staged)` → unlink the staging file.
//! - `ObjectFile(Uploaded)` → delete the remote object, then unlink the staging file.
//! - `DroppedLocalTableRoot` → no-op (table survived).
//!
//! This abort behaviour is what makes a mid-statement writer failure safe with
//! respect to remote orphan files. A rolling writer can upload one batch before
//! a later batch fails; abort cleanup deletes every registered uploaded object
//! and unlinks every staging file. Orphan-file maintenance remains necessary for
//! cases this backend-local registry cannot cover, including process loss between
//! upload and state transition or replay of an aborted transaction's file WAL.
//!
//! Abort cleanup is deliberately primary-local best effort. We do not emit
//! `DELETE_FILE` WAL for `CreatedLocalFile`: extensions cannot attach those paths
//! to PostgreSQL's core abort record, and a separate post-abort maintenance stream
//! would still have a crash gap.

use std::path::PathBuf;
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use lagodb_storage::ObjectLocation;
use pg_lakebase_core::storage::service::BackendStorageService;

use super::{LocalStorage, PostCommitDeletePurpose, PostCommitFileDeleteBatch};
use crate::error::{IcebergError, IcebergResult};

use self::registry::{MetadataAttemptId, StorageTransactionResource};
use self::resource::{ObjectFileState, StorageResource};

mod registry;
mod resource;

/// Owns every storage resource produced by one metadata materialization attempt.
///
/// Files created while this scope is active are isolated from transaction-owned
/// data files and savepoint state. A successful catalog publication moves them
/// to the promoted metadata set: top-level commit preserves them, while top-level
/// abort removes them. A rejected attempt performs abort-style cleanup immediately.
/// The type neither reads nor updates a metadata location and contains no CAS policy.
#[must_use = "a metadata materialization attempt must be promoted or discarded"]
pub(crate) struct MetadataMaterializationAttempt {
    resource: Rc<StorageTransactionResource>,
    id: MetadataAttemptId,
    resolved: bool,
}

impl MetadataMaterializationAttempt {
    pub(crate) fn begin() -> IcebergResult<Self> {
        let resource = StorageTransactionResource::current();
        let id = resource.begin_metadata_attempt()?;
        Ok(Self {
            resource,
            id,
            resolved: false,
        })
    }

    pub(crate) fn promote(mut self) -> IcebergResult<()> {
        self.resource.promote_metadata_attempt(self.id)?;
        self.resolved = true;
        Ok(())
    }

    pub(crate) fn discard(mut self) -> IcebergResult<()> {
        self.resource.discard_metadata_attempt(self.id)?;
        self.resolved = true;
        Ok(())
    }
}

impl Drop for MetadataMaterializationAttempt {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        if let Err(error) = self.resource.discard_metadata_attempt(self.id) {
            pg_lakebase_core::diag::report_warning(format_args!(
                "failed to discard unresolved metadata resource attempt: {}",
                error
            ));
        }
    }
}

/// Register a local data file for abort cleanup.
pub(crate) fn register_local_file_created(path: PathBuf) {
    StorageTransactionResource::current()
        .track(StorageResource::CreatedLocalFile { path });
}

/// Delete transaction-created files that the final metadata commit does not
/// reference. Registration during pre-commit is safe: the transaction
/// framework includes newly registered resources in the later commit/abort
/// callbacks even though they do not receive another pre-commit callback.
pub(crate) fn register_canceled_files_for_commit(
    file_io: FileIO,
    paths: Vec<String>,
) {
    PostCommitFileDeleteBatch::register(
        file_io,
        paths,
        PostCommitDeletePurpose::CanceledCreatedFiles,
    );
}

/// Register a newly-created table directory for abort cleanup.
pub(crate) fn register_table_dir_created(location: String, file_io: FileIO) {
    StorageTransactionResource::current()
        .track(StorageResource::CreatedTableDir { location, file_io });
}

/// Register a local table root to be removed on commit (DROP TABLE).
///
/// # Errors
///
/// Returns an invariant error if a remote `FileIO` crosses this local-only
/// boundary.
pub(crate) fn register_local_table_root_dropped(
    location: String,
    file_io: FileIO,
) -> IcebergResult<()> {
    if file_io
        .storage()
        .as_any()
        .downcast_ref::<LocalStorage>()
        .is_none()
    {
        return Err(IcebergError::InvariantViolated(
            "remote storage passed to local table-root cleanup",
        ));
    }
    StorageTransactionResource::current()
        .track(StorageResource::DroppedLocalTableRoot { location, file_io });
    Ok(())
}

/// Register a staging file for an object-storage write.
pub(crate) fn register_object_file_staged(
    location: ObjectLocation,
    staging_path: PathBuf,
    service: BackendStorageService,
) {
    StorageTransactionResource::current().track(StorageResource::ObjectFile {
        location,
        staging_path,
        service,
        state: ObjectFileState::Staged,
    });
}

/// Verify that a staged entry exists for the given object location.
///
/// This MUST be called before attempting an upload. Errors indicate
/// lifecycle bugs (missing writer registration or duplicate finalize).
pub(crate) fn ensure_object_file_staged(
    location: &ObjectLocation,
) -> std::result::Result<(), String> {
    StorageTransactionResource::ensure_object_file_staged(location)
}

/// Transition a staged object file to the Uploaded state.
///
/// Only the `Staged → Uploaded` transition is valid. Any other state
/// (not found, already uploaded) is an internal lifecycle error.
pub(crate) fn mark_object_file_uploaded(
    location: &ObjectLocation,
) -> std::result::Result<(), String> {
    StorageTransactionResource::mark_object_file_uploaded(location)
}
