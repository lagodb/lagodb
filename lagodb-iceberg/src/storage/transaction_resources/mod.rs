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
//! [`lagodb_core::transaction::TransactionResource`] the first time any
//! storage resource is recorded.
//!
//! **Commit behaviour (deferred until `RESOURCE_RELEASE_AFTER_LOCKS`):**
//! - `ObjectFile(Uploaded)` → unlink the staging file (best-effort).
//! - `ObjectFile(Staged)` → warn, then unlink the staging file (best-effort).
//! - unresolved metadata-materialization resources → abort-style cleanup instead
//!   of preserving files that no successful catalog publication references.
//! - Everything else → no-op.
//!
//! Final-action-canceled data/delete files and retired local generations have
//! separate post-commit batch resources ([`super::PostCommitFileDeleteBatch`]
//! and [`super::LocalTableRetirement`]) executed after transaction lock release.
//!
//! Pending resources reject `PREPARE TRANSACTION` unless savepoint rollback or
//! attempt discard has already left the registry empty.
//!
//! **Abort behaviour:**
//! - `CreatedLocalFile` → unlink the local data file.
//! - `CreatedTableDir` → remove the table directory.
//! - `ObjectFile(Staged)` → unlink the staging file.
//! - `ObjectFile(Uploaded)` → delete the remote object, then unlink the staging file.
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

mod registry;
mod resource;

use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use lagodb_core::diag::report_warning;
use lagodb_core::storage::service::BackendStorageService;
use lagodb_storage::ObjectLocation;

use self::registry::{MetadataAttemptId, StorageTransactionResource};
use self::resource::{ObjectFileState, StorageResource};
use crate::error::IcebergResult;

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
            report_warning(format_args!(
                "failed to discard unresolved metadata resource attempt: {}",
                error
            ));
        }
    }
}

/// Register a local data file for abort cleanup.
pub(in crate::storage) fn register_local_file_created(path: PathBuf) {
    StorageTransactionResource::current()
        .track(StorageResource::CreatedLocalFile { path });
}

/// Register a newly-created table directory for abort cleanup.
pub(crate) fn register_table_dir_created(location: String, file_io: FileIO) {
    StorageTransactionResource::current()
        .track(StorageResource::CreatedTableDir { location, file_io });
}

/// Register a staging file for an object-storage write.
pub(in crate::storage) fn register_object_file_staged(
    location: ObjectLocation,
    staging_path: PathBuf,
    service: BackendStorageService,
) {
    StorageTransactionResource::current().track(StorageResource::ObjectFile {
        location,
        staging_path,
        service,
        state: Rc::new(Cell::new(ObjectFileState::Staged)),
    });
}

/// Verify that a staged entry exists for the given object location.
///
/// This MUST be called before attempting an upload. Errors indicate
/// lifecycle bugs (missing writer registration or duplicate finalize).
pub(in crate::storage) fn ensure_object_file_staged(
    location: &ObjectLocation,
) -> IcebergResult<()> {
    StorageTransactionResource::ensure_object_file_staged(location)
}

/// Transition a staged object file to the Uploaded state.
///
/// Only the `Staged → Uploaded` transition is valid. Any other state
/// (not found, already uploaded) is an internal lifecycle error.
pub(in crate::storage) fn mark_object_file_uploaded(
    location: &ObjectLocation,
) -> IcebergResult<()> {
    StorageTransactionResource::mark_object_file_uploaded(location)
}
