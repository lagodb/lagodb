//! Transaction-scoped lifecycle for storage artifacts.
//!
//! Manages the lifecycle of files created during a transaction: local data files,
//! table directories, and object-storage staging/uploaded files. Transaction-owned
//! artifacts carry their savepoint nesting level. Metadata materialization
//! artifacts live in a separate top-level-only registry until their catalog CAS
//! succeeds or fails.
//!
//! The module exposes a small set of domain-level registration functions. Internally
//! a single [`StorageArtifactResource`] is lazily registered as a
//! [`TransactionResource`] the first time any artifact is recorded.
//!
//! **Commit behaviour:**
//! - `DroppedLocalTableRoot` → after PostgreSQL commit, WAL-log local deletion only
//!   when the relation WAL policy requires it, then remove the table directory
//!   recursively.
//! - `ObjectFile(Uploaded)` → unlink the staging file (best-effort).
//! - `ObjectFile(Staged)` → warn, then unlink the staging file (best-effort).
//! - unresolved metadata-attempt artifacts → abort-style cleanup instead of
//!   preserving files that no successful catalog CAS references.
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
//! respect to remote orphan files. A typical concern: a rolling writer flushes
//! batch N successfully (staging file uploaded to S3, registered as
//! `Uploaded`), then batch N+1 fails inside `flush_buffer` / `close_writer`.
//! The error propagates out of `end_modify`, the transaction aborts, and
//! `on_abort` walks every registered `Uploaded` entry and issues a remote
//! `delete`. The partial staging file from batch N+1 is `Staged` and gets
//! unlinked locally. So the remote store does not accumulate orphan data
//! files from aborted transactions on this primary; orphan-file maintenance
//! flows are still required for cases this registry cannot cover (process
//! crashes between upload and `mark_object_file_uploaded`, lost backend on a
//! standby, prior `WRITE_FILE` redo on aborted xacts, etc.).
//!
//! Abort cleanup is deliberately primary-local best effort. We do not emit
//! `DELETE_FILE` WAL for `CreatedLocalFile`: extensions cannot attach those
//! paths to PostgreSQL's core abort record, and a separate post-abort maintenance
//! stream would still have a crash gap. If a prior `WRITE_FILE` is replayed by
//! standby WAL replay or archive recovery for an aborted transaction, the file
//! is an Iceberg orphan because committed table metadata never references it.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use pg_lakebase_core::transaction::{self, TransactionResource};
use pg_lakebase_core::wal::flush_wal;
use pg_lakebase_storage::{ObjectLocation, StorageClient, StorageErrorKind};
use pgrx::pg_sys;

use crate::error::{IcebergError, IcebergResult};
use crate::storage::{
    LocalStorage, PostCommitDeletePurpose, PostCommitFileDeleteBatch,
};
use crate::wal::record::log_delete_directory;

const TOP_LEVEL_NEST_LEVEL: i32 = 1;

// ---------------------------------------------------------------------------
// Artifact model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectFileState {
    Staged,
    Uploaded,
}

enum ArtifactKind {
    CreatedLocalFile {
        path: PathBuf,
    },
    CreatedTableDir {
        location: String,
        file_io: FileIO,
    },
    DroppedLocalTableRoot {
        location: String,
        file_io: FileIO,
    },
    ObjectFile {
        location: ObjectLocation,
        staging_path: PathBuf,
        client: StorageClient,
        state: ObjectFileState,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataAttemptId(u32);

impl std::fmt::Debug for ArtifactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreatedLocalFile { path } => f
                .debug_struct("CreatedLocalFile")
                .field("path", path)
                .finish(),
            Self::CreatedTableDir { location, .. } => f
                .debug_struct("CreatedTableDir")
                .field("location", location)
                .finish(),
            Self::DroppedLocalTableRoot { location, .. } => f
                .debug_struct("DroppedLocalTableRoot")
                .field("location", location)
                .finish(),
            Self::ObjectFile {
                location,
                staging_path,
                state,
                ..
            } => f
                .debug_struct("ObjectFile")
                .field("location", location)
                .field("staging_path", staging_path)
                .field("state", state)
                .finish(),
        }
    }
}

#[derive(Debug)]
struct ArtifactEntry {
    nest_level: i32,
    kind: ArtifactKind,
}

#[derive(Debug)]
struct MetadataAttemptState {
    id: MetadataAttemptId,
    artifacts: Vec<ArtifactKind>,
}

/// Metadata materialization artifacts have a top-level-only lifecycle. They do
/// not carry savepoint nesting state and are never visited by subtransaction
/// callbacks.
#[derive(Debug)]
struct MetadataArtifactRegistry {
    active_attempt: Option<MetadataAttemptState>,
    /// Artifacts selected by a successful CAS and retained until transaction end.
    promoted: Vec<ArtifactKind>,
    /// Rejected artifacts whose immediate cleanup failed.
    cleanup_required: Vec<ArtifactKind>,
    next_attempt_id: u32,
}

impl MetadataArtifactRegistry {
    fn new() -> Self {
        Self {
            active_attempt: None,
            promoted: Vec::new(),
            cleanup_required: Vec::new(),
            next_attempt_id: 1,
        }
    }

    fn begin_attempt(&mut self) -> IcebergResult<MetadataAttemptId> {
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        if nest_level != TOP_LEVEL_NEST_LEVEL {
            return Err(IcebergError::InvariantViolated(
                "metadata artifact attempts require top-level transaction context",
            ));
        }
        if self.active_attempt.is_some() {
            return Err(IcebergError::InvariantViolated(
                "metadata artifact attempts cannot be nested",
            ));
        }

        let id = MetadataAttemptId(self.next_attempt_id);
        self.next_attempt_id = self.next_attempt_id.checked_add(1).ok_or(
            IcebergError::InvariantViolated("metadata artifact attempt id overflow"),
        )?;
        self.active_attempt = Some(MetadataAttemptState {
            id,
            artifacts: Vec::new(),
        });
        Ok(id)
    }

    fn promote_attempt(&mut self, id: MetadataAttemptId) -> IcebergResult<()> {
        let artifacts = self.take_attempt(id)?;
        self.promoted.extend(artifacts);
        Ok(())
    }

    fn take_attempt(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<Vec<ArtifactKind>> {
        let Some(attempt) = self.active_attempt.take() else {
            return Err(IcebergError::InvariantViolated(
                "metadata artifact attempt is not active",
            ));
        };
        if attempt.id != id {
            self.active_attempt = Some(attempt);
            return Err(IcebergError::InvariantViolated(
                "metadata artifact attempt is not active",
            ));
        }
        Ok(attempt.artifacts)
    }

    fn drain_top_level(&mut self) -> (Vec<ArtifactKind>, Vec<ArtifactKind>) {
        let promoted = std::mem::take(&mut self.promoted);
        let mut cleanup_required = std::mem::take(&mut self.cleanup_required);
        if let Some(active) = self.active_attempt.take() {
            cleanup_required.extend(active.artifacts.into_iter().rev());
        }
        (promoted, cleanup_required)
    }
}

struct TopLevelArtifacts {
    transaction: Vec<ArtifactEntry>,
    promoted_metadata: Vec<ArtifactKind>,
    cleanup_metadata: Vec<ArtifactKind>,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ArtifactRegistry {
    entries: Vec<ArtifactEntry>,
    metadata: MetadataArtifactRegistry,
}

impl ArtifactRegistry {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            metadata: MetadataArtifactRegistry::new(),
        }
    }

    fn add(&mut self, kind: ArtifactKind) {
        if let Some(attempt) = self.metadata.active_attempt.as_mut() {
            attempt.artifacts.push(kind);
            return;
        }
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        self.entries.push(ArtifactEntry { nest_level, kind });
    }

    fn begin_metadata_attempt(&mut self) -> IcebergResult<MetadataAttemptId> {
        self.metadata.begin_attempt()
    }

    fn promote_metadata_attempt(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<()> {
        self.metadata.promote_attempt(id)
    }

    fn take_metadata_attempt_artifacts(
        &mut self,
        id: MetadataAttemptId,
    ) -> IcebergResult<Vec<ArtifactKind>> {
        self.metadata.take_attempt(id)
    }

    fn current_write_artifacts(&self) -> impl Iterator<Item = &ArtifactKind> {
        self.metadata
            .active_attempt
            .iter()
            .flat_map(|attempt| attempt.artifacts.iter())
            .chain(self.entries.iter().map(|entry| &entry.kind))
    }

    fn current_write_artifacts_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut ArtifactKind> {
        self.metadata
            .active_attempt
            .iter_mut()
            .flat_map(|attempt| attempt.artifacts.iter_mut())
            .chain(self.entries.iter_mut().map(|entry| &mut entry.kind))
    }

    fn assert_staged(
        &self,
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        for kind in self.current_write_artifacts() {
            if let ArtifactKind::ObjectFile {
                location: loc,
                state,
                ..
            } = kind
                && loc == location
            {
                return match state {
                    ObjectFileState::Staged => Ok(()),
                    ObjectFileState::Uploaded => Err(format!(
                        "object '{}' is already in Uploaded state; \
                             duplicate finalize_write?",
                        location,
                    )),
                };
            }
        }
        Err(format!(
            "no staged entry found for '{}'; \
             finalize_write called without a prior writer() registration",
            location,
        ))
    }

    fn mark_uploaded(
        &mut self,
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        for kind in self.current_write_artifacts_mut() {
            if let ArtifactKind::ObjectFile {
                location: loc,
                state,
                ..
            } = kind
                && loc == location
            {
                if *state == ObjectFileState::Staged {
                    *state = ObjectFileState::Uploaded;
                    return Ok(());
                } else {
                    return Err(format!(
                        "object '{}' is already Uploaded; state machine error",
                        location,
                    ));
                }
            }
        }
        Err(format!(
            "no entry found for '{}' during mark_uploaded",
            location,
        ))
    }

    // -- transaction callbacks ------------------------------------------------

    fn drain_top_level(&mut self) -> TopLevelArtifacts {
        let (promoted_metadata, cleanup_metadata) = self.metadata.drain_top_level();
        TopLevelArtifacts {
            transaction: std::mem::take(&mut self.entries),
            promoted_metadata,
            cleanup_metadata,
        }
    }

    // Savepoint callbacks only affect transaction entries. Metadata artifacts
    // are top-level-only and intentionally absent from both operations.
    fn handle_commit_sub(&mut self, nest_level: i32) {
        for entry in &mut self.entries {
            if entry.nest_level >= nest_level {
                entry.nest_level = nest_level - 1;
            }
        }
    }

    fn take_abort_sub_entries(&mut self, nest_level: i32) -> Vec<ArtifactEntry> {
        let mut aborted = Vec::new();
        let mut kept = Vec::new();
        for entry in self.entries.drain(..) {
            if entry.nest_level >= nest_level {
                aborted.push(entry);
            } else {
                kept.push(entry);
            }
        }
        self.entries = kept;
        aborted
    }

    // -- per-entry actions ----------------------------------------------------

    fn commit_one(kind: ArtifactKind) {
        match kind {
            ArtifactKind::DroppedLocalTableRoot {
                ref location,
                ref file_io,
            } => {
                if Self::local_needs_wal(file_io) {
                    // PostgreSQL does not let extensions attach arbitrary paths
                    // to core commit/abort records, and PG17 smgr is not
                    // extension-customizable. XACT_EVENT_COMMIT is invoked
                    // after RecordTransactionCommit(), so this delete WAL is a
                    // separate post-commit cleanup fact and cannot be embedded
                    // in the transaction commit record.
                    //
                    // Flush the delete WAL before removing the primary
                    // directory. With synchronous_commit=off, this flush may
                    // also force the preceding commit WAL. If XLogFlush panics
                    // because of an I/O failure, crash recovery either loses the
                    // async commit together with the cleanup, or replays the
                    // committed transaction while missing this cleanup fact. The
                    // latter is the documented cleanup gap; it is preferable to
                    // deleting the primary directory before standby WAL replay or
                    // archive recovery can learn about the deletion.
                    let lsn = log_delete_directory(location);
                    flush_wal(lsn);
                }
                if let Err(e) = file_io.remove_dir_all(location) {
                    pg_lakebase_core::diag::report_warning(format_args!(
                        "failed to delete table directory '{}': {}",
                        location, e
                    ));
                }
            }
            ArtifactKind::ObjectFile {
                ref staging_path,
                state: ObjectFileState::Uploaded,
                ..
            } => {
                let _ = best_effort_unlink(staging_path);
            }
            ArtifactKind::ObjectFile {
                ref location,
                ref staging_path,
                state: ObjectFileState::Staged,
                ..
            } => {
                pg_lakebase_core::diag::report_warning(format_args!(
                    "committing staged object file '{}' before upload completed; removing staging file '{}'",
                    location,
                    staging_path.display()
                ));
                let _ = best_effort_unlink(staging_path);
            }
            _ => {}
        }
    }

    fn abort_one(kind: ArtifactKind) -> Option<ArtifactKind> {
        let cleaned = match &kind {
            ArtifactKind::CreatedLocalFile { path } => best_effort_unlink(path),
            ArtifactKind::CreatedTableDir { location, file_io } => {
                match file_io.remove_dir_all(location) {
                    Ok(()) => true,
                    Err(e) => {
                        pg_lakebase_core::diag::report_warning(format_args!(
                            "failed to delete table directory '{}': {}",
                            location, e
                        ));
                        false
                    }
                }
            }
            ArtifactKind::ObjectFile {
                location,
                staging_path,
                client,
                state,
            } => {
                let remote_deleted = if *state == ObjectFileState::Uploaded {
                    match client.delete(
                        location.store_id().as_str(),
                        location.bucket(),
                        location.key(),
                    ) {
                        Ok(()) => true,
                        Err(e) if e.kind() == StorageErrorKind::NotFound => true,
                        Err(e) => {
                            pg_lakebase_core::diag::report_warning(format_args!(
                                "failed to delete uploaded object '{}': {}",
                                location, e
                            ));
                            false
                        }
                    }
                } else {
                    true
                };
                let staging_unlinked = best_effort_unlink(staging_path);
                remote_deleted && staging_unlinked
            }
            ArtifactKind::DroppedLocalTableRoot { .. } => true,
        };
        (!cleaned).then_some(kind)
    }

    fn local_needs_wal(file_io: &FileIO) -> bool {
        file_io
            .storage()
            .as_any()
            .downcast_ref::<LocalStorage>()
            .map(LocalStorage::needs_wal)
            .unwrap_or(false)
    }
}

fn best_effort_unlink(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => true,
        Err(e) => {
            pg_lakebase_core::diag::report_warning(format_args!(
                "failed to unlink '{}': {}",
                path.display(),
                e
            ));
            false
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionResource adapter
// ---------------------------------------------------------------------------

thread_local! {
    static REGISTRY: RefCell<Option<Rc<StorageArtifactResource>>> =
        const { RefCell::new(None) };
}

#[derive(Debug)]
struct StorageArtifactResource {
    inner: RefCell<ArtifactRegistry>,
    nest_level: Cell<i32>,
}

impl StorageArtifactResource {
    fn promote_metadata_attempt(&self, id: MetadataAttemptId) -> IcebergResult<()> {
        self.inner
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "artifact registry is already borrowed",
                )
            })?
            .promote_metadata_attempt(id)
    }

    fn discard_metadata_attempt(&self, id: MetadataAttemptId) -> IcebergResult<()> {
        let artifacts = self
            .inner
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "artifact registry is already borrowed",
                )
            })?
            .take_metadata_attempt_artifacts(id)?;
        self.cleanup_metadata_attempt(artifacts);
        Ok(())
    }

    fn cleanup_metadata_attempt(&self, artifacts: Vec<ArtifactKind>) {
        let cleanup_required: Vec<_> = artifacts
            .into_iter()
            .rev()
            .filter_map(ArtifactRegistry::abort_one)
            .collect();
        if !cleanup_required.is_empty() {
            self.inner
                .borrow_mut()
                .metadata
                .cleanup_required
                .extend(cleanup_required);
        }
    }
}

impl TransactionResource for StorageArtifactResource {
    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }

    fn on_commit(&self) {
        let artifacts = self.inner.borrow_mut().drain_top_level();
        for kind in artifacts
            .transaction
            .into_iter()
            .map(|entry| entry.kind)
            .chain(artifacts.promoted_metadata)
        {
            ArtifactRegistry::commit_one(kind);
        }
        for kind in artifacts.cleanup_metadata {
            let _ = ArtifactRegistry::abort_one(kind);
        }
        REGISTRY.with(|r| *r.borrow_mut() = None);
    }

    fn on_abort(&self) {
        let artifacts = self.inner.borrow_mut().drain_top_level();
        for kind in artifacts
            .transaction
            .into_iter()
            .map(|entry| entry.kind)
            .chain(artifacts.promoted_metadata)
            .chain(artifacts.cleanup_metadata)
        {
            let _ = ArtifactRegistry::abort_one(kind);
        }
        REGISTRY.with(|r| *r.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        self.inner
            .borrow_mut()
            .handle_commit_sub(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        let entries = self
            .inner
            .borrow_mut()
            .take_abort_sub_entries(current_nest_level);
        for entry in entries {
            let _ = ArtifactRegistry::abort_one(entry.kind);
        }
    }
}

fn ensure_registry() -> Rc<StorageArtifactResource> {
    REGISTRY.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let resource = Rc::new(StorageArtifactResource {
                inner: RefCell::new(ArtifactRegistry::new()),
                nest_level: Cell::new(TOP_LEVEL_NEST_LEVEL),
            });
            transaction::register_resource(resource.clone());
            *borrow = Some(resource.clone());
        }
        borrow.as_ref().unwrap().clone()
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Owns every storage artifact produced by one metadata materialization and
/// catalog CAS attempt.
///
/// Files created while this scope is active are isolated from transaction-owned
/// data files and savepoint state. A successful CAS moves them to the promoted
/// metadata set: top-level commit preserves them, while top-level abort removes
/// them. A rejected or dropped attempt performs abort-style cleanup immediately.
#[must_use = "a metadata attempt must be promoted or discarded"]
pub(crate) struct MetadataAttempt {
    resource: Rc<StorageArtifactResource>,
    id: MetadataAttemptId,
    resolved: bool,
}

impl MetadataAttempt {
    pub(crate) fn begin() -> IcebergResult<Self> {
        let resource = ensure_registry();
        let id = resource
            .inner
            .try_borrow_mut()
            .map_err(|_| {
                IcebergError::InvariantViolated(
                    "artifact registry is already borrowed",
                )
            })?
            .begin_metadata_attempt()?;
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

impl Drop for MetadataAttempt {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        if let Err(error) = self.resource.discard_metadata_attempt(self.id) {
            pg_lakebase_core::diag::report_warning(format_args!(
                "failed to discard unresolved metadata artifact attempt: {}",
                error
            ));
        }
    }
}

/// Register a local data file for abort cleanup.
pub fn register_local_file_created(path: PathBuf) {
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::CreatedLocalFile { path });
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
pub fn register_table_dir_created(location: String, file_io: FileIO) {
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::CreatedTableDir { location, file_io });
}

/// Register a local table root to be removed on commit (DROP TABLE).
///
/// # Errors
///
/// Returns an invariant error if a remote `FileIO` crosses this local-only
/// boundary.
pub fn register_local_table_root_dropped(
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
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::DroppedLocalTableRoot { location, file_io });
    Ok(())
}

/// Register a staging file for an object-storage write.
pub fn register_object_file_staged(
    location: ObjectLocation,
    staging_path: PathBuf,
    client: StorageClient,
) {
    let res = ensure_registry();
    res.inner.borrow_mut().add(ArtifactKind::ObjectFile {
        location,
        staging_path,
        client,
        state: ObjectFileState::Staged,
    });
}

/// Verify that a staged entry exists for the given object location.
///
/// This MUST be called before attempting an upload. Errors indicate
/// lifecycle bugs (missing writer registration or duplicate finalize).
pub fn ensure_object_file_staged(
    location: &ObjectLocation,
) -> std::result::Result<(), String> {
    REGISTRY.with(|cell| {
        let borrow = cell.borrow();
        let registry = borrow.as_ref().ok_or_else(|| {
            format!(
                "artifact registry not initialized; \
                 finalize_write called without a prior writer() for '{}'",
                location,
            )
        })?;
        registry.inner.borrow().assert_staged(location)
    })
}

/// Transition a staged object file to the Uploaded state.
///
/// Only the `Staged → Uploaded` transition is valid. Any other state
/// (not found, already uploaded) is an internal lifecycle error.
pub fn mark_object_file_uploaded(
    location: &ObjectLocation,
) -> std::result::Result<(), String> {
    REGISTRY.with(|cell| {
        let borrow = cell.borrow();
        let registry = borrow.as_ref().ok_or_else(|| {
            format!(
                "artifact registry not initialized during mark_uploaded for '{}'",
                location,
            )
        })?;
        registry.inner.borrow_mut().mark_uploaded(location)
    })
}
