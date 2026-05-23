//! Transaction-scoped lifecycle for storage artifacts.
//!
//! Manages the lifecycle of files created during a transaction: local data files,
//! table directories, and object-storage staging/uploaded files. Each artifact is
//! tracked with its transaction nesting level so subtransaction commit (promote)
//! and abort (cleanup) work correctly.
//!
//! The module exposes a small set of domain-level registration functions. Internally
//! a single [`StorageArtifactResource`] is lazily registered as a
//! [`TransactionResource`] the first time any artifact is recorded.
//!
//! **Commit behaviour:**
//! - `DroppedTableDir` → after PostgreSQL commit, WAL-log local deletion only
//!   when the relation WAL policy requires it, then remove the table directory
//!   or object-storage prefix.
//! - `ObjectFile(Uploaded)` → unlink the staging file (best-effort).
//! - `ObjectFile(Staged)` → warn, then unlink the staging file (best-effort).
//! - Everything else → no-op.
//!
//! **Abort behaviour:**
//! - `CreatedLocalFile` → unlink the local data file.
//! - `CreatedTableDir` → remove the table directory.
//! - `ObjectFile(Staged)` → unlink the staging file.
//! - `ObjectFile(Uploaded)` → delete the remote object, then unlink the staging file.
//! - `DroppedTableDir` → no-op (table survived).
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
use pg_lakebase_storage::{ObjectLocation, StorageClient};
use pgrx::pg_sys;

use crate::storage::LocalStorage;
use crate::wal::record::log_delete_directory;

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
    DroppedTableDir {
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
            Self::DroppedTableDir { location, .. } => f
                .debug_struct("DroppedTableDir")
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

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ArtifactRegistry {
    entries: Vec<ArtifactEntry>,
}

impl ArtifactRegistry {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    fn add(&mut self, kind: ArtifactKind) {
        let nest_level = unsafe { pg_sys::GetCurrentTransactionNestLevel() };
        self.entries.push(ArtifactEntry { nest_level, kind });
    }

    fn assert_staged(
        &self,
        location: &ObjectLocation,
    ) -> std::result::Result<(), String> {
        for entry in &self.entries {
            if let ArtifactKind::ObjectFile {
                location: ref loc,
                ref state,
                ..
            } = entry.kind
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
        for entry in &mut self.entries {
            if let ArtifactKind::ObjectFile {
                location: ref loc,
                ref mut state,
                ..
            } = entry.kind
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

    fn take_commit_entries(&mut self) -> Vec<ArtifactEntry> {
        self.entries.drain(..).collect()
    }

    fn take_abort_entries(&mut self) -> Vec<ArtifactEntry> {
        self.entries.drain(..).collect()
    }

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

    fn commit_one(entry: ArtifactEntry) {
        match entry.kind {
            ArtifactKind::DroppedTableDir {
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
                    pg_lakebase_core::diag::report_warning(&format!(
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
                best_effort_unlink(staging_path);
            }
            ArtifactKind::ObjectFile {
                ref location,
                ref staging_path,
                state: ObjectFileState::Staged,
                ..
            } => {
                pg_lakebase_core::diag::report_warning(&format!(
                    "committing staged object file '{}' before upload completed; removing staging file '{}'",
                    location,
                    staging_path.display()
                ));
                best_effort_unlink(staging_path);
            }
            _ => {}
        }
    }

    fn abort_one(entry: ArtifactEntry) {
        match entry.kind {
            ArtifactKind::CreatedLocalFile { ref path } => {
                best_effort_unlink(path);
            }
            ArtifactKind::CreatedTableDir {
                ref location,
                ref file_io,
            } => {
                if let Err(e) = file_io.remove_dir_all(location) {
                    pg_lakebase_core::diag::report_warning(&format!(
                        "failed to delete table directory '{}': {}",
                        location, e
                    ));
                }
            }
            ArtifactKind::ObjectFile {
                ref location,
                ref staging_path,
                ref client,
                state,
            } => {
                if state == ObjectFileState::Uploaded
                    && let Err(e) = client.delete(
                        location.store_id().as_str(),
                        location.bucket(),
                        location.key(),
                    )
                {
                    pg_lakebase_core::diag::report_warning(&format!(
                        "failed to delete uploaded object '{}': {}",
                        location, e
                    ));
                }
                best_effort_unlink(staging_path);
            }
            _ => {}
        }
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

fn best_effort_unlink(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            pg_lakebase_core::diag::report_warning(&format!(
                "failed to unlink '{}': {}",
                path.display(),
                e
            ));
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

impl TransactionResource for StorageArtifactResource {
    fn nest_level(&self) -> i32 {
        self.nest_level.get()
    }

    fn set_nest_level(&self, level: i32) {
        self.nest_level.set(level);
    }

    fn on_commit(&self) {
        let entries = self.inner.borrow_mut().take_commit_entries();
        for entry in entries {
            ArtifactRegistry::commit_one(entry);
        }
        REGISTRY.with(|r| *r.borrow_mut() = None);
    }

    fn on_abort(&self) {
        let entries = self.inner.borrow_mut().take_abort_entries();
        for entry in entries {
            ArtifactRegistry::abort_one(entry);
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
            ArtifactRegistry::abort_one(entry);
        }
    }
}

fn ensure_registry() -> Rc<StorageArtifactResource> {
    REGISTRY.with(|cell| {
        let mut borrow = cell.borrow_mut();
        if borrow.is_none() {
            let resource = Rc::new(StorageArtifactResource {
                inner: RefCell::new(ArtifactRegistry::new()),
                nest_level: Cell::new(1),
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

/// Register a local data file for abort cleanup.
pub fn register_local_file_created(path: PathBuf) {
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::CreatedLocalFile { path });
}

/// Register a newly-created table directory for abort cleanup.
pub fn register_table_dir_created(location: String, file_io: FileIO) {
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::CreatedTableDir { location, file_io });
}

/// Register a table directory to be removed on commit (DROP TABLE).
pub fn register_table_dir_dropped(location: String, file_io: FileIO) {
    let res = ensure_registry();
    res.inner
        .borrow_mut()
        .add(ArtifactKind::DroppedTableDir { location, file_io });
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
