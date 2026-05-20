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
//! - `DroppedTableDir` → remove the table directory (WAL-logged for local).
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

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use iceberg_lite::io::FileIO;
use pg_lakebase_core::transaction::{self, TransactionResource};
use pg_lakebase_storage::{ObjectLocation, StorageClient};
use pgrx::pg_sys;

use crate::storage::ObjectStorage;
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
            {
                if loc == location {
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
            {
                if loc == location {
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
        }
        Err(format!(
            "no entry found for '{}' during mark_uploaded",
            location,
        ))
    }

    // -- transaction callbacks ------------------------------------------------

    fn handle_commit(&mut self) {
        for entry in self.entries.drain(..) {
            Self::commit_one(entry);
        }
    }

    fn handle_abort(&mut self) {
        for entry in self.entries.drain(..) {
            Self::abort_one(entry);
        }
    }

    fn handle_commit_sub(&mut self, nest_level: i32) {
        for entry in &mut self.entries {
            if entry.nest_level >= nest_level {
                entry.nest_level = nest_level - 1;
            }
        }
    }

    fn handle_abort_sub(&mut self, nest_level: i32) {
        let mut kept = Vec::new();
        for entry in self.entries.drain(..) {
            if entry.nest_level >= nest_level {
                Self::abort_one(entry);
            } else {
                kept.push(entry);
            }
        }
        self.entries = kept;
    }

    // -- per-entry actions ----------------------------------------------------

    fn commit_one(entry: ArtifactEntry) {
        match entry.kind {
            ArtifactKind::DroppedTableDir {
                ref location,
                ref file_io,
            } => {
                if Self::is_local_storage(file_io) {
                    log_delete_directory(location);
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
                if state == ObjectFileState::Uploaded {
                    if let Err(e) = client.delete(
                        location.store_id().as_str(),
                        location.bucket(),
                        location.key(),
                    ) {
                        pg_lakebase_core::diag::report_warning(&format!(
                            "failed to delete uploaded object '{}': {}",
                            location, e
                        ));
                    }
                }
                best_effort_unlink(staging_path);
            }
            _ => {}
        }
    }

    fn is_local_storage(file_io: &FileIO) -> bool {
        file_io
            .storage()
            .as_any()
            .downcast_ref::<ObjectStorage>()
            .is_none()
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
        self.inner.borrow_mut().handle_commit();
        REGISTRY.with(|r| *r.borrow_mut() = None);
    }

    fn on_abort(&self) {
        self.inner.borrow_mut().handle_abort();
        REGISTRY.with(|r| *r.borrow_mut() = None);
    }

    fn on_commit_sub(&self, current_nest_level: i32) {
        self.inner
            .borrow_mut()
            .handle_commit_sub(current_nest_level);
    }

    fn on_abort_sub(&self, current_nest_level: i32) {
        self.inner.borrow_mut().handle_abort_sub(current_nest_level);
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
