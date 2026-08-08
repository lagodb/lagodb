use std::path::{Path, PathBuf};

use iceberg_lite::io::FileIO;
use pg_lakebase_core::storage::service::BackendStorageService;
use pg_lakebase_core::wal::flush_wal;
use pg_lakebase_storage::{ObjectLocation, StorageErrorKind};

use crate::storage::LocalStorage;
use crate::wal::record::log_delete_directory;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObjectFileState {
    Staged,
    Uploaded,
}

pub(super) enum StorageResource {
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
        service: BackendStorageService,
        state: ObjectFileState,
    },
}

impl std::fmt::Debug for StorageResource {
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

impl StorageResource {
    pub(super) fn on_commit(self) {
        match self {
            Self::DroppedLocalTableRoot {
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
                if let Err(error) = file_io.remove_dir_all(location) {
                    pg_lakebase_core::diag::report_warning(format_args!(
                        "failed to delete table directory '{}': {}",
                        location, error
                    ));
                }
            }
            Self::ObjectFile {
                ref staging_path,
                state: ObjectFileState::Uploaded,
                ..
            } => {
                let _ = Self::unlink_file(staging_path);
            }
            Self::ObjectFile {
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
                let _ = Self::unlink_file(staging_path);
            }
            _ => {}
        }
    }

    pub(super) fn on_abort(self) -> Option<Self> {
        let cleaned = match &self {
            Self::CreatedLocalFile { path } => Self::unlink_file(path),
            Self::CreatedTableDir { location, file_io } => {
                match file_io.remove_dir_all(location) {
                    Ok(()) => true,
                    Err(error) => {
                        pg_lakebase_core::diag::report_warning(format_args!(
                            "failed to delete table directory '{}': {}",
                            location, error
                        ));
                        false
                    }
                }
            }
            Self::ObjectFile {
                location,
                staging_path,
                service,
                state,
            } => {
                let remote_deleted = if *state == ObjectFileState::Uploaded {
                    match service.delete(location.bucket(), location.key()) {
                        Ok(()) => true,
                        Err(error) if error.kind() == StorageErrorKind::NotFound => {
                            true
                        }
                        Err(error) => {
                            pg_lakebase_core::diag::report_warning(format_args!(
                                "failed to delete uploaded object '{}': {}",
                                location, error
                            ));
                            false
                        }
                    }
                } else {
                    true
                };
                let staging_unlinked = Self::unlink_file(staging_path);
                remote_deleted && staging_unlinked
            }
            Self::DroppedLocalTableRoot { .. } => true,
        };
        (!cleaned).then_some(self)
    }

    fn local_needs_wal(file_io: &FileIO) -> bool {
        file_io
            .storage()
            .as_any()
            .downcast_ref::<LocalStorage>()
            .map(LocalStorage::needs_wal)
            .unwrap_or(false)
    }

    fn unlink_file(path: &Path) -> bool {
        match std::fs::remove_file(path) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => {
                pg_lakebase_core::diag::report_warning(format_args!(
                    "failed to unlink '{}': {}",
                    path.display(),
                    error
                ));
                false
            }
        }
    }
}
