use std::path::{Path, PathBuf};

use iceberg_lite::io::FileIO;
use lagodb_core::storage::service::BackendStorageService;
use lagodb_storage::{ObjectLocation, StorageErrorKind};

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
                lagodb_core::diag::report_warning(format_args!(
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
                        lagodb_core::diag::report_warning(format_args!(
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
                            lagodb_core::diag::report_warning(format_args!(
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
        };
        (!cleaned).then_some(self)
    }

    fn unlink_file(path: &Path) -> bool {
        match std::fs::remove_file(path) {
            Ok(()) => true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => {
                lagodb_core::diag::report_warning(format_args!(
                    "failed to unlink '{}': {}",
                    path.display(),
                    error
                ));
                false
            }
        }
    }
}
