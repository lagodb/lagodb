//! Local publication paired with the existing format encoder lifecycle.

use std::fs::{self, File};
use std::io;
use std::path::PathBuf;

use lagodb_core::diag::report_warning;
use lagodb_core::transaction::cleanup::{PendingDelete, register_pending_delete};
use lagodb_storage::{StorageError, StorageResult};
use uuid::Uuid;

use super::local::{LocalFdReservation, LocalFile};

pub(crate) struct LocalPublication {
    path: PathBuf,
    staging_path: Option<PathBuf>,
    // The Send-safe encoder owns the file; the backend owns its accounting.
    _reservation: LocalFdReservation,
}

impl LocalPublication {
    pub(super) fn start(
        path: PathBuf,
        delete_on_abort: bool,
    ) -> StorageResult<(File, Self)> {
        if !delete_on_abort {
            // Exact COPY TO follows PostgreSQL's truncate/overwrite semantics,
            // including symlinks and a partial file on statement failure.
            let (file, reservation) = LocalFile::create(&path, false)?;
            return Ok((
                file,
                Self {
                    path,
                    staging_path: None,
                    _reservation: reservation,
                },
            ));
        }
        let parent = path
            .parent()
            .expect("generated local output has a directory");
        fs::create_dir_all(parent)?;
        let staging_path = parent.join(format!(".lagodb-{}.tmp", Uuid::now_v7()));
        let (file, reservation) = LocalFile::create(&staging_path, true)?;
        Ok((
            file,
            Self {
                path,
                staging_path: Some(staging_path),
                _reservation: reservation,
            },
        ))
    }

    pub(super) fn finish(mut self) -> StorageResult<()> {
        let Some(staging_path) = &self.staging_path else {
            return Ok(());
        };
        // Both paths are in the same directory. A hard link publishes a
        // complete file atomically and cannot replace an existing filename.
        fs::hard_link(staging_path, &self.path).map_err(|error| {
            StorageError::io(
                format!("publish local file {}", self.path.display()),
                error,
            )
        })?;
        register_pending_delete(Box::new(LocalFileDelete {
            path: self.path.clone(),
        }));
        if let Err(error) = self.remove_staging() {
            report_warning(format_args!(
                "local file publication succeeded but staging cleanup failed: {error}"
            ));
        }
        Ok(())
    }

    fn remove_staging(&mut self) -> io::Result<()> {
        if let Some(path) = &self.staging_path {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            self.staging_path = None;
        }
        Ok(())
    }
}

impl Drop for LocalPublication {
    fn drop(&mut self) {
        if let Err(error) = self.remove_staging() {
            report_warning(format_args!(
                "failed to remove abandoned local staging file: {error}"
            ));
        }
    }
}

#[derive(Debug)]
struct LocalFileDelete {
    path: PathBuf,
}

impl PendingDelete for LocalFileDelete {
    fn execute(&self) {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => report_warning(format_args!(
                "failed to delete transaction-created local file {} during rollback: {error}",
                self.path.display(),
            )),
        }
    }
}
