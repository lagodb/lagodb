//! Best-effort exact-file deletion after PostgreSQL commit and lock release.
//!
//! The batch owns the common execution policy used by VACUUM cleanup and
//! canceled transaction-created files. Local WAL-enabled storage records and
//! flushes every representable delete before primary unlink begins. Object
//! storage and WAL-free local storage proceed directly to deletion; remote
//! deletes still run under PostgreSQL's interrupt hold, so they go through
//! [`FileIO::delete_cleanup`], which waits under a deadline without
//! reconnecting.

use std::fmt;

use iceberg_lite::io::FileIO;
use lagodb_core::diag;
use lagodb_core::transaction::cleanup::{
    CleanupTiming, PendingDelete, register_pending_delete,
};
use lagodb_core::wal::flush_wal;

use super::LocalStorage;
use super::local_file_wal::{
    count_delete_files_wal_batches, delete_file_fits_wal, log_delete_files,
};

const MAX_DELETE_FAILURE_WARNINGS: usize = 8;
const MAX_DELETE_PATH_WARNING_CHARS: usize = 512;

#[derive(Clone, Copy, Debug)]
enum DeletePurpose {
    Vacuum,
    CanceledCreatedFiles,
}

impl DeletePurpose {
    fn description(self) -> &'static str {
        match self {
            Self::Vacuum => "Iceberg VACUUM cleanup",
            Self::CanceledCreatedFiles => "Iceberg transaction-created file cleanup",
        }
    }
}

pub(crate) struct PostCommitFileDeleteBatch {
    file_io: FileIO,
    paths: Box<[String]>,
    needs_wal: bool,
    purpose: DeletePurpose,
}

impl fmt::Debug for PostCommitFileDeleteBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostCommitFileDeleteBatch")
            .field("path_count", &self.paths.len())
            .field("needs_wal", &self.needs_wal)
            .field("purpose", &self.purpose)
            .finish()
    }
}

impl PostCommitFileDeleteBatch {
    /// Register post-commit deletion of local files selected by VACUUM and
    /// return the number of bounded `DELETE_FILES` WAL records that will be
    /// emitted when the relation requires WAL.
    pub(crate) fn register_vacuum_files(file_io: FileIO, paths: Vec<String>) -> u64 {
        let Some(batch) = Self::new(file_io, paths, DeletePurpose::Vacuum) else {
            return 0;
        };
        let wal_batches = if batch.needs_wal {
            count_delete_files_wal_batches(batch.paths.iter().map(String::as_str))
        } else {
            0
        };
        register_pending_delete(Box::new(batch));
        wal_batches
    }

    /// Register post-commit deletion of transaction-created data/delete files
    /// that the final Iceberg metadata publication does not reference.
    ///
    /// Registration during pre-commit is safe: the transaction framework
    /// includes newly registered resources in the later commit/abort callbacks
    /// even though they do not receive another pre-commit callback.
    pub(crate) fn register_canceled_files(file_io: FileIO, paths: Vec<String>) {
        if let Some(batch) =
            Self::new(file_io, paths, DeletePurpose::CanceledCreatedFiles)
        {
            register_pending_delete(Box::new(batch));
        }
    }

    fn new(
        file_io: FileIO,
        paths: Vec<String>,
        purpose: DeletePurpose,
    ) -> Option<Self> {
        if paths.is_empty() {
            return None;
        }
        let needs_wal = file_io
            .storage()
            .as_any()
            .downcast_ref::<LocalStorage>()
            .is_some_and(LocalStorage::needs_wal);
        Some(Self {
            file_io,
            paths: paths.into_boxed_slice(),
            needs_wal,
            purpose,
        })
    }

    fn log_local_delete_wal(&self) {
        if !self.needs_wal {
            return;
        }

        let mut unrepresentable = 0_usize;
        let last_lsn = log_delete_files(self.paths.iter().filter_map(|path| {
            if delete_file_fits_wal(path) {
                Some(path.as_str())
            } else {
                unrepresentable += 1;
                None
            }
        }));
        if let Some(lsn) = last_lsn {
            // Flushing the last record also flushes every preceding bounded
            // batch. Primary unlink must not begin before that point.
            flush_wal(lsn);
        }
        if unrepresentable > 0 {
            diag::report_warning(format_args!(
                "post-commit {} has {} path(s) that cannot be represented in \
                 DELETE_FILES WAL; primary deletion will still be attempted",
                self.purpose.description(),
                unrepresentable,
            ));
        }
    }

    fn delete_all(&self) {
        let mut failed = 0_usize;
        for path in &self.paths {
            // Pending deletes run only from transaction-end callbacks.
            if let Err(error) = self.file_io.delete_cleanup(path) {
                failed += 1;
                if failed <= MAX_DELETE_FAILURE_WARNINGS {
                    if path.as_bytes().contains(&0) {
                        diag::report_warning(format_args!(
                            "post-commit {} could not delete a path containing \
                             NUL (path_bytes={}): {}",
                            self.purpose.description(),
                            path.len(),
                            error,
                        ));
                    } else {
                        let purpose = self.purpose.description();
                        diag::report_warning(format_args!(
                            "post-commit {purpose} could not delete \
                             '{path:.max_chars$}' \
                             (path_bytes={path_bytes}): {error}",
                            max_chars = MAX_DELETE_PATH_WARNING_CHARS,
                            path_bytes = path.len(),
                        ));
                    }
                }
            }
        }

        if failed > 0 {
            let succeeded = self.paths.len() - failed;
            let suppressed = failed.saturating_sub(MAX_DELETE_FAILURE_WARNINGS);
            diag::report_warning(format_args!(
                "post-commit {} completed with deletion failures: total={}, \
                 succeeded={}, failed={}, suppressed_failure_warnings={}",
                self.purpose.description(),
                self.paths.len(),
                succeeded,
                failed,
                suppressed,
            ));
        }
    }
}

impl PendingDelete for PostCommitFileDeleteBatch {
    fn execute(&self) {
        self.log_local_delete_wal();
        self.delete_all();
    }

    fn timing(&self) -> CleanupTiming {
        CleanupTiming::OnCommit
    }
}
