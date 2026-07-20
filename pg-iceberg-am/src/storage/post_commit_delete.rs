//! Best-effort deletion of independent exact files after PostgreSQL commit.
//!
//! The batch owns the common execution policy used by VACUUM cleanup and by
//! transaction-created files canceled by the final metadata action. Local
//! WAL-enabled storage records and flushes every representable delete before
//! primary unlink begins. Object storage and WAL-free local storage proceed
//! directly to deletion.

use std::fmt;

use iceberg_lite::io::FileIO;
use pg_lakebase_core::diag;
use pg_lakebase_core::transaction::cleanup::{
    CleanupTiming, PendingDelete, register_pending_delete,
};
use pg_lakebase_core::wal::flush_wal;

use super::LocalStorage;
use crate::wal::record::{delete_file_fits_wal, log_delete_files};

const MAX_DELETE_FAILURE_WARNINGS: usize = 8;
const MAX_DELETE_PATH_WARNING_CHARS: usize = 512;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PostCommitDeletePurpose {
    Vacuum,
    CanceledCreatedFiles,
}

impl PostCommitDeletePurpose {
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
    purpose: PostCommitDeletePurpose,
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
    pub(crate) fn register(
        file_io: FileIO,
        paths: Vec<String>,
        purpose: PostCommitDeletePurpose,
    ) {
        if paths.is_empty() {
            return;
        }
        let needs_wal = file_io
            .storage()
            .as_any()
            .downcast_ref::<LocalStorage>()
            .is_some_and(LocalStorage::needs_wal);
        register_pending_delete(Box::new(Self {
            file_io,
            paths: paths.into_boxed_slice(),
            needs_wal,
            purpose,
        }));
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
            if let Err(error) = self.file_io.delete(path) {
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
