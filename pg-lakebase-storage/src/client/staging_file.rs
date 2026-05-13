//! Staging file handle for in-progress writes.

use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::{StorageError, StorageResult};

/// Handle to an in-progress staged write.
///
/// # Client-side semantic contract
///
/// The server does not observe writes to the staging file, so the following rules live here
/// rather than as server-enforced invariants:
///
/// * **Append-only.** The local file is opened with `O_APPEND`, so bytes always land at EOF.
/// * **Single writer.** Only one `StagingFile` should exist for a given `(store_id, bucket,
///   key)` at a time. Duplicates get surfaced by the server on `StageCreate` via `O_EXCL`.
/// * **No readers before commit.** The staged bytes are not referenced anywhere outside the
///   staging tree until the database transaction that owns them commits; it is the caller's
///   responsibility not to read the staged path before `commit`.
///
/// # Drop does not abort
///
/// Dropping a `StagingFile` only closes the local file descriptor. It does not send an `Abort`
/// to the server, because the staging file is intentionally long-lived: a database
/// transaction may keep it around for hours before calling
/// [`super::StorageClient::commit`] / [`super::StorageClient::abort`]. Orphaned staging files
/// left behind by a crashed client are swept by the server's startup `wipe()`.
pub struct StagingFile {
    file: std::fs::File,
    path: PathBuf,
}

impl StagingFile {
    pub(super) fn new(file: std::fs::File, path: PathBuf) -> Self {
        Self { file, path }
    }

    /// Absolute path of the staging file on disk (useful for logging or diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends `data` to the staging file.
    pub fn write(&mut self, data: &[u8]) -> StorageResult<()> {
        self.file.write_all(data).map_err(|error| {
            StorageError::io(
                format!("append to staging file {}", self.path.display()),
                error,
            )
        })
    }

    /// Flushes pending writes and fsyncs the local file. Callers can rely on durability of the
    /// staged bytes before issuing `Commit` by running this first; for most database workloads
    /// the crash-safety of the later backend upload is what actually matters, so `sync` is
    /// optional.
    pub fn sync(&self) -> StorageResult<()> {
        self.file.sync_data().map_err(|error| {
            StorageError::io(
                format!("sync staging file {}", self.path.display()),
                error,
            )
        })
    }
}

impl std::fmt::Debug for StagingFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagingFile")
            .field("path", &self.path)
            .finish()
    }
}
