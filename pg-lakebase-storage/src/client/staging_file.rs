//! Staging file handle for in-progress writes.

use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use crate::backend::BackendDataIdentity;
use crate::error::{StorageError, StorageResult};
use crate::object::ObjectLocation;
use crate::staging::StagingPathResolver;

use super::{ExternalFdLease, ExternalFdPolicy};

/// Handle to an in-progress staged write.
///
/// # Constructing one
///
/// The database creates a staging file directly through the filesystem using
/// [`StagingFile::create`], which atomically creates an empty file under the staging tree
/// derived from a [`StagingPathResolver`]. There is no server round-trip for staging file
/// creation — the database owns the staging directory and the server is not in the data path
/// for writes.
///
/// # Client-side semantic contract
///
/// The server does not observe writes to the staging file, so the following rules live here
/// rather than as server-enforced invariants:
///
/// * **Append-only.** The local file is opened with `O_APPEND`, so bytes always land at EOF.
/// * **Single writer.** Only one `StagingFile` should exist for a given physical
///   `(backend identity, bucket, key)` at a time. Duplicate creates surface as `Busy` because [`StagingFile::create`]
///   uses `O_CREAT | O_EXCL`.
/// * **No readers before upload/publication.** The staged bytes are not referenced anywhere
///   outside the staging tree until the caller uploads them and publishes metadata through its
///   own transaction; it is the caller's responsibility not to read the staged path before then.
///
/// # Drop does not delete the staging file
///
/// Dropping a `StagingFile` only closes the local file descriptor. It does not unlink the
/// staging file and it does not contact the server, because the staging file is intentionally
/// long-lived: a database transaction may keep it around for hours before calling
/// [`super::StorageClient::upload`]. Cleanup is the caller's responsibility — the database
/// owns the staging directory and removes individual files when its transaction succeeds,
/// fails, or is rolled forward through crash recovery.
pub struct StagingFile {
    // Drop the OS descriptor before releasing its accounting lease.
    file: std::fs::File,
    _fd_lease: Option<Box<dyn ExternalFdLease>>,
    path: PathBuf,
}

impl StagingFile {
    /// Atomically creates an empty staging file for one physical object and returns a
    /// handle for appending bytes.
    ///
    /// The path is derived from `resolver` (a [`StagingPathResolver`] rooted at the same
    /// `cache_dir` the database passed to the storage server). The file is opened with
    /// `O_CREAT | O_EXCL | O_APPEND | O_CLOEXEC`: the kernel guarantees the create is atomic
    /// even across concurrent writers, so two callers racing on the same key cannot both
    /// succeed — the loser observes [`StorageErrorKind::Busy`](crate::error::StorageErrorKind::Busy).
    /// `O_APPEND` enforces the documented append-only contract.
    pub fn create(
        resolver: &StagingPathResolver,
        backend_identity: &BackendDataIdentity,
        bucket: &str,
        key: &str,
    ) -> StorageResult<Self> {
        Self::create_inner(resolver, backend_identity, bucket, key, None)
    }

    /// Creates a staging file while accounting for its descriptor through the
    /// embedding runtime.
    ///
    /// The reservation is acquired before opening the file and remains owned by
    /// the returned `StagingFile`. An open failure releases it immediately.
    pub fn create_with_fd_policy(
        resolver: &StagingPathResolver,
        backend_identity: &BackendDataIdentity,
        bucket: &str,
        key: &str,
        fd_policy: &dyn ExternalFdPolicy,
    ) -> StorageResult<Self> {
        let fd_lease = fd_policy.acquire()?;
        Self::create_inner(resolver, backend_identity, bucket, key, Some(fd_lease))
    }

    fn create_inner(
        resolver: &StagingPathResolver,
        backend_identity: &BackendDataIdentity,
        bucket: &str,
        key: &str,
        fd_lease: Option<Box<dyn ExternalFdLease>>,
    ) -> StorageResult<Self> {
        let location = ObjectLocation::new(backend_identity.clone(), bucket, key)?;
        let path = resolver.path_for(&location)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                StorageError::io(
                    format!(
                        "failed to create staging parent directory {}",
                        parent.display()
                    ),
                    error,
                )
            })?;
        }

        let file = OpenOptions::new()
            .append(true)
            .read(false)
            .create_new(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(&path)
            .map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    StorageError::busy(format!(
                        "staging file for {location} already exists; remove it before re-staging"
                    ))
                } else {
                    StorageError::io(
                        format!("failed to create staging file {}", path.display()),
                        error,
                    )
                }
            })?;
        Ok(Self {
            file,
            _fd_lease: fd_lease,
            path,
        })
    }

    /// Absolute path of the staging file on disk. The caller (database) records this so it can
    /// later issue an `Upload` for the same physical object and unlink the file once
    /// the surrounding transaction settles.
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
    /// staged bytes before issuing `Upload` by running this first; for most database workloads
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
