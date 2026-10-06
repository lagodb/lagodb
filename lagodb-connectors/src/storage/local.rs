//! PostgreSQL-accounted local descriptors and physical file operations.

use std::fs::{File, OpenOptions};
use std::io;
use std::marker::PhantomData;
use std::os::fd::IntoRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::rc::Rc;

use lagodb_storage::{StorageError, StorageResult};
use libc::{S_IWGRP, S_IWOTH, close, mode_t, umask};
use pgrx::pg_sys;

/// Kept on the backend thread, including when an encoder owns the OS file.
pub(super) struct LocalFdReservation(PhantomData<Rc<()>>);

impl LocalFdReservation {
    pub(super) fn acquire() -> StorageResult<Self> {
        // SAFETY: connector storage is opened only on the backend thread.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Ok(Self(PhantomData))
        } else {
            Err(StorageError::resource_exhausted(
                "PostgreSQL external file descriptor budget exhausted",
            ))
        }
    }
}

impl Drop for LocalFdReservation {
    fn drop(&mut self) {
        // SAFETY: acquisition succeeded and the !Send guard stays on its backend.
        unsafe { pg_sys::ReleaseExternalFD() };
    }
}

/// Restore the backend's creation mask even if opening the file unwinds.
struct LocalFileCreationMask(mode_t);

impl LocalFileCreationMask {
    fn install() -> Self {
        // SAFETY: LocalFile::create scopes this process-wide change to the
        // synchronous open, matching PostgreSQL COPY TO's umask policy.
        Self(unsafe { umask(S_IWGRP | S_IWOTH) })
    }
}

impl Drop for LocalFileCreationMask {
    fn drop(&mut self) {
        // SAFETY: restore the mask captured immediately before opening.
        unsafe { umask(self.0) };
    }
}

pub(super) struct LocalFile {
    // Close the descriptor before returning its PostgreSQL reservation.
    pub(super) file: File,
    pub(super) size: u64,
    _reservation: LocalFdReservation,
}

impl LocalFile {
    pub(super) fn open(path: &Path) -> StorageResult<Self> {
        let reservation = LocalFdReservation::acquire()?;
        let file = File::open(path).map_err(|error| {
            StorageError::io(format!("open local file {}", path.display()), error)
        })?;
        let metadata = file.metadata()?;
        // Match PostgreSQL COPY: opening a FIFO may wait for its producer.
        // Sequential formats can consume it, but directories are not inputs.
        if metadata.is_dir() {
            return Err(StorageError::invalid_path(
                "local format input is a directory",
            ));
        }
        Ok(Self {
            file,
            size: metadata.len(),
            _reservation: reservation,
        })
    }

    pub(super) fn create(
        path: &Path,
        exclusive: bool,
    ) -> StorageResult<(File, LocalFdReservation)> {
        let reservation = LocalFdReservation::acquire()?;
        let mut options = OpenOptions::new();
        // Match fopen's creation mode; the scoped COPY mask below determines
        // new-file permissions without changing an existing file's mode.
        options.write(true).mode(0o666);
        if exclusive {
            options.create_new(true);
        } else {
            options.create(true).truncate(true);
        }
        let opened = {
            let _mask = LocalFileCreationMask::install();
            options.open(path)
        };
        let file = opened.map_err(|error| {
            StorageError::io(format!("create local file {}", path.display()), error)
        })?;
        Ok((file, reservation))
    }

    /// Complete local output with a checked close, as PostgreSQL COPY does.
    /// The publication retains the FD reservation until this returns.
    pub(super) fn close(file: File) -> StorageResult<()> {
        let fd = file.into_raw_fd();
        // SAFETY: ownership was transferred out of File, so close is attempted
        // exactly once and Drop cannot close the descriptor again. Like PG's
        // FreeDesc, do not retry close after an error: the FD can already have
        // been released, including on EINTR on Linux.
        if unsafe { close(fd) } == 0 {
            Ok(())
        } else {
            Err(StorageError::io(
                "close local output file",
                io::Error::last_os_error(),
            ))
        }
    }
}
