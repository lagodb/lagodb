//! PostgreSQL temporary files with independent positioned access.
//!
//! `BufFile` keeps one shared position, so it cannot serve several readers of
//! one file. This owner uses the layer below it, the temporary-file VFD, which
//! keeps `temp_file_limit`, `temp_tablespaces`, `log_temp_files`,
//! `pg_stat_database.temp_bytes`, ResourceOwner cleanup and backend-exit
//! deletion. It reports I/O the way `buffile.c` does, because `FileRead` and
//! `FileWrite` leave the `temp_blks_*` counters to their caller.

#[cfg(feature = "pg15")]
use std::ffi::c_long;
use std::io::{self, ErrorKind};
use std::marker::PhantomData;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use pgrx::{PgSqlErrorCode, pg_sys};

use super::VfdHandle;
use crate::diag::PgReportError;
use crate::resource::pg_cleanup_suppressed;

/// One temporary file owned by the current ResourceOwner.
///
/// `FD_DELETE_AT_CLOSE` makes closing the file delete it, and PostgreSQL holds
/// no fsync obligation for temporary files.
pub struct PgTempFile {
    file: pg_sys::File,
    _backend_thread: PhantomData<Rc<()>>,
}

impl PgTempFile {
    /// Create an empty file in the next configured temporary tablespace.
    pub fn create() -> Result<Self, PgReportError> {
        // SAFETY: called on the backend thread inside a transaction, the
        // context in which executor nodes create their temporary files.
        let file = PgReportError::capture(|| unsafe {
            pg_sys::PrepareTempTablespaces();
            pg_sys::OpenTemporaryFile(false)
        })?;
        Ok(Self {
            file,
            _backend_thread: PhantomData,
        })
    }

    /// Read up to `buffer.len()` bytes at `offset`, returning the count read.
    /// Zero means `offset` is at or past the written end of the file.
    pub fn read_at(
        &self,
        buffer: &mut [u8],
        offset: u64,
    ) -> Result<usize, PgReportError> {
        PgReportError::capture(|| {
            let started = Self::io_timer();
            let read = self.vfd().read_at(
                buffer,
                offset as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_BUFFILE_READ,
            );
            if read < 0 {
                return Err(Self::io_error("read from", io::Error::last_os_error()));
            }
            let read = read as usize;
            let elapsed = started.map(|started| started.elapsed());
            Self::account_read(read, elapsed);
            Ok(read)
        })?
    }

    /// Write all of `bytes` at `offset`.
    pub fn write_at(&self, bytes: &[u8], offset: u64) -> Result<(), PgReportError> {
        PgReportError::capture(|| {
            let started = Self::io_timer();
            let mut remaining = bytes;
            let mut position = offset;
            while !remaining.is_empty() {
                let written = self.vfd().write_at(
                    remaining,
                    position as pg_sys::off_t,
                    pg_sys::WaitEventIO::WAIT_EVENT_BUFFILE_WRITE,
                );
                if written <= 0 {
                    return Err(Self::io_error(
                        "write to",
                        io::Error::last_os_error(),
                    ));
                }
                let written = written as usize;
                remaining = &remaining[written..];
                position += written as u64;
            }
            let elapsed = started.map(|started| started.elapsed());
            Self::account_write(bytes.len(), elapsed);
            Ok(())
        })?
    }

    #[inline]
    fn vfd(&self) -> VfdHandle {
        // SAFETY: this owner keeps the VFD open until Drop.
        unsafe { VfdHandle::from_raw(self.file) }
    }

    fn io_timer() -> Option<Instant> {
        // SAFETY: backend-local GUC read on the backend thread.
        unsafe { pg_sys::track_io_timing }.then(Instant::now)
    }

    fn account_read(bytes: usize, elapsed: Option<Duration>) {
        // SAFETY: backend-local instrumentation updated on the backend thread.
        unsafe {
            pg_sys::pgBufferUsage.temp_blks_read += Self::blocks(bytes);
            if let Some(elapsed) = elapsed {
                Self::add_io_time(
                    &raw mut pg_sys::pgBufferUsage.temp_blk_read_time,
                    elapsed,
                );
            }
        }
    }

    fn account_write(bytes: usize, elapsed: Option<Duration>) {
        // SAFETY: backend-local instrumentation updated on the backend thread.
        unsafe {
            pg_sys::pgBufferUsage.temp_blks_written += Self::blocks(bytes);
            if let Some(elapsed) = elapsed {
                Self::add_io_time(
                    &raw mut pg_sys::pgBufferUsage.temp_blk_write_time,
                    elapsed,
                );
            }
        }
    }

    /// `INSTR_TIME_ADD` of `elapsed`. PostgreSQL 16 counts nanosecond ticks;
    /// PostgreSQL 15 keeps a normalized `timespec`.
    ///
    /// # Safety
    ///
    /// `time` points to a backend-local `instr_time` that nothing else
    /// accesses during the call.
    unsafe fn add_io_time(time: *mut pg_sys::instr_time, elapsed: Duration) {
        // SAFETY: guaranteed by the caller. The binding names `time_t`
        // differently per platform (`__time_t` on glibc, `__darwin_time_t` on
        // macOS), but both define it as `long`; POSIX declares `tv_nsec` as
        // `long`.
        #[cfg(feature = "pg15")]
        unsafe {
            (*time).tv_sec += elapsed.as_secs() as c_long;
            (*time).tv_nsec += c_long::from(elapsed.subsec_nanos());
            while (*time).tv_nsec >= 1_000_000_000 {
                (*time).tv_nsec -= 1_000_000_000;
                (*time).tv_sec += 1;
            }
        }
        // SAFETY: guaranteed by the caller.
        #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
        unsafe {
            (*time).ticks += elapsed.as_nanos() as i64;
        }
    }

    /// Blocks `buffile.c` would load or dump to move `bytes` from an empty
    /// buffer. It counts every partial buffer it flushes, so each transfer
    /// counts on its own; callers gather small writes the way its buffer does.
    /// A transfer a caller splits without a flush can count one extra block.
    fn blocks(bytes: usize) -> i64 {
        bytes.div_ceil(pg_sys::BLCKSZ as usize) as i64
    }

    fn io_error(operation: &str, error: io::Error) -> PgReportError {
        let code = match error.kind() {
            ErrorKind::StorageFull => PgSqlErrorCode::ERRCODE_DISK_FULL,
            _ => PgSqlErrorCode::ERRCODE_IO_ERROR,
        };
        PgReportError::from_message(
            code,
            format!("could not {operation} temporary file: {error}"),
        )
    }
}

impl Drop for PgTempFile {
    fn drop(&mut self) {
        // The ResourceOwner closes the file on abort. It does so before the
        // owner-release callback that drops abandoned query state, and during
        // an unwind the ERROR's abort has not reached it yet, so closing here
        // would release the VFD twice in the first case and call into
        // PostgreSQL mid-unwind in the second.
        if thread::panicking() || pg_cleanup_suppressed() {
            return;
        }
        // SAFETY: this owner holds the only reference to a VFD that its
        // ResourceOwner has not released.
        unsafe { pg_sys::FileClose(self.file) };
    }
}
