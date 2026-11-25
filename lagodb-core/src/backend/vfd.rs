//! Positioned I/O on a PostgreSQL virtual file descriptor.
//!
//! PostgreSQL 16 changed `FileRead`/`FileWrite` to take `void *` buffers and
//! `size_t` lengths, and PostgreSQL 17 widened their result from `int` to
//! `ssize_t`. This handle keeps those differences out of VFD owners.

use std::ffi::CStr;
#[cfg(feature = "pg15")]
use std::ffi::c_int;
use std::marker::PhantomData;
use std::rc::Rc;

use pgrx::pg_sys;

/// A VFD borrowed from its owner, which keeps the file open while this handle
/// is used.
#[derive(Debug, Clone, Copy)]
pub struct VfdHandle(pg_sys::File);

impl VfdHandle {
    /// Before PostgreSQL 16 one call moves at most `c_int::MAX` bytes. Both
    /// calls may transfer fewer bytes than requested, so a shorter request is
    /// within their contract.
    #[cfg(feature = "pg15")]
    const MAX_TRANSFER: usize = c_int::MAX as usize;

    /// # Safety
    ///
    /// `file` must be an open VFD that stays open while the handle is used.
    #[inline]
    pub const unsafe fn from_raw(file: pg_sys::File) -> Self {
        Self(file)
    }

    /// `FileRead`: the number of bytes read, or a negative value with `errno`
    /// set.
    pub fn read_at(
        self,
        buffer: &mut [u8],
        offset: pg_sys::off_t,
        wait_event: u32,
    ) -> isize {
        // SAFETY: `buffer` is writable for the requested length, and the
        // caller of `from_raw` keeps the VFD open.
        unsafe {
            #[cfg(feature = "pg15")]
            let read = pg_sys::FileRead(
                self.0,
                buffer.as_mut_ptr().cast(),
                buffer.len().min(Self::MAX_TRANSFER) as c_int,
                offset,
                wait_event,
            );
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let read = pg_sys::FileRead(
                self.0,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                offset,
                wait_event,
            );
            // Before PostgreSQL 17 the result is `int`; widening is lossless.
            #[cfg(any(feature = "pg15", feature = "pg16"))]
            let read = read as isize;
            read
        }
    }

    /// `FileWrite`: the number of bytes written, or a negative value with
    /// `errno` set.
    pub fn write_at(
        self,
        bytes: &[u8],
        offset: pg_sys::off_t,
        wait_event: u32,
    ) -> isize {
        // SAFETY: `bytes` is readable for the requested length; PostgreSQL 15
        // declares the buffer non-const but only reads it. The caller of
        // `from_raw` keeps the VFD open.
        unsafe {
            #[cfg(feature = "pg15")]
            let written = pg_sys::FileWrite(
                self.0,
                bytes.as_ptr().cast_mut().cast(),
                bytes.len().min(Self::MAX_TRANSFER) as c_int,
                offset,
                wait_event,
            );
            #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
            let written = pg_sys::FileWrite(
                self.0,
                bytes.as_ptr().cast(),
                bytes.len(),
                offset,
                wait_event,
            );
            // Before PostgreSQL 17 the result is `int`; widening is lossless.
            #[cfg(any(feature = "pg15", feature = "pg16"))]
            let written = written as isize;
            written
        }
    }
}

/// Exclusive backend-local owner of a PostgreSQL virtual file descriptor
/// opened with `PathNameOpenFile`.
///
/// Owns only the VFD lifecycle (`FileClose` on `Drop`) and thin positioned I/O
/// operations; callers attach path diagnostics and WAL policy at their own
/// layer. Unlike `OpenTemporaryFile`, `PathNameOpenFile` does not register the
/// VFD with a `ResourceOwner`, so `Drop` always closes it.
///
/// `pg_sys::File` is a plain integer index into backend-local `VfdCache` state
/// in `fd.c`, so `_backend_thread` intentionally makes `OwnedVfd` `!Send + !Sync`
/// instead of letting the compiler auto-derive `Send + Sync` from the integer.
#[derive(Debug)]
pub struct OwnedVfd {
    file: pg_sys::File,
    _backend_thread: PhantomData<Rc<()>>,
}

impl OwnedVfd {
    /// Open `c_path` with `flags` via PostgreSQL's `PathNameOpenFile`.
    /// Returns `None` on failure with OS `errno` preserved.
    pub fn open(c_path: &CStr, flags: i32) -> Option<Self> {
        // SAFETY: `c_path` is a valid NUL-terminated path and `flags` are valid
        // open flags accepted by PostgreSQL's VFD layer on the backend thread.
        let file = unsafe { pg_sys::PathNameOpenFile(c_path.as_ptr(), flags) };
        if file < 0 {
            None
        } else {
            Some(Self {
                file,
                _backend_thread: PhantomData,
            })
        }
    }

    #[inline]
    pub fn size(&self) -> pg_sys::off_t {
        // SAFETY: `self.file` is a valid open VFD owned by `Self`.
        unsafe { pg_sys::FileSize(self.file) }
    }

    #[inline]
    pub fn read_at(
        &self,
        buf: &mut [u8],
        offset: pg_sys::off_t,
        wait_event: u32,
    ) -> isize {
        // SAFETY: `self` keeps the VFD open for the duration of the call.
        unsafe { VfdHandle::from_raw(self.file) }.read_at(buf, offset, wait_event)
    }

    #[inline]
    pub fn write_at(
        &self,
        buf: &[u8],
        offset: pg_sys::off_t,
        wait_event: u32,
    ) -> isize {
        // SAFETY: `self` keeps the VFD open for the duration of the call.
        unsafe { VfdHandle::from_raw(self.file) }.write_at(buf, offset, wait_event)
    }

    #[inline]
    pub fn sync(&self, wait_event: u32) -> i32 {
        // SAFETY: `self.file` is a valid open VFD owned by `Self`.
        unsafe { pg_sys::FileSync(self.file, wait_event) }
    }
}

impl Drop for OwnedVfd {
    fn drop(&mut self) {
        // SAFETY: `OwnedVfd` is constructed only from a non-negative VFD
        // returned by `PathNameOpenFile` on this backend thread and closes it
        // at most once.
        unsafe {
            pg_sys::FileClose(self.file);
        }
    }
}

/// Backend-thread-scoped reservation for one non-VFD OS descriptor (such as a
/// `std::fs::ReadDir` directory handle) accounted against PostgreSQL's
/// `AcquireExternalFD` / `ReleaseExternalFD` budget.
///
/// `PhantomData<Rc<()>>` intentionally makes the guard `!Send + !Sync` so
/// `AcquireExternalFD` and `ReleaseExternalFD` remain paired on the same
/// backend thread.
#[must_use = "if unused, the external FD reservation is released immediately"]
#[derive(Debug)]
pub struct ExternalFdReservation(PhantomData<Rc<()>>);

impl ExternalFdReservation {
    pub fn try_acquire() -> Option<Self> {
        // SAFETY: `!Send` keeps construction and drop on the owning backend
        // thread, matching `fd.c` backend-local accounting. Construct `Self`
        // only in the `true` branch so a failed `AcquireExternalFD` never drops
        // a guard or calls `ReleaseExternalFD`.
        if unsafe { pg_sys::AcquireExternalFD() } {
            Some(Self(PhantomData))
        } else {
            None
        }
    }
}

impl Drop for ExternalFdReservation {
    fn drop(&mut self) {
        // SAFETY: constructed only after `AcquireExternalFD` succeeded on this
        // backend thread.
        unsafe { pg_sys::ReleaseExternalFD() };
    }
}
