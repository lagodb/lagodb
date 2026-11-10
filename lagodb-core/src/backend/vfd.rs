//! Positioned I/O on a PostgreSQL virtual file descriptor.
//!
//! PostgreSQL 16 changed `FileRead`/`FileWrite` to take `void *` buffers and
//! `size_t` lengths, and PostgreSQL 17 widened their result from `int` to
//! `ssize_t`. This handle keeps those differences out of VFD owners.

#[cfg(feature = "pg15")]
use std::ffi::c_int;

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
