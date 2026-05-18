use pgrx::{fcinfo, pg_sys};
use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr::NonNull;

/// Owns a PostgreSQL `palloc`/`pstrdup` allocated `cstring`.
///
/// PostgreSQL type output functions return `cstring` values allocated in the
/// current memory context. This guard keeps that ownership explicit and frees
/// the string after Rust has copied or formatted it.
#[derive(Debug)]
pub(crate) struct PgOutputCString {
    ptr: NonNull<c_char>,
}

impl PgOutputCString {
    /// Calls a PostgreSQL function that returns an owned `cstring`.
    ///
    /// # Safety
    ///
    /// `func` must return either NULL or a `cstring` allocated by PostgreSQL
    /// memory management. The returned guard must not outlive the memory
    /// context that owns the allocation.
    pub(crate) unsafe fn from_function_call(
        func: unsafe fn(pg_sys::FunctionCallInfo) -> pg_sys::Datum,
        args: &[Option<pg_sys::Datum>],
    ) -> Option<Self> {
        let datum = unsafe { fcinfo::direct_function_call_as_datum(func, args)? };
        Some(unsafe { Self::from_datum(datum) })
    }

    /// # Safety
    ///
    /// `datum` must be a non-null pointer to a NUL-terminated string allocated
    /// by PostgreSQL memory management.
    unsafe fn from_datum(datum: pg_sys::Datum) -> Self {
        let ptr = NonNull::new(datum.cast_mut_ptr::<c_char>())
            .expect("PostgreSQL output function returned a null cstring");
        Self { ptr }
    }

    pub(crate) fn as_cstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.ptr.as_ptr()) }
    }
}

impl Drop for PgOutputCString {
    fn drop(&mut self) {
        unsafe {
            pg_sys::pfree(self.ptr.as_ptr().cast());
        }
    }
}
