//! PostgreSQL allocations whose lifetime follows a Rust-owned codec.

use std::ffi::CStr;

use pgrx::pg_sys;

/// Mutation buffers live on the Rust heap and can be constructed while a
/// per-tuple context is current. Their cached PG state must survive resets of
/// that context. Existing executor/resource-owner cleanup drops the codec and
/// therefore this independent child of TopMemoryContext on success and ERROR.
pub(super) struct CodecContext(pg_sys::MemoryContext);

impl CodecContext {
    pub(super) fn new(name: &'static CStr) -> Self {
        Self(unsafe {
            pg_sys::AllocSetContextCreateExtended(
                pg_sys::TopMemoryContext,
                name.as_ptr(),
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            )
        })
    }

    pub(super) fn as_ptr(&self) -> pg_sys::MemoryContext {
        self.0
    }

    pub(super) fn with<R>(&self, allocate: impl FnOnce() -> R) -> R {
        let _restore = RestoreContext(unsafe { pg_sys::CurrentMemoryContext });
        unsafe { pg_sys::CurrentMemoryContext = self.0 };
        allocate()
    }
}

/// pgrx's switch_to restores only on normal return. A Drop guard also restores
/// during guarded PG ERROR unwinding, before CodecContext can be deleted.
struct RestoreContext(pg_sys::MemoryContext);

impl Drop for RestoreContext {
    fn drop(&mut self) {
        unsafe { pg_sys::CurrentMemoryContext = self.0 };
    }
}

impl Drop for CodecContext {
    fn drop(&mut self) {
        unsafe { pg_sys::MemoryContextDelete(self.0) };
    }
}
