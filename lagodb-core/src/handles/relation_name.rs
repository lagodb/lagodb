//! A relation name copied from PostgreSQL's mutable relcache.

use std::ffi::{CStr, c_char};

use pgrx::pg_sys;

/// An owned PostgreSQL relation name with fixed-capacity inline storage.
///
/// The name preserves server-encoding bytes without allocating and remains
/// valid across PostgreSQL calls, relcache rebuilds, and relation closure.
#[derive(Clone, Copy, Debug)]
pub struct RelationName {
    data: [c_char; pg_sys::NAMEDATALEN as usize],
}

impl RelationName {
    /// Copy a PostgreSQL name without retaining a borrow of its storage.
    ///
    /// # Safety
    ///
    /// `name` must point to a valid, fully initialized, NUL-terminated
    /// `NameData` that remains readable for this call.
    #[inline]
    pub(super) unsafe fn from_raw(name: *const pg_sys::NameData) -> Self {
        Self {
            // SAFETY: the caller guarantees that the full NameData is readable.
            data: unsafe { (*name).data },
        }
    }

    #[inline]
    pub fn as_c_str(&self) -> &CStr {
        // SAFETY: construction copies a NUL-terminated NameData. The private
        // array is owned by self and cannot change during this borrow.
        unsafe { CStr::from_ptr(self.data.as_ptr()) }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.as_c_str().to_bytes()
    }
}
