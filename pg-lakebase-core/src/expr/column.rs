//! Stage-neutral PostgreSQL column metadata helpers.

use core::ffi::{CStr, c_void};

use pgrx::pg_sys;

/// Resolve a relation attribute number to its PostgreSQL catalog name.
///
/// The resolver is shared by plan-time column metadata collection and
/// provider-side translation. It does not own relation or catalog state.
#[derive(Debug, Clone, Copy)]
pub struct ColumnNameResolver {
    rel_oid: pg_sys::Oid,
}

impl ColumnNameResolver {
    #[inline]
    pub fn new(rel_oid: pg_sys::Oid) -> Self {
        Self { rel_oid }
    }

    /// Resolve an attribute number to its plan-time column name.
    pub fn resolve(self, attno: pg_sys::AttrNumber) -> Option<String> {
        self.try_resolve(attno).ok().flatten()
    }

    /// Checked variant of resolve that preserves invalid-UTF8 diagnostics.
    pub fn try_resolve(
        self,
        attno: pg_sys::AttrNumber,
    ) -> Result<Option<String>, core::str::Utf8Error> {
        if attno <= 0 {
            return Ok(None);
        }

        // SAFETY: get_attname accepts any OID and returns NULL for a missing
        // row when missing_ok is true. Its result is palloc'd in the current
        // memory context and is copied before being freed.
        let raw = unsafe {
            pg_sys::get_attname(self.rel_oid, attno, /*missing_ok=*/ true)
        };
        if raw.is_null() {
            return Ok(None);
        }

        // SAFETY: raw is a non-null NUL-terminated string returned by
        // get_attname.
        let name = unsafe { CStr::from_ptr(raw) }
            .to_str()
            .map(|value| value.to_owned());
        // SAFETY: raw was allocated by PostgreSQL for the current memory
        // context and has not been freed yet.
        unsafe { pg_sys::pfree(raw.cast::<c_void>()) };
        name.map(Some)
    }
}
