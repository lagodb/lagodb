//! Borrowed index column values passed to build and insert callbacks.

use std::slice;

use pgrx::pg_sys;

/// One entry per index column, `INCLUDE` columns included. A value has the
/// opclass input type, not necessarily the index attribute's type
/// (`ConstructTupleDescriptor` stores `opckeytype` there), so the view
/// carries no type metadata.
#[derive(Clone, Copy)]
pub struct IndexValues<'a> {
    values: &'a [pg_sys::Datum],
    nulls: &'a [bool],
}

impl<'a> IndexValues<'a> {
    /// # Safety
    ///
    /// Both arrays must hold at least `len` initialized entries that stay
    /// valid for `'a`.
    #[inline]
    pub(crate) unsafe fn from_raw(
        values: *const pg_sys::Datum,
        nulls: *const bool,
        len: usize,
    ) -> Self {
        unsafe {
            Self {
                values: slice::from_raw_parts(values, len),
                nulls: slice::from_raw_parts(nulls, len),
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// `None` is SQL NULL.
    #[inline]
    pub fn value(&self, column: usize) -> Option<pg_sys::Datum> {
        (!self.nulls[column]).then_some(self.values[column])
    }

    #[inline]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<pg_sys::Datum>> + 'a {
        let nulls = self.nulls;
        self.values
            .iter()
            .zip(nulls)
            .map(|(value, is_null)| (!is_null).then_some(*value))
    }

    #[inline]
    pub(crate) fn as_ptrs(&self) -> (*const pg_sys::Datum, *const bool) {
        (self.values.as_ptr(), self.nulls.as_ptr())
    }
}
