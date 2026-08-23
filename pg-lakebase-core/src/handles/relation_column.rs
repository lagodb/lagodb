//! Owned metadata for a live PostgreSQL relation column.

use std::ffi::{CStr, c_char};
use std::slice;

use pgrx::pg_sys;

/// Metadata copied from one non-dropped `TupleDesc` attribute.
#[derive(Clone, Debug)]
pub struct RelationColumn {
    attno: pg_sys::AttrNumber,
    name: Box<CStr>,
    type_oid: pg_sys::Oid,
    type_mod: i32,
    not_null: bool,
}

impl RelationColumn {
    #[inline]
    pub fn attno(&self) -> pg_sys::AttrNumber {
        self.attno
    }

    #[inline]
    pub fn name(&self) -> &CStr {
        &self.name
    }

    #[inline]
    pub fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub fn type_mod(&self) -> i32 {
        self.type_mod
    }

    #[inline]
    pub fn is_not_null(&self) -> bool {
        self.not_null
    }

    /// Copy all non-dropped attributes from a tuple descriptor.
    ///
    /// # Safety
    ///
    /// `tuple_desc` must be a valid descriptor whose attribute array remains
    /// live for this call.
    pub(super) unsafe fn live_from_tuple_desc(
        tuple_desc: pg_sys::TupleDesc,
    ) -> Box<[Self]> {
        // SAFETY: the caller holds the relation that owns this valid tuple
        // descriptor; `attrs` contains exactly `natts` entries.
        let attrs = unsafe {
            slice::from_raw_parts(
                (*tuple_desc).attrs.as_ptr(),
                (*tuple_desc).natts as usize,
            )
        };
        attrs
            .iter()
            .filter(|attribute| !attribute.attisdropped)
            .map(|attribute| {
                // SAFETY: PostgreSQL stores `attname` as a NUL-terminated
                // NameData value. Copying preserves its server-encoding bytes
                // beyond the descriptor borrow.
                let name = unsafe {
                    CStr::from_ptr(attribute.attname.data.as_ptr().cast::<c_char>())
                        .to_owned()
                        .into_boxed_c_str()
                };
                Self {
                    attno: attribute.attnum,
                    name,
                    type_oid: attribute.atttypid,
                    type_mod: attribute.atttypmod,
                    not_null: attribute.attnotnull,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice()
    }
}
