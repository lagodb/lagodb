//! PostgreSQL COPY column layout exposed to provider destinations.

use std::ffi::CStr;

use pgrx::pg_sys;

use super::error::CopyError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyColumn {
    name: Box<CStr>,
    attno: pg_sys::AttrNumber,
    relation_index: usize,
    type_oid: pg_sys::Oid,
    type_mod: i32,
    type_len: i16,
    type_by_value: bool,
}

impl CopyColumn {
    pub fn name(&self) -> &CStr {
        &self.name
    }

    pub fn attno(&self) -> pg_sys::AttrNumber {
        self.attno
    }

    pub(crate) fn relation_index(&self) -> usize {
        self.relation_index
    }

    pub fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    pub fn type_mod(&self) -> i32 {
        self.type_mod
    }

    pub(crate) fn datum_size(&self, value: pg_sys::Datum) -> usize {
        if self.type_by_value {
            return 0;
        }
        if self.type_len > 0 {
            return self.type_len as usize;
        }
        // SAFETY: the value was produced for this column's type by a bound
        // typed source. Table columns cannot use the cstring pseudo-type, so
        // PostgreSQL performs constant-time header inspection here for a
        // variable-length value.
        debug_assert_eq!(self.type_len, -1);
        unsafe { pg_sys::datumGetSize(value, false, i32::from(self.type_len)) }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyColumnLayout {
    columns: Box<[CopyColumn]>,
}

impl CopyColumnLayout {
    pub fn columns(&self) -> &[CopyColumn] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub(crate) unsafe fn from_descriptor(
        descriptor: pg_sys::TupleDesc,
        attnums: *mut pg_sys::List,
    ) -> Result<Self, CopyError> {
        let count = unsafe { pg_sys::list_length(attnums) };
        let mut columns = Vec::with_capacity(count as usize);
        for index in 0..count {
            let attno = unsafe { pg_sys::list_nth_int(attnums, index) };
            debug_assert!(attno > 0 && attno <= unsafe { (*descriptor).natts });
            let relation_index = (attno - 1) as usize;
            let attribute =
                unsafe { &*(*descriptor).attrs.as_ptr().add(relation_index) };
            let name = unsafe {
                CStr::from_ptr(attribute.attname.data.as_ptr())
                    .to_owned()
                    .into_boxed_c_str()
            };
            columns.push(CopyColumn {
                name,
                attno: attno as pg_sys::AttrNumber,
                relation_index,
                type_oid: attribute.atttypid,
                type_mod: attribute.atttypmod,
                type_len: attribute.attlen,
                type_by_value: attribute.attbyval,
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
        })
    }
}
