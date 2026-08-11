//! PostgreSQL COPY column layout exposed to provider destinations.

use std::ffi::CStr;

use pgrx::pg_sys;

use super::error::CopyError;

#[derive(Clone, Debug)]
pub struct CopyColumn {
    name: Box<CStr>,
    type_oid: pg_sys::Oid,
    type_mod: i32,
}

impl CopyColumn {
    pub fn name(&self) -> &CStr {
        &self.name
    }

    pub fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    pub fn type_mod(&self) -> i32 {
        self.type_mod
    }
}

#[derive(Clone, Debug)]
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
            let relation_index = usize::try_from(attno - 1).map_err(|_| {
                CopyError::invalid_column_layout(
                    "column list contains a non-positive attribute",
                )
            })?;
            let attribute =
                unsafe { &*(*descriptor).attrs.as_ptr().add(relation_index) };
            let name = unsafe {
                CStr::from_ptr(attribute.attname.data.as_ptr())
                    .to_owned()
                    .into_boxed_c_str()
            };
            columns.push(CopyColumn {
                name,
                type_oid: attribute.atttypid,
                type_mod: attribute.atttypmod,
            });
        }
        Ok(Self {
            columns: columns.into_boxed_slice(),
        })
    }
}
