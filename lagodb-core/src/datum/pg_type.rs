//! PostgreSQL type metadata used while binding storage codecs.

use std::ffi::CStr;

use pgrx::{PgTupleDesc, pg_sys};

/// A declared type, including typmods inherited from a domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgType {
    pub oid: pg_sys::Oid,
    pub typmod: i32,
}

#[derive(Debug, Clone)]
pub struct PgCompositeField {
    /// Physical attribute index, including positions occupied by dropped fields.
    pub index: usize,
    pub name: String,
    pub pg_type: PgType,
}

impl PgType {
    pub const fn new(oid: pg_sys::Oid, typmod: i32) -> Self {
        Self { oid, typmod }
    }

    /// PostgreSQL's display spelling, including any typmod (`numeric(8,2)`),
    /// as core diagnostics report type mismatches.
    pub fn display_name(self) -> String {
        // SAFETY: the caller supplies a catalog type on the backend thread;
        // PostgreSQL returns a current-context C string that is copied now.
        let name = unsafe { pg_sys::format_type_with_typemod(self.oid, self.typmod) };
        unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned()
    }

    pub fn base(self) -> Self {
        let mut typmod = self.typmod;
        // SAFETY: the caller supplies a catalog type on the backend thread.
        let oid = unsafe { pg_sys::getBaseTypeAndTypmod(self.oid, &mut typmod) };
        Self { oid, typmod }
    }

    pub fn element(self) -> Option<Self> {
        let base = self.base();
        let oid = unsafe { pg_sys::get_element_type(base.oid) };
        (oid != pg_sys::InvalidOid).then_some(Self::new(oid, base.typmod))
    }

    pub fn is_composite(self) -> bool {
        unsafe {
            pg_sys::get_typtype(self.base().oid) as u8 == pg_sys::TYPTYPE_COMPOSITE
        }
    }

    pub fn is_enum(self) -> bool {
        unsafe { pg_sys::get_typtype(self.oid) as u8 == pg_sys::TYPTYPE_ENUM }
    }

    /// PostgreSQL object identifiers share an unsigned 32-bit representation.
    /// Their display names are search_path-dependent and are not an identity.
    pub fn is_object_id(self) -> bool {
        matches!(
            self.oid,
            pg_sys::OIDOID
                | pg_sys::REGPROCOID
                | pg_sys::REGPROCEDUREOID
                | pg_sys::REGOPEROID
                | pg_sys::REGOPERATOROID
                | pg_sys::REGCLASSOID
                | pg_sys::REGTYPEOID
                | pg_sys::REGCONFIGOID
                | pg_sys::REGDICTIONARYOID
                | pg_sys::REGNAMESPACEOID
                | pg_sys::REGROLEOID
                | pg_sys::REGCOLLATIONOID
        )
    }

    /// Snapshot live attributes without retaining a typcache pointer in a plan.
    pub fn composite_fields(self) -> Vec<PgCompositeField> {
        let base = self.base();
        // lookup_rowtype_tupdesc acquires the reference released by PgTupleDesc.
        let desc = unsafe {
            PgTupleDesc::from_pg(pg_sys::lookup_rowtype_tupdesc(
                base.oid,
                base.typmod,
            ))
        };
        desc.iter()
            .enumerate()
            .filter(|(_, attr)| !attr.attisdropped)
            .map(|(index, attr)| PgCompositeField {
                index,
                name: unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
                    .to_string_lossy()
                    .into_owned(),
                pg_type: Self::new(attr.atttypid, attr.atttypmod),
            })
            .collect()
    }
}
