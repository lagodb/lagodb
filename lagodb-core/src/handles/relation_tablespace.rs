use pgrx::pg_sys;

/// PostgreSQL tablespace placement recorded for a relation.
///
/// PostgreSQL stores [`pg_sys::InvalidOid`] in `pg_class.reltablespace` when a
/// relation uses the database default. This value preserves that provenance
/// while providing the resolved OID required by storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationTablespace {
    catalog_oid: pg_sys::Oid,
}

impl RelationTablespace {
    #[inline]
    pub(crate) fn from_catalog_oid(oid: pg_sys::Oid) -> Self {
        Self { catalog_oid: oid }
    }

    /// The actual tablespace OID after resolving the database default.
    #[inline]
    pub fn resolved_oid(self) -> pg_sys::Oid {
        if self.is_database_default() {
            // SAFETY: a live relation handle exists only in a connected
            // backend after PostgreSQL initialized the database identity.
            unsafe { pg_sys::MyDatabaseTableSpace }
        } else {
            self.catalog_oid
        }
    }

    /// Whether `pg_class.reltablespace` delegates to the database default.
    #[inline]
    pub fn is_database_default(self) -> bool {
        self.catalog_oid == pg_sys::InvalidOid
    }
}
