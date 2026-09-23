use pgrx::pg_sys;

use super::{CatalogRelation, CatalogScanKey, CatalogSnapshot, search_syscache1};
use crate::diag::PgError;
use crate::handles::RelationTablespace;

/// Stable fields copied from one `pg_class` tuple.
///
/// Copying the fields lets callers inspect catalog state without retaining a
/// scan tuple or exposing PostgreSQL tuple lifetimes outside the catalog layer.
#[derive(Debug, Clone, Copy)]
pub struct RelationCatalogEntry {
    oid: pg_sys::Oid,
    relkind: i8,
    access_method_oid: pg_sys::Oid,
    tablespace: RelationTablespace,
}

impl RelationCatalogEntry {
    /// Read current catalog identity without opening or locking the target.
    ///
    /// A missing row is a normal outcome for an unlocked admission probe.
    /// This does not provide the current-command visibility of
    /// [`CatalogSnapshot::SelfVisible`] or protect against concurrent DDL.
    pub fn find_cached(oid: pg_sys::Oid) -> Option<Self> {
        let tuple =
            search_syscache1(pg_sys::SysCacheIdentifier::RELOID as _, oid.into())?;
        // SAFETY: RELOID pins this pg_class tuple until `tuple` is dropped.
        let class =
            unsafe { &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class) };
        Some(Self::from_class(class))
    }

    /// Find a relation's `pg_class` row using the requested catalog snapshot.
    ///
    /// [`CatalogSnapshot::SelfVisible`] must be used when an object-access hook
    /// needs catalog changes made by the current command.
    pub fn find(
        oid: pg_sys::Oid,
        snapshot: CatalogSnapshot<'_>,
    ) -> Result<Option<Self>, PgError> {
        let pg_class = CatalogRelation::open(
            pg_sys::RelationRelationId,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;
        let mut scan = pg_class.begin_scan(
            pg_sys::ClassOidIndexId.into(),
            true,
            snapshot,
            [CatalogScanKey::oid_eq(pg_sys::Anum_pg_class_oid as _, oid)],
        )?;
        let Some(tuple) = scan.get_next()? else {
            return Ok(None);
        };

        // SAFETY: the tuple comes from a live scan of pg_class, and its fixed
        // fields remain valid until the scan advances or is dropped.
        let class =
            unsafe { &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class) };
        Ok(Some(Self::from_class(class)))
    }

    fn from_class(class: &pg_sys::FormData_pg_class) -> Self {
        Self {
            oid: class.oid,
            relkind: class.relkind,
            access_method_oid: class.relam,
            tablespace: RelationTablespace::from_catalog_oid(class.reltablespace),
        }
    }

    /// OID stored in the `pg_class` row.
    #[inline]
    pub fn oid(self) -> pg_sys::Oid {
        self.oid
    }

    /// PostgreSQL relation kind (`pg_class.relkind`).
    #[inline]
    pub fn relkind(self) -> i8 {
        self.relkind
    }

    /// Table access method OID (`pg_class.relam`).
    #[inline]
    pub fn access_method_oid(self) -> pg_sys::Oid {
        self.access_method_oid
    }

    /// Logical tablespace placement recorded by `pg_class.reltablespace`.
    #[inline]
    pub fn tablespace(self) -> RelationTablespace {
        self.tablespace
    }
}
