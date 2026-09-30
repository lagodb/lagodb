//! Iceberg partitioned tables cannot participate in PostgreSQL inheritance.
//!
//! PostgreSQL's StoreCatalogInheritance1() emits pg_inherits OAT_POST_ALTER after
//! acquiring the parent/child locks and recording the relationship, but before
//! ATTACH clones indexes, triggers, or foreign keys. The same event covers
//! CREATE TABLE PARTITION OF. Enforce topology here instead of taking child
//! locks in a ProcessUtility pre-hook. Returning an error rolls back the DDL.

use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, RelationCatalogEntry,
};
use lagodb_core::hooks::{
    HookError, OBJECT_ACCESS_POST_ALTER, ObjectAccessEvent, ObjectAccessFilter,
    ObjectAccessHook, ObjectAccessHookError,
};
use pgrx::{PgSqlErrorCode, pg_sys};

use crate::managed_table::catalog::IcebergAccessMethod;

pub(super) struct IcebergPartitionTopologyGuard;

impl IcebergPartitionTopologyGuard {
    // pgrx does not export pg_inherits' catalog definitions. These identifiers
    // and attribute numbers come from PostgreSQL's src/include/catalog/pg_inherits.h;
    // re-audit them with the PostgreSQL-major-version boundary. Read attributes
    // through CatalogRelation rather than reproducing the C tuple layout.
    const CATALOG_OID: pg_sys::Oid = pg_sys::Oid::from_u32(2611);
    const CHILD_INDEX_OID: pg_sys::Oid = pg_sys::Oid::from_u32(2680);
    const CHILD_ATTRIBUTE: pg_sys::AttrNumber = 1;
    const PARENT_ATTRIBUTE: pg_sys::AttrNumber = 2;

    fn is_managed_partitioned_table(
        oid: pg_sys::Oid,
    ) -> Result<bool, ObjectAccessHookError> {
        // The event can precede a command-counter increment, including during
        // CREATE. PostgreSQL holds the target locks and both relations exist.
        let relation = RelationCatalogEntry::find(oid, CatalogSnapshot::SelfVisible)?
            .expect("PostgreSQL inheritance event identifies a live relation");
        Ok(
            relation.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
                && IcebergAccessMethod::matches_oid(relation.access_method_oid()),
        )
    }

    fn validate(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<(), ObjectAccessHookError> {
        if !Self::is_managed_partitioned_table(child)?
            && !Self::is_managed_partitioned_table(parent)?
        {
            return Ok(());
        }

        // Removal also emits OAT_POST_ALTER with the same identifiers. Inspect
        // the command's own catalog changes so unlinking a relation is allowed.
        // Do not use relispartition: ATTACH sets it after this creation event.
        let inherits =
            CatalogRelation::open(Self::CATALOG_OID, pg_sys::AccessShareLock as _)?;
        let mut scan = inherits.begin_scan(
            Self::CHILD_INDEX_OID,
            true,
            CatalogSnapshot::SelfVisible,
            [CatalogScanKey::oid_eq(Self::CHILD_ATTRIBUTE, child)],
        )?;
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: this tuple belongs to the live pg_inherits scan and is
            // read before advancing that scan.
            let inherited_parent =
                unsafe { inherits.get_attr(tuple, Self::PARENT_ATTRIBUTE) }
                    .expect("pg_inherits.inhparent is not null");
            if inherited_parent.value() == u32::from(parent) as usize {
                return Err(HookError::with_code(
                    PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                    "Iceberg partitioned tables cannot participate in PostgreSQL partition or inheritance relationships",
                ));
            }
        }
        Ok(())
    }
}

impl ObjectAccessHook for IcebergPartitionTopologyGuard {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_ALTER).for_class(Self::CATALOG_OID)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        match event {
            ObjectAccessEvent::PostAlter { object_id, arg, .. } => {
                // PostgreSQL's inheritance hook supplies child as object_id and
                // parent as auxiliary_id for both creation and removal.
                let parent = arg
                    .expect(
                        "PostgreSQL inheritance event supplies post-alter arguments",
                    )
                    .auxiliary_id;
                Self::validate(*object_id, parent)
            }
            _ => Ok(()),
        }
    }
}
