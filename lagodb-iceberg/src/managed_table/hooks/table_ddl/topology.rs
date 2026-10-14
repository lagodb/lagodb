//! Iceberg tables cannot participate in PostgreSQL declarative partitioning.
//! Provider-owned partitioned roots describe Iceberg's internal partitioning;
//! they cannot have PG partition or inheritance relationships. Ordinary
//! Iceberg tables retain normal INHERITS support: COPY targets the named table
//! rather than routing rows to inheritance children.
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

    fn rejection_reason(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<Option<&'static str>, ObjectAccessHookError> {
        // The event can precede a command-counter increment, including during
        // CREATE. PostgreSQL holds the target locks and both relations exist.
        let child = RelationCatalogEntry::find(child, CatalogSnapshot::SelfVisible)?
            .expect("PostgreSQL inheritance event identifies a live relation");
        let parent =
            RelationCatalogEntry::find(parent, CatalogSnapshot::SelfVisible)?
                .expect("PostgreSQL inheritance event identifies a live relation");
        let child_is_iceberg =
            IcebergAccessMethod::matches_oid(child.access_method_oid());
        let parent_is_iceberg =
            IcebergAccessMethod::matches_oid(parent.access_method_oid());
        let child_is_partitioned =
            child.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE;
        let parent_is_partitioned =
            parent.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE;

        if (child_is_iceberg && child_is_partitioned)
            || (parent_is_iceberg && parent_is_partitioned)
        {
            return Ok(Some(
                "Iceberg partitioned tables cannot participate in PostgreSQL partition or inheritance relationships",
            ));
        }
        // A declarative partition's parent already has this relkind when
        // pg_inherits is recorded; the child's relispartition is set later.
        if child_is_iceberg && parent_is_partitioned {
            return Ok(Some(
                "Iceberg tables cannot be PostgreSQL partition children",
            ));
        }
        Ok(None)
    }

    fn validate(
        child: pg_sys::Oid,
        parent: pg_sys::Oid,
    ) -> Result<(), ObjectAccessHookError> {
        let Some(reason) = Self::rejection_reason(child, parent)? else {
            return Ok(());
        };

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
                    reason,
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
