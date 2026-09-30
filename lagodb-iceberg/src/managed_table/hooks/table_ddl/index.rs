//! Reject uniqueness metadata that PostgreSQL cannot enforce on Iceberg partitioned tables.

use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, RelationCatalogEntry,
};
use lagodb_core::hooks::{
    HookError, OBJECT_ACCESS_POST_CREATE, ObjectAccessEvent, ObjectAccessFilter,
    ObjectAccessHook, ObjectAccessHookError,
};
use pgrx::{PgSqlErrorCode, pg_sys};

use crate::managed_table::catalog::IcebergAccessMethod;

pub(super) struct IcebergPartitionedTableIndexGuard;

impl IcebergPartitionedTableIndexGuard {
    fn validate(index_oid: pg_sys::Oid) -> Result<(), ObjectAccessHookError> {
        // PostgreSQL's index_create() emits OAT_POST_CREATE after inserting pg_class
        // and pg_index, before its command-counter increment and index build.
        // SnapshotSelf sees implicit constraint indexes as well as CREATE INDEX.
        let index =
            RelationCatalogEntry::find(index_oid, CatalogSnapshot::SelfVisible)?
                .expect("PostgreSQL post-create event identifies a live relation");
        if index.relkind() as u8 != pg_sys::RELKIND_PARTITIONED_INDEX {
            return Ok(());
        }

        let pg_index = CatalogRelation::open(
            pg_sys::IndexRelationId,
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = pg_index.begin_scan(
            pg_sys::IndexRelidIndexId.into(),
            true,
            CatalogSnapshot::SelfVisible,
            [CatalogScanKey::oid_eq(
                pg_sys::Anum_pg_index_indexrelid as _,
                index_oid,
            )],
        )?;
        let tuple = scan
            .get_next()?
            .expect("PostgreSQL index post-create event has a pg_index row");
        // SAFETY: the tuple belongs to this pg_index scan; both fixed
        // attributes are non-null and copied before the scan is released.
        let (unique, relation_oid) = unsafe {
            let unique = pg_index
                .get_attr(tuple, pg_sys::Anum_pg_index_indisunique as _)
                .expect("pg_index.indisunique is not null")
                .value()
                != 0;
            let relation_oid = pg_index
                .get_attr(tuple, pg_sys::Anum_pg_index_indrelid as _)
                .expect("pg_index.indrelid is not null")
                .value() as u32;
            (unique, pg_sys::Oid::from_u32(relation_oid))
        };
        if !unique {
            return Ok(());
        }
        let relation =
            RelationCatalogEntry::find(relation_oid, CatalogSnapshot::SelfVisible)?
                .expect("PostgreSQL index creation holds its table lock");
        if relation.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
            && IcebergAccessMethod::matches_oid(relation.access_method_oid())
        {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "unique indexes are not supported on Iceberg partitioned tables",
            ));
        }
        Ok(())
    }
}

impl ObjectAccessHook for IcebergPartitionedTableIndexGuard {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(OBJECT_ACCESS_POST_CREATE)
            .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        match event {
            ObjectAccessEvent::PostCreate {
                object_id,
                sub_id: 0,
                ..
            } => Self::validate(*object_id),
            _ => Ok(()),
        }
    }
}
