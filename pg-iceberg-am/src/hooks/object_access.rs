use crate::catalog::IcebergRelationExt;
use crate::catalog::metadata_table::IcebergMetadata;
use crate::catalog::table_lifecycle::IcebergTableLifecycle;
use pg_lakebase_core::handles::{RelationGuard, RelationHandle};
use pg_lakebase_core::hooks::{
    self, HookError, ObjectAccessEvent, ObjectAccessHook, ObjectAccessHookError,
};
use pg_lakebase_core::options::TableOptions;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

pub struct IcebergObjectAccessHook;

impl ObjectAccessHook for IcebergObjectAccessHook {
    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        match event {
            // sub_id == 0 means the main relation, not a column.
            ObjectAccessEvent::Drop {
                class_id,
                object_id,
                sub_id,
                ..
            } if *class_id == pg_sys::RelationRelationId && *sub_id == 0 => {
                let Some(guard) = Self::open_iceberg_physical_relation(*object_id)? else {
                    return Ok(());
                };
                Self::handle_drop_relation(&guard.as_handle())?;
            }
            ObjectAccessEvent::Truncate { object_id } => {
                let Some(guard) = Self::open_iceberg_physical_relation(*object_id)? else {
                    return Ok(());
                };
                Self::handle_truncate_relation(&guard.as_handle())?;
            }
            _ => {}
        }

        Ok(())
    }
}

impl IcebergObjectAccessHook {
    fn open_iceberg_physical_relation(
        oid: pg_sys::Oid,
    ) -> Result<Option<RelationGuard>, ObjectAccessHookError> {
        // Check relation kind before opening to avoid "wrong object type"
        // errors when dropping indexes, sequences, etc.
        let relkind = unsafe { pg_sys::get_rel_relkind(oid) } as u8;
        if relkind != pg_sys::RELKIND_RELATION
            && relkind != pg_sys::RELKIND_MATVIEW
        {
            return Ok(None);
        }

        // OAT_DROP is called before the object is removed, and OAT_TRUNCATE is
        // called while PostgreSQL is operating on the live relation.
        let guard =
            RelationGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)?;

        let is_iceberg = guard.as_handle().is_iceberg();

        Ok(is_iceberg.then_some(guard))
    }

    /// Handle DROP event for a relation by removing transactional catalog
    /// state and registering a pending storage delete for commit cleanup.
    fn handle_drop_relation(
        rel: &RelationHandle<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        // `regclass NOT NULL PRIMARY KEY` stores an OID value; it is not a
        // PostgreSQL dependency or foreign key. These Lakebase catalog rows
        // must be deleted explicitly in the same transaction as DROP TABLE.
        IcebergMetadata::delete_if_exists(rel.oid())?;
        TableOptions::delete_from_catalog(rel.oid())?;

        // DROP directory cleanup is a post-commit WAL action for local
        // permanent relations. `IcebergTableStorage` resolves the storage
        // context with the relation-aware WAL policy and computes the table
        // location in exactly the same way as CREATE TABLE, so DROP can
        // never disagree with CREATE on layout.
        IcebergTableLifecycle::new(rel)?.register_drop_cleanup();

        Ok(())
    }

    fn handle_truncate_relation(
        rel: &RelationHandle<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        Err(HookError::with_code(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "cannot TRUNCATE Iceberg table \"{}\": Iceberg truncate requires metadata rewrite support",
                rel.relation_name()
            ),
        ))
    }
}

pub fn init_hook() {
    hooks::register_object_access_hook(Box::new(IcebergObjectAccessHook));
}
