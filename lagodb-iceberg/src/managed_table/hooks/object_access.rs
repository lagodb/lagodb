use crate::managed_table::catalog::table_drop::IcebergTableDrop;
use crate::managed_table::catalog::{IcebergAccessMethod, IcebergRelationExt};
use crate::managed_table::hooks::column_drop_guard::ControlledColumnDrops;
use lagodb_core::catalog::{
    CatalogSnapshot, RelationCatalogEntry, TypeDependencies,
    record_tablespace_dependency,
};
use lagodb_core::handles::{RelationGuard, RelationHandle};
use lagodb_core::hooks::{
    self, HookError, OBJECT_ACCESS_DROP, OBJECT_ACCESS_POST_ALTER,
    OBJECT_ACCESS_POST_CREATE, ObjectAccessEvent, ObjectAccessFilter,
    ObjectAccessHook, ObjectAccessHookError,
};
use lagodb_core::storage::tablespace::TablespaceStorage;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

pub struct IcebergObjectAccessHook;

impl ObjectAccessHook for IcebergObjectAccessHook {
    fn filter(&self) -> ObjectAccessFilter {
        ObjectAccessFilter::new(
            OBJECT_ACCESS_DROP | OBJECT_ACCESS_POST_CREATE | OBJECT_ACCESS_POST_ALTER,
        )
        .for_class(pg_sys::RelationRelationId)
    }

    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        // Composite relations do not use the Iceberg AM themselves. Enforce
        // their dependent storage contract before the table-specific dispatch.
        Self::check_composite_attribute_change(event)?;
        match event {
            ObjectAccessEvent::PostCreate {
                class_id,
                object_id,
                sub_id: 0,
                ..
            } if *class_id == pg_sys::RelationRelationId => {
                Self::enforce_volume_tablespace_policy(*object_id, true)?;
            }
            ObjectAccessEvent::PostAlter {
                class_id,
                object_id,
                sub_id: 0,
                ..
            } if *class_id == pg_sys::RelationRelationId => {
                Self::enforce_volume_tablespace_policy(*object_id, false)?;
            }
            ObjectAccessEvent::Drop {
                class_id,
                object_id,
                sub_id,
                ..
            } if *class_id == pg_sys::RelationRelationId && *sub_id > 0 => {
                let Some(guard) = Self::open_iceberg_relation(*object_id)? else {
                    return Ok(());
                };
                if !ControlledColumnDrops::consume(*object_id, *sub_id) {
                    // TODO(schema-evolution): dependency-driven drops could be
                    // supported by staging schema actions from actual OAT order.
                    // Until that design handles multi-object CASCADE and avoids
                    // duplicate ALTER TABLE staging, rejecting is required to
                    // keep PostgreSQL and Iceberg schemas consistent.
                    return Err(HookError::with_code(
                        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                        format!(
                            "cannot drop column attribute {} from Iceberg relation \"{}\" outside supported ALTER TABLE DROP COLUMN",
                            sub_id,
                            guard
                                .as_handle()
                                .relation_name()
                                .as_c_str()
                                .to_string_lossy()
                        ),
                    ));
                }
            }
            // sub_id == 0 means the main relation, not a column.
            ObjectAccessEvent::Drop {
                class_id,
                object_id,
                sub_id,
                ..
            } if *class_id == pg_sys::RelationRelationId && *sub_id == 0 => {
                let Some(guard) = Self::open_iceberg_relation(*object_id)? else {
                    return Ok(());
                };
                Self::handle_drop_relation(&guard.as_handle())?;
            }
            _ => {}
        }

        Ok(())
    }
}

impl IcebergObjectAccessHook {
    /// Keep a referenced composite's layout fixed until nested schema evolution
    /// is supported. PG emits PostCreate for ADD ATTRIBUTE, Drop for deletion
    /// (including dependency-driven deletion), and PostAlter for rename/type
    /// changes. The SQL command being ALTER TYPE does not imply PostAlter.
    fn check_composite_attribute_change(
        event: &ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        let oid = match event {
            ObjectAccessEvent::PostCreate {
                class_id,
                object_id,
                sub_id,
                ..
            }
            | ObjectAccessEvent::PostAlter {
                class_id,
                object_id,
                sub_id,
                ..
            }
            | ObjectAccessEvent::Drop {
                class_id,
                object_id,
                sub_id,
                ..
            } if *class_id == pg_sys::RelationRelationId && *sub_id > 0 => *object_id,
            // Whole-object deletion remains the responsibility of PG dependency
            // handling and the existing managed table deletion policy.
            _ => return Ok(()),
        };
        // SAFETY: oid identifies the attribute's relation on the backend thread.
        // PG holds its DDL lock, including for dependency-driven attribute drops.
        if unsafe { pg_sys::get_rel_relkind(oid) as u8 }
            != pg_sys::RELKIND_COMPOSITE_TYPE
        {
            return Ok(());
        }
        let Some(am) = IcebergAccessMethod::oid() else {
            return Ok(());
        };
        let relation = RelationGuard::open_relation(oid, pg_sys::NoLock as _)?;
        // SAFETY: PG owns the DDL lock and the guard pins rd_rel.
        let type_oid = unsafe { (*(*relation.as_raw()).rd_rel).reltype };
        // Only the existing type identity and its dependents are needed, not
        // the changed attribute's pre-command-counter-increment catalog tuple.
        // Reject through the existing FFI boundary so PG rolls back the DDL.
        if TypeDependencies::used_by_access_method(type_oid, am)? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot alter a composite attribute used by a managed Iceberg column",
            ));
        }
        Ok(())
    }

    fn enforce_volume_tablespace_policy(
        oid: pg_sys::Oid,
        record_missing_dependency: bool,
    ) -> Result<(), ObjectAccessHookError> {
        // PostgreSQL invokes both events before the next command-counter
        // increment. SnapshotSelf is therefore required to observe the
        // pg_class row created or updated by the current command.
        let relation = RelationCatalogEntry::find(oid, CatalogSnapshot::SelfVisible)?
            .ok_or_else(|| {
                HookError::with_code(
                    PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                    format!("could not find pg_class tuple for relation {oid}"),
                )
            })?;
        let relkind = relation.relkind() as u8;
        if !matches!(
            relkind,
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_PARTITIONED_TABLE
                | pg_sys::RELKIND_MATVIEW
                | pg_sys::RELKIND_INDEX
                | pg_sys::RELKIND_PARTITIONED_INDEX
                | pg_sys::RELKIND_SEQUENCE
                | pg_sys::RELKIND_TOASTVALUE
        ) {
            return Ok(());
        }

        let tablespace = relation.tablespace();
        if !TablespaceStorage::is_object_backed(tablespace.resolved_oid())? {
            return Ok(());
        }
        let is_managed_iceberg =
            matches!(
                relkind,
                pg_sys::RELKIND_RELATION | pg_sys::RELKIND_PARTITIONED_TABLE
            ) && IcebergAccessMethod::matches_oid(relation.access_method_oid());
        if !is_managed_iceberg {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "volume-backed tablespaces may only contain managed Iceberg tables",
            ));
        }

        // Ordinary tables are protected by their native main forks. PG records
        // dependencies for partitioned roots with an explicit reltablespace;
        // supplement only roots placed in the database's default tablespace.
        if record_missing_dependency
            && relkind == pg_sys::RELKIND_PARTITIONED_TABLE
            && tablespace.is_database_default()
        {
            record_tablespace_dependency(
                pg_sys::RelationRelationId,
                relation.oid(),
                tablespace.resolved_oid(),
            )?;
        }
        Ok(())
    }

    fn open_iceberg_relation(
        oid: pg_sys::Oid,
    ) -> Result<Option<RelationGuard>, ObjectAccessHookError> {
        // Check relation kind before opening to avoid "wrong object type"
        // errors when dropping indexes, sequences, etc.
        let relkind = unsafe { pg_sys::get_rel_relkind(oid) } as u8;
        if !matches!(
            relkind,
            pg_sys::RELKIND_RELATION
                | pg_sys::RELKIND_PARTITIONED_TABLE
                | pg_sys::RELKIND_MATVIEW
        ) {
            return Ok(None);
        }

        // OAT_DROP is called before the object is removed.
        let guard = RelationGuard::open_table(
            oid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        )?;

        let is_iceberg = guard.as_handle().is_iceberg();

        Ok(is_iceberg.then_some(guard))
    }

    /// Handle DROP event for a relation by removing transactional catalog
    /// state and registering a pending storage delete for commit cleanup.
    fn handle_drop_relation(
        rel: &RelationHandle<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        IcebergTableDrop::for_relation(rel)?.stage()?;
        Ok(())
    }
}

pub fn init_hook() {
    hooks::register_object_access_hook(Box::new(IcebergObjectAccessHook));
}
