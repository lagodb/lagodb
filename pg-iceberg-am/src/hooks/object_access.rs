use crate::catalog::generate_table_location;
use crate::catalog::is_iceberg_table;
use crate::storage::create_storage_context;
use crate::storage::transactional_artifacts::register_table_dir_dropped;
use pg_lakebase_core::handles::{RelationGuard, RelationHandle};
use pg_lakebase_core::hooks::{
    self, ObjectAccessEvent, ObjectAccessHook, ObjectAccessHookError,
};
use pgrx::pg_sys;

pub struct IcebergObjectAccessHook;

impl ObjectAccessHook for IcebergObjectAccessHook {
    fn on_access(
        &self,
        event: &mut ObjectAccessEvent<'_>,
    ) -> Result<(), ObjectAccessHookError> {
        // Handle DROP event
        // Register pending delete for Iceberg table data cleanup on commit
        // sub_id == 0 means it's the main relation (not a column)
        let ObjectAccessEvent::Drop {
            class_id,
            object_id,
            sub_id,
            ..
        } = event
        else {
            return Ok(());
        };

        if *class_id == pg_sys::RelationRelationId && *sub_id == 0 {
            let oid = *object_id;

            // Check relation kind before opening to avoid "wrong object type" errors
            // when dropping indexes, sequences, etc.
            let relkind = unsafe { pg_sys::get_rel_relkind(oid) } as i8;
            if relkind != pg_sys::RELKIND_RELATION as i8
                && relkind != pg_sys::RELKIND_MATVIEW as i8
            {
                return Ok(());
            }

            // Try to open the table with AccessShareLock
            // This is safe because OAT_DROP is called before the object is actually removed
            let guard = RelationGuard::open(
                oid,
                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
            )?;
            let rel = guard.as_handle();

            // Check if this is an Iceberg table
            if !is_iceberg_table(&rel) {
                return Ok(());
            }

            handle_drop_relation(&rel)?;
        }

        Ok(())
    }
}

/// Handle DROP event for a relation.
/// If the relation is an Iceberg table, register a pending delete for cleanup.
fn handle_drop_relation(
    rel: &RelationHandle<'_>,
) -> Result<(), ObjectAccessHookError> {
    // Create storage context based on tablespace type
    let spc_oid = rel.tablespace_oid();
    let ctx = create_storage_context(spc_oid)?;

    // Generate table location directly
    let table_location =
        generate_table_location(rel, &ctx.base_path, ctx.is_distributed);

    // Register pending delete for commit cleanup
    register_table_dir_dropped(table_location, ctx.file_io);

    Ok(())
}

pub fn init_hook() {
    hooks::register_object_access_hook(Box::new(IcebergObjectAccessHook));
}
