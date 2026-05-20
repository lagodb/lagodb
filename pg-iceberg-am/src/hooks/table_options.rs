use crate::catalog::iceberg_metadata::IcebergMetadata;
use crate::catalog::init_table_storage_metadata;
use crate::constants::ICEBERG_AM_NAME;
use crate::options::ICEBERG_TABLE_OPTIONS;
use pg_lakebase_core::catalog::range_var_get_relid;
use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::hooks::{
    CreateStmtNode, PostUtilityContext, UtilityHook, UtilityHookError, UtilityNode,
    register_utility_hook,
};
use pg_lakebase_core::options::TableOptions;
use pgrx::pg_sys;
use std::ffi::CStr;

struct IcebergTableHook;

/// Check if the CREATE TABLE statement uses the 'iceberg' access method.
fn is_iceberg_access_method(stmt: &pg_sys::CreateStmt) -> bool {
    unsafe {
        let am = stmt.accessMethod;
        if am.is_null() {
            return false;
        }
        CStr::from_ptr(am).to_bytes() == ICEBERG_AM_NAME.as_bytes()
    }
}

impl UtilityHook for IcebergTableHook {
    fn name(&self) -> &'static str {
        "iceberg table options"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast_mut::<CreateStmtNode>()
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        TableOptions::extract_from_stmt(stmt, ICEBERG_TABLE_OPTIONS)?;
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let stmt = context
            .original_stmt()
            .cast::<CreateStmtNode>()
            .expect("Hook registered for T_CreateStmt");

        if !is_iceberg_access_method(stmt) {
            return Ok(());
        }

        // SAFETY: `stmt` is a live CreateStmt from the utility hook, and its
        // RangeVar belongs to that PostgreSQL parse tree for this callback.
        let oid = unsafe {
            range_var_get_relid(
                stmt.relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                false,
            )
        }?;

        if let Some(opts) = TableOptions::read_from_stmt(stmt, ICEBERG_TABLE_OPTIONS)?
        {
            opts.persist_to_catalog(oid)?;
        }

        let guard =
            RelationGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)?;

        let metadata_location = init_table_storage_metadata(&guard.as_handle())?;

        IcebergMetadata::new(oid)
            .with_metadata_location(metadata_location)
            .with_default_spec_id(0)
            .insert()?;
        Ok(())
    }
}

pub fn init_hook() {
    register_utility_hook(pg_sys::NodeTag::T_CreateStmt, Box::new(IcebergTableHook));
}
