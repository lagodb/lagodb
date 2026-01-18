use pg_lakehouse_core::pg_wrapper::PgWrapper;
use pg_lakehouse_core::prelude::*;
use pgrx::pg_sys;
use std::ffi::CStr;

struct IcebergTablespaceHook;

impl UtilityHook for IcebergTablespaceHook {
    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateTableSpaceStmt>(pg_sys::NodeTag::T_CreateTableSpaceStmt)
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        TablespaceOptions::extract_from_stmt(stmt).map_err(|e| {
            UtilityHookError::Message(format!("tablespace: option extraction failed - {}", e))
        })?;
        Ok(())
    }

    fn on_post(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .is_a_mut::<pg_sys::CreateTableSpaceStmt>(pg_sys::NodeTag::T_CreateTableSpaceStmt)
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        if let Ok(Some(opts)) = TablespaceOptions::extract_from_stmt(stmt) {
            let spcname = unsafe { CStr::from_ptr(stmt.tablespacename) };
            let oid = PgWrapper::get_tablespace_oid(spcname, false).map_err(|e| {
                UtilityHookError::Message(format!(
                    "tablespace: failed to get tablespace OID - {}",
                    e
                ))
            })?;
            opts.persist_to_catalog(oid).map_err(|e| {
                UtilityHookError::Message(format!(
                    "tablespace: failed to persist options to catalog - {}",
                    e
                ))
            })?;
        }
        Ok(())
    }
}

pub fn init_hook() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreateTableSpaceStmt,
        Box::new(IcebergTablespaceHook),
    );
}
