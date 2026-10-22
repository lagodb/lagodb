//! Keep an Iceberg catalog server bound to its creation-time catalog identity.

use std::ffi::CStr;

use lagodb_core::catalog::ForeignServerDdlTarget;
use lagodb_core::hooks::{
    AlterForeignServerStmtNode, PostUtilityContext, UtilityHook, UtilityHookError,
    UtilityNode,
};
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::options::IcebergFdwOptions;
use super::super::provider::LagodbIceberg;

pub(super) struct IcebergCatalogServerDdlGuard;

impl UtilityHook for IcebergCatalogServerDdlGuard {
    fn name(&self) -> &'static str {
        "Iceberg catalog-server identity"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let statement = statement
            .cast::<AlterForeignServerStmtNode>()
            .expect("hook is registered for AlterForeignServerStmt");
        // SAFETY: PostgreSQL owns this NUL-terminated name for the live utility
        // statement. Core performs native ownership admission before provider
        // policy is inspected.
        let server_name = unsafe { CStr::from_ptr(statement.servername) };
        let Some(target) = ForeignServerDdlTarget::resolve(server_name)? else {
            // Let PostgreSQL report its standard undefined-server error.
            return Ok(());
        };
        if !LagodbIceberg::handles_server(target.oid()) {
            return Ok(());
        }
        let server = unsafe { &*pg_sys::GetForeignServer(target.oid()) };

        let option_count = unsafe { pg_sys::list_length(statement.options) };
        for index in 0..option_count {
            // PostgreSQL guarantees AlterForeignServerStmt::options contains
            // DefElem nodes owned by the live utility parse tree.
            let option = unsafe {
                &*pg_sys::list_nth(statement.options, index).cast::<pg_sys::DefElem>()
            };
            let name =
                unsafe { CStr::from_ptr(option.defname) }
                    .to_str()
                    .map_err(|_| IcebergFdwError::InvalidUtf8 {
                        subject: "foreign server option name",
                    })?;
            if IcebergFdwOptions::is_immutable_catalog_server_option(server, name)? {
                return Err(IcebergFdwError::ImmutableCatalogIdentityOption {
                    name: name.to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}
