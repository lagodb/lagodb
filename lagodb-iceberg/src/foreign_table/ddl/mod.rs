//! Foreign DDL admission and credential-query redaction.
//!
//! TODO(iceberg-fdw-after-triggers): reject creation of AFTER ROW and AFTER
//! STATEMENT triggers on tables belonging to this FDW. The publication timing
//! and intentional support restriction are documented in
//! `IcebergFdwModifyState::finish`; this DDL restriction is not yet implemented.

mod alter;
mod create;
mod drop;
mod server;

use core::ptr;
use std::ffi::CStr;

use lagodb_core::hooks::{
    AlterUserMappingStmtNode, CreateUserMappingStmtNode, PostUtilityContext,
    PreUtilityContext, UtilityHook, UtilityHookError, UtilityNode, VacuumStmtNode,
    register_object_access_hook, register_utility_hook,
};
use lagodb_core::storage::foreign::ForeignOption;
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use super::provider::LagodbIceberg;
use alter::{IcebergStagedOptionsGuard, IcebergStagedSchemaGuard};
use create::IcebergForeignTableDdl;
use drop::IcebergForeignTableDropHook;
use server::IcebergCatalogServerDdlGuard;

struct IcebergForeignTableVacuumGuard;
struct UserMappingSecretRedaction;

impl UtilityHook for UserMappingSecretRedaction {
    fn name(&self) -> &'static str {
        "foreign user-mapping credential redaction"
    }

    fn on_pre(&self, _statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        Ok(())
    }

    fn on_pre_context(
        &self,
        context: &mut PreUtilityContext<'_>,
    ) -> Result<(), UtilityHookError> {
        let options = if let Some(statement) =
            context.statement_mut().cast::<CreateUserMappingStmtNode>()
        {
            statement.options
        } else if let Some(statement) =
            context.statement_mut().cast::<AlterUserMappingStmtNode>()
        {
            statement.options
        } else {
            return Ok(());
        };
        if Self::contains_secret(options) {
            context.redact_statement("<redacted: USER MAPPING with credentials>");
        }
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

impl UtilityHook for IcebergForeignTableVacuumGuard {
    fn name(&self) -> &'static str {
        "Iceberg foreign-table VACUUM guard"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let vacuum = statement
            .cast_mut::<VacuumStmtNode>()
            .expect("hook is registered for VacuumStmt");
        if !vacuum.is_vacuumcmd {
            return Ok(());
        }
        let relation_count = unsafe { pg_sys::list_length(vacuum.rels) };
        for index in 0..relation_count {
            let relation = unsafe {
                &*pg_sys::list_nth(vacuum.rels, index)
                    .cast::<pg_sys::VacuumRelation>()
            };
            let relation_oid = unsafe {
                pg_sys::RangeVarGetRelidExtended(
                    relation.relation,
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                    pg_sys::RVROption::RVR_MISSING_OK,
                    None,
                    ptr::null_mut(),
                )
            };
            if relation_oid == pg_sys::InvalidOid
                || unsafe { pg_sys::get_rel_relkind(relation_oid) }
                    != pg_sys::RELKIND_FOREIGN_TABLE as i8
            {
                continue;
            }
            let foreign = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
            if LagodbIceberg::handles_server(foreign.serverid) {
                return Err(IcebergFdwError::UnsupportedOperation {
                    operation: "VACUUM on an Iceberg foreign table",
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

impl UserMappingSecretRedaction {
    fn contains_secret(options: *mut pg_sys::List) -> bool {
        let count = unsafe { pg_sys::list_length(options) };
        (0..count).any(|index| {
            let option =
                unsafe { pg_sys::list_nth(options, index).cast::<pg_sys::DefElem>() };
            let name = unsafe { CStr::from_ptr((*option).defname) }.to_bytes();
            ForeignOption::is_secret_name(name)
        })
    }
}

pub(super) fn register() {
    register_object_access_hook(Box::new(IcebergForeignTableDdl));
    register_object_access_hook(Box::new(IcebergForeignTableDropHook));
    register_object_access_hook(Box::new(IcebergStagedSchemaGuard));
    register_object_access_hook(Box::new(IcebergStagedOptionsGuard));
    register_utility_hook(
        pg_sys::NodeTag::T_CreateForeignTableStmt,
        Box::new(IcebergForeignTableDdl),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterForeignServerStmt,
        Box::new(IcebergCatalogServerDdlGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_VacuumStmt,
        Box::new(IcebergForeignTableVacuumGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_CreateUserMappingStmt,
        Box::new(UserMappingSecretRedaction),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterUserMappingStmt,
        Box::new(UserMappingSecretRedaction),
    );
}
