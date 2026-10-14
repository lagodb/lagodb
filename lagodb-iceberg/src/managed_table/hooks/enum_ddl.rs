//! Enum labels are persisted as text; renaming them requires rewriting storage.

use std::ptr;

use lagodb_core::catalog::TypeDependencies;
use lagodb_core::hooks::{
    AlterEnumStmtNode, HookError, PostUtilityContext, UtilityHook, UtilityHookError,
    UtilityNode, register_utility_hook,
};
use pgrx::{pg_sys, prelude::PgSqlErrorCode};

use crate::managed_table::catalog::IcebergAccessMethod;

struct IcebergEnumHook;

impl UtilityHook for IcebergEnumHook {
    fn name(&self) -> &'static str {
        "iceberg enum storage"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<AlterEnumStmtNode>()
            .expect("registered for AlterEnumStmt");
        // ADD VALUE preserves all stored labels and remains a native PG DDL.
        if stmt.oldVal.is_null() {
            return Ok(());
        }
        let Some(am) = IcebergAccessMethod::oid() else {
            return Ok(());
        };
        let oid = unsafe {
            let name = pg_sys::makeTypeNameFromNameList(stmt.typeName);
            let oid = pg_sys::typenameTypeId(ptr::null_mut(), name);
            // Same lock as RenameEnumLabel; serialize with managed schema
            // admission before inspecting the committed dependency graph.
            pg_sys::LockDatabaseObject(
                pg_sys::TypeRelationId,
                oid,
                0,
                pg_sys::ExclusiveLock as pg_sys::LOCKMODE,
            );
            oid
        };
        if TypeDependencies::used_by_access_method(oid, am)? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot rename an enum value used by a managed Iceberg column",
            ));
        }
        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        // This guard rejects managed label renames before PostgreSQL executes
        // them. Accepted enum DDL leaves stored labels unchanged, so it needs
        // no Iceberg storage update after execution.
        Ok(())
    }
}

pub(super) fn init_hook() {
    register_utility_hook(
        pg_sys::NodeTag::T_AlterEnumStmt,
        Box::new(IcebergEnumHook),
    );
}
