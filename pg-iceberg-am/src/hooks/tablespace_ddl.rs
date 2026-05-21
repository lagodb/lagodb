//! Distributed tablespace utility hooks.
//!
//! These hooks enforce the storage-identity contract from
//! `pg-lakebase-core/src/options/tablespace/cache.rs`:
//!
//! - The `store_id` is the tablespace name. To keep the storage cache and
//!   staging directories human-readable, the name is part of the storage
//!   identity, so distributed tablespaces must not be renamed.
//! - The first version treats distributed tablespaces as immutable: option
//!   changes via `ALTER TABLESPACE ... SET/RESET (...)` are rejected. Updates
//!   require `DROP TABLESPACE` + `CREATE TABLESPACE` so the reconciler sees
//!   them as a clean unregister/register pair.
//!
//! The hooks are intentionally limited to the rules above; no helper
//! functions for catalog mutation or option parsing leak in here. The shared
//! tablespace option parser owns all validation.

use pg_lakebase_core::catalog::get_tablespace_oid;
use pg_lakebase_core::hooks::{
    AlterTableSpaceOptionsStmtNode, CreateTableSpaceStmtNode, HookError,
    PostUtilityContext, RenameStmtNode, UtilityHook, UtilityHookError, UtilityNode,
    register_utility_hook,
};
use pg_lakebase_core::options::{TablespaceOptions, is_distributed_tablespace};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use std::ffi::CStr;

// ---------------------------------------------------------------------------
//  CREATE TABLESPACE
// ---------------------------------------------------------------------------

struct IcebergCreateTablespaceHook;

impl UtilityHook for IcebergCreateTablespaceHook {
    fn name(&self) -> &'static str {
        "iceberg create tablespace options"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast_mut::<CreateTableSpaceStmtNode>()
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        TablespaceOptions::extract_from_stmt(stmt)?;
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let stmt = context
            .original_stmt()
            .cast::<CreateTableSpaceStmtNode>()
            .expect("Hook registered for T_CreateTableSpaceStmt but received different node type");

        if let Some(opts) = TablespaceOptions::read_from_stmt(stmt)? {
            let spcname = unsafe { CStr::from_ptr(stmt.tablespacename) };
            let oid = get_tablespace_oid(spcname, false)?;
            opts.persist_to_catalog(oid)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
//  ALTER TABLESPACE ... RENAME TO
// ---------------------------------------------------------------------------

/// Rejects `ALTER TABLESPACE name RENAME TO new_name` for distributed
/// tablespaces. The store id is the tablespace name; renaming would either
/// orphan cache/staging directories or require non-trivial migration.
struct IcebergRenameTablespaceGuard;

impl UtilityHook for IcebergRenameTablespaceGuard {
    fn name(&self) -> &'static str {
        "iceberg rename tablespace guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context.cast::<RenameStmtNode>().expect(
            "Hook registered for T_RenameStmt but received different node type",
        );

        if stmt.renameType != pg_sys::ObjectType::OBJECT_TABLESPACE {
            return Ok(());
        }

        let old_name_ptr = stmt.subname;
        if old_name_ptr.is_null() {
            return Ok(());
        }

        let old_name = unsafe { CStr::from_ptr(old_name_ptr) };
        // `missing_ok = true` so a non-existent tablespace is left for
        // PostgreSQL to error on with its standard "tablespace does not
        // exist" message.
        let oid = get_tablespace_oid(old_name, true)?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }

        if is_distributed_tablespace(oid)? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                format!(
                    "cannot rename distributed tablespace \"{}\": the tablespace name is part of the storage identity",
                    old_name.to_string_lossy()
                ),
            ));
        }

        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
//  ALTER TABLESPACE ... SET / RESET (...)
// ---------------------------------------------------------------------------

/// Rejects every `ALTER TABLESPACE ... SET (...)` and `RESET (...)` against a
/// distributed tablespace. Distributed tablespaces are immutable in this
/// release: option changes have to go through `DROP TABLESPACE` +
/// `CREATE TABLESPACE` so the reconciler sees them as a clean
/// unregister/register cycle.
struct IcebergAlterTablespaceOptionsGuard;

impl UtilityHook for IcebergAlterTablespaceOptionsGuard {
    fn name(&self) -> &'static str {
        "iceberg alter tablespace options guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<AlterTableSpaceOptionsStmtNode>()
            .expect("Hook registered for T_AlterTableSpaceOptionsStmt but received different node type");

        let name_ptr = stmt.tablespacename;
        if name_ptr.is_null() {
            return Ok(());
        }

        let name = unsafe { CStr::from_ptr(name_ptr) };
        let oid = get_tablespace_oid(name, true)?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }

        if !is_distributed_tablespace(oid)? {
            return Ok(());
        }

        Err(HookError::with_code(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            format!(
                "cannot ALTER options on distributed tablespace \"{}\": distributed tablespaces are immutable; recreate with DROP TABLESPACE + CREATE TABLESPACE",
                name.to_string_lossy()
            ),
        ))
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
//  Registration
// ---------------------------------------------------------------------------

pub fn init_hook() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreateTableSpaceStmt,
        Box::new(IcebergCreateTablespaceHook),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_RenameStmt,
        Box::new(IcebergRenameTablespaceGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt,
        Box::new(IcebergAlterTablespaceOptionsGuard),
    );
}
