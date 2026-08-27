mod planning;

use crate::managed_table::catalog::metadata_table::IcebergMetadata;
use crate::managed_table::catalog::schema_evolution::SchemaEvolutionUpdate;
use crate::managed_table::catalog::table_lifecycle::IcebergTableLifecycle;
use crate::managed_table::catalog::table_properties::ManagedTablePropertyUpdate;
use crate::managed_table::catalog::{IcebergAccessMethod, IcebergRelationExt};
use crate::managed_table::hooks::column_drop_guard::ControlledColumnDrops;
use crate::managed_table::options::{ICEBERG_TABLE_OPTIONS, ResolvedIcebergOptions};
use lagodb_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, get_tablespace_oid,
    range_var_get_relid,
};
use lagodb_core::handles::RelationGuard;
use lagodb_core::hooks::{
    AlterTableMoveAllStmtNode, AlterTableStmtNode, CreateStmtNode,
    CreateTableAsStmtNode, HookError, PostUtilityContext, RenameStmtNode,
    UtilityHook, UtilityHookError, UtilityNode, register_utility_hook,
};
use lagodb_core::options::{TableOptionAlterations, TableOptions};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use std::ffi::CStr;

use self::planning::{
    AlterTableIcebergOperations, IcebergStmtProbe, SchemaEvolutionTarget,
};

struct IcebergTableHook;

struct IcebergCreateTableAsGuard;
struct IcebergAlterTableGuard;
struct IcebergAlterTableMoveAllGuard;
struct IcebergRenameColumnHook;

impl UtilityHook for IcebergTableHook {
    fn name(&self) -> &'static str {
        "iceberg table options"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast_mut::<CreateStmtNode>()
            .expect("Hook registered for T_CreateStmt");

        if !IcebergStmtProbe::create_stmt_may_use_iceberg(stmt)? {
            return Ok(());
        }

        if stmt.oncommit == pg_sys::OnCommitAction::ONCOMMIT_DELETE_ROWS {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "Iceberg tables do not support ON COMMIT DELETE ROWS",
            ));
        }

        TableOptions::extract_from_stmt(stmt, ICEBERG_TABLE_OPTIONS)?;
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let stmt = context
            .original_stmt()
            .cast::<CreateStmtNode>()
            .expect("Hook registered for T_CreateStmt");

        // NoLock is safe here because post-success CREATE TABLE already holds
        // the relation lock; this lookup only resolves the RangeVar to an OID.
        // SAFETY: `stmt` is a live CreateStmt from the utility hook, and its
        // RangeVar belongs to that PostgreSQL parse tree for this callback.
        let oid = unsafe {
            range_var_get_relid(
                stmt.relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                false,
            )
        }?;

        let guard =
            RelationGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)?;
        let rel = guard.as_handle();

        if !rel.is_iceberg() {
            return Ok(());
        }

        if stmt.if_not_exists && IcebergMetadata::exists(oid)? {
            return Ok(());
        }

        // Partitioned roots carry a table access method in PostgreSQL catalog
        // state, but they do not own physical storage. Iceberg metadata is
        // initialized only for physical child relations.
        if rel.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
            return Ok(());
        }

        if !Self::relkind_has_physical_storage(rel.relkind()) {
            return Ok(());
        }

        let table_options =
            TableOptions::read_from_stmt(stmt, ICEBERG_TABLE_OPTIONS)?;
        let creation_options =
            ResolvedIcebergOptions::from_table_options(table_options.as_ref())?;
        if let Some(opts) = table_options.as_ref() {
            opts.persist_to_catalog(oid)?;
        }

        let metadata_location =
            IcebergTableLifecycle::new(&rel)?.init(creation_options)?;

        IcebergMetadata::new(oid)
            .with_metadata_location(metadata_location)
            .with_default_spec_id(0)
            .insert()?;
        Ok(())
    }
}

impl IcebergTableHook {
    fn relkind_has_physical_storage(relkind: i8) -> bool {
        relkind as u8 == pg_sys::RELKIND_RELATION
    }
}

impl UtilityHook for IcebergCreateTableAsGuard {
    fn name(&self) -> &'static str {
        "iceberg create table as guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<CreateTableAsStmtNode>()
            .expect("Hook registered for T_CreateTableAsStmt");

        if stmt.objtype != pg_sys::ObjectType::OBJECT_TABLE
            && stmt.objtype != pg_sys::ObjectType::OBJECT_MATVIEW
        {
            return Ok(());
        }

        if !IcebergStmtProbe::create_table_as_stmt_uses_iceberg(stmt) {
            return Ok(());
        }

        Err(HookError::with_code(
            PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            "cannot use Iceberg table access method with CREATE TABLE AS or CREATE MATERIALIZED VIEW",
        ))
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

impl UtilityHook for IcebergAlterTableGuard {
    fn name(&self) -> &'static str {
        "iceberg alter table DDL guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast_mut::<AlterTableStmtNode>()
            .expect("Hook registered for T_AlterTableStmt");

        if stmt.objtype != pg_sys::ObjectType::OBJECT_TABLE
            && stmt.objtype != pg_sys::ObjectType::OBJECT_MATVIEW
        {
            return Ok(());
        }

        let ops = AlterTableIcebergOperations::from_command_list(stmt.cmds);
        if !ops.has_guarded_operation() {
            return Ok(());
        }

        let oid =
            unsafe { range_var_get_relid(stmt.relation, ops.lockmode(), true) }?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }

        let guard = RelationGuard::open(oid, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        let rel = guard.as_handle();
        let current_is_iceberg = rel.is_iceberg();

        if ops.sets_access_method_to_iceberg
            || (ops.sets_access_method && current_is_iceberg)
        {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot ALTER TABLE SET ACCESS METHOD for Iceberg relations: access-method migration needs an explicit Iceberg lifecycle design",
            ));
        }

        if ops.sets_tablespace && current_is_iceberg {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot ALTER TABLE SET TABLESPACE for Iceberg relations: storage identity migration is not supported",
            ));
        }

        if current_is_iceberg {
            if let Some(message) = ops.unsupported_schema_operation.as_deref() {
                return Err(HookError::with_code(
                    PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                    message,
                ));
            }
            if !ops.schema_update.is_empty() {
                let Some(target) = SchemaEvolutionTarget::resolve(
                    &rel,
                    pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
                )?
                else {
                    return Ok(());
                };
                target.preflight(&ops.schema_update)?;
                ControlledColumnDrops::authorize(
                    target.controlled_column_drop_keys(&ops.schema_update)?,
                );
            }
            if ops.alters_table_options {
                if rel.relkind() as u8 == pg_sys::RELKIND_PARTITIONED_TABLE {
                    return Err(HookError::with_code(
                        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "ALTER TABLE options on partitioned Iceberg roots are not supported",
                    ));
                }
                let alterations = unsafe {
                    TableOptionAlterations::extract_from_commands(
                        stmt.cmds,
                        ICEBERG_TABLE_OPTIONS,
                    )?
                };
                let options = alterations
                    .apply_to_overrides(TableOptions::load_from_catalog(oid)?);
                ResolvedIcebergOptions::from_table_options(Some(&options))?;
                options.replace_in_catalog(oid)?;
            }
        }

        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let stmt = context
            .original_stmt()
            .cast::<AlterTableStmtNode>()
            .expect("Hook registered for T_AlterTableStmt");

        if stmt.objtype != pg_sys::ObjectType::OBJECT_TABLE
            && stmt.objtype != pg_sys::ObjectType::OBJECT_MATVIEW
        {
            return Ok(());
        }

        let ops = AlterTableIcebergOperations::from_command_list(stmt.cmds);
        if ops.schema_update.is_empty() && !ops.alters_table_options {
            return Ok(());
        }

        let oid = unsafe {
            range_var_get_relid(
                stmt.relation,
                pg_sys::NoLock as pg_sys::LOCKMODE,
                true,
            )
        }?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }

        let guard = RelationGuard::open(oid, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        let rel = guard.as_handle();
        if ops.alters_table_options {
            let options = TableOptions::load_from_catalog(oid)?;
            let resolved =
                ResolvedIcebergOptions::from_table_options(options.as_ref())?;
            ManagedTablePropertyUpdate::from_options(resolved)
                .stage_for_relation(&rel)?;
        }
        if ops.schema_update.is_empty() {
            return Ok(());
        }
        let Some(target) =
            SchemaEvolutionTarget::resolve(&rel, pg_sys::NoLock as pg_sys::LOCKMODE)?
        else {
            return Ok(());
        };
        target.stage(&ops.schema_update)?;
        if ops.schema_update.has_drop_column() {
            ControlledColumnDrops::finish()?;
        }
        Ok(())
    }
}

impl IcebergRenameColumnHook {
    fn update_from_stmt(stmt: &pg_sys::RenameStmt) -> Option<SchemaEvolutionUpdate> {
        if stmt.renameType != pg_sys::ObjectType::OBJECT_COLUMN {
            return None;
        }
        if stmt.relation.is_null() {
            return None;
        }

        let old_name = optional_string_from_ptr(stmt.subname)?;
        let new_name = optional_string_from_ptr(stmt.newname)?;

        let mut update = SchemaEvolutionUpdate::new();
        update.rename_column(old_name, new_name);
        Some(update)
    }

    fn iceberg_relation_guard(
        stmt: &pg_sys::RenameStmt,
        lookup_lockmode: pg_sys::LOCKMODE,
    ) -> Result<Option<RelationGuard>, UtilityHookError> {
        if stmt.renameType != pg_sys::ObjectType::OBJECT_COLUMN
            || stmt.relation.is_null()
        {
            return Ok(None);
        }

        let oid =
            unsafe { range_var_get_relid(stmt.relation, lookup_lockmode, true) }?;
        if oid == pg_sys::InvalidOid {
            return Ok(None);
        }

        let guard = RelationGuard::open(oid, pg_sys::NoLock as pg_sys::LOCKMODE)?;
        if guard.as_handle().is_iceberg() {
            Ok(Some(guard))
        } else {
            Ok(None)
        }
    }
}

impl UtilityHook for IcebergRenameColumnHook {
    fn name(&self) -> &'static str {
        "iceberg rename column schema evolution"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<RenameStmtNode>()
            .expect("Hook registered for T_RenameStmt");
        let Some(update) = Self::update_from_stmt(stmt) else {
            return Ok(());
        };
        let Some(guard) = Self::iceberg_relation_guard(
            stmt,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        )?
        else {
            return Ok(());
        };
        let rel = guard.as_handle();
        let Some(target) = SchemaEvolutionTarget::resolve(
            &rel,
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
        )?
        else {
            return Ok(());
        };
        target.preflight(&update)?;
        Ok(())
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        let stmt = context
            .original_stmt()
            .cast::<RenameStmtNode>()
            .expect("Hook registered for T_RenameStmt");
        let Some(update) = Self::update_from_stmt(stmt) else {
            return Ok(());
        };
        let Some(guard) =
            Self::iceberg_relation_guard(stmt, pg_sys::NoLock as pg_sys::LOCKMODE)?
        else {
            return Ok(());
        };
        let rel = guard.as_handle();
        let Some(target) =
            SchemaEvolutionTarget::resolve(&rel, pg_sys::NoLock as pg_sys::LOCKMODE)?
        else {
            return Ok(());
        };
        target.stage(&update)?;
        Ok(())
    }
}

impl UtilityHook for IcebergAlterTableMoveAllGuard {
    fn name(&self) -> &'static str {
        "iceberg alter table all in tablespace guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<AlterTableMoveAllStmtNode>()
            .expect("Hook registered for T_AlterTableMoveAllStmt");

        if stmt.objtype != pg_sys::ObjectType::OBJECT_TABLE
            && stmt.objtype != pg_sys::ObjectType::OBJECT_MATVIEW
        {
            return Ok(());
        }

        if stmt.orig_tablespacename.is_null() {
            return Ok(());
        }

        let spcname = unsafe { CStr::from_ptr(stmt.orig_tablespacename) };
        let spc_oid = get_tablespace_oid(spcname, true)?;
        if spc_oid == pg_sys::InvalidOid {
            return Ok(());
        }

        if Self::tablespace_contains_iceberg_relation(spc_oid, stmt.objtype)? {
            return Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "cannot ALTER TABLE ALL IN TABLESPACE when it would move Iceberg relations: storage identity migration is not supported",
            ));
        }

        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

impl IcebergAlterTableMoveAllGuard {
    fn tablespace_contains_iceberg_relation(
        spc_oid: pg_sys::Oid,
        objtype: pg_sys::ObjectType::Type,
    ) -> Result<bool, UtilityHookError> {
        let Some(iceberg_am_oid) = IcebergAccessMethod::oid() else {
            return Ok(false);
        };

        let catalog_values = if spc_oid == unsafe { pg_sys::MyDatabaseTableSpace } {
            [pg_sys::InvalidOid, spc_oid]
        } else {
            [spc_oid, pg_sys::InvalidOid]
        };
        let scan_count = if spc_oid == unsafe { pg_sys::MyDatabaseTableSpace } {
            2
        } else {
            1
        };

        let pg_class = CatalogRelation::open(
            pg_sys::RelationRelationId,
            pg_sys::AccessShareLock as _,
        )?;

        for catalog_spc_oid in catalog_values.into_iter().take(scan_count) {
            let mut scan = pg_class.begin_scan(
                pg_sys::ClassTblspcRelfilenodeIndexId.into(),
                true,
                CatalogSnapshot::Default,
                [CatalogScanKey::oid_eq(
                    pg_sys::Anum_pg_class_reltablespace as _,
                    catalog_spc_oid,
                )],
            )?;

            while let Some(tuple) = scan.get_next()? {
                let form = unsafe {
                    pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_class
                };
                if form.is_null() {
                    continue;
                }

                let class = unsafe { &*form };
                if class.relam == iceberg_am_oid
                    && Self::objtype_matches_relkind(objtype, class.relkind)
                {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    fn objtype_matches_relkind(
        objtype: pg_sys::ObjectType::Type,
        relkind: std::ffi::c_char,
    ) -> bool {
        match objtype {
            pg_sys::ObjectType::OBJECT_TABLE => {
                relkind as u8 == pg_sys::RELKIND_RELATION
                    || relkind as u8 == pg_sys::RELKIND_PARTITIONED_TABLE
            }
            pg_sys::ObjectType::OBJECT_MATVIEW => {
                relkind as u8 == pg_sys::RELKIND_MATVIEW
            }
            _ => false,
        }
    }
}

/// Copy a PostgreSQL parse-tree C string into an owned `String`, treating a
/// null pointer as absent. Non-UTF-8 bytes are replaced lossily.
///
/// Shared by the ALTER TABLE planners in [`planning`] and the RENAME COLUMN
/// hook; kept as a module-local helper because it is a pure FFI concern with
/// no ties to any particular hook type.
fn optional_string_from_ptr(ptr: *const std::ffi::c_char) -> Option<String> {
    (!ptr.is_null())
        // SAFETY: a non-null parse-tree name pointer is a NUL-terminated C
        // string owned by the current utility statement's memory context.
        .then(|| unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() })
}

pub fn init_hook() {
    register_utility_hook(pg_sys::NodeTag::T_CreateStmt, Box::new(IcebergTableHook));
    register_utility_hook(
        pg_sys::NodeTag::T_CreateTableAsStmt,
        Box::new(IcebergCreateTableAsGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterTableStmt,
        Box::new(IcebergAlterTableGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_RenameStmt,
        Box::new(IcebergRenameColumnHook),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterTableMoveAllStmt,
        Box::new(IcebergAlterTableMoveAllGuard),
    );
}
