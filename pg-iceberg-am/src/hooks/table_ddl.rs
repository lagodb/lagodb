use crate::catalog::metadata_table::IcebergMetadata;
use crate::catalog::table_lifecycle::IcebergTableLifecycle;
use crate::catalog::{IcebergAccessMethod, IcebergRelationExt};
use crate::options::ICEBERG_TABLE_OPTIONS;
use pg_lakebase_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, get_tablespace_oid,
    range_var_get_relid,
};
use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::hooks::{
    AlterTableMoveAllStmtNode, AlterTableStmtNode, CreateStmtNode,
    CreateTableAsStmtNode, HookError, PostUtilityContext, UtilityHook,
    UtilityHookError, UtilityNode, register_utility_hook,
};
use pg_lakebase_core::options::TableOptions;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use std::ffi::CStr;

struct IcebergTableHook;

struct IcebergCreateTableAsGuard;
struct IcebergAlterTableGuard;
struct IcebergAlterTableMoveAllGuard;

#[derive(Debug, Default)]
struct AlterTableIcebergOperations {
    sets_access_method: bool,
    sets_access_method_to_iceberg: bool,
    sets_tablespace: bool,
}

/// Parse-tree probes that decide whether a DDL statement targets the Iceberg
/// AM. These live in the hook layer because they depend on PostgreSQL
/// parse-tree types; the catalog layer only owns the AM identity itself.
struct IcebergStmtProbe;

impl IcebergStmtProbe {
    /// True iff the cluster-default `default_table_access_method` is Iceberg.
    fn default_is_iceberg() -> bool {
        // SAFETY: `default_table_access_method` is a process-wide GUC value
        // exposed as a `*const c_char` and is null-terminated for the duration
        // of the GUC machinery's lifetime.
        let default = unsafe { pg_sys::default_table_access_method };
        if default.is_null() {
            return false;
        }
        IcebergAccessMethod::matches_name(unsafe { CStr::from_ptr(default) })
    }

    /// True iff the user-supplied AM pointer (or the cluster default, if the
    /// pointer is null) names the Iceberg AM.
    ///
    /// # Safety
    ///
    /// If `am` is non-null it must point to a NUL-terminated C string owned by
    /// the current parse-tree context.
    unsafe fn explicit_or_default_is_iceberg(am: *const std::ffi::c_char) -> bool {
        if am.is_null() {
            return Self::default_is_iceberg();
        }
        IcebergAccessMethod::matches_name(unsafe { CStr::from_ptr(am) })
    }

    fn create_stmt_may_use_iceberg(
        stmt: &pg_sys::CreateStmt,
    ) -> Result<bool, UtilityHookError> {
        if !stmt.accessMethod.is_null() {
            return Ok(IcebergAccessMethod::matches_name(unsafe {
                CStr::from_ptr(stmt.accessMethod)
            }));
        }

        if stmt.partbound.is_null() {
            return Ok(Self::default_is_iceberg());
        }

        if stmt.inhRelations.is_null() {
            return Ok(false);
        }
        let parent = unsafe {
            if pg_sys::list_length(stmt.inhRelations) == 0 {
                return Ok(false);
            }
            pg_sys::list_nth(stmt.inhRelations, 0) as *mut pg_sys::RangeVar
        };

        if parent.is_null() {
            return Ok(false);
        }

        let parent_oid = unsafe {
            range_var_get_relid(
                parent,
                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                true,
            )
        }?;
        if parent_oid == pg_sys::InvalidOid {
            return Ok(false);
        }

        Ok(IcebergAccessMethod::matches_oid(unsafe {
            pg_sys::get_rel_relam(parent_oid)
        }))
    }

    fn create_table_as_stmt_uses_iceberg(stmt: &pg_sys::CreateTableAsStmt) -> bool {
        if stmt.into.is_null() {
            return false;
        }

        let into = unsafe { &*stmt.into };
        // SAFETY: `into.accessMethod` is owned by the same parse-tree node we
        // were handed by the utility hook.
        unsafe { Self::explicit_or_default_is_iceberg(into.accessMethod) }
    }
}

impl AlterTableIcebergOperations {
    fn from_command_list(cmds: *mut pg_sys::List) -> Self {
        let mut result = Self::default();
        if cmds.is_null() {
            return result;
        }

        let len = unsafe { pg_sys::list_length(cmds) };
        for idx in 0..len {
            let cmd = unsafe {
                pg_sys::list_nth(cmds, idx) as *const pg_sys::AlterTableCmd
            };
            if cmd.is_null() {
                continue;
            }

            let (subtype, name_ptr) = unsafe { ((*cmd).subtype, (*cmd).name) };
            match subtype {
                pg_sys::AlterTableType::AT_SetAccessMethod => {
                    result.sets_access_method = true;
                    // SAFETY: `name_ptr` belongs to the same AlterTableCmd we
                    // borrow from the parse tree.
                    result.sets_access_method_to_iceberg |= unsafe {
                        IcebergStmtProbe::explicit_or_default_is_iceberg(name_ptr)
                    };
                }
                pg_sys::AlterTableType::AT_SetTableSpace => {
                    result.sets_tablespace = true;
                }
                // TODO(schema-evolution): ALTER TABLE subcommands that mutate
                // the column set (notably AT_DropColumn, also AT_AddColumn /
                // AT_AlterColumnType) fall through here and are never
                // propagated to the stored Iceberg metadata schema. This is a
                // real, reachable data-path bug, not a theoretical edge:
                // dropping a NOT NULL column leaves a `required` field with no
                // live PG source lingering in the Iceberg schema, so every
                // subsequent INSERT fails permanently in
                // `WriteColumns::resolve_columns` with
                // `RequiredColumnMissingSource`. The fix is to translate these
                // subcommands into proper Iceberg schema-evolution updates
                // (and at minimum downgrade the dropped field's `required`
                // bit), not to widen this guard. Deferred for now.
                _ => {}
            }
        }

        result
    }

    fn has_guarded_operation(&self) -> bool {
        self.sets_access_method || self.sets_tablespace
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

        if !IcebergStmtProbe::create_stmt_may_use_iceberg(stmt)? {
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

        if let Some(opts) = TableOptions::read_from_stmt(stmt, ICEBERG_TABLE_OPTIONS)?
        {
            opts.persist_to_catalog(oid)?;
        }

        let metadata_location = IcebergTableLifecycle::new(&rel)?.init()?;

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
        "iceberg alter table storage identity guard"
    }

    fn on_pre(&self, context: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let stmt = context
            .cast::<AlterTableStmtNode>()
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

        let oid = unsafe {
            range_var_get_relid(
                stmt.relation,
                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                true,
            )
        }?;
        if oid == pg_sys::InvalidOid {
            return Ok(());
        }

        let guard =
            RelationGuard::open(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)?;
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

        Ok(())
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
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
        pg_sys::NodeTag::T_AlterTableMoveAllStmt,
        Box::new(IcebergAlterTableMoveAllGuard),
    );
}
