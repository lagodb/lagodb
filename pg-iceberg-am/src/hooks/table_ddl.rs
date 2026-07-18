use crate::catalog::metadata_table::IcebergMetadata;
use crate::catalog::schema_evolution::SchemaEvolutionUpdate;
use crate::catalog::table_lifecycle::IcebergTableLifecycle;
use crate::catalog::table_properties::PreparedTablePropertyUpdate;
use crate::catalog::{IcebergAccessMethod, IcebergRelationExt};
use crate::hooks::column_drop_guard::ControlledColumnDrops;
use crate::options::{ICEBERG_TABLE_OPTIONS, ResolvedIcebergOptions};
use pg_lakebase_core::catalog::{
    CatalogRelation, CatalogScanKey, CatalogSnapshot, find_all_inheritors,
    get_tablespace_oid, range_var_get_relid,
};
use pg_lakebase_core::handles::{RelationGuard, RelationHandle};
use pg_lakebase_core::hooks::{
    AlterTableMoveAllStmtNode, AlterTableStmtNode, CreateStmtNode,
    CreateTableAsStmtNode, HookError, PostUtilityContext, RenameStmtNode,
    UtilityHook, UtilityHookError, UtilityNode, register_utility_hook,
};
use pg_lakebase_core::options::{TableOptionAlterations, TableOptions};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;
use std::ffi::CStr;

struct IcebergTableHook;

struct IcebergCreateTableAsGuard;
struct IcebergAlterTableGuard;
struct IcebergAlterTableMoveAllGuard;
struct IcebergRenameColumnHook;

#[derive(Debug, Default)]
struct AlterTableIcebergOperations {
    sets_access_method: bool,
    sets_access_method_to_iceberg: bool,
    sets_tablespace: bool,
    alters_table_options: bool,
    schema_update: SchemaEvolutionUpdate,
    unsupported_schema_operation: Option<String>,
    required_lockmode: pg_sys::LOCKMODE,
}

#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum AlterTableSchemaPass {
    Drop,
    AddColumn,
}

#[derive(Debug)]
enum AlterTableSchemaOp {
    AddNullableColumn(String),
    DropColumn(String),
    DropNotNull(String),
}

#[derive(Debug)]
struct PlannedSchemaOp {
    pass: AlterTableSchemaPass,
    original_index: i32,
    op: AlterTableSchemaOp,
}

#[derive(Debug, Default)]
struct AlterTableSchemaPlan {
    ops: Vec<PlannedSchemaOp>,
}

enum SchemaEvolutionTarget {
    Physical(pg_sys::Oid),
    PartitionedRoot { descendant_relids: Vec<pg_sys::Oid> },
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
                pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
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

impl AlterTableSchemaOp {
    fn append_to(self, update: &mut SchemaEvolutionUpdate) {
        match self {
            Self::AddNullableColumn(name) => update.add_nullable_column(name),
            Self::DropColumn(name) => update.drop_column(name),
            Self::DropNotNull(name) => update.drop_not_null(name),
        }
    }
}

impl AlterTableSchemaPlan {
    fn push(
        &mut self,
        pass: AlterTableSchemaPass,
        original_index: i32,
        op: AlterTableSchemaOp,
    ) {
        self.ops.push(PlannedSchemaOp {
            pass,
            original_index,
            op,
        });
    }

    fn into_update(mut self) -> SchemaEvolutionUpdate {
        self.ops.sort_by_key(|op| (op.pass, op.original_index));
        let mut update = SchemaEvolutionUpdate::new();
        for op in self.ops {
            op.op.append_to(&mut update);
        }
        update
    }
}

impl SchemaEvolutionTarget {
    fn resolve(
        rel: &RelationHandle<'_>,
        descendant_lockmode: pg_sys::LOCKMODE,
    ) -> Result<Option<Self>, UtilityHookError> {
        if !rel.is_iceberg() {
            return Ok(None);
        }

        match rel.relkind() as u8 {
            pg_sys::RELKIND_RELATION => Ok(Some(Self::Physical(rel.oid()))),
            pg_sys::RELKIND_PARTITIONED_TABLE => {
                let descendant_relids =
                    find_all_inheritors(rel.oid(), descendant_lockmode)?
                        .into_iter()
                        .filter(|relid| *relid != rel.oid())
                        .collect();
                Ok(Some(Self::PartitionedRoot { descendant_relids }))
            }
            _ => Err(HookError::with_code(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "schema evolution is only supported for physical and partitioned Iceberg relations",
            )),
        }
    }

    fn for_each_physical_relation<F>(&self, mut f: F) -> Result<(), UtilityHookError>
    where
        F: FnMut(&RelationHandle<'_>) -> Result<(), UtilityHookError>,
    {
        // Callers acquire the target/descendant locks before constructing this
        // target, matching PostgreSQL's ALTER TABLE recursion path. Open with
        // NoLock here so we do not introduce a weaker pre-lock or a lock
        // upgrade path that differs from tablecmds.c.
        match self {
            Self::Physical(relid) => {
                let guard =
                    RelationGuard::open(*relid, pg_sys::NoLock as pg_sys::LOCKMODE)?;
                let rel = guard.as_handle();
                f(&rel)
            }
            Self::PartitionedRoot { descendant_relids } => {
                for relid in descendant_relids {
                    let guard = RelationGuard::open(
                        *relid,
                        pg_sys::NoLock as pg_sys::LOCKMODE,
                    )?;
                    let rel = guard.as_handle();
                    match rel.relkind() as u8 {
                        pg_sys::RELKIND_RELATION => {
                            if !rel.is_iceberg() {
                                return Err(HookError::with_code(
                                    PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                                    "partitioned Iceberg schema evolution requires every physical partition to use the Iceberg access method",
                                ));
                            }
                            f(&rel)?;
                        }
                        pg_sys::RELKIND_PARTITIONED_TABLE => {}
                        _ => {
                            return Err(HookError::with_code(
                                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                                "partitioned Iceberg schema evolution found an unsupported partition relation kind",
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn preflight(
        &self,
        update: &SchemaEvolutionUpdate,
    ) -> Result<(), UtilityHookError> {
        self.for_each_physical_relation(|rel| {
            update.preflight_existing_schema_for_relation(rel)?;
            Ok(())
        })
    }

    fn stage(&self, update: &SchemaEvolutionUpdate) -> Result<(), UtilityHookError> {
        self.for_each_physical_relation(|rel| {
            update.stage_for_relation(rel)?;
            Ok(())
        })
    }

    fn controlled_column_drop_keys(
        &self,
        update: &SchemaEvolutionUpdate,
    ) -> Result<Vec<(pg_sys::Oid, i32)>, UtilityHookError> {
        let names: Vec<&str> = update.drop_column_names().collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        self.for_each_physical_relation(|rel| {
            let columns = rel.live_columns();
            for name in &names {
                let (attnum, _) = columns
                    .iter()
                    .find(|(_, column_name)| column_name.as_str() == *name)
                    .ok_or_else(|| {
                        HookError::with_code(
                            PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN,
                            format!(
                                "column \"{}\" does not exist on Iceberg relation \"{}\"",
                                name,
                                rel.relation_name()
                            ),
                        )
                    })?;
                keys.push((rel.oid(), i32::from(*attnum)));
            }
            Ok(())
        })?;
        Ok(keys)
    }
}

impl AlterTableIcebergOperations {
    fn from_command_list(cmds: *mut pg_sys::List) -> Self {
        let mut result = Self::default();
        if cmds.is_null() {
            return result;
        }

        let mut schema_plan = AlterTableSchemaPlan::default();
        result.alters_table_options = unsafe {
            TableOptionAlterations::commands_contain_options(
                cmds,
                ICEBERG_TABLE_OPTIONS,
            )
        };
        if result.alters_table_options {
            // PostgreSQL cannot infer a lock level for AM-owned option names.
            // Serialize catalog replacement and rd_amcache invalidation.
            result.require_lock(pg_sys::AccessExclusiveLock as _);
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
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.sets_access_method = true;
                    // SAFETY: `name_ptr` belongs to the same AlterTableCmd we
                    // borrow from the parse tree.
                    result.sets_access_method_to_iceberg |= unsafe {
                        IcebergStmtProbe::explicit_or_default_is_iceberg(name_ptr)
                    };
                }
                pg_sys::AlterTableType::AT_SetTableSpace => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.sets_tablespace = true;
                }
                pg_sys::AlterTableType::AT_SetLogged
                | pg_sys::AlterTableType::AT_SetUnLogged => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "ALTER TABLE SET LOGGED/UNLOGGED is not supported for Iceberg relations",
                    );
                }
                pg_sys::AlterTableType::AT_AddColumn => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    if let Some(op) = result.inspect_add_column(cmd) {
                        schema_plan.push(AlterTableSchemaPass::AddColumn, idx, op);
                    }
                }
                pg_sys::AlterTableType::AT_DropColumn => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    if let Some(op) = result.inspect_drop_column(cmd) {
                        schema_plan.push(AlterTableSchemaPass::Drop, idx, op);
                    }
                }
                pg_sys::AlterTableType::AT_DropNotNull => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    if let Some(op) = result.inspect_drop_not_null(cmd) {
                        schema_plan.push(AlterTableSchemaPass::Drop, idx, op);
                    }
                }
                pg_sys::AlterTableType::AT_ColumnDefault
                | pg_sys::AlterTableType::AT_CookedColumnDefault => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "ALTER COLUMN SET/DROP DEFAULT is not supported for Iceberg relations",
                    );
                }
                pg_sys::AlterTableType::AT_SetNotNull => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "ALTER COLUMN SET NOT NULL is not supported for Iceberg relations",
                    );
                }
                pg_sys::AlterTableType::AT_AlterColumnType => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "ALTER COLUMN TYPE is not supported for Iceberg relations",
                    );
                }
                pg_sys::AlterTableType::AT_AddConstraint => {
                    result.require_lock(Self::add_constraint_lockmode(cmd));
                    result.reject_schema_operation(
                        "column constraints, identity, and generated columns are not supported for Iceberg relations",
                    );
                }
                pg_sys::AlterTableType::AT_DropConstraint
                | pg_sys::AlterTableType::AT_AlterConstraint
                | pg_sys::AlterTableType::AT_AddIdentity
                | pg_sys::AlterTableType::AT_SetIdentity
                | pg_sys::AlterTableType::AT_DropIdentity
                | pg_sys::AlterTableType::AT_DropExpression => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "column constraints, identity, and generated columns are not supported for Iceberg relations",
                    );
                }
                #[cfg(feature = "pg17")]
                pg_sys::AlterTableType::AT_SetExpression => {
                    result.require_lock(pg_sys::AccessExclusiveLock as _);
                    result.reject_schema_operation(
                        "column constraints, identity, and generated columns are not supported for Iceberg relations",
                    );
                }
                _ => {}
            }
        }

        result.schema_update = schema_plan.into_update();
        result
    }

    fn require_lock(&mut self, lockmode: pg_sys::LOCKMODE) {
        if lockmode > self.required_lockmode {
            self.required_lockmode = lockmode;
        }
    }

    fn lockmode(&self) -> pg_sys::LOCKMODE {
        debug_assert!(self.has_guarded_operation());
        if self.required_lockmode == pg_sys::NoLock as pg_sys::LOCKMODE {
            pg_sys::AccessShareLock as pg_sys::LOCKMODE
        } else {
            self.required_lockmode
        }
    }

    fn add_constraint_lockmode(
        cmd: *const pg_sys::AlterTableCmd,
    ) -> pg_sys::LOCKMODE {
        let command = unsafe { &*cmd };
        if command.def.is_null() {
            return pg_sys::AccessExclusiveLock as _;
        }

        let constraint = command.def as *const pg_sys::Constraint;
        if unsafe { (*constraint).contype } == pg_sys::ConstrType::CONSTR_FOREIGN {
            pg_sys::ShareRowExclusiveLock as _
        } else {
            pg_sys::AccessExclusiveLock as _
        }
    }

    fn has_guarded_operation(&self) -> bool {
        self.sets_access_method
            || self.sets_tablespace
            || self.alters_table_options
            || !self.schema_update.is_empty()
            || self.unsupported_schema_operation.is_some()
    }

    fn reject_schema_operation(&mut self, message: impl Into<String>) {
        if self.unsupported_schema_operation.is_none() {
            self.unsupported_schema_operation = Some(message.into());
        }
    }

    fn inspect_add_column(
        &mut self,
        cmd: *const pg_sys::AlterTableCmd,
    ) -> Option<AlterTableSchemaOp> {
        let command = unsafe { &*cmd };
        if command.missing_ok {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN IF NOT EXISTS is not supported for Iceberg relations",
            );
            return None;
        }

        let coldef = command.def as *const pg_sys::ColumnDef;
        if coldef.is_null() {
            self.reject_schema_operation(
                "invalid ALTER TABLE ADD COLUMN command tree for Iceberg schema evolution",
            );
            return None;
        }

        let coldef = unsafe { &*coldef };
        if coldef.is_not_null {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN with NOT NULL is not supported for Iceberg relations",
            );
            return None;
        }
        if !coldef.raw_default.is_null() || !coldef.cooked_default.is_null() {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN with DEFAULT is not supported for Iceberg relations",
            );
            return None;
        }
        if coldef.identity != 0 {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN with identity is not supported for Iceberg relations",
            );
            return None;
        }
        if coldef.generated != 0 {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN with generated expression is not supported for Iceberg relations",
            );
            return None;
        }
        if Self::has_extra_column_constraints(coldef) {
            self.reject_schema_operation(
                "ALTER TABLE ADD COLUMN constraints are not supported for Iceberg relations",
            );
            return None;
        }

        match Self::string_from_ptr(coldef.colname) {
            Some(name) => Some(AlterTableSchemaOp::AddNullableColumn(name)),
            None => {
                self.reject_schema_operation(
                    "invalid ALTER TABLE ADD COLUMN command tree for Iceberg schema evolution",
                );
                None
            }
        }
    }

    fn inspect_drop_column(
        &mut self,
        cmd: *const pg_sys::AlterTableCmd,
    ) -> Option<AlterTableSchemaOp> {
        let command = unsafe { &*cmd };
        if command.missing_ok {
            self.reject_schema_operation(
                "ALTER TABLE DROP COLUMN IF EXISTS is not supported for Iceberg relations",
            );
            return None;
        }
        if command.behavior == pg_sys::DropBehavior::DROP_CASCADE {
            self.reject_schema_operation(
                "ALTER TABLE DROP COLUMN CASCADE is not supported for Iceberg relations",
            );
            return None;
        }

        match Self::string_from_ptr(command.name) {
            Some(name) => Some(AlterTableSchemaOp::DropColumn(name)),
            None => {
                self.reject_schema_operation(
                    "invalid ALTER TABLE DROP COLUMN command tree for Iceberg schema evolution",
                );
                None
            }
        }
    }

    fn inspect_drop_not_null(
        &mut self,
        cmd: *const pg_sys::AlterTableCmd,
    ) -> Option<AlterTableSchemaOp> {
        let command = unsafe { &*cmd };
        match Self::string_from_ptr(command.name) {
            Some(name) => Some(AlterTableSchemaOp::DropNotNull(name)),
            None => {
                self.reject_schema_operation(
                    "invalid ALTER TABLE ALTER COLUMN DROP NOT NULL command tree for Iceberg schema evolution",
                );
                None
            }
        }
    }

    fn has_extra_column_constraints(coldef: &pg_sys::ColumnDef) -> bool {
        if coldef.constraints.is_null() {
            return false;
        }

        let len = unsafe { pg_sys::list_length(coldef.constraints) };
        for idx in 0..len {
            let constraint = unsafe { pg_sys::list_nth(coldef.constraints, idx) }
                as *const pg_sys::Constraint;
            if constraint.is_null() {
                return true;
            }
            let contype = unsafe { (*constraint).contype };
            if contype != pg_sys::ConstrType::CONSTR_NULL {
                return true;
            }
        }
        false
    }

    fn string_from_ptr(ptr: *const std::ffi::c_char) -> Option<String> {
        (!ptr.is_null())
            .then(|| unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() })
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
            PreparedTablePropertyUpdate::from_options(resolved)
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

        let old_name = AlterTableIcebergOperations::string_from_ptr(stmt.subname)?;
        let new_name = AlterTableIcebergOperations::string_from_ptr(stmt.newname)?;

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
