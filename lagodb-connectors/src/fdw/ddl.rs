//! Foreign-table DDL policy and empty-schema inference.

use std::ffi::CStr;
use std::ptr;

use pg_lakebase_core::handles::RelationGuard;
use pg_lakebase_core::hooks::{
    AlterTableStmtNode, CreateForeignTableStmtNode, PostUtilityContext, UtilityHook,
    UtilityHookError, UtilityNode, register_utility_hook,
};
use pg_lakebase_core::storage::foreign::{ForeignOptionView, StorageManager};
use pgrx::pg_sys;

use crate::error::ConnectorError;
use crate::format::ResolvedForeignFormat;
use crate::storage::{ObjectInput, ResolvedStorageLocation};

use super::{ResolvedForeignRelation, ResolvedTableOptions, resolve_table_options};

struct ForeignTableDdlHook;

impl UtilityHook for ForeignTableDdlHook {
    fn name(&self) -> &'static str {
        "lagodb-connectors foreign-table DDL"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        match statement.tag() {
            pg_sys::NodeTag::T_CreateForeignTableStmt => {
                let create = statement
                    .cast_mut::<CreateForeignTableStmtNode>()
                    .expect("hook is registered for CreateForeignTableStmt");
                self.prepare_create(create)
            }
            pg_sys::NodeTag::T_AlterTableStmt => {
                let alter = statement
                    .cast_mut::<AlterTableStmtNode>()
                    .expect("hook is registered for AlterTableStmt");
                self.validate_alter(alter)
            }
            _ => unreachable!("foreign-table DDL hook registered for an invalid tag"),
        }
    }

    fn on_post(&self, context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        if context.tag() != pg_sys::NodeTag::T_AlterTableStmt {
            return Ok(());
        }
        let statement = context
            .original_stmt()
            .cast::<AlterTableStmtNode>()
            .expect("hook is registered for AlterTableStmt");
        if !ForeignTableDefinitionPolicy::changes_format_invariant(statement.cmds) {
            return Ok(());
        }
        self.validate_alter_result(statement)
    }
}

impl ForeignTableDdlHook {
    fn prepare_create(
        &self,
        statement: &mut pg_sys::CreateForeignTableStmt,
    ) -> Result<(), UtilityHookError> {
        let server_name = unsafe { CStr::from_ptr(statement.servername) };
        if !ResolvedStorageLocation::server_uses_lakebase(server_name) {
            return Ok(());
        }

        let mut existing_relation = pg_sys::InvalidOid;
        unsafe {
            pg_sys::RangeVarGetAndCheckCreationNamespace(
                statement.base.relation,
                pg_sys::NoLock as _,
                &mut existing_relation,
            );
        }
        if existing_relation != pg_sys::InvalidOid {
            return Ok(());
        }

        ForeignTableDefinitionPolicy::validate_create(&statement.base)?;
        let option_view = unsafe { ForeignOptionView::from_raw(statement.options) };
        let ResolvedTableOptions { object, format } =
            resolve_table_options(option_view)?;
        ForeignTableDefinitionPolicy::validate_create_column_options(
            &statement.base,
            &format,
        )?;
        let location = ResolvedStorageLocation::resolve_for_ddl(object, server_name)?;
        if unsafe { pg_sys::list_length(statement.base.tableElts) } != 0 {
            return Ok(());
        }

        let manager = StorageManager::from_pg_gucs()?;
        let mut files =
            ObjectInput::resolve(&location, &manager, format.kind())?.open();
        let mut file = files.next().ok_or_else(|| {
            ConnectorError::invalid_object_schema(
                format.kind(),
                "the resolved input contains no objects",
            )
        })??;
        let schema = format.infer_schema(&mut file);
        let close = file.close();
        let schema = schema?;
        close?;
        statement.base.tableElts = schema.into_pg_list()?;
        Ok(())
    }

    fn validate_alter(
        &self,
        statement: &mut pg_sys::AlterTableStmt,
    ) -> Result<(), UtilityHookError> {
        if !matches!(
            statement.objtype,
            pg_sys::ObjectType::OBJECT_TABLE
                | pg_sys::ObjectType::OBJECT_FOREIGN_TABLE
        ) {
            return Ok(());
        }

        let lockmode = unsafe { pg_sys::AlterTableGetLockLevel(statement.cmds) };
        let relation_oid =
            unsafe { pg_sys::AlterTableLookupRelation(statement, lockmode) };
        if relation_oid == pg_sys::InvalidOid {
            return Ok(());
        }
        let relation = RelationGuard::open(relation_oid, pg_sys::NoLock as _)?;
        let target_uses_lakebase = relation.as_handle().relkind() as u8
            == pg_sys::RELKIND_FOREIGN_TABLE
            && ResolvedStorageLocation::relation_uses_lakebase(relation_oid);

        self.validate_alter_references(statement.cmds)?;
        if target_uses_lakebase {
            ForeignTableDefinitionPolicy::validate_alter(statement.cmds)?;
        }
        Ok(())
    }

    fn validate_alter_result(
        &self,
        statement: &pg_sys::AlterTableStmt,
    ) -> Result<(), UtilityHookError> {
        if !matches!(
            statement.objtype,
            pg_sys::ObjectType::OBJECT_TABLE
                | pg_sys::ObjectType::OBJECT_FOREIGN_TABLE
        ) {
            return Ok(());
        }
        const RVR_MISSING_OK: u32 = 1;
        let flags = if statement.missing_ok {
            RVR_MISSING_OK
        } else {
            0
        };
        // SAFETY: the post hook receives PostgreSQL's live statement after
        // successful execution; missing_ok is passed through unchanged.
        let relation_oid = unsafe {
            pg_sys::RangeVarGetRelidExtended(
                statement.relation,
                pg_sys::NoLock as _,
                flags,
                None,
                ptr::null_mut(),
            )
        };
        // PostgreSQL returned a live relation OID unless missing_ok resolved
        // to InvalidOid, which is handled by the first branch.
        if relation_oid == pg_sys::InvalidOid
            || unsafe { pg_sys::get_rel_relkind(relation_oid) } as u8
                != pg_sys::RELKIND_FOREIGN_TABLE
            || !ResolvedStorageLocation::relation_uses_lakebase(relation_oid)
        {
            return Ok(());
        }
        let relation = RelationGuard::open(relation_oid, pg_sys::NoLock as _)?;
        ResolvedForeignRelation::resolve(relation_oid)?
            .validate_relation_columns(relation_oid, relation.as_handle().natts())?;
        Ok(())
    }

    fn validate_alter_references(
        &self,
        commands: *mut pg_sys::List,
    ) -> Result<(), ConnectorError> {
        let length = unsafe { pg_sys::list_length(commands) };
        for index in 0..length {
            let command = unsafe {
                &*(pg_sys::list_nth(commands, index) as *const pg_sys::AlterTableCmd)
            };
            let referenced = match command.subtype {
                pg_sys::AlterTableType::AT_AddInherit => {
                    command.def.cast::<pg_sys::RangeVar>()
                }
                pg_sys::AlterTableType::AT_AttachPartition => unsafe {
                    (*command.def.cast::<pg_sys::PartitionCmd>()).name
                },
                _ => continue,
            };
            let relation_oid = unsafe {
                pg_sys::RangeVarGetRelidExtended(
                    referenced,
                    pg_sys::NoLock as _,
                    0,
                    None,
                    ptr::null_mut(),
                )
            };
            if unsafe { pg_sys::get_rel_relkind(relation_oid) } as u8
                == pg_sys::RELKIND_FOREIGN_TABLE
                && ResolvedStorageLocation::relation_uses_lakebase(relation_oid)
            {
                return Err(ConnectorError::unsupported_foreign_table_definition(
                    "inheritance or partition attachment",
                ));
            }
        }
        Ok(())
    }
}

struct ForeignTableDefinitionPolicy;

impl ForeignTableDefinitionPolicy {
    fn changes_format_invariant(commands: *mut pg_sys::List) -> bool {
        // PostgreSQL owns the parsed command list throughout the utility hook.
        let length = unsafe { pg_sys::list_length(commands) };
        for index in 0..length {
            // Every entry of AlterTableStmt::cmds is an AlterTableCmd.
            let command = unsafe {
                &*(pg_sys::list_nth(commands, index) as *const pg_sys::AlterTableCmd)
            };
            if matches!(
                command.subtype,
                pg_sys::AlterTableType::AT_GenericOptions
                    | pg_sys::AlterTableType::AT_AlterColumnGenericOptions
                    | pg_sys::AlterTableType::AT_AddColumn
            ) {
                return true;
            }
        }
        false
    }

    fn validate_create(statement: &pg_sys::CreateStmt) -> Result<(), ConnectorError> {
        if unsafe { pg_sys::list_length(statement.inhRelations) } != 0
            || !statement.partbound.is_null()
            || !statement.partspec.is_null()
            || !statement.ofTypename.is_null()
        {
            return Err(ConnectorError::unsupported_foreign_table_definition(
                "inherited, typed, or partitioned foreign tables",
            ));
        }
        if unsafe { pg_sys::list_length(statement.constraints) } != 0 {
            return Err(ConnectorError::unsupported_foreign_table_definition(
                "table constraints",
            ));
        }

        let length = unsafe { pg_sys::list_length(statement.tableElts) };
        for index in 0..length {
            let node = unsafe {
                pg_sys::list_nth(statement.tableElts, index) as *const pg_sys::Node
            };
            match unsafe { (*node).type_ } {
                pg_sys::NodeTag::T_ColumnDef => {
                    let column = unsafe { &*node.cast::<pg_sys::ColumnDef>() };
                    Self::validate_column(column)?;
                }
                pg_sys::NodeTag::T_Constraint => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "table constraints",
                        ),
                    );
                }
                pg_sys::NodeTag::T_TableLikeClause => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "LIKE column definitions",
                        ),
                    );
                }
                _ => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "this column definition",
                        ),
                    );
                }
            }
        }
        Ok(())
    }

    fn validate_create_column_options(
        statement: &pg_sys::CreateStmt,
        format: &ResolvedForeignFormat,
    ) -> Result<(), ConnectorError> {
        let length = unsafe { pg_sys::list_length(statement.tableElts) };
        for index in 0..length {
            let node = unsafe {
                pg_sys::list_nth(statement.tableElts, index) as *const pg_sys::Node
            };
            // `validate_create` already established that every table element
            // is a live ColumnDef before this format-specific pass.
            let column = unsafe { &*node.cast::<pg_sys::ColumnDef>() };
            let options = unsafe { ForeignOptionView::from_raw(column.fdwoptions) };
            format.validate_column_view(options)?;
        }
        Ok(())
    }

    fn validate_alter(commands: *mut pg_sys::List) -> Result<(), ConnectorError> {
        let length = unsafe { pg_sys::list_length(commands) };
        for index in 0..length {
            let command = unsafe {
                &*(pg_sys::list_nth(commands, index) as *const pg_sys::AlterTableCmd)
            };
            match command.subtype {
                pg_sys::AlterTableType::AT_AddColumn => {
                    let column = unsafe { &*command.def.cast::<pg_sys::ColumnDef>() };
                    Self::validate_column(column)?;
                }
                pg_sys::AlterTableType::AT_ColumnDefault => {
                    if !command.def.is_null() {
                        return Err(
                            ConnectorError::unsupported_foreign_table_definition(
                                "column DEFAULT",
                            ),
                        );
                    }
                }
                pg_sys::AlterTableType::AT_CookedColumnDefault => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "column DEFAULT",
                        ),
                    );
                }
                pg_sys::AlterTableType::AT_AddConstraint
                | pg_sys::AlterTableType::AT_ReAddConstraint
                | pg_sys::AlterTableType::AT_ReAddDomainConstraint => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "table constraints",
                        ),
                    );
                }
                pg_sys::AlterTableType::AT_SetExpression
                | pg_sys::AlterTableType::AT_AddIdentity
                | pg_sys::AlterTableType::AT_SetIdentity => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "generated or identity columns",
                        ),
                    );
                }
                pg_sys::AlterTableType::AT_AddInherit
                | pg_sys::AlterTableType::AT_AddOf
                | pg_sys::AlterTableType::AT_AttachPartition => {
                    return Err(
                        ConnectorError::unsupported_foreign_table_definition(
                            "inherited, typed, or partitioned foreign tables",
                        ),
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_column(column: &pg_sys::ColumnDef) -> Result<(), ConnectorError> {
        if !column.raw_default.is_null() || !column.cooked_default.is_null() {
            return Err(ConnectorError::unsupported_foreign_table_definition(
                "column DEFAULT",
            ));
        }
        if column.generated != 0 || column.identity != 0 {
            return Err(ConnectorError::unsupported_foreign_table_definition(
                "generated or identity columns",
            ));
        }

        let length = unsafe { pg_sys::list_length(column.constraints) };
        for index in 0..length {
            let constraint = unsafe {
                &*(pg_sys::list_nth(column.constraints, index)
                    as *const pg_sys::Constraint)
            };
            if !matches!(
                constraint.contype,
                pg_sys::ConstrType::CONSTR_NULL | pg_sys::ConstrType::CONSTR_NOTNULL
            ) {
                return Err(ConnectorError::unsupported_foreign_table_definition(
                    "column constraints, DEFAULT, generated, or identity definitions",
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn register() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreateForeignTableStmt,
        Box::new(ForeignTableDdlHook),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterTableStmt,
        Box::new(ForeignTableDdlHook),
    );
}
