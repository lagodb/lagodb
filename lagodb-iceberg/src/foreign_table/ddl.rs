//! Foreign-table creation preflight and credential-query redaction.

use core::ffi::c_void;
use core::ptr;
use std::ffi::{CStr, CString};

use lagodb_core::hooks::{
    AlterTableStmtNode, AlterUserMappingStmtNode, CreateForeignTableStmtNode,
    CreateUserMappingStmtNode, PostUtilityContext, PreUtilityContext, RenameStmtNode,
    UtilityHook, UtilityHookError, UtilityNode, VacuumStmtNode,
    register_utility_hook,
};
use lagodb_core::storage::foreign::{ForeignOption, ForeignOptionView};
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use super::options::{
    ForeignTableIdentity, MaterializedForeignOptions, RestCatalogConnection,
};
use super::provider::LagodbIceberg;
use super::relation::RestForeignTable;
use super::schema::ForeignTableSchema;

struct IcebergForeignTableDdl;
struct IcebergForeignTableAlterGuard;
struct IcebergForeignTableRenameGuard;
struct IcebergForeignTableVacuumGuard;
struct UserMappingSecretRedaction;

impl UtilityHook for IcebergForeignTableDdl {
    fn name(&self) -> &'static str {
        "lagodb-iceberg foreign-table DDL"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let create = statement
            .cast_mut::<CreateForeignTableStmtNode>()
            .expect("hook is registered for CreateForeignTableStmt");
        ForeignTableCreateOperation::prepare(create)
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

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

impl UtilityHook for IcebergForeignTableAlterGuard {
    fn name(&self) -> &'static str {
        "Iceberg foreign-table ALTER guard"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let alter = statement
            .cast_mut::<AlterTableStmtNode>()
            .expect("hook is registered for AlterTableStmt");
        let relation_oid = unsafe {
            pg_sys::RangeVarGetRelidExtended(
                alter.relation,
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
            return Ok(());
        }
        let foreign = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        if !LagodbIceberg::handles_server(foreign.serverid) {
            return Ok(());
        }
        Err(IcebergFdwError::UnsupportedOperation {
            operation: "ALTER FOREIGN TABLE",
        }
        .into())
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

impl UtilityHook for IcebergForeignTableRenameGuard {
    fn name(&self) -> &'static str {
        "Iceberg foreign-table RENAME guard"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let rename = statement
            .cast_mut::<RenameStmtNode>()
            .expect("hook is registered for RenameStmt");
        if rename.relation.is_null() {
            return Ok(());
        }
        let relation_oid = unsafe {
            pg_sys::RangeVarGetRelidExtended(
                rename.relation,
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
            return Ok(());
        }
        let foreign = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        if !LagodbIceberg::handles_server(foreign.serverid) {
            return Ok(());
        }
        Err(IcebergFdwError::UnsupportedOperation {
            operation: "RENAME on an Iceberg foreign table",
        }
        .into())
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

struct ForeignTableCreateOperation<'a> {
    statement: &'a mut pg_sys::CreateForeignTableStmt,
    server_oid: pg_sys::Oid,
    effective_user: pg_sys::Oid,
    identity: ForeignTableIdentity,
}

impl<'a> ForeignTableCreateOperation<'a> {
    fn prepare(
        statement: &'a mut pg_sys::CreateForeignTableStmt,
    ) -> Result<(), UtilityHookError> {
        let server_name = unsafe { CStr::from_ptr(statement.servername) };
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return Ok(());
        }
        if !LagodbIceberg::handles_server(server_oid) {
            return Ok(());
        }
        let effective_user = unsafe { pg_sys::GetUserId() };
        Self::check_server_usage(server_oid, effective_user, server_name)?;
        let mut existing_relation = pg_sys::InvalidOid;
        let namespace_oid = unsafe {
            pg_sys::RangeVarGetAndCheckCreationNamespace(
                statement.base.relation,
                pg_sys::NoLock as _,
                &mut existing_relation,
            )
        };
        if existing_relation != pg_sys::InvalidOid {
            return Ok(());
        }
        RestCatalogConnection::validate_server(server_oid)?;

        let database_name = Self::catalog_name()?;
        let namespace_name = Self::namespace_name(namespace_oid)?;
        let table_name =
            unsafe { CStr::from_ptr((*statement.base.relation).relname) }
                .to_str()
                .map_err(|_| IcebergFdwError::InvalidUtf8 {
                    subject: "local foreign table name",
                })?
                .to_owned();
        let options = unsafe { ForeignOptionView::from_raw(statement.options) };
        let (identity, materialized) = ForeignTableIdentity::complete(
            options,
            database_name,
            namespace_name,
            table_name,
        )?;
        Self::materialize_options(statement, materialized)?;

        let operation = Self {
            statement,
            server_oid,
            effective_user,
            identity,
        };
        operation.bind_remote_schema()
    }

    fn check_server_usage(
        server_oid: pg_sys::Oid,
        effective_user: pg_sys::Oid,
        server_name: &CStr,
    ) -> Result<(), IcebergFdwError> {
        let acl_result = unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                effective_user,
                pg_sys::ACL_USAGE.into(),
            )
        };
        if acl_result == pg_sys::AclResult::ACLCHECK_OK {
            return Ok(());
        }
        Err(IcebergFdwError::ServerUsageDenied {
            server: server_name.to_string_lossy().into_owned(),
        })
    }

    fn catalog_name() -> Result<String, IcebergFdwError> {
        let name = unsafe { pg_sys::get_database_name(pg_sys::MyDatabaseId) };
        unsafe { CStr::from_ptr(name) }
            .to_str()
            .map(str::to_owned)
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "current database name",
            })
    }

    fn namespace_name(namespace_oid: pg_sys::Oid) -> Result<String, IcebergFdwError> {
        let name = unsafe { pg_sys::get_namespace_name(namespace_oid) };
        unsafe { CStr::from_ptr(name) }
            .to_str()
            .map(str::to_owned)
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "local schema name",
            })
    }

    fn materialize_options(
        statement: &mut pg_sys::CreateForeignTableStmt,
        options: MaterializedForeignOptions,
    ) -> Result<(), IcebergFdwError> {
        for (name, value) in options {
            let name =
                CString::new(name).map_err(|_| IcebergFdwError::InteriorNul {
                    subject: "foreign option name",
                })?;
            let value =
                CString::new(value).map_err(|_| IcebergFdwError::InteriorNul {
                    subject: "foreign option value",
                })?;
            let option = unsafe {
                pg_sys::makeDefElem(
                    pg_sys::pstrdup(name.as_ptr()),
                    pg_sys::makeString(pg_sys::pstrdup(value.as_ptr())).cast(),
                    -1,
                )
            };
            statement.options = unsafe {
                pg_sys::lappend(statement.options, option.cast::<c_void>())
            };
        }
        Ok(())
    }

    fn bind_remote_schema(self) -> Result<(), UtilityHookError> {
        let table = RestForeignTable::load(
            self.server_oid,
            self.effective_user,
            self.identity,
        )?;
        let schema = ForeignTableSchema::from_iceberg(
            table.table().metadata().current_schema(),
        )?;
        if unsafe { pg_sys::list_length(self.statement.base.tableElts) } == 0 {
            self.statement.base.tableElts = schema.into_pg_list();
        } else {
            schema.validate_pg_list(self.statement.base.tableElts)?;
        }
        Ok(())
    }
}

pub(super) fn register() {
    register_utility_hook(
        pg_sys::NodeTag::T_CreateForeignTableStmt,
        Box::new(IcebergForeignTableDdl),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_AlterTableStmt,
        Box::new(IcebergForeignTableAlterGuard),
    );
    register_utility_hook(
        pg_sys::NodeTag::T_RenameStmt,
        Box::new(IcebergForeignTableRenameGuard),
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
