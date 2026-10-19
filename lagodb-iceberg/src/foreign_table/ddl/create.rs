//! Admit explicit columns and infer empty definitions before PG transforms CREATE.

use std::{ffi::CStr, panic::AssertUnwindSafe, ptr};

use lagodb_core::datum::PgType;
use lagodb_core::diag::PgError;
use lagodb_core::hooks::{
    CreateForeignTableStmtNode, HookError, PostUtilityContext, UtilityHook,
    UtilityHookError, UtilityNode,
};
use lagodb_core::storage::foreign::ForeignOptionView;
use pgrx::{PgSqlErrorCode, PgTryBuilder, pg_sys};

use super::super::error::IcebergFdwError;
use super::super::options::ForeignTableIdentity;
use super::super::provider::LagodbIceberg;
use super::super::relation::IcebergForeignTable;
use super::super::schema::ForeignTableSchema;
use super::super::type_mapping::ForeignType;

pub(super) struct IcebergForeignTableDdl;

impl UtilityHook for IcebergForeignTableDdl {
    fn name(&self) -> &'static str {
        "lagodb-iceberg foreign-table DDL"
    }

    fn on_pre(&self, statement: &mut UtilityNode) -> Result<(), UtilityHookError> {
        let create = statement
            .cast_mut::<CreateForeignTableStmtNode>()
            .expect("registered for CreateForeignTableStmt");
        PgTryBuilder::new(AssertUnwindSafe(|| Self::prepare(create)))
            .catch_others(|error| Err(PgError::from(error).into()))
            .execute()
    }

    fn on_post(&self, _context: &PostUtilityContext) -> Result<(), UtilityHookError> {
        Ok(())
    }
}

impl IcebergForeignTableDdl {
    fn prepare(
        statement: &mut pg_sys::CreateForeignTableStmt,
    ) -> Result<(), UtilityHookError> {
        let server_name = unsafe { CStr::from_ptr(statement.servername) };
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid
            || !LagodbIceberg::handles_server(server_oid)
        {
            return Ok(());
        }
        let user = unsafe { pg_sys::GetUserId() };
        Self::check_server_usage(server_oid, user, server_name)?;
        let mut existing = pg_sys::InvalidOid;
        unsafe {
            pg_sys::RangeVarGetAndCheckCreationNamespace(
                statement.base.relation,
                pg_sys::NoLock as _,
                &mut existing,
            );
        }
        if existing != pg_sys::InvalidOid {
            // PostgreSQL owns duplicate errors and IF NOT EXISTS notices. In
            // particular, an existing object must not trigger remote I/O.
            return Ok(());
        }
        let infer = statement.base.tableElts.is_null()
            && statement.base.inhRelations.is_null();
        let columns = Self::declared_columns(&statement.base)?;
        if !infer && columns.is_empty() {
            // PG expands LIKE and INHERITS. Derived columns retain their
            // execution-time validation; do not reproduce PG's transformation.
            return Ok(());
        }
        let identity = ForeignTableIdentity::from_view(unsafe {
            ForeignOptionView::from_raw(statement.options)
        })?;
        let remote = IcebergForeignTable::load(server_oid, user, identity)?;
        let schema = remote.definition_schema()?;
        for (name, local_type) in columns {
            let field = schema.as_struct().field_by_name(name).ok_or_else(|| {
                IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "local column {:?} is absent from Iceberg schema {}",
                        name,
                        schema.schema_id()
                    ),
                }
            })?;
            // CREATE checks column names and types against the selected definition.
            // Deliberately leave declared NOT NULL constraints to the user, as in
            // PostgreSQL's CREATE FOREIGN TABLE contract: users must ensure those
            // constraints match remote guarantees, including after schema changes.
            // IMPORT and inferred CREATE map Iceberg required to NOT NULL; explicit
            // declarations are not checked for nullability here. A CREATE-time
            // check would only establish consistency at that moment. Execution-time
            // checks cover required columns, but cannot enforce this responsibility:
            // PG trusts NOT NULL during optimization and can eliminate the scan.
            // Current-schema dependencies and complete writes are bound later.
            ForeignType::new(&field.field_type).validate(name, local_type, false)?;
        }
        if !infer {
            return Ok(());
        }
        let definition = ForeignTableSchema::from_iceberg(schema, false)?;
        if definition.is_empty() {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail:
                    "Iceberg table has no columns with a supported PostgreSQL mapping"
                        .to_owned(),
            }
            .into());
        }
        statement.base.tableElts = definition.into_pg_list();
        Ok(())
    }

    fn declared_columns(
        statement: &pg_sys::CreateStmt,
    ) -> Result<Vec<(&str, PgType)>, UtilityHookError> {
        let mut columns = Vec::new();
        for index in 0..unsafe { pg_sys::list_length(statement.tableElts) } {
            // SAFETY: the live CREATE parse tree owns these nodes and strings.
            let node = unsafe { pg_sys::list_nth(statement.tableElts, index) }
                .cast::<pg_sys::Node>();
            if unsafe { (*node).type_ } != pg_sys::NodeTag::T_ColumnDef {
                continue;
            }
            let column = unsafe { &*node.cast::<pg_sys::ColumnDef>() };
            if column.typeName.is_null() {
                // An inherited column's WITH OPTIONS entry has no declared type.
                continue;
            }
            let name =
                unsafe { CStr::from_ptr(column.colname) }
                    .to_str()
                    .map_err(|_| IcebergFdwError::InvalidUtf8 {
                        subject: "PostgreSQL column name",
                    })?;
            columns.push((name, Self::declared_type(unsafe { &*column.typeName })?));
        }
        Ok(columns)
    }

    fn declared_type(source: &pg_sys::TypeName) -> Result<PgType, UtilityHookError> {
        // A shallow copy changes only scalar fields for type lookup. PG retains
        // the original SERIAL declaration to generate its sequence and default.
        let mut type_name = *source;
        if !type_name.pct_type && unsafe { pg_sys::list_length(type_name.names) } == 1
        {
            let name = unsafe {
                &*pg_sys::list_nth(type_name.names, 0).cast::<pg_sys::String>()
            };
            let name = unsafe { CStr::from_ptr(name.sval) }.to_bytes();
            // Match PG17 transformColumnDefinition's SERIAL aliases.
            let serial_oid = match name {
                b"smallserial" | b"serial2" => Some(pg_sys::INT2OID),
                b"serial" | b"serial4" => Some(pg_sys::INT4OID),
                b"bigserial" | b"serial8" => Some(pg_sys::INT8OID),
                _ => None,
            };
            if let Some(oid) = serial_oid {
                if !type_name.arrayBounds.is_null() {
                    return Err(HookError::with_code(
                        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                        "array of serial is not implemented",
                    ));
                }
                type_name.names = ptr::null_mut();
                type_name.typeOid = oid;
            }
        }
        let (mut oid, mut typmod) = (pg_sys::InvalidOid, -1);
        // SAFETY: the TypeName and its PG-owned children remain live throughout
        // lookup. on_pre catches PG errors and returns them to the hook boundary.
        unsafe {
            pg_sys::typenameTypeIdAndMod(
                ptr::null_mut(),
                &type_name,
                &mut oid,
                &mut typmod,
            );
        }
        Ok(PgType::new(oid, typmod))
    }

    fn check_server_usage(
        server_oid: pg_sys::Oid,
        user: pg_sys::Oid,
        server_name: &CStr,
    ) -> Result<(), IcebergFdwError> {
        let result = unsafe {
            pg_sys::object_aclcheck(
                pg_sys::ForeignServerRelationId,
                server_oid,
                user,
                pg_sys::ACL_USAGE.into(),
            )
        };
        if result != pg_sys::AclResult::ACLCHECK_OK {
            return Err(IcebergFdwError::ServerUsageDenied {
                server: server_name.to_string_lossy().into_owned(),
            });
        }
        Ok(())
    }
}
