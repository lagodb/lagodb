//! `IMPORT FOREIGN SCHEMA` implementation for REST catalogs.

use std::ffi::{CStr, CString};

use iceberg_lite::catalog::{Catalog, NamespaceIdent};
use lagodb_core::fdw::{ForeignImportError, ForeignImportSchemaContext};
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use super::options::{ForeignTableMode, RestCatalogConnection};
use super::schema::ForeignTableSchema;

pub(crate) struct IcebergSchemaImporter;

impl IcebergSchemaImporter {
    pub(crate) fn import(
        context: &ForeignImportSchemaContext<'_>,
    ) -> Result<Vec<CString>, ForeignImportError> {
        let mode = Self::mode(context)?;
        let remote_schema = context.remote_schema().to_str().map_err(|_| {
            IcebergFdwError::InvalidUtf8 {
                subject: "remote schema name",
            }
        })?;
        let catalog_name_ptr =
            unsafe { pg_sys::get_database_name(pg_sys::MyDatabaseId) };
        let catalog_name = unsafe { CStr::from_ptr(catalog_name_ptr) }
            .to_str()
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "current database name",
            })?;
        let catalog = RestCatalogConnection::resolve(
            context.server_oid(),
            unsafe { pg_sys::GetUserId() },
            catalog_name.to_owned(),
        )?
        .connect()?;
        let namespace = NamespaceIdent::new(remote_schema.to_owned());
        let tables = catalog
            .list_tables(&namespace)
            .map_err(IcebergFdwError::from)?;
        let mut commands = Vec::with_capacity(tables.len());
        for identifier in tables {
            let table_name =
                CString::new(identifier.name().as_bytes()).map_err(|_| {
                    IcebergFdwError::InvalidIdentifier {
                        kind: "Iceberg table name",
                        name: identifier.name().to_owned(),
                        reason: "contains a NUL byte",
                    }
                })?;
            if !context.includes_table(&table_name) {
                continue;
            }
            let table = catalog
                .load_table(&identifier)
                .map_err(IcebergFdwError::from)?;
            let schema =
                ForeignTableSchema::from_iceberg(table.metadata().current_schema())?;
            commands.push(CreateForeignTableCommand::build(
                context,
                &table_name,
                catalog_name,
                remote_schema,
                identifier.name(),
                &schema,
                mode,
            )?);
        }
        Ok(commands)
    }

    fn mode(
        context: &ForeignImportSchemaContext<'_>,
    ) -> Result<ForeignTableMode, ForeignImportError> {
        let mut options = context.options().iter();
        let Some(option) = options.next() else {
            return Ok(ForeignTableMode::ReadOnly);
        };
        let name =
            option
                .name()
                .to_str()
                .map_err(|_| IcebergFdwError::InvalidUtf8 {
                    subject: "IMPORT FOREIGN SCHEMA option name",
                })?;
        if name != "mode" {
            return Err(IcebergFdwError::unsupported_option(name).into());
        }
        let value = option
            .value_str()
            .map_err(|_| IcebergFdwError::InvalidUtf8 {
                subject: "IMPORT FOREIGN SCHEMA mode",
            })?;
        let mode = ForeignTableMode::parse(value)?;
        if let Some(extra) = options.next() {
            return Err(IcebergFdwError::unsupported_option(
                extra.name().to_string_lossy().into_owned(),
            )
            .into());
        }
        Ok(mode)
    }
}

struct CreateForeignTableCommand {
    sql: String,
}

impl CreateForeignTableCommand {
    fn build(
        context: &ForeignImportSchemaContext<'_>,
        table_name: &CStr,
        catalog_name: &str,
        remote_schema: &str,
        remote_table: &str,
        schema: &ForeignTableSchema,
        mode: ForeignTableMode,
    ) -> Result<CString, IcebergFdwError> {
        if table_name.to_bytes().len() >= pg_sys::NAMEDATALEN as usize {
            return Err(IcebergFdwError::InvalidIdentifier {
                kind: "Iceberg table name",
                name: table_name.to_string_lossy().into_owned(),
                reason: "exceeds PostgreSQL's identifier length limit",
            });
        }
        let mut command = Self {
            sql: "CREATE FOREIGN TABLE ".to_owned(),
        };
        command.push_identifier(context.local_schema())?;
        command.sql.push('.');
        command.push_identifier(table_name)?;
        command.sql.push_str(" (");
        schema.append_sql(&mut command.sql)?;
        command.sql.push_str(") SERVER ");
        command.push_identifier(context.server_name())?;
        command.sql.push_str(" OPTIONS (catalog_name ");
        command.push_literal(catalog_name)?;
        command.sql.push_str(", catalog_namespace ");
        command.push_literal(remote_schema)?;
        command.sql.push_str(", catalog_table_name ");
        command.push_literal(remote_table)?;
        command.sql.push_str(", mode ");
        command.push_literal(mode.as_str())?;
        command.sql.push(')');
        CString::new(command.sql).map_err(|_| IcebergFdwError::InteriorNul {
            subject: "generated CREATE FOREIGN TABLE command",
        })
    }

    fn push_identifier(&mut self, value: &CStr) -> Result<(), IcebergFdwError> {
        // SAFETY: `value` is a live NUL-terminated PostgreSQL identifier and
        // the returned current-context string is copied immediately.
        let quoted = unsafe { pg_sys::quote_identifier(value.as_ptr()) };
        let quoted = unsafe { CStr::from_ptr(quoted) }.to_str().map_err(|_| {
            IcebergFdwError::InvalidUtf8 {
                subject: "quoted PostgreSQL identifier",
            }
        })?;
        self.sql.push_str(quoted);
        Ok(())
    }

    fn push_literal(&mut self, value: &str) -> Result<(), IcebergFdwError> {
        let value = CString::new(value.as_bytes()).map_err(|_| {
            IcebergFdwError::InteriorNul {
                subject: "Iceberg identifier",
            }
        })?;
        // SAFETY: CString established a NUL-free terminated input; the quoted
        // current-context result is copied into the owned SQL buffer now.
        let quoted = unsafe { pg_sys::quote_literal_cstr(value.as_ptr()) };
        let quoted = unsafe { CStr::from_ptr(quoted) }.to_str().map_err(|_| {
            IcebergFdwError::InvalidUtf8 {
                subject: "quoted PostgreSQL literal",
            }
        })?;
        self.sql.push_str(quoted);
        Ok(())
    }
}
