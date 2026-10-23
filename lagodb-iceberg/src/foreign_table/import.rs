//! `IMPORT FOREIGN SCHEMA` implementation for Iceberg catalogs.

use std::ffi::{CStr, CString};
use std::fmt::Write;

use iceberg_lite::catalog::TableIdent;
use lagodb_core::fdw::{ForeignImportError, ForeignImportSchemaContext};
use pgrx::{notice, pg_sys};

use super::catalog::CatalogConnection;
use super::error::IcebergFdwError;
use super::options::{ForeignIdentifier, ForeignImportOptions};
use super::schema::ForeignTableSchema;

pub(crate) struct IcebergSchemaImporter;

impl IcebergSchemaImporter {
    pub(crate) fn import(
        context: &ForeignImportSchemaContext<'_>,
    ) -> Result<Vec<CString>, ForeignImportError> {
        let options = ForeignImportOptions::from_view(context.options())?;
        let remote_schema = context.remote_schema().to_str().map_err(|_| {
            IcebergFdwError::InvalidUtf8 {
                subject: "remote schema name",
            }
        })?;
        let namespace = ForeignIdentifier::import_namespace(remote_schema)?;
        let catalog = CatalogConnection::resolve(context.server_oid(), unsafe {
            pg_sys::GetUserId()
        })?
        .connect()?;
        let tables = catalog
            .as_catalog()
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
            if table_name.to_bytes().len() >= pg_sys::NAMEDATALEN as usize {
                return Err(IcebergFdwError::InvalidIdentifier {
                    kind: "Iceberg table name",
                    name: table_name.to_string_lossy().into_owned(),
                    reason: "exceeds PostgreSQL's identifier length limit",
                }
                .into());
            }
            let table = catalog
                .as_catalog()
                .load_table(&identifier)
                .map_err(IcebergFdwError::from)?;
            let current = table.metadata().current_schema();
            let schema =
                ForeignTableSchema::from_iceberg(current, current, options.strict)?;
            if schema.is_empty() {
                notice!(
                    "skipping Iceberg table {}: no supported columns",
                    ForeignIdentifier::table_path(&identifier)
                );
                continue;
            }
            commands.push(CreateForeignTableCommand::build(
                context,
                &table_name,
                &identifier,
                &schema,
            )?);
        }
        Ok(commands)
    }
}

struct CreateForeignTableCommand {
    sql: String,
}

impl CreateForeignTableCommand {
    fn build(
        context: &ForeignImportSchemaContext<'_>,
        table_name: &CStr,
        identifier: &TableIdent,
        schema: &ForeignTableSchema,
    ) -> Result<CString, IcebergFdwError> {
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
        command.sql.push_str(" OPTIONS (table ");
        command.push_literal(&ForeignIdentifier::table_path(identifier))?;
        write!(command.sql, ", schema_id '{}'", schema.schema_id())
            .expect("writing to String cannot fail");
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
