//! Type admission shared by IMPORT and inferred CREATE definitions.

use std::ffi::{CStr, CString};
use std::ptr;

use iceberg_lite::spec::Schema as IcebergSchema;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::type_mapping::ForeignType;
use crate::error::IcebergError;
use crate::schema::arrow_type::SemanticArrowType;

pub(crate) struct ForeignTableSchema {
    schema_id: i32,
    columns: Box<[ForeignColumn]>,
}

struct ForeignColumn {
    name: CString,
    oid: pg_sys::Oid,
    typmod: i32,
    required: bool,
}

impl ForeignTableSchema {
    /// Declare `schema`'s supported fields. A field becomes NOT NULL only when
    /// it is also required in `current`, the schema scans read: the PostgreSQL
    /// planner trusts NOT NULL, and `ForeignSchemaBinding::validate_definition`
    /// rejects declarations that the current schema does not guarantee.
    pub(crate) fn from_iceberg(
        schema: &IcebergSchema,
        current: &IcebergSchema,
        strict: bool,
    ) -> Result<Self, IcebergFdwError> {
        let mut columns = Vec::with_capacity(schema.as_struct().fields().len());
        for field in schema.as_struct().fields() {
            let mapping = SemanticArrowType::new(&field.field_type)
                .validate_supported()
                .and_then(|()| {
                    ForeignType::new(&field.field_type)
                        .canonical()
                        .ok_or_else(|| {
                            IcebergError::UnsupportedColumnType(
                                field.field_type.to_string(),
                            )
                        })
                });
            let postgres = match mapping {
                Ok(postgres) => postgres,
                Err(IcebergError::UnsupportedColumnType(_)) if !strict => continue,
                Err(error) => return Err(error.into()),
            };
            if field.name.len() >= pg_sys::NAMEDATALEN as usize {
                return Err(IcebergFdwError::InvalidIdentifier {
                    kind: "Iceberg column name",
                    name: field.name.clone(),
                    reason: "exceeds PostgreSQL's identifier length limit",
                });
            }
            let name = CString::new(field.name.as_bytes()).map_err(|_| {
                IcebergFdwError::InvalidIdentifier {
                    kind: "Iceberg column name",
                    name: field.name.clone(),
                    reason: "contains a NUL byte",
                }
            })?;
            columns.push(ForeignColumn {
                name,
                oid: postgres.oid,
                typmod: postgres.typmod,
                required: field.required
                    && current
                        .as_struct()
                        .field_by_name(&field.name)
                        .is_some_and(|current_field| current_field.required),
            });
        }
        Ok(Self {
            schema_id: schema.schema_id(),
            columns: columns.into_boxed_slice(),
        })
    }

    /// Build PostgreSQL-owned `ColumnDef` nodes for a utility statement.
    pub(crate) fn into_pg_list(self) -> *mut pg_sys::List {
        let mut list = ptr::null_mut();
        for column in self.columns {
            // SAFETY: `name` is NUL-terminated, the OID/typmod pair came from
            // the canonical mapping, and PostgreSQL owns both the ColumnDef
            // allocation and list cell in the current utility context.
            let definition = unsafe {
                pg_sys::makeColumnDef(
                    column.name.as_ptr(),
                    column.oid,
                    column.typmod,
                    pg_sys::InvalidOid,
                )
            };
            unsafe { (*definition).is_not_null = column.required };
            list = unsafe { pg_sys::lappend(list, definition.cast()) };
        }
        list
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub(crate) fn append_sql(
        &self,
        command: &mut String,
    ) -> Result<(), IcebergFdwError> {
        for (index, column) in self.columns.iter().enumerate() {
            if index != 0 {
                command.push_str(", ");
            }
            // SAFETY: column names are validated C strings. PostgreSQL returns
            // current-context C strings, which are copied into `command` now.
            let quoted = unsafe { pg_sys::quote_identifier(column.name.as_ptr()) };
            let quoted =
                unsafe { CStr::from_ptr(quoted) }.to_str().map_err(|_| {
                    IcebergFdwError::InvalidUtf8 {
                        subject: "quoted PostgreSQL identifier",
                    }
                })?;
            command.push_str(quoted);
            command.push(' ');
            let type_name = unsafe {
                pg_sys::format_type_with_typemod(column.oid, column.typmod)
            };
            let type_name =
                unsafe { CStr::from_ptr(type_name) }.to_str().map_err(|_| {
                    IcebergFdwError::InvalidUtf8 {
                        subject: "formatted PostgreSQL type name",
                    }
                })?;
            command.push_str(type_name);
            if column.required {
                command.push_str(" NOT NULL");
            }
        }
        Ok(())
    }
}
