//! Shared REST-table schema description for import DDL and empty-schema DDL.

use std::ffi::{CStr, CString};
use std::ptr;

use iceberg_lite::spec::Schema as IcebergSchema;
use pg_lakebase_core::handles::RelationHandle;
use pgrx::pg_sys;

use super::error::IcebergFdwError;
use crate::engine::schema::relation::RelationShape;
use crate::engine::schema::type_mapping::{IcebergTypeExt, ValidateSupported};
use crate::error::IcebergError;

/// A strict, statement-lifetime contract between the local foreign relation
/// and the current remote Iceberg schema.
pub(crate) struct ForeignSchemaBinding {
    shape: RelationShape,
}

impl ForeignSchemaBinding {
    pub(crate) fn bind(
        relation: &RelationHandle<'_>,
        schema: &IcebergSchema,
    ) -> Result<Self, IcebergFdwError> {
        let shape = RelationShape::from_relation(relation)?;
        let remote_fields = schema.as_struct().fields();
        if shape.live_columns().len() != remote_fields.len() {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "local relation has {} live columns but Iceberg schema {} has {} fields",
                    shape.live_columns().len(),
                    schema.schema_id(),
                    remote_fields.len(),
                ),
            });
        }

        for (local, remote) in shape.live_columns().iter().zip(remote_fields) {
            if local.name != remote.name {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "local column {:?} at attno {} corresponds to remote field {:?}",
                        local.name, local.attno, remote.name,
                    ),
                });
            }
            remote.field_type.validate_supported()?;
            let canonical = remote.field_type.postgres_type().ok_or_else(|| {
                IcebergError::UnsupportedColumnType(remote.field_type.to_string())
            })?;
            let local_type = shape.attr_types()[(local.attno - 1) as usize];
            if local_type != (canonical.oid(), canonical.typmod()) {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "column {:?} has PostgreSQL type (oid {}, typmod {}) but Iceberg requires (oid {}, typmod {})",
                        local.name,
                        u32::from(local_type.0),
                        local_type.1,
                        u32::from(canonical.oid()),
                        canonical.typmod(),
                    ),
                });
            }
            if local.required != remote.required {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "column {:?} has local NOT NULL={} but Iceberg required={}",
                        local.name, local.required, remote.required,
                    ),
                });
            }
        }
        Ok(Self { shape })
    }

    pub(crate) fn into_relation_shape(self) -> RelationShape {
        self.shape
    }
}

pub(crate) struct ForeignTableSchema {
    columns: Box<[ForeignColumn]>,
}

struct ForeignColumn {
    name: CString,
    oid: pg_sys::Oid,
    typmod: i32,
    required: bool,
}

impl ForeignTableSchema {
    pub(crate) fn from_iceberg(
        schema: &IcebergSchema,
    ) -> Result<Self, IcebergFdwError> {
        let mut columns = Vec::with_capacity(schema.as_struct().fields().len());
        for field in schema.as_struct().fields() {
            field.field_type.validate_supported()?;
            let postgres = field.field_type.postgres_type().ok_or_else(|| {
                IcebergError::UnsupportedColumnType(field.field_type.to_string())
            })?;
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
                oid: postgres.oid(),
                typmod: postgres.typmod(),
                required: field.required,
            });
        }
        Ok(Self {
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

    pub(crate) fn validate_pg_list(
        &self,
        definitions: *mut pg_sys::List,
    ) -> Result<(), IcebergFdwError> {
        let definition_count = unsafe { pg_sys::list_length(definitions) };
        if definition_count as usize != self.columns.len() {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "CREATE FOREIGN TABLE declares {definition_count} columns but the Iceberg table has {} fields",
                    self.columns.len(),
                ),
            });
        }
        for (index, expected) in self.columns.iter().enumerate() {
            let node = unsafe { pg_sys::list_nth(definitions, index as i32) }
                .cast::<pg_sys::Node>();
            if unsafe { (*node).type_ } != pg_sys::NodeTag::T_ColumnDef {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail:
                        "CREATE FOREIGN TABLE contains a non-column table element"
                            .to_owned(),
                });
            }
            let actual = node.cast::<pg_sys::ColumnDef>();
            let actual_name = unsafe { CStr::from_ptr((*actual).colname) };
            if actual_name != expected.name.as_c_str() {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "local column {:?} at position {} does not match Iceberg field {:?}",
                        actual_name.to_string_lossy(),
                        index + 1,
                        expected.name.to_string_lossy(),
                    ),
                });
            }
            let mut oid = pg_sys::InvalidOid;
            let mut typmod = -1;
            unsafe {
                pg_sys::typenameTypeIdAndMod(
                    ptr::null_mut(),
                    (*actual).typeName,
                    &mut oid,
                    &mut typmod,
                );
            }
            if (oid, typmod) != (expected.oid, expected.typmod) {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "column {:?} has PostgreSQL type (oid {}, typmod {}) but Iceberg requires (oid {}, typmod {})",
                        expected.name.to_string_lossy(),
                        u32::from(oid),
                        typmod,
                        u32::from(expected.oid),
                        expected.typmod,
                    ),
                });
            }
            if unsafe { (*actual).is_not_null } != expected.required {
                return Err(IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "column {:?} has local NOT NULL={} but Iceberg required={}",
                        expected.name.to_string_lossy(),
                        unsafe { (*actual).is_not_null },
                        expected.required,
                    ),
                });
            }
        }
        Ok(())
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
