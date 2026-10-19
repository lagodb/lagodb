use iceberg_lite::spec::{
    PrimitiveType, Schema as IcebergSchema, TableMetadata, Type,
};
use lagodb_core::datum::PgType;
use lagodb_core::handles::RelationHandle;
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::options::ForeignTableIdentity;
use super::super::type_mapping::ForeignType;
use crate::error::IcebergError;
use crate::schema::relation::{LiveColumn, RelationLayout};

/// PostgreSQL columns admitted for the foreign definition and current scan.
pub(crate) struct ForeignSchemaBinding {
    layout: RelationLayout,
}

impl ForeignSchemaBinding {
    /// UPDATE rewrites the complete PG row, including columns absent from SET.
    /// Native timestamp values lose nanoseconds when materialized in PG;
    /// structs, maps and complex lists use JSONB and preserve that precision.
    pub(crate) fn validate_update(
        schema: &IcebergSchema,
    ) -> Result<(), IcebergFdwError> {
        for field in schema.as_struct().fields() {
            let value_type = match field.field_type.as_ref() {
                Type::List(list) => list.element_field.field_type.as_ref(),
                value_type => value_type,
            };
            if matches!(
                value_type,
                Type::Primitive(
                    PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs
                )
            ) {
                return Err(IcebergFdwError::LossyTimestampUpdate {
                    column: field.name.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn new(
        relation: &RelationHandle<'_>,
    ) -> Result<Self, IcebergFdwError> {
        Ok(Self {
            layout: RelationLayout::from_relation(relation)?,
        })
    }

    /// Validate only query dependencies, including predicate-only columns.
    pub(crate) fn validate_read(
        &self,
        metadata: &TableMetadata,
        identity: &ForeignTableIdentity,
        columns: impl Iterator<Item = pg_sys::AttrNumber>,
    ) -> Result<(), IcebergFdwError> {
        let definition = identity.definition_schema(metadata)?;
        let current = metadata.current_schema();
        for attno in columns {
            let local = self.layout.column(attno)?;
            self.validate_column(local, definition, false)?;
            if definition.schema_id() != current.schema_id() {
                self.validate_column(local, current, false)?;
            }
        }
        Ok(())
    }

    /// Writes retain the complete, canonical current-schema contract.
    pub(crate) fn validate_write(
        &self,
        schema: &IcebergSchema,
    ) -> Result<(), IcebergFdwError> {
        if self.layout.live_columns().len() != schema.as_struct().fields().len() {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "local relation has {} live columns but Iceberg schema {} has {} fields",
                    self.layout.live_columns().len(),
                    schema.schema_id(),
                    schema.as_struct().fields().len(),
                ),
            });
        }
        for local in self.layout.live_columns() {
            self.validate_column(local, schema, true)?;
        }
        Ok(())
    }

    /// Columnar execution has no PostgreSQL input conversion at the scan edge.
    pub(crate) fn supports_columnar_read(
        &self,
        schema: &IcebergSchema,
        columns: &[pg_sys::AttrNumber],
    ) -> Result<bool, IcebergFdwError> {
        for &attno in columns {
            let local = self.layout.column(attno)?;
            let remote = schema
                .as_struct()
                .field_by_name(&local.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(local.name.clone()))?;
            let (oid, typmod) = self.layout.attr_types()[(attno - 1) as usize];
            if !ForeignType::new(&remote.field_type).accepts(oid, typmod) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn validate_column(
        &self,
        local: &LiveColumn,
        schema: &IcebergSchema,
        writable: bool,
    ) -> Result<(), IcebergFdwError> {
        let remote =
            schema
                .as_struct()
                .field_by_name(&local.name)
                .ok_or_else(|| IcebergFdwError::SchemaContractMismatch {
                    detail: format!(
                        "local column {:?} is absent from Iceberg schema {}",
                        local.name,
                        schema.schema_id()
                    ),
                })?;
        let local_type = self.layout.attr_types()[(local.attno - 1) as usize];
        ForeignType::new(&remote.field_type).validate(
            &local.name,
            PgType::new(local_type.0, local_type.1),
            writable,
        )?;
        if (writable || local.required) && local.required != remote.required {
            return Err(IcebergFdwError::SchemaContractMismatch {
                detail: format!(
                    "column {:?} has local NOT NULL={} but Iceberg required={}",
                    local.name, local.required, remote.required,
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn layout(&self) -> &RelationLayout {
        &self.layout
    }

    pub(crate) fn into_relation_layout(self) -> RelationLayout {
        self.layout
    }
}
