//! PostgreSQL slot projections and Iceberg storage-field projections.

use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema};
use iceberg_lite::spec::Schema as IcebergSchema;
use pgrx::pg_sys;

use super::type_mapping::IcebergTypeExt;
use crate::error::{IcebergError, IcebergResult};

/// One source PostgreSQL attribute and its destination in the scan tuple.
#[derive(Debug, Clone)]
pub(crate) struct ProjectedAttribute {
    /// 1-based PG attribute number of a live (non-dropped) user column.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Zero-based destination in the actual executor scan slot.
    pub(crate) destination: usize,
}

impl ProjectedAttribute {
    pub(crate) fn new(attno: pg_sys::AttrNumber, destination: usize) -> Self {
        Self { attno, destination }
    }
}

/// Source attributes and slot destinations in base-attribute read order.
///
/// Scan adapters represent select-all as `None` before constructing a prepared
/// read. An empty projection is valid for a Modify identity-only scan, where
/// Iceberg metadata columns still drive one output row but no business column
/// is decoded.
#[derive(Debug, Clone)]
pub(crate) struct SlotProjection {
    columns: Vec<ProjectedAttribute>,
}

impl SlotProjection {
    /// Build a projection and normalize it to stable base-schema read order.
    /// Destinations remain attached to their source fields, so compact scan
    /// tuple order is preserved independently of storage order.
    pub(crate) fn from_outputs(mut columns: Vec<ProjectedAttribute>) -> Self {
        columns.sort_unstable_by_key(|column| column.attno);
        Self { columns }
    }

    /// Selected columns in storage read order. Each entry independently
    /// carries its compact scan-slot destination.
    pub(crate) fn columns(&self) -> &[ProjectedAttribute] {
        &self.columns
    }
}

/// Storage schema and selected field IDs, without PostgreSQL datum decoding.
pub(crate) struct StorageProjection {
    schema: Arc<IcebergSchema>,
    field_ids: Box<[i32]>,
}

impl StorageProjection {
    /// Zero-column output with row visibility preserved by the storage scan.
    pub(crate) fn empty(schema: Arc<IcebergSchema>) -> Self {
        Self {
            schema,
            field_ids: Box::new([]),
        }
    }

    pub(crate) fn from_field_ids(
        schema: Arc<IcebergSchema>,
        field_ids: Box<[i32]>,
    ) -> Self {
        Self { schema, field_ids }
    }

    pub(crate) fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        let fields = self
            .field_ids
            .iter()
            .map(|field_id| {
                let field = self
                    .schema
                    .as_struct()
                    .field_by_id(*field_id)
                    .ok_or_else(|| {
                        IcebergError::ColumnNotFound(format!("field id {field_id}"))
                    })?;
                Ok(Field::new(
                    &field.name,
                    field.field_type.to_arrow_type()?,
                    !field.required,
                ))
            })
            .collect::<IcebergResult<Vec<_>>>()?;
        Ok(ArrowSchema::new(fields))
    }

    pub(crate) fn schema(&self) -> &IcebergSchema {
        self.schema.as_ref()
    }

    pub(crate) fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }
}
