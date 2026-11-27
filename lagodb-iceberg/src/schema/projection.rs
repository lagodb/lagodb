//! PostgreSQL slot projections and Iceberg storage-field projections.

use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use iceberg_lite::spec::Schema as IcebergSchema;
use pgrx::pg_sys;

use super::iceberg::{IcebergFieldExt, IcebergSchemaExt};
use super::relation::RelationLayout;
use crate::error::IcebergResult;

/// One source PostgreSQL attribute and its destination in the scan tuple.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProjectedAttribute {
    /// 1-based PG attribute number of a live (non-dropped) user column.
    attno: pg_sys::AttrNumber,
    /// Zero-based destination in the actual executor scan slot.
    destination: usize,
}

impl ProjectedAttribute {
    #[inline]
    pub(crate) const fn new(attno: pg_sys::AttrNumber, destination: usize) -> Self {
        Self { attno, destination }
    }

    #[inline]
    pub(crate) const fn attno(self) -> pg_sys::AttrNumber {
        self.attno
    }

    #[inline]
    pub(crate) const fn destination(self) -> usize {
        self.destination
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
        columns.sort_unstable_by_key(|column| column.attno());
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

    pub(super) fn from_field_ids(
        schema: Arc<IcebergSchema>,
        field_ids: Box<[i32]>,
    ) -> Self {
        Self { schema, field_ids }
    }

    /// Resolve a PostgreSQL attribute projection directly to Iceberg storage
    /// field IDs without constructing slot destinations or datum decoders.
    pub(crate) fn bind_relation_fields(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
        projected: impl ExactSizeIterator<Item = pg_sys::AttrNumber>,
    ) -> IcebergResult<Self> {
        let mut field_ids = Vec::with_capacity(projected.len());
        for attno in projected {
            let column = layout.live_column(attno)?;
            let field = schema.require_field_by_name(column.name())?;
            field_ids.push(field.id);
        }
        Ok(Self {
            schema,
            field_ids: field_ids.into_boxed_slice(),
        })
    }

    pub(crate) fn to_arrow_schema(&self) -> IcebergResult<ArrowSchema> {
        let fields = self
            .field_ids
            .iter()
            .map(|field_id| {
                self.schema
                    .require_field_by_id(*field_id)?
                    .to_projection_field()
            })
            .collect::<IcebergResult<Vec<_>>>()?;
        Ok(ArrowSchema::new(fields))
    }

    pub(crate) fn shared_schema(&self) -> Arc<IcebergSchema> {
        Arc::clone(&self.schema)
    }

    pub(crate) fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }
}
