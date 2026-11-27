//! Relation-bound PostgreSQL read plans.
//!
//! `PgReadPlan` pairs an Iceberg storage projection with its compiled
//! PostgreSQL column decoder. Type rules and target slot positions are resolved
//! during construction, never in the row loop.

use std::sync::Arc;

use iceberg_lite::spec::{NestedField, Schema as IcebergSchema};
use lagodb_arrow::{ArrowColumnDecoder, BoundDatumPlan, DecodedColumn};
use lagodb_core::datum::PgType;
use pgrx::pg_sys;

use super::iceberg::{IcebergFieldExt, IcebergSchemaExt};
use super::projection::{SlotProjection, StorageProjection};
use super::relation::RelationLayout;
use crate::error::{IcebergError, IcebergResult};

/// A storage projection paired with its compiled PostgreSQL decoder.
pub(crate) struct PgReadPlan {
    read: StorageProjection,
    decoder: ArrowColumnDecoder,
}

impl PgReadPlan {
    /// Bind all live relation columns to their relation-shaped slot positions.
    pub(crate) fn bind_full(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
    ) -> IcebergResult<Self> {
        let len = layout.live_count();
        let mut field_ids = Vec::with_capacity(len);
        let mut columns = Vec::with_capacity(len);
        for (source_column, col) in layout.live_columns().enumerate() {
            let field = schema.require_field_by_name(col.name())?;
            columns.push(Self::bind_column(
                field,
                col.pg_type(),
                source_column,
                col.slot_index(),
            )?);
            field_ids.push(field.id);
        }
        Ok(Self {
            read: StorageProjection::from_field_ids(
                schema,
                field_ids.into_boxed_slice(),
            ),
            decoder: ArrowColumnDecoder::new(columns),
        })
    }

    /// Bind an already ordered, unique base-attribute subset to a relation-shaped slot.
    pub(crate) fn bind_relation_projection(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
        attnos: &[pg_sys::AttrNumber],
    ) -> IcebergResult<Self> {
        if attnos.is_empty() {
            return Ok(Self::empty(schema));
        }
        let mut field_ids = Vec::with_capacity(attnos.len());
        let mut columns = Vec::with_capacity(attnos.len());
        for (source_column, &attno) in attnos.iter().enumerate() {
            let col = layout.live_column(attno)?;
            let field = schema.require_field_by_name(col.name())?;
            columns.push(Self::bind_column(
                field,
                col.pg_type(),
                source_column,
                col.slot_index(),
            )?);
            field_ids.push(field.id);
        }
        Ok(Self {
            read: StorageProjection::from_field_ids(
                schema,
                field_ids.into_boxed_slice(),
            ),
            decoder: ArrowColumnDecoder::new(columns),
        })
    }

    /// Bind selected attributes to the actual output scan-slot layout.
    pub(crate) fn bind_projection(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
        projection: &SlotProjection,
        slot_attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let projected = projection.columns();
        if projected.is_empty() {
            return Ok(Self::empty(schema));
        }
        let mut field_ids = Vec::with_capacity(projected.len());
        let mut columns = Vec::with_capacity(projected.len());
        for (source_column, entry) in projected.iter().enumerate() {
            let col = layout.live_column(entry.attno())?;
            let field = schema.require_field_by_name(col.name())?;
            let destination = entry.destination();
            let &(target_oid, target_typmod) = slot_attr_types
                .get(destination)
                .ok_or(IcebergError::InvariantViolated(
                    "ColumnMapping: projected destination is outside the slot width",
                ))?;
            columns.push(Self::bind_column(
                field,
                PgType::new(target_oid, target_typmod),
                source_column,
                destination,
            )?);
            field_ids.push(field.id);
        }
        Ok(Self {
            read: StorageProjection::from_field_ids(
                schema,
                field_ids.into_boxed_slice(),
            ),
            decoder: ArrowColumnDecoder::new(columns),
        })
    }

    fn empty(schema: Arc<IcebergSchema>) -> Self {
        Self {
            read: StorageProjection::empty(schema),
            decoder: ArrowColumnDecoder::new(Vec::new()),
        }
    }

    fn bind_column(
        field: &NestedField,
        pg_type: PgType,
        source_column: usize,
        destination: usize,
    ) -> IcebergResult<DecodedColumn> {
        let arrow_field = field.to_projection_field()?;
        // SAFETY: managed json/jsonb fields retain PG-validated, NUL-free
        // type-output text, including array/composite leaves. The canonical
        // foreign mapping uses text for strings and structural JSONB for
        // containers, so it does not admit unvalidated PG JSON, and Managed
        // TextIo fields retain PgTextIo's NUL-free output. Managed writers
        // preserve the int2/OID/interval component ranges when widening their
        // storage, while the canonical foreign mapping never binds external
        // wider numbers to those narrower PG declarations. Callers bind
        // destinations and actual OIDs from the output slot layout.
        let plan = unsafe {
            BoundDatumPlan::bind_with_prevalidated_json_text(&arrow_field, pg_type)
        }?;
        Ok(unsafe { DecodedColumn::bind(plan, source_column, destination) }?)
    }

    pub(crate) fn into_parts(self) -> (StorageProjection, ArrowColumnDecoder) {
        (self.read, self.decoder)
    }
}
