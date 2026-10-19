//! Relation-bound read and write column plans.
//!
//! Read bindings pair storage field IDs with PostgreSQL destinations. Write bindings
//! cover every Iceberg output field and are consumed by the write layer's buffer.
//! Type rules are resolved during construction, never in the row loop.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use iceberg_lite::spec::Schema as IcebergSchema;
use lagodb_arrow::{
    ArrowColumnDecoder, BoundDatumPlan, BoundWriteColumnPlan, DecodedColumn,
};
use lagodb_core::datum::PgType;
use pgrx::pg_sys;

use super::arrow_type::{IcebergSchemaExt, SemanticArrowType};
use super::projection::{SlotProjection, StorageProjection};
use super::relation::{RelationFieldBinding, RelationFieldMap, RelationLayout};
use super::write_mapping::WriteField;
use crate::error::{IcebergError, IcebergResult};

/// One selected Arrow column and its PostgreSQL destination and codec.
pub(crate) struct ReadColumnBinding {
    /// Source relation attribute, independent of the output destination.
    pub(crate) source_base_attno: pg_sys::AttrNumber,
    /// Index in the batch requested in field-map order.
    pub(crate) source_column: usize,
    /// Zero-based destination in the actual scan tuple.
    pub(crate) destination: usize,
    pub(crate) plan: BoundDatumPlan,
}

/// Read rules and destinations in requested storage-column order.
/// Unselected destinations are untouched; callers must not read those positions.
pub(crate) struct ReadColumnPlan {
    pub(crate) columns: Box<[ReadColumnBinding]>,
}

impl ReadColumnPlan {
    /// Bind only selected fields against the output slot types.
    pub(crate) fn bind(
        schema: &IcebergSchema,
        field_map: &RelationFieldMap,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let mut columns = Vec::with_capacity(field_map.bindings().len());
        for (source_column, binding) in field_map.bindings().iter().enumerate() {
            let field = schema
                .as_struct()
                .field_by_id(binding.field_id)
                .ok_or_else(|| {
                    IcebergError::ColumnNotFound(binding.debug_name.clone())
                })?;
            let destination = RelationFieldMap::validate_destination(
                binding.destination,
                slot_width,
            )?;
            let target_oid = attr_types[destination].0;
            let semantic = SemanticArrowType::new(&field.field_type);
            semantic.validate_supported()?;
            // SAFETY: managed json/jsonb fields retain PG-validated, NUL-free
            // type-output text, including array/composite leaves. The
            // canonical foreign mapping uses text for strings and structural
            // JSONB for containers, so it does not admit unvalidated PG JSON.
            // Managed writers preserve the int2/OID/interval component ranges
            // when widening their storage. The canonical foreign mapping never
            // binds external wider numbers to those narrower PG declarations.
            let plan = unsafe {
                BoundDatumPlan::bind_with_prevalidated_json_text(
                    &semantic.field(&field.name, !field.required)?,
                    PgType::new(target_oid, attr_types[destination].1),
                )
            }?;
            columns.push(ReadColumnBinding {
                source_base_attno: binding.attno,
                source_column,
                destination,
                plan,
            });
        }
        Ok(Self {
            columns: columns.into(),
        })
    }

    fn into_decoder(self) -> IcebergResult<ArrowColumnDecoder> {
        self.columns
            .into_vec()
            .into_iter()
            .map(|e| {
                debug_assert!(e.source_base_attno > 0);
                // SAFETY: construction bound the destination and actual OID to
                // the output layout and selected the matching provider codec.
                // Managed TextIo fields retain PgTextIo's NUL-free output.
                // ForeignSchemaBinding enforces the canonical foreign mapping:
                // external strings become PG text, while nested JSONB uses
                // the structural codec; neither enters the TextIo reader.
                unsafe { DecodedColumn::bind(e.plan, e.source_column, e.destination) }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(ArrowColumnDecoder::new)
            .map_err(IcebergError::from)
    }
}

/// A storage projection paired with its compiled PostgreSQL decoder.
pub(crate) struct PgReadPlan {
    read: StorageProjection,
    decoder: ArrowColumnDecoder,
}

impl PgReadPlan {
    /// Bind all live relation columns.
    pub(crate) fn bind_full(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
    ) -> IcebergResult<Self> {
        let field_map = RelationFieldMap::bind(&schema, layout)?;
        Self::bind_field_map(
            schema,
            &field_map,
            layout.slot_width(),
            layout.attr_types(),
        )
    }

    /// Bind selected attributes to the actual output slot layout.
    pub(crate) fn bind_projection(
        schema: Arc<IcebergSchema>,
        layout: &RelationLayout,
        projection: &SlotProjection,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        if projection.columns().is_empty() {
            return Ok(Self {
                read: StorageProjection::empty(schema),
                decoder: ArrowColumnDecoder::new(Vec::new()),
            });
        }

        let field_map = RelationFieldMap::bind_projection(
            &schema,
            layout,
            projection
                .columns()
                .iter()
                .map(|field| (field.attno, field.destination)),
        )?;
        Self::bind_field_map(schema, &field_map, slot_width, attr_types)
    }

    fn bind_field_map(
        schema: Arc<IcebergSchema>,
        field_map: &RelationFieldMap,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let field_ids = field_map.field_ids().into_boxed_slice();
        let plan = ReadColumnPlan::bind(&schema, field_map, slot_width, attr_types)?;
        let decoder = plan.into_decoder()?;
        Ok(Self {
            read: StorageProjection::from_field_ids(schema, field_ids),
            decoder,
        })
    }

    pub(crate) fn into_parts(self) -> (StorageProjection, ArrowColumnDecoder) {
        (self.read, self.decoder)
    }
}

/// Output schema and source-bound column plans, without runtime builder state.
pub(crate) struct WriteColumnPlan {
    schema: Arc<ArrowSchema>,
    columns: Box<[BoundWriteColumnPlan]>,
}

impl WriteColumnPlan {
    /// Bind every Iceberg field in schema order before constructing the buffer.
    pub(crate) fn bind(
        schema: &IcebergSchema,
        layout: &RelationLayout,
    ) -> IcebergResult<Self> {
        let field_map = RelationFieldMap::bind(schema, layout)?;
        let columns = Self::bind_columns(
            schema,
            &field_map,
            layout.slot_width(),
            layout.attr_types(),
        )?;
        let arrow_schema = Arc::new(schema.to_arrow_schema()?);
        Ok(Self {
            schema: arrow_schema,
            columns: columns.into_boxed_slice(),
        })
    }

    fn bind_columns(
        schema: &IcebergSchema,
        field_map: &RelationFieldMap,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Vec<BoundWriteColumnPlan>> {
        let fields = schema.as_struct().fields();
        let mut columns = Vec::with_capacity(fields.len());
        // Both managed and foreign writers require exact schema coverage.
        // A SELECT projection is not a write contract, and SQL defaults have
        // already been materialized into the source slot by PostgreSQL.
        let bindings_by_field_id: HashMap<i32, &RelationFieldBinding> = field_map
            .bindings()
            .iter()
            .map(|binding| (binding.field_id, binding))
            .collect();
        if fields.len() != bindings_by_field_id.len() {
            return Err(IcebergError::InvariantViolated(
                "Iceberg writes require an exact relation-to-storage field mapping",
            ));
        }
        for field in fields {
            let binding = bindings_by_field_id.get(&field.id).ok_or(
                IcebergError::InvariantViolated(
                    "Iceberg writes require a relation source for every storage field",
                ),
            )?;
            let destination = RelationFieldMap::validate_destination(
                binding.destination,
                slot_width,
            )?;
            let plan = WriteField::new(field).plan(PgType::new(
                attr_types[destination].0,
                attr_types[destination].1,
            ))?;
            columns.push(BoundWriteColumnPlan::bind(plan, destination, slot_width)?);
        }
        Ok(columns)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Arc<ArrowSchema>, Box<[BoundWriteColumnPlan]>) {
        (self.schema, self.columns)
    }
}
