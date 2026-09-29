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
    ArrowColumnDecoder, BoundWriteColumnPlan, ColumnRule, DatumCodec, DecodedColumn,
};
use pgrx::pg_sys;

use super::projection::{SlotProjection, StorageProjection};
use super::relation::{RelationFieldBinding, RelationFieldMap, RelationLayout};
use super::type_mapping::{IcebergSchemaExt, IcebergTypeExt};
use crate::error::{IcebergError, IcebergResult};

/// One selected Arrow column and its PostgreSQL destination and codec.
pub(crate) struct ReadColumnBinding {
    /// Source relation attribute, independent of the output destination.
    pub(crate) source_base_attno: pg_sys::AttrNumber,
    /// Index in the batch requested in field-map order.
    pub(crate) source_column: usize,
    /// Zero-based destination in the actual scan tuple.
    pub(crate) destination: usize,
    pub(crate) rule: ColumnRule,
    pub(crate) target_oid: pg_sys::Oid,
    pub(crate) codec: DatumCodec,
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
            let rule = field.field_type.resolve_rule_for_oid(target_oid)?;
            let codec = match (target_oid, &rule) {
                (pg_sys::JSONBOID, ColumnRule::PostgresJsonbVarlena) => {
                    // SAFETY: the provider-selected rule is backed by the
                    // Iceberg JSONB writer, which emits complete PostgreSQL
                    // JSONB varlena bytes.
                    unsafe { DatumCodec::postgres_jsonb_varlena() }
                }
                (pg_sys::JSONOID, ColumnRule::Utf8) => {
                    // SAFETY: PostgreSQL JSON values entering this relation
                    // have already passed json_in; the writer stores their
                    // validated text payload unchanged.
                    unsafe { DatumCodec::prevalidated_json_text() }
                }
                (_, _) => DatumCodec::standard(target_oid)?,
            };
            columns.push(ReadColumnBinding {
                source_base_attno: binding.attno,
                source_column,
                destination,
                rule,
                target_oid,
                codec,
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
                unsafe {
                    DecodedColumn::new(
                        e.rule,
                        e.source_column,
                        e.destination,
                        e.target_oid,
                        e.codec,
                    )
                }
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

        let full_map = RelationFieldMap::bind(&schema, layout)?;
        let field_map = full_map.project(
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
        let mut matched_live = 0usize;
        let bindings_by_field_id: HashMap<i32, &RelationFieldBinding> = field_map
            .bindings()
            .iter()
            .map(|field| (field.field_id, field))
            .collect();
        for field in fields.iter() {
            let column = match bindings_by_field_id.get(&field.id) {
                Some(binding) => {
                    let destination = RelationFieldMap::validate_destination(
                        binding.destination,
                        slot_width,
                    )?;
                    let rule = field
                        .field_type
                        .resolve_rule_for_oid(attr_types[destination].0)?;
                    matched_live += 1;
                    BoundWriteColumnPlan::bind(
                        rule,
                        Some(destination),
                        Some(attr_types[destination].0),
                        slot_width,
                    )?
                }
                None => {
                    // An absent optional source produces a typed all-NULL column.
                    if field.required {
                        return Err(IcebergError::RequiredColumnMissingSource(
                            field.name.clone(),
                        ));
                    }
                    let pg = field.field_type.canonical_column_type().ok_or_else(
                        || {
                            IcebergError::UnsupportedColumnType(format!(
                                "{:?} has no target PostgreSQL column type",
                                field.field_type
                            ))
                        },
                    )?;
                    BoundWriteColumnPlan::bind(
                        field.field_type.resolve_rule(pg)?,
                        None,
                        None,
                        slot_width,
                    )?
                }
            };
            columns.push(column);
        }
        // Every live binding must feed a top-level output field.
        if matched_live != field_map.bindings().len() {
            let missing = field_map
                .bindings()
                .iter()
                .find(|binding| {
                    !fields.iter().any(|field| field.id == binding.field_id)
                })
                .map(|binding| binding.debug_name.clone())
                .unwrap_or_else(|| "<unknown>".to_string());
            return Err(IcebergError::ColumnNotFound(missing));
        }
        Ok(columns)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Arc<ArrowSchema>, Box<[BoundWriteColumnPlan]>) {
        (self.schema, self.columns)
    }
}
