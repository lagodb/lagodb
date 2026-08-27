//! Binds per-column conversion rules to the relation's column layout.
//!
//! This module owns the **position-aware** half of the conversion layer. The
//! per-field type mapping (Iceberg → Arrow/PG, rule resolution, supportability)
//! lives in [`type_mapping`](super::type_mapping); here those resolved rules
//! get *bound* to concrete slot/attno/Arrow-column positions:
//!
//! - [`RelationShape`](super::relation::RelationShape) captures the
//!   PostgreSQL tuple layout once and supplies the same descriptor-derived
//!   inputs to both directions and to projected scan adapters.
//! - [`ScanColumns`] (read): holds the bound `IcebergSchema`, projected field
//!   ids, and the compiled slot-first decoder shared by cursors in this scan.
//! - [`WriteColumns`] (write): binds Iceberg fields to the relation's source
//!   slots and hands the resulting source/Arrow plan to the generic bound
//!   columnar writer.
//!
//! ## Position arithmetic
//!
//! [`RelationFieldMap`] owns PostgreSQL position validation, while the
//! CustomScan-only relation index owns direct `attno - 1` lookup.
//! [`ColumnMapping`] consumes validated bindings and owns Arrow-column to
//! tuple-slot conversion planning. Scan and provider lifecycle code perform no
//! position arithmetic.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::spec::Schema as IcebergSchema;
use lagodb_core::batch::BatchBuffer;
use lagodb_core::tuple::TupleSlotRow;
use pg_arrow_conv::{
    ArrowColumnDecoder, BoundWriteBuffer, BoundWriteColumnPlan, ColumnRule,
    DatumCodec, DecodedColumn, PgColumnType,
};
use pgrx::pg_sys;

use super::relation::{RelationFieldBinding, RelationFieldMap, RelationShape};
use super::type_mapping::{IcebergFieldExt, IcebergSchemaExt, IcebergTypeExt};
use crate::engine::scan::projection::Projection;
use crate::error::{IcebergError, IcebergResult};

// ---------------------------------------------------------------------------
// ColumnMapping: the single owner of scan position arithmetic
// ---------------------------------------------------------------------------

/// One selected column: the Iceberg field to decode, the index of the Arrow
/// batch column it is read from, and the destination slot index it must be
/// written to.
pub(crate) struct ProjectedColumn {
    /// Original base-relation attribute number used to resolve the Iceberg
    /// source field. It is intentionally distinct from `dest`.
    pub(crate) source_base_attno: pg_sys::AttrNumber,
    /// Index of the source column in the Arrow batch the scan produces.
    ///
    /// This is decoupled from the entry's position because the two scan
    /// shapes order their batch columns differently:
    ///
    /// - full relation scan: the batch carries the live relation fields in
    ///   Iceberg field-id binding order, so `src_col` follows the
    ///   [`RelationFieldMap`] request order.
    /// - projected scan: `TableScan::select_field_ids` preserves requested
    ///   field-id order, so `src_col` is the projection entry index.
    pub(crate) src_col: usize,
    /// Destination cell index in the actual PG scan tuple.
    pub(crate) dest: usize,
    /// The `pg-arrow-conv` conversion rule for this column, resolved once at
    /// construction from the Iceberg field and the live PostgreSQL target (see
    /// [`IcebergFieldExt::resolve_rule_for_column`]). The hot loops dispatch
    /// through this already-resolved rule rather than re-resolving per row.
    pub(crate) rule: ColumnRule,
    /// The relation attribute OID the decoder will write. It travels with the
    /// physical codec so JSONB bytes cannot be bound to an unrelated slot type.
    pub(crate) target_oid: pg_sys::Oid,
    /// Provider-selected Datum codec, bound once with the rule.
    pub(crate) codec: DatumCodec,
}

/// Projection-aware column plan: the single owner of position arithmetic.
///
/// Each entry decodes one selected column: it reads the Arrow batch column at
/// `entry.src_col` and writes it to slot `entry.dest`. The relation's full PG
/// tuple width (`natts`, counting dropped-column positions) is consumed during
/// construction to bounds-check each `dest`. The decoder only writes the `dest`
/// slots present here; projected-away / dropped positions are left untouched,
/// which is safe only because they are never read (a whole-row reference forces
/// the full-schema plan, and a projection maps exactly the referenced columns).
/// This does not depend on the cleared slot reading those positions as NULL —
/// `ExecClearTuple` does not reset `tts_isnull`.
///
/// Entry order matches the storage read order, while `src_col`
/// locates each entry's column in the produced batch — they coincide
/// (`src_col == j`) for projections and a clean full-table scan, but can
/// diverge for a full-table scan whose Iceberg schema is wider than the
/// live PG columns (a dropped column that still lingers in the Iceberg
/// metadata schema).
pub(crate) struct ColumnMapping {
    pub(crate) entries: Box<[ProjectedColumn]>,
}

impl ColumnMapping {
    /// Build a scan decoder plan from a field-id relation binding.
    pub(crate) fn from_field_map(
        schema: &IcebergSchema,
        field_map: &RelationFieldMap,
    ) -> IcebergResult<Self> {
        let mut entries = Vec::with_capacity(field_map.bindings().len());
        for (src_col, binding) in field_map.bindings().iter().enumerate() {
            let field = schema
                .as_struct()
                .field_by_id(binding.field_id)
                .ok_or_else(|| {
                    IcebergError::ColumnNotFound(binding.debug_name.clone())
                })?;
            let dest = RelationFieldMap::validate_destination(
                binding.destination,
                field_map.slot_width(),
            )?;
            let target_oid = field_map.attr_types()[dest].0;
            let pg = Self::pg_target_at(field_map.attr_types(), dest)?;
            let rule = field.resolve_rule_for_column(pg, target_oid)?;
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
            entries.push(ProjectedColumn {
                source_base_attno: binding.attno,
                src_col,
                dest,
                rule,
                target_oid,
                codec,
            });
        }
        Ok(Self {
            entries: entries.into(),
        })
    }

    /// Build the slot-first decoder plan from this mapping, pairing each
    /// already-resolved entry with its explicit physical datum codec.
    ///
    /// Reuses each entry's already-bound `rule`/`src_col`/`dest`/`codec`
    /// verbatim. Physical codec selection belongs to the provider's planning
    /// boundary, not to the generic Arrow decoder or its row loop.
    fn into_decoder(self) -> IcebergResult<ArrowColumnDecoder> {
        self.entries
            .into_vec()
            .into_iter()
            .map(|e| {
                debug_assert!(e.source_base_attno > 0);
                unsafe {
                    DecodedColumn::new(
                        e.rule,
                        e.src_col,
                        e.dest,
                        e.target_oid,
                        e.codec,
                    )
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(ArrowColumnDecoder::new)
            .map_err(IcebergError::from)
    }

    /// Resolve the destination column's real PostgreSQL target bucket from the
    /// relation's `(oid, typmod)` list.
    ///
    /// Rules are resolved against the column's *actual* type (via
    /// [`PgColumnType::from_pg_type`]) rather than a type round-tripped back from
    /// the Iceberg schema, so an incompatible stored type is caught at
    /// construction. `dest` is already bounds-checked against `slot_width` (which
    /// equals `attr_types.len()`), so the index is in range. A PG type this
    /// layer cannot target is an `UnsupportedColumnType`.
    fn pg_target_at(
        attr_types: &[(pg_sys::Oid, i32)],
        dest: usize,
    ) -> IcebergResult<PgColumnType> {
        let (oid, _typmod) = attr_types[dest];
        PgColumnType::from_pg_type(oid).ok_or_else(|| {
            IcebergError::UnsupportedColumnType(format!(
                "PostgreSQL OID {} has no Arrow conversion target",
                u32::from(oid)
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// ScanColumns: bound read-side column plan
// ---------------------------------------------------------------------------

/// Holds the bound Iceberg schema and compiled decoder for one scan.
pub(crate) struct ScanColumns {
    schema: Arc<IcebergSchema>,
    decoder: ArrowColumnDecoder,
    project_field_ids: Box<[i32]>,
}

impl ScanColumns {
    /// Select-all plan (full-schema plan).
    ///
    /// The relation shape owns the live `(attno, name)` columns and the full
    /// tuple layout. This module owns the name→Iceberg-field resolution and
    /// the `dest = attno - 1` arithmetic.
    pub(crate) fn new(
        schema: Arc<IcebergSchema>,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        // Per-column `(Arrow DataType, PgColumnType)` validation against the
        // relation's real column types — including the supported-shape gate
        // that rejects Struct/Map/nested-list/oversized-`Fixed` columns — runs
        // once per mapped column as a byproduct of `resolve_rule` inside the
        // mapping builder below. There is no separate whole-schema dry-run
        // pass: a select-all plan maps every live column, so the gate still
        // covers each column the scan will decode, while a lingering
        // dropped-but-unsupported field that no live column maps to is never
        // decoded and so is correctly left unchecked.
        let field_map = RelationFieldMap::from_shape(&schema, shape)?;
        Self::from_field_map(schema, &field_map)
    }

    /// Projected plan.
    ///
    /// The projection carries `(attno, destination)` pairs in scan order
    /// (`== iceberg select_field_ids order`); `slot_width` and `attr_types`
    /// describe the PostgreSQL scan tuple receiving the projected columns.
    ///
    /// The supported-shape gate is applied per projected column through
    /// [`ColumnMapping::from_field_map`], not over the whole stored schema. A
    /// whole-schema gate here would reject `SELECT a FROM t` merely because
    /// some *unprojected* column has an unsupported shape, even though the
    /// underlying scan only ever selects the projected field ids.
    pub(crate) fn with_projection(
        schema: Arc<IcebergSchema>,
        shape: &RelationShape,
        projection: &Projection,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let full_map = RelationFieldMap::from_shape(&schema, shape)?;
        let field_map = full_map.project(
            projection
                .columns()
                .iter()
                .map(|field| (field.attno, field.destination)),
            slot_width,
            attr_types,
        )?;
        Self::from_field_map(schema, &field_map)
    }

    fn from_field_map(
        schema: Arc<IcebergSchema>,
        field_map: &RelationFieldMap,
    ) -> IcebergResult<Self> {
        let project_field_ids = field_map.field_ids().into_boxed_slice();
        let plan = ColumnMapping::from_field_map(&schema, field_map)?;
        let decoder = plan.into_decoder()?;
        Ok(Self {
            schema,
            decoder,
            project_field_ids,
        })
    }

    /// Bound Iceberg schema. Cheap (no allocation): exposes the inner `Arc`'s
    /// referent.
    pub(crate) fn schema(&self) -> &IcebergSchema {
        self.schema.as_ref()
    }

    /// Clone the scan-lifetime decoder plan without rebuilding its columns.
    pub(crate) fn decoder(&self) -> ArrowColumnDecoder {
        self.decoder.clone()
    }

    pub(crate) fn project_field_ids(&self) -> &[i32] {
        &self.project_field_ids
    }
}

// ---------------------------------------------------------------------------
// WriteColumns: bound write-side column plan
// ---------------------------------------------------------------------------

/// The relation-bound columnar write buffer for the mutation path — the write-side
/// analogue of [`ScanColumns`] (and a sibling of the read cursor, which bundles
/// its decoder and batch source the same way).
///
/// It owns the per-column Arrow write buffer, whose columns each carry their
/// source-slot binding, so the mutation sink drives one cohesive plan rather
/// than coordinating parallel mapping and encoder vectors.
pub(crate) struct WriteColumns {
    /// Relation-bound source codecs and Arrow encoders in Iceberg schema order.
    /// The buffer owns the complete hot-path plan, including source positions.
    buffer: BoundWriteBuffer,
}

impl WriteColumns {
    /// Resolve the write column plan for the full relation schema and build the
    /// bound columnar buffer.
    ///
    /// Runs the supported-shape gate per Iceberg field through
    /// [`IcebergFieldExt::resolve_rule`] or
    /// [`IcebergFieldExt::resolve_rule_for_column`], binds one rule per
    /// column, and resolves each output column's source slot, so an unsupported
    /// column or a column/field desync surfaces at session begin rather than
    /// mid-INSERT. The bound schema and per-column rules are consumed into the
    /// buffer here and not retained.
    ///
    pub(crate) fn resolve(
        schema: &IcebergSchema,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        // `resolve_columns` resolves every Iceberg field's rule via one of the
        // two field resolvers, both of which apply the supported-shape gate per
        // field. An unsupported column therefore surfaces here at session
        // begin rather than mid-INSERT — and before `to_arrow_schema` can
        // silently truncate an oversized `Fixed` width. The writer emits every
        // field, so iterating all fields here is the correct scope (no
        // projection on the write path).
        let field_map = RelationFieldMap::from_shape(schema, shape)?;
        let columns = Self::resolve_columns(schema, &field_map)?;
        let arrow_schema = Arc::new(schema.to_arrow_schema()?);
        let buffer = BoundWriteBuffer::new(arrow_schema, columns.into_boxed_slice())?;
        Ok(Self { buffer })
    }

    /// Resolve, per Iceberg output column (in schema order), its conversion
    /// rule and the source slot index that feeds it — the write-side twin of
    /// [`ColumnMapping::from_field_map`].
    ///
    /// The Parquet writer emits every Iceberg field, so this iterates Iceberg
    /// fields (not live columns): a field id bound to a live PG column binds to
    /// that column's slot index (`attno - 1`, bounds-checked) and resolves its
    /// rule against the column's *real* PG type, so a stored type incompatible
    /// with the relation column fails here; a field with no live column (a
    /// dropped column still lingering in the Iceberg schema) binds to `None`
    /// (written as SQL NULL) and resolves its rule from the Iceberg-derived
    /// type, since there is no relation column to validate against.
    fn resolve_columns(
        schema: &IcebergSchema,
        field_map: &RelationFieldMap,
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
                    let dest = RelationFieldMap::validate_destination(
                        binding.destination,
                        field_map.slot_width(),
                    )?;
                    let pg =
                        ColumnMapping::pg_target_at(field_map.attr_types(), dest)?;
                    matched_live += 1;
                    let rule = field.resolve_rule_for_column(
                        pg,
                        field_map.attr_types()[dest].0,
                    )?;
                    BoundWriteColumnPlan::bind(
                        rule,
                        Some(dest),
                        Some(field_map.attr_types()[dest].0),
                        field_map.slot_width(),
                    )?
                }
                None => {
                    // No live PG column feeds this Iceberg field. That is only
                    // valid for an OPTIONAL field — a dropped/extra column we
                    // write as SQL NULL every row. A REQUIRED field with no
                    // source cannot be satisfied: fail fast here rather than
                    // letting the all-NULL column surface later as an opaque
                    // non-nullable `RecordBatch::try_new` Arrow error at flush.
                    // The DDL hook removes dropped columns from the current
                    // Iceberg schema. Keep this guard anyway: it catches stale
                    // metadata, unsupported external schema changes, or a
                    // future DDL path that bypasses the schema-evolution
                    // planner.
                    if field.required {
                        return Err(IcebergError::RequiredColumnMissingSource(
                            field.name.clone(),
                        ));
                    }
                    // An optional lingering column has no relation column to
                    // validate against; resolve its rule from the Iceberg-derived
                    // type so the encoder still emits a typed all-NULL column
                    // matching the Arrow schema the writer expects.
                    let pg = field.field_type.pg_column_type().ok_or_else(|| {
                        IcebergError::UnsupportedColumnType(format!(
                            "{:?} has no target PostgreSQL column type",
                            field.field_type
                        ))
                    })?;
                    BoundWriteColumnPlan::bind(
                        field.resolve_rule(pg)?,
                        None,
                        None,
                        field_map.slot_width(),
                    )?
                }
            };
            columns.push(column);
        }
        // Field names are unique and live-column names are unique, so a
        // shortfall means some live column found no field to write into (e.g.
        // an `ALTER TABLE RENAME COLUMN` attname/field desync). Fail loud
        // rather than silently writing NULLs for it.
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

    /// Append one tuple-slot row, pulling each output column from its bound
    /// source slot (SQL NULL for columns with no live source).
    ///
    /// The bound writer deforms the slot once and directly iterates its fixed
    /// source/encoder column plan; no parallel source-slot and encoder indices
    /// are synchronized in this row path.
    pub(crate) unsafe fn append_slot_row(
        &mut self,
        row: TupleSlotRow<'_>,
    ) -> IcebergResult<()> {
        // SAFETY: `WriteColumns` is resolved from this relation's
        // `RelationShape`, and callers pass the tuple slot received by the
        // same relation-local mutation callback. The slot layout and source
        // OIDs therefore match the bound plan.
        unsafe { self.buffer.append_slot_row(row)? };
        Ok(())
    }

    /// Whether buffered data has reached the memory threshold and should flush.
    pub(crate) fn should_flush(&self, max_bytes: usize) -> bool {
        self.buffer.should_flush(max_bytes)
    }

    /// Whether no rows are currently buffered.
    pub(crate) fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Finish the buffered columns into a `RecordBatch`, resetting the buffer
    /// for reuse.
    pub(crate) fn finish_batch(&mut self) -> IcebergResult<RecordBatch> {
        Ok(self.buffer.finish_batch()?)
    }

    /// Drop buffered rows without producing a batch (failure-path cleanup).
    pub(crate) fn clear(&mut self) {
        self.buffer.clear();
    }
}
