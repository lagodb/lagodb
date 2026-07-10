//! Binds per-column conversion rules to the relation's column layout.
//!
//! This module owns the **position-aware** half of the conversion layer. The
//! per-field type mapping (Iceberg → Arrow/PG, rule resolution, supportability)
//! lives in [`type_mapping`](super::type_mapping); here those resolved rules
//! get *bound* to concrete slot/attno/Arrow-column positions:
//!
//! - [`RelationShape`] captures the PostgreSQL tuple layout once and supplies
//!   the same descriptor-derived inputs to both directions.
//! - [`ScanColumns`] (read): holds the bound `IcebergSchema` and the cached
//!   [`ColumnMapping`]; produces the slot-first decoder plan (`decoded_columns`)
//!   both scan paths consume.
//! - [`WriteColumns`] (write): holds the bound Arrow schema and one
//!   [`pg_arrow_conv::ColumnRule`] per column, and builds the columnar slot
//!   buffer the mutation write path appends tuple slots into directly.
//!
//! ## Position arithmetic lives here
//!
//! [`ColumnMapping`] is the *single owner* of all "Arrow column ↔ slot
//! position" arithmetic for the scan path. `scan.rs` and
//! `provider.rs` contain no `attno - 1` / `dest` index math: they hand this
//! module a [`RelationShape`] (full-schema path) or a resolved projection
//! (projected path), and `ColumnMapping` turns those into destination slot
//! indices.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::spec::Schema as IcebergSchema;
use pg_arrow_conv::{ColumnRule, DecodedColumn, PgColumnType, SlotRecordBatchBuffer};
use pg_lakebase_core::batch::{BatchBuffer, SlotColumnarBatchBuffer};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::TupleSlotRow;
use pgrx::pg_sys;

use super::projection::Projection;
use super::type_mapping::{IcebergFieldExt, IcebergSchemaExt, IcebergTypeExt};
use crate::error::{IcebergError, IcebergResult};

// ---------------------------------------------------------------------------
// Relation column layout
// ---------------------------------------------------------------------------

/// One live (non-dropped) PG relation column: its 1-based attribute number and
/// PostgreSQL column name.
///
/// Names are used only at the catalog boundary to bind PG attributes to
/// Iceberg field ids. Execution plans carry the resolved field id.
#[derive(Debug, Clone)]
pub(crate) struct LiveColumn {
    /// 1-based PG attribute number of the live column.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Column name (== Iceberg field name).
    pub(crate) name: String,
}

impl LiveColumn {
    pub(crate) fn new(attno: pg_sys::AttrNumber, name: String) -> Self {
        Self { attno, name }
    }
}

/// Relation layout shared by the read and write column-mapping paths.
///
/// Derived once from a relation's `TupleDesc`: live columns retain their
/// names and one-based attribute numbers, while `slot_width` and `attr_types`
/// retain the full tuple layout including dropped-column positions.
#[derive(Debug, Clone)]
pub(crate) struct RelationShape {
    live_columns: Vec<LiveColumn>,
    slot_width: usize,
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationShape {
    /// Capture the column layout from a live PostgreSQL relation.
    pub(crate) fn from_relation(rel: &RelationHandle) -> Self {
        let live_columns = rel
            .live_columns()
            .into_iter()
            .map(|(attno, name)| LiveColumn::new(attno, name))
            .collect();

        Self {
            live_columns,
            slot_width: rel.natts(),
            attr_types: rel.attr_types(),
        }
    }

    #[cfg(feature = "pg_test")]
    pub(crate) fn for_test(
        live_columns: Vec<LiveColumn>,
        slot_width: usize,
        attr_types: Vec<(pg_sys::Oid, i32)>,
    ) -> Self {
        Self {
            live_columns,
            slot_width,
            attr_types,
        }
    }

    fn live_columns(&self) -> &[LiveColumn] {
        &self.live_columns
    }

    fn slot_width(&self) -> usize {
        self.slot_width
    }

    fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }
}

// ---------------------------------------------------------------------------
// RelationFieldMap: PG attno -> Iceberg field id binding
// ---------------------------------------------------------------------------

/// One live relation field bound to an Iceberg field id.
#[derive(Debug, Clone)]
pub(crate) struct RelationFieldBinding {
    /// 1-based PostgreSQL attribute number.
    pub(crate) attno: pg_sys::AttrNumber,
    /// Zero-based destination in the target tuple slot.
    pub(crate) destination: usize,
    /// Iceberg field id. This is the execution identity.
    pub(crate) field_id: i32,
    /// Current PostgreSQL/Iceberg name, retained for diagnostics only.
    pub(crate) debug_name: String,
}

impl RelationFieldBinding {
    fn with_destination(&self, destination: usize) -> Self {
        Self {
            attno: self.attno,
            destination,
            field_id: self.field_id,
            debug_name: self.debug_name.clone(),
        }
    }
}

/// Field-id binding for one scan/write descriptor.
#[derive(Debug, Clone)]
pub(crate) struct RelationFieldMap {
    fields: Vec<RelationFieldBinding>,
    slot_width: usize,
    attr_types: Vec<(pg_sys::Oid, i32)>,
}

impl RelationFieldMap {
    /// Bind every live PG column in `shape` to the current Iceberg schema.
    ///
    /// This is the only catalog-boundary name lookup in the scan/write path.
    /// The returned map carries field ids, and later execution code must not
    /// use column names as identity.
    pub(crate) fn from_shape(
        schema: &IcebergSchema,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        let mut fields = Vec::with_capacity(shape.live_columns().len());
        for col in shape.live_columns() {
            let field = schema
                .field_by_name(&col.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(col.name.clone()))?;
            let destination =
                ColumnMapping::dest_from_attno(col.attno, shape.slot_width())?;
            fields.push(RelationFieldBinding {
                attno: col.attno,
                destination,
                field_id: field.id,
                debug_name: col.name.clone(),
            });
        }
        Ok(Self {
            fields,
            slot_width: shape.slot_width(),
            attr_types: shape.attr_types().to_vec(),
        })
    }

    /// Build the compact custom-scan binding for a projection.
    pub(crate) fn project(
        &self,
        projection: &Projection,
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let mut fields = Vec::with_capacity(projection.columns().len());
        for projected in projection.columns() {
            if projected.attno <= 0 {
                return Err(IcebergError::InvariantViolated(
                    "RelationFieldMap: projected source attno must be >= 1",
                ));
            }
            let binding =
                self.binding_for_attno(projected.attno).ok_or_else(|| {
                    IcebergError::ColumnNotFound(format!("attno {}", projected.attno))
                })?;
            let destination =
                ColumnMapping::validate_dest(projected.destination, slot_width)?;
            fields.push(binding.with_destination(destination));
        }
        Ok(Self {
            fields,
            slot_width,
            attr_types: attr_types.to_vec(),
        })
    }

    fn binding_for_attno(
        &self,
        attno: pg_sys::AttrNumber,
    ) -> Option<&RelationFieldBinding> {
        self.fields.iter().find(|binding| binding.attno == attno)
    }

    pub(crate) fn bindings(&self) -> &[RelationFieldBinding] {
        &self.fields
    }

    fn slot_width(&self) -> usize {
        self.slot_width
    }

    fn attr_types(&self) -> &[(pg_sys::Oid, i32)] {
        &self.attr_types
    }

    fn field_ids(&self) -> Vec<i32> {
        self.fields.iter().map(|field| field.field_id).collect()
    }
}

// ---------------------------------------------------------------------------
// ColumnMapping: the single owner of scan position arithmetic
// ---------------------------------------------------------------------------

/// One selected column: the Iceberg field to decode, the index of the Arrow
/// batch column it is read from, and the destination slot index it must be
/// written to.
#[derive(Clone)]
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
    /// construction from the pair `(Arrow DataType, PgColumnType)` (see
    /// [`IcebergFieldExt::resolve_rule`]). The hot loops dispatch through this
    /// already-resolved rule rather than re-resolving per row.
    pub(crate) rule: ColumnRule,
    /// Actual destination descriptor OID, cached once for decoder binding.
    pub(crate) target_oid: pg_sys::Oid,
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
#[derive(Clone)]
pub(crate) struct ColumnMapping {
    pub(crate) entries: Arc<[ProjectedColumn]>,
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
            let dest =
                Self::validate_dest(binding.destination, field_map.slot_width())?;
            let rule = field
                .resolve_rule(Self::pg_target_at(field_map.attr_types(), dest)?)?;
            let target_oid = field_map.attr_types()[dest].0;
            entries.push(ProjectedColumn {
                source_base_attno: binding.attno,
                src_col,
                dest,
                rule,
                target_oid,
            });
        }
        Ok(Self {
            entries: entries.into(),
        })
    }

    /// Build the slot-first decoder plan from this mapping, pairing each
    /// already-resolved entry with the destination column's target type OID.
    ///
    /// Reuses the entries' bound `rule`/`src_col`/`dest` verbatim and looks up
    /// `dest_oid` at `attr_types[dest]`. The target OID is load-bearing for rules
    /// a single `ColumnRule` cannot disambiguate (`text`/`json`/`name`,
    /// `bytea`/`jsonb`); the decoder classifies it into a `DatumTarget` once at
    /// bind. `attr_types` is the relation's full-width `(oid, typmod)` list
    /// indexed by `attno - 1`, so `dest` indexes it directly.
    fn decoded_columns(&self) -> Vec<DecodedColumn> {
        self.entries
            .iter()
            .map(|e| {
                debug_assert!(e.source_base_attno > 0);
                DecodedColumn::new(e.rule.clone(), e.src_col, e.dest, e.target_oid)
            })
            .collect()
    }

    fn validate_dest(dest: usize, slot_width: usize) -> IcebergResult<usize> {
        if dest >= slot_width {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: projected destination is outside the slot width",
            ));
        }
        Ok(dest)
    }

    /// Compute and bounds-check `dest = attno - 1` against `slot_width`.
    fn dest_from_attno(
        attno: pg_sys::AttrNumber,
        slot_width: usize,
    ) -> IcebergResult<usize> {
        if attno < 1 {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: projected column attno must be >= 1",
            ));
        }
        let dest = (attno as usize) - 1;
        if dest >= slot_width {
            return Err(IcebergError::InvariantViolated(
                "ColumnMapping: computed dest is outside the slot width",
            ));
        }
        Ok(dest)
    }

    /// Resolve the destination column's real PostgreSQL target bucket from the
    /// relation's `(oid, typmod)` list — the companion to [`Self::dest_from_attno`].
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

/// Holds the bound Iceberg schema and the cached [`ColumnMapping`] for one
/// scan.
///
/// Constructed once per scan from the bound Iceberg schema plus the position
/// mapping (full-schema or projected). It produces the slot-first decoder plan
/// (`decoded_columns`) both scan paths consume. The bound schema is also
/// exposed so callers that need it for adjacent work (e.g. translating
/// PostgreSQL `ScanKey`s into Iceberg `Predicate`s) do not need to keep a
/// second reference.
#[derive(Clone)]
pub(crate) struct ScanColumns {
    schema: Arc<IcebergSchema>,
    plan: ColumnMapping,
    project_field_ids: Arc<[i32]>,
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
        let project_field_ids = field_map.field_ids().into();
        let plan = ColumnMapping::from_field_map(&schema, &field_map)?;
        Ok(Self {
            schema,
            plan,
            project_field_ids,
        })
    }

    /// Projected plan.
    ///
    /// The projection carries `(attno, destination)` pairs in scan order
    /// (`== iceberg select_field_ids order`); `slot_width` is the relation's
    /// full `natts`; `attr_types` is the relation's full-width `(oid,
    /// typmod)` list.
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
        let field_map = full_map.project(projection, slot_width, attr_types)?;
        let project_field_ids = field_map.field_ids().into();
        let plan = ColumnMapping::from_field_map(&schema, &field_map)?;
        Ok(Self {
            schema,
            plan,
            project_field_ids,
        })
    }

    /// Bound Iceberg schema. Cheap (no allocation): exposes the inner `Arc`'s
    /// referent.
    pub(crate) fn schema(&self) -> &IcebergSchema {
        self.schema.as_ref()
    }

    /// Slot-first decoder plan for the TableAM scan path, derived from this
    /// plan's already-bound [`ColumnMapping`] and the relation's full-width
    /// `(oid, typmod)` list. Lets the AM build an `ArrowColumnDecoder` without
    /// re-resolving rules or re-deriving `dest` arithmetic.
    pub(crate) fn decoded_columns(&self) -> Vec<DecodedColumn> {
        self.plan.decoded_columns()
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
/// It owns both the per-column Arrow write buffer and the source-slot mapping,
/// so the mutation sink drives a single cohesive object rather than coordinating a
/// loose buffer and a separate plan.
pub(crate) struct WriteColumns {
    /// Per-column Arrow write buffer (one encoder per output column), bound to
    /// the relation's Arrow schema. Produces a `RecordBatch` on flush.
    buffer: SlotRecordBatchBuffer,
    /// Source slot index feeding each output column, in Arrow/Iceberg column
    /// order. `Some(attno - 1)` for a column backed by a live PG column;
    /// `None` for an Iceberg field with no live PG column (a dropped column
    /// lingering in the Iceberg metadata schema), which is written as SQL NULL.
    source_slots: Vec<Option<usize>>,
}

impl WriteColumns {
    /// Resolve the write column plan for the full relation schema and build the
    /// bound columnar buffer.
    ///
    /// Runs the supported-shape gate per Iceberg field through
    /// [`IcebergFieldExt::resolve_rule`] (the single resolution+validation
    /// point), binds one rule per column, and resolves each output column's
    /// source slot, so an unsupported column or a column/field desync surfaces
    /// at session begin rather than mid-INSERT. The bound schema and per-column
    /// rules are consumed into the buffer here and not retained.
    ///
    pub(crate) fn resolve(
        schema: &IcebergSchema,
        shape: &RelationShape,
    ) -> IcebergResult<Self> {
        // `resolve_columns` resolves every Iceberg field's rule via
        // `resolve_rule`, which applies the supported-shape gate per field, so
        // an unsupported column surfaces here at session begin rather than
        // mid-INSERT — and before `to_arrow_schema` can silently truncate an
        // oversized `Fixed` width. The writer emits every field, so iterating
        // all fields here is the correct scope (no projection on the write
        // path).
        let field_map = RelationFieldMap::from_shape(schema, shape)?;
        let (rules, source_slots) = Self::resolve_columns(schema, &field_map)?;
        let arrow_schema = Arc::new(schema.to_arrow_schema()?);
        let buffer = SlotRecordBatchBuffer::new(arrow_schema, &rules);
        Ok(Self {
            buffer,
            source_slots,
        })
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
    ) -> IcebergResult<(Vec<ColumnRule>, Vec<Option<usize>>)> {
        let fields = schema.as_struct().fields();
        let mut rules = Vec::with_capacity(fields.len());
        let mut source_slots = Vec::with_capacity(fields.len());
        let mut matched_live = 0usize;
        let bindings_by_field_id: HashMap<i32, &RelationFieldBinding> = field_map
            .bindings()
            .iter()
            .map(|field| (field.field_id, field))
            .collect();
        for field in fields.iter() {
            let (rule, source) = match bindings_by_field_id.get(&field.id) {
                Some(binding) => {
                    let dest = ColumnMapping::validate_dest(
                        binding.destination,
                        field_map.slot_width(),
                    )?;
                    let pg =
                        ColumnMapping::pg_target_at(field_map.attr_types(), dest)?;
                    matched_live += 1;
                    (field.resolve_rule(pg)?, Some(dest))
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
                    (field.resolve_rule(pg)?, None)
                }
            };
            rules.push(rule);
            source_slots.push(source);
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
        Ok((rules, source_slots))
    }

    /// Append one tuple-slot row, pulling each output column from its bound
    /// source slot (SQL NULL for columns with no live source).
    ///
    /// This is the write-side twin of the read path's per-column decode: the
    /// position arithmetic stays here, and the buffer only ever sees
    /// already-mapped per-column datums. The slot is deformed once via
    /// [`TupleSlotRow::datums`] so each source lookup is an O(1) index rather
    /// than rebuilding the slot's backing slices per column.
    pub(crate) fn append_slot_row(
        &mut self,
        row: TupleSlotRow<'_>,
    ) -> IcebergResult<()> {
        let datums = row.datums();
        for (col_idx, &source) in self.source_slots.iter().enumerate() {
            let value = source.and_then(|slot_idx| datums.datum_at(slot_idx));
            self.buffer.append_datum_to_column(col_idx, value)?;
        }
        self.buffer.finish_row()?;
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
