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
//!   buffer the DML write path appends tuple slots into directly.
//!
//! ## Position arithmetic lives here
//!
//! [`ColumnMapping`] is the *single owner* of all "Arrow column ↔ slot
//! position" arithmetic for the scan path. `scan.rs` and
//! `provider.rs` contain no `attno - 1` / `dest` index math: they hand this
//! module a [`RelationShape`] (full-schema path) or a resolved projection
//! (projected path), and `ColumnMapping` turns those into destination slot
//! indices.

use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg_lite::spec::Schema as IcebergSchema;
use pg_arrow_conv::{ColumnRule, DecodedColumn, PgColumnType, SlotRecordBatchBuffer};
use pg_lakebase_core::batch::{BatchBuffer, SlotColumnarBatchBuffer};
use pg_lakebase_core::handles::RelationHandle;
use pg_lakebase_core::tuple::TupleSlotRow;
use pgrx::pg_sys;

use super::projection::ProjectedName;
use super::type_mapping::{IcebergFieldExt, IcebergSchemaExt, IcebergTypeExt};
use crate::error::{IcebergError, IcebergResult};

// ---------------------------------------------------------------------------
// Relation column layout
// ---------------------------------------------------------------------------

/// One live (non-dropped) PG relation column: its 1-based
/// attribute number and its column name (which is also the Iceberg field
/// name, since the Iceberg schema is built from PG column names).
///
/// Carried by [`RelationShape`] into the full-schema
/// [`ColumnMapping`], where the `name` resolves the Iceberg field (tolerating an
/// Iceberg schema wider than the live PG columns — see
/// [`ColumnMapping::from_full_schema`]) and `attno` becomes `dest = attno - 1`.
#[derive(Debug, Clone)]
struct LiveColumn {
    /// 1-based PG attribute number of the live column.
    attno: pg_sys::AttrNumber,
    /// Column name (== Iceberg field name).
    name: String,
}

impl LiveColumn {
    fn new(attno: pg_sys::AttrNumber, name: String) -> Self {
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
// ColumnMapping: the single owner of scan position arithmetic
// ---------------------------------------------------------------------------

/// One selected column: the Iceberg field to decode, the index of the Arrow
/// batch column it is read from, and the destination slot index it must be
/// written to.
#[derive(Clone)]
struct ProjectedColumn {
    /// Original base-relation attribute number used to resolve the Iceberg
    /// source field. It is intentionally distinct from `dest`.
    source_base_attno: pg_sys::AttrNumber,
    /// Index of the source column in the Arrow batch the scan produces.
    ///
    /// This is decoupled from the entry's position because the two scan
    /// shapes order their batch columns differently:
    ///
    /// - select-all (`from_full_schema`): the batch carries one column per
    ///   Iceberg schema field, in schema field order, so `src_col` is the
    ///   field's index in the Iceberg schema — resolved **by name**, not by
    ///   lockstep position, so it tolerates an Iceberg schema that is wider
    ///   than the live PG columns (see `from_full_schema`).
    /// - projection (`from_projection`): `TableScan::select` preserves request
    ///   order, so `src_col` is the projection entry index.
    src_col: usize,
    /// Destination cell index in the actual PG scan tuple.
    dest: usize,
    /// The `pg-arrow-conv` conversion rule for this column, resolved once at
    /// construction from the pair `(Arrow DataType, PgColumnType)` (see
    /// [`IcebergFieldExt::resolve_rule`]). The hot loops dispatch through this
    /// already-resolved rule rather than re-resolving per row.
    rule: ColumnRule,
    /// Actual destination descriptor OID, cached once for decoder binding.
    target_oid: pg_sys::Oid,
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
/// metadata schema — see [`ColumnMapping::from_full_schema`]).
#[derive(Clone)]
struct ColumnMapping {
    entries: Arc<[ProjectedColumn]>,
}

impl ColumnMapping {
    /// Full-table plan keyed by live-attno (the scan/read direction).
    ///
    /// One entry per live (non-dropped) user column, in live-attno (== PG
    /// tuple) order, with `dest = live_columns[k].attno - 1` (so
    /// dropped-column gaps stay NULL). Each column is resolved to its Iceberg
    /// `NestedField` **by name** (`live_columns[k].name`), and `src_col` is
    /// that field's index in the Iceberg schema — the position it occupies in
    /// the `select_all()` Arrow batch.
    ///
    /// Resolving by name (rather than zipping the Iceberg field list against
    /// the live-attno list in lockstep) is what makes this correct when the
    /// stored Iceberg schema is *wider* than the relation's live columns —
    /// e.g. after `ALTER TABLE ... DROP COLUMN`, which removes the PG
    /// attribute (leaving an `attisdropped` gap) but does not rewrite the
    /// Iceberg metadata schema, so a dropped column's field lingers in the
    /// Iceberg schema with no live PG column. The select-all batch still
    /// carries that lingering column, so `src_col` (its schema index) is what
    /// lines the live columns up with their batch columns.
    ///
    fn from_full_schema(
        schema: &IcebergSchema,
        live_columns: &[LiveColumn],
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let struct_ty = schema.as_struct();
        let mut entries = Vec::with_capacity(live_columns.len());
        for col in live_columns {
            // Resolve the live PG column to its Iceberg field by name. A miss
            // means the name desynced from the Iceberg schema (out of scope;
            // fail loud rather than silently mis-align).
            let field = schema
                .field_by_name(&col.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(col.name.clone()))?;
            // The select-all batch is in Iceberg schema field order, so the
            // source batch column is this field's index in the schema struct.
            let src_col = struct_ty
                .fields()
                .iter()
                .position(|f| f.id == field.id)
                .ok_or_else(|| IcebergError::ColumnNotFound(col.name.clone()))?;
            let dest = Self::dest_from_attno(col.attno, slot_width)?;
            // Resolve the rule against the column's real PG type, so a stored
            // Iceberg type that is incompatible with the relation column fails
            // here rather than at datum construction.
            let rule = field.resolve_rule(Self::pg_target_at(attr_types, dest)?)?;
            let target_oid = attr_types[dest].0;
            entries.push(ProjectedColumn {
                source_base_attno: col.attno,
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

    /// Projected plan keyed by resolved `(attno, name)` pairs.
    ///
    /// Resolves each [`ProjectedName::name`] to its Iceberg `NestedField`,
    /// uses the plan-time `destination`, and keeps entries in storage read order.
    /// `TableScan::select` preserves this order into the Arrow batch, so
    /// `src_col == j`. Errors — producing no
    /// `ColumnMapping` — when a name does not resolve, an `attno < 1`, or a
    /// computed `dest >= slot_width`.
    fn from_projection(
        schema: &IcebergSchema,
        pairs: &[ProjectedName],
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let mut entries = Vec::with_capacity(pairs.len());
        for (src_col, pair) in pairs.iter().enumerate() {
            let field = schema
                .field_by_name(&pair.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(pair.name.clone()))?;
            if pair.attno <= 0 {
                return Err(IcebergError::InvariantViolated(
                    "ColumnMapping: projected source attno must be >= 1",
                ));
            }
            let dest = Self::validate_dest(pair.destination, slot_width)?;
            let rule = field.resolve_rule(Self::pg_target_at(attr_types, dest)?)?;
            entries.push(ProjectedColumn {
                source_base_attno: pair.attno,
                src_col,
                dest,
                rule,
                target_oid: attr_types[dest].0,
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
pub(crate) struct ScanColumns {
    schema: Arc<IcebergSchema>,
    plan: ColumnMapping,
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
        let plan = ColumnMapping::from_full_schema(
            &schema,
            shape.live_columns(),
            shape.slot_width(),
            shape.attr_types(),
        )?;
        Ok(Self { schema, plan })
    }

    /// Projected plan.
    ///
    /// `pairs` are the resolved `(attno, name)` columns in scan order
    /// (`== iceberg select order`); `slot_width` is the relation's full
    /// `natts`; `attr_types` is the relation's full-width `(oid, typmod)` list.
    ///
    /// The supported-shape gate is applied per projected column inside
    /// `from_projection` (via `resolve_rule`), not over the whole stored
    /// schema. A whole-schema gate here would reject `SELECT a FROM t` merely
    /// because some *unprojected* column has an unsupported shape, even though
    /// the underlying scan only ever selects the projected names.
    pub(crate) fn with_projection(
        schema: Arc<IcebergSchema>,
        pairs: &[ProjectedName],
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<Self> {
        let plan =
            ColumnMapping::from_projection(&schema, pairs, slot_width, attr_types)?;
        Ok(Self { schema, plan })
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
}

// ---------------------------------------------------------------------------
// WriteColumns: bound write-side column plan
// ---------------------------------------------------------------------------

/// The relation-bound columnar write buffer for the DML path — the write-side
/// analogue of [`ScanColumns`] (and a sibling of the read cursor, which bundles
/// its decoder and batch source the same way).
///
/// It owns both the per-column Arrow write buffer and the source-slot mapping,
/// so the DML sink drives a single cohesive object rather than coordinating a
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
        let (rules, source_slots) = Self::resolve_columns(
            schema,
            shape.live_columns(),
            shape.slot_width(),
            shape.attr_types(),
        )?;
        let arrow_schema = Arc::new(schema.to_arrow_schema()?);
        let buffer = SlotRecordBatchBuffer::new(arrow_schema, &rules);
        Ok(Self {
            buffer,
            source_slots,
        })
    }

    /// Resolve, per Iceberg output column (in schema order), its conversion
    /// rule and the source slot index that feeds it — the write-side twin of
    /// [`ColumnMapping::from_full_schema`].
    ///
    /// The Parquet writer emits every Iceberg field, so this iterates Iceberg
    /// fields (not live columns): a field whose name matches a live PG column
    /// binds to that column's slot index (`attno - 1`, bounds-checked) and
    /// resolves its rule against the column's *real* PG type, so a stored type
    /// incompatible with the relation column fails here; a field with no live
    /// column (a dropped column still lingering in the Iceberg schema) binds to
    /// `None` (written as SQL NULL) and resolves its rule from the
    /// Iceberg-derived type, since there is no relation column to validate
    /// against.
    fn resolve_columns(
        schema: &IcebergSchema,
        live_columns: &[LiveColumn],
        slot_width: usize,
        attr_types: &[(pg_sys::Oid, i32)],
    ) -> IcebergResult<(Vec<ColumnRule>, Vec<Option<usize>>)> {
        let fields = schema.as_struct().fields();
        let mut rules = Vec::with_capacity(fields.len());
        let mut source_slots = Vec::with_capacity(fields.len());
        let mut matched_live = 0usize;
        for field in fields.iter() {
            let (rule, source) = match live_columns
                .iter()
                .find(|c| c.name == field.name)
            {
                Some(col) => {
                    let dest = ColumnMapping::dest_from_attno(col.attno, slot_width)?;
                    let pg = ColumnMapping::pg_target_at(attr_types, dest)?;
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
                    // TODO(schema-evolution): this branch is reachable as a
                    // permanent write failure today. `DROP COLUMN` on a NOT
                    // NULL column is not propagated to the Iceberg metadata
                    // schema (see the `_ => {}` arm in
                    // `AlterTableIcebergOperations::from_command_list`), so the
                    // dropped field lingers here as `required` with no live
                    // source and every later INSERT fails. The real fix lives
                    // in the DDL hook (sync schema evolution / downgrade the
                    // `required` bit), not here. Deferred for now.
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
        if matched_live != live_columns.len() {
            let missing = live_columns
                .iter()
                .find(|c| !fields.iter().any(|f| f.name == c.name))
                .map(|c| c.name.clone())
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

// ---------------------------------------------------------------------------
// Tests — ColumnMapping position arithmetic
// ---------------------------------------------------------------------------
//
// The assertions are pure position arithmetic (`entries` order, `dest`,
// `src_col`), but building a `ColumnMapping` resolves each column's rule through
// `PgColumnType::from_pg_type`, which calls `pg_sys::get_element_type` (a
// `#[pg_guard]` syscache lookup) to recognize array types. That pulls in
// PostgreSQL backend symbols, so the path cannot link into a host `#[test]`
// binary on Linux (see `docs/testing.md`). These therefore run as `#[pg_test]`
// inside the backend — the `#[pg_test]` body is compiled into the extension
// `.so`, not the host test binary, so the backend symbol never reaches the
// host link step.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use iceberg_lite::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use super::*;

    /// Build an Iceberg schema with `names.len()` required `int` fields,
    /// assigning sequential field ids `1..=n`. The field *names* model the
    /// relation's live (non-dropped) columns in attno order.
    fn int_schema(names: &[&str]) -> IcebergSchema {
        let fields: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                Arc::new(NestedField::required(
                    (i + 1) as i32,
                    *name,
                    Type::Primitive(PrimitiveType::Int),
                ))
            })
            .collect();
        IcebergSchema::builder()
            .with_fields(fields)
            .build()
            .expect("failed to build test iceberg schema")
    }

    /// Build a `LiveColumn` list from `(attno, name)` pairs.
    fn live_cols(cols: &[(i16, &str)]) -> Vec<LiveColumn> {
        cols.iter()
            .map(|(attno, name)| LiveColumn::new(*attno, (*name).to_string()))
            .collect()
    }

    /// Full-width `(oid, typmod)` list of `n` `int4` columns — the relation-side
    /// `TupleDesc` view a real scan supplies. `int4` pairs with the `Int32`
    /// Arrow type the [`int_schema`] `int` fields produce, so every column's
    /// rule resolves cleanly.
    fn int_attr_types(n: usize) -> Vec<(pg_sys::Oid, i32)> {
        vec![(pg_sys::INT4OID, -1); n]
    }

    // --- from_full_schema -------------------------------------------------

    #[pg_test]
    fn from_full_schema_no_dropped_columns_is_identity() {
        let schema = int_schema(&["a", "b", "c"]);
        let plan = ColumnMapping::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "b"), (3, "c")]),
            3,
            &int_attr_types(3),
        )
        .unwrap();

        assert_eq!(plan.entries.len(), 3);
        for (j, entry) in plan.entries.iter().enumerate() {
            assert_eq!(entry.dest, j, "identity dest at entry {j}");
            assert_eq!(entry.src_col, j, "identity src_col at entry {j}");
        }
    }

    #[pg_test]
    fn from_full_schema_with_dropped_column_leaves_gap() {
        let schema = int_schema(&["a", "b", "d"]);
        let plan = ColumnMapping::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "b"), (4, "d")]),
            4,
            &int_attr_types(4),
        )
        .unwrap();

        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![0, 1, 3]);
        let srcs: Vec<usize> = plan.entries.iter().map(|e| e.src_col).collect();
        assert_eq!(srcs, vec![0, 1, 2]);
    }

    #[pg_test]
    fn from_full_schema_iceberg_wider_than_live_columns_resolves_by_name() {
        let schema = int_schema(&["a", "b", "c"]);
        let plan = ColumnMapping::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (3, "c")]),
            3,
            &int_attr_types(3),
        )
        .unwrap();

        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[0].src_col, 0);
        assert_eq!(plan.entries[0].dest, 0);
        assert_eq!(plan.entries[1].src_col, 2);
        assert_eq!(plan.entries[1].dest, 2);
    }

    #[pg_test]
    fn from_full_schema_errors_on_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let err = ColumnMapping::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "z")]),
            2,
            &int_attr_types(2),
        );
        assert!(matches!(err, Err(IcebergError::ColumnNotFound(_))));
    }

    // --- from_projection --------------------------------------------------

    #[pg_test]
    fn from_projection_decouples_source_order_from_scan_destination() {
        let schema = int_schema(&["a", "b", "c", "d", "e"]);
        let pairs = vec![
            ProjectedName::new(2, 1, "b".to_string()),
            ProjectedName::new(5, 0, "e".to_string()),
        ];
        let plan =
            ColumnMapping::from_projection(&schema, &pairs, 2, &int_attr_types(2))
                .unwrap();

        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![1, 0]);
        let sources: Vec<usize> = plan.entries.iter().map(|e| e.src_col).collect();
        assert_eq!(sources, vec![0, 1]);
    }

    #[pg_test]
    fn from_projection_with_dropped_column_uses_attno_minus_one() {
        let schema = int_schema(&["a", "b", "e"]);
        let pairs = vec![
            ProjectedName::new(2, 0, "b".to_string()),
            ProjectedName::new(4, 1, "e".to_string()),
        ];
        let plan =
            ColumnMapping::from_projection(&schema, &pairs, 2, &int_attr_types(2))
                .unwrap();

        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![0, 1]);
    }

    #[pg_test]
    fn from_projection_errors_on_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(1, 0, "does_not_exist".to_string())];
        let err =
            ColumnMapping::from_projection(&schema, &pairs, 2, &int_attr_types(2));
        assert!(matches!(err, Err(IcebergError::ColumnNotFound(_))));
    }

    #[pg_test]
    fn from_projection_errors_on_attno_below_one() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(0, 0, "a".to_string())];
        let err =
            ColumnMapping::from_projection(&schema, &pairs, 2, &int_attr_types(2));
        assert!(matches!(err, Err(IcebergError::InvariantViolated(_))));
    }

    #[pg_test]
    fn from_projection_errors_on_dest_out_of_range() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(2, 5, "b".to_string())];
        let err =
            ColumnMapping::from_projection(&schema, &pairs, 2, &int_attr_types(2));
        assert!(matches!(err, Err(IcebergError::InvariantViolated(_))));
    }
}
