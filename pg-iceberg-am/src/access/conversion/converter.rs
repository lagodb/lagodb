//! Schema-bound converter objects between Arrow `RecordBatch`es and
//! PostgreSQL `Row`s.
//!
//! Both directions are wrapped in an object that resolves per-column
//! descriptors once, at the point the schema is known, and then exposes a
//! tight inner loop the hot paths can call repeatedly.
//!
//! - [`RecordBatchRowReader`] for scan: holds the bound `IcebergSchema` and
//!   the cached [`ColumnPlan`]; produces a `Row` from a `(batch, row_idx)`.
//! - [`RowRecordBatchBuilder`] for DML: holds the resolved Arrow schema and
//!   a full-schema [`ColumnPlan`]; produces a `RecordBatch` from a slice of
//!   buffered `Row`s.
//!
//! Both share the per-column dispatch path implemented in
//! [`super::traits`]; the converters are the place those traits get *bound*
//! to a specific schema rather than re-resolved per call.
//!
//! ## Position arithmetic lives here
//!
//! [`ColumnPlan`] is the *single owner* of all "Arrow column ↔ slot
//! position" arithmetic for the scan path (Requirement 8.1). `scan.rs` and
//! `provider.rs` contain no `attno - 1` / `dest` index math: they hand this
//! module the relation's live columns (full-schema path) or a resolved
//! projection (projected path), and `ColumnPlan` turns those into
//! destination slot indices.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use iceberg_lite::spec::{NestedFieldRef, Schema as IcebergSchema};
use pg_lakebase_core::tuple::Row;
use pgrx::pg_sys;

use super::schema::{ValidateSupported, iceberg_schema_to_arrow_schema};
use super::traits::{ArrowToCell, RowsToArrow};
use crate::access::projection::ProjectedName;
use crate::error::{IcebergError, IcebergResult};

/// One live (non-dropped) PG column of the scan relation: its 1-based
/// attribute number and its column name (which is also the Iceberg field
/// name, since the Iceberg schema is built from PG column names).
///
/// Carried by [`RelationShape`](crate::access::scan) into the full-schema
/// [`ColumnPlan`], where the `name` resolves the Iceberg field (tolerating an
/// Iceberg schema wider than the live PG columns — see
/// [`ColumnPlan::from_full_schema`]) and `attno` becomes `dest = attno - 1`.
#[derive(Debug, Clone)]
pub struct LiveColumn {
    /// 1-based PG attribute number of the live column.
    pub attno: pg_sys::AttrNumber,
    /// Column name (== Iceberg field name).
    pub name: String,
}

impl LiveColumn {
    pub fn new(attno: pg_sys::AttrNumber, name: String) -> Self {
        Self { attno, name }
    }
}

/// One selected column: the Iceberg field to decode, the index of the Arrow
/// batch column it is read from, and the destination slot index it must be
/// written to.
#[derive(Clone)]
struct ProjectedColumn {
    /// Iceberg field to decode for this column.
    field: NestedFieldRef,
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
    /// - projection (`from_projection`): `select(names)` preserves the passed
    ///   name order, so `src_col` equals the entry index `j`.
    src_col: usize,
    /// Destination cell index in the PG `Row`, equal to `attno - 1`. Encodes
    /// the dropped-column gap directly, so both scan paths get correct
    /// alignment.
    dest: usize,
}

/// Projection-aware column plan: the single owner of position arithmetic.
///
/// Each entry decodes one selected column: it reads the Arrow batch column at
/// `entry.src_col` and writes it to slot `entry.dest`. `slot_width` is the
/// full PG tuple width (`natts`) of the scan relation, counting
/// dropped-column positions; the output `Row` is sized to it so
/// projected-away / dropped positions stay SQL NULL.
///
/// Entry order matches the live-column / projection order, while `src_col`
/// locates each entry's column in the produced batch — they coincide
/// (`src_col == j`) for the projection path and for a clean full-table scan,
/// but diverge for a full-table scan whose Iceberg schema is wider than the
/// live PG columns (a dropped column that still lingers in the Iceberg
/// metadata schema — see [`ColumnPlan::from_full_schema`]).
#[derive(Clone)]
struct ColumnPlan {
    entries: Arc<[ProjectedColumn]>,
    slot_width: usize,
}

impl ColumnPlan {
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
    /// Errors — producing no `ColumnPlan` — when a live column name does not
    /// resolve to an Iceberg field (Requirement 3.6 / 10.4, e.g. an
    /// `ALTER TABLE RENAME COLUMN` attname/field desync), an `attno < 1`, or a
    /// computed `dest >= slot_width` (Requirement 3.7).
    fn from_full_schema(
        schema: &IcebergSchema,
        live_columns: &[LiveColumn],
        slot_width: usize,
    ) -> IcebergResult<Self> {
        let struct_ty = schema.as_struct();
        let mut entries = Vec::with_capacity(live_columns.len());
        for col in live_columns {
            // Resolve the live PG column to its Iceberg field by name. A miss
            // means the name desynced from the Iceberg schema (out of scope;
            // fail loud rather than silently mis-align — Requirement 3.6).
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
            entries.push(ProjectedColumn {
                field: field.clone(),
                src_col,
                dest,
            });
        }
        Ok(Self {
            entries: entries.into(),
            slot_width,
        })
    }

    /// Projected plan keyed by resolved `(attno, name)` pairs.
    ///
    /// Resolves each [`ProjectedName::name`] to its Iceberg `NestedField`,
    /// sets `dest = attno - 1`, and keeps `entries` in the passed
    /// (Arrow-batch) order — `select(names)` preserves that order into the
    /// batch, so `src_col == j` (the entry index). Errors — producing no
    /// `ColumnPlan` — when a name does not resolve, an `attno < 1`, or a
    /// computed `dest >= slot_width`.
    fn from_projection(
        schema: &IcebergSchema,
        pairs: &[ProjectedName],
        slot_width: usize,
    ) -> IcebergResult<Self> {
        let mut entries = Vec::with_capacity(pairs.len());
        for (j, pair) in pairs.iter().enumerate() {
            // Hard error if the projected name does not resolve to a direct
            // field of the Iceberg schema (Requirement 3.6 / 10.4 — a column
            // rename desync surfaces here rather than returning wrong data).
            let field = schema
                .field_by_name(&pair.name)
                .ok_or_else(|| IcebergError::ColumnNotFound(pair.name.clone()))?;
            let dest = Self::dest_from_attno(pair.attno, slot_width)?;
            entries.push(ProjectedColumn {
                field: field.clone(),
                src_col: j,
                dest,
            });
        }
        Ok(Self {
            entries: entries.into(),
            slot_width,
        })
    }

    /// Full-schema identity plan for the DML write direction.
    ///
    /// One entry per Iceberg field in schema order with `src_col = dest = j`
    /// (the degenerate identity case) and `slot_width = field count`. The DML
    /// builder always writes every column and reads each row positionally by
    /// Iceberg field index, so this preserves the pre-projection behavior
    /// exactly — the projection path only affects the scan/read direction.
    fn from_schema_identity(schema: &IcebergSchema) -> Self {
        let fields = schema.as_struct().fields();
        let entries: Vec<ProjectedColumn> = fields
            .iter()
            .enumerate()
            .map(|(j, field)| ProjectedColumn {
                field: field.clone(),
                src_col: j,
                dest: j,
            })
            .collect();
        Self {
            entries: entries.into(),
            slot_width: fields.len(),
        }
    }

    /// Compute and bounds-check `dest = attno - 1` against `slot_width`.
    ///
    /// This is the only place `attno - 1` is computed for the scan path
    /// (Requirement 8.1). A non-positive `attno` or an out-of-range `dest`
    /// is an AM-internal contract violation (the provider only ever passes
    /// live user columns whose `attno - 1 < natts == slot_width`).
    fn dest_from_attno(
        attno: pg_sys::AttrNumber,
        slot_width: usize,
    ) -> IcebergResult<usize> {
        if attno < 1 {
            return Err(IcebergError::InvariantViolated(
                "ColumnPlan: projected column attno must be >= 1",
            ));
        }
        let dest = (attno as usize) - 1;
        if dest >= slot_width {
            return Err(IcebergError::InvariantViolated(
                "ColumnPlan: computed dest is outside the slot width",
            ));
        }
        Ok(dest)
    }
}

// ---------------------------------------------------------------------------
// Arrow -> Row
// ---------------------------------------------------------------------------

/// Reads rows out of `RecordBatch`es produced by an Iceberg scan.
///
/// Constructed once per scan from the bound Iceberg schema plus the position
/// mapping (full-schema or projected) and reused for every batch / row. The
/// bound schema is also exposed so callers that need it for adjacent work
/// (e.g. translating PostgreSQL `ScanKey`s into Iceberg `Predicate`s) do not
/// need to keep a second reference.
pub struct RecordBatchRowReader {
    schema: Arc<IcebergSchema>,
    plan: ColumnPlan,
}

impl RecordBatchRowReader {
    /// Select-all reader (full-schema plan).
    ///
    /// `live_columns` are the relation's live (non-dropped) columns —
    /// `(attno, name)` in PG tuple order — and `slot_width` is the relation's
    /// full `natts`. Both come from the relation's `TupleDesc` in `scan.rs`;
    /// this module owns the name→Iceberg-field resolution and the
    /// `dest = attno - 1` arithmetic, the caller owns reading the descriptor.
    pub fn new(
        schema: Arc<IcebergSchema>,
        live_columns: &[LiveColumn],
        slot_width: usize,
    ) -> IcebergResult<Self> {
        // Same boundary check the DML side runs in `RowRecordBatchBuilder::new`.
        // Without this, an externally-defined Iceberg table whose schema
        // contains shapes the per-column dispatch can't materialize (Struct,
        // Map, oversized `Fixed(len > i32::MAX)`, unsupported list-element
        // types, ...) would surface as an opaque `UnsupportedColumnType` /
        // `ArrowTypeMismatch` deep inside the scan's per-row `extract` loop.
        // Failing fast at scan construction makes that the same loud error
        // both directions.
        schema.validate_supported()?;
        let plan = ColumnPlan::from_full_schema(&schema, live_columns, slot_width)?;
        Ok(Self { schema, plan })
    }

    /// Projected reader (projected plan).
    ///
    /// `pairs` are the resolved `(attno, name)` columns in scan order
    /// (`== iceberg select order`); `slot_width` is the relation's full
    /// `natts`.
    pub fn with_projection(
        schema: Arc<IcebergSchema>,
        pairs: &[ProjectedName],
        slot_width: usize,
    ) -> IcebergResult<Self> {
        schema.validate_supported()?;
        let plan = ColumnPlan::from_projection(&schema, pairs, slot_width)?;
        Ok(Self { schema, plan })
    }

    /// Bound Iceberg schema. Cheap (no allocation): exposes the inner `Arc`'s
    /// referent.
    pub fn schema(&self) -> &IcebergSchema {
        self.schema.as_ref()
    }

    /// Materialize the row at `row_idx` of `batch` into `row` (Algorithm 3).
    ///
    /// Sizes `row` to the full slot width on the first call (so dropped /
    /// projected-away positions stay SQL NULL), then writes each entry's
    /// source Arrow column `entries[k].src_col` to its destination slot
    /// `entries[k].dest == attno-1`. On extraction failure it stops and
    /// returns the decode error without finishing the row.
    pub fn read_row(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        row: &mut Row,
    ) -> IcebergResult<()> {
        row.ensure_len(self.plan.slot_width);
        for entry in self.plan.entries.iter() {
            let column = batch.column(entry.src_col);
            let cell = if column.is_null(row_idx) {
                None
            } else {
                entry.field.field_type.extract(column.as_ref(), row_idx)?
            };
            row.set_cell(entry.dest, cell);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Row -> Arrow
// ---------------------------------------------------------------------------

/// Builds Arrow `RecordBatch`es out of in-memory `Row`s.
///
/// Holds the resolved Arrow schema (so every `build` call hands the Parquet
/// writer a stable `Arc`) and a full-schema identity [`ColumnPlan`] (so every
/// `build` call is a tight loop over already-resolved descriptors). The DML
/// side always writes all columns, so it stays on the full-schema identity
/// plan regardless of any projection on the scan side.
pub struct RowRecordBatchBuilder {
    arrow_schema: Arc<arrow_schema::Schema>,
    plan: ColumnPlan,
}

impl RowRecordBatchBuilder {
    pub fn new(schema: &IcebergSchema) -> IcebergResult<Self> {
        // Fail fast on Struct/Map (and other unsupported shapes) at session
        // begin rather than on the first batch build. The DML hot path's
        // per-column dispatch in `traits.rs` would otherwise surface this as
        // an opaque `UnsupportedColumnType` mid-INSERT.
        schema.validate_supported()?;
        let arrow_schema = Arc::new(iceberg_schema_to_arrow_schema(schema)?);
        let plan = ColumnPlan::from_schema_identity(schema);
        Ok(Self { arrow_schema, plan })
    }

    /// Encode `rows` as a single `RecordBatch`.
    ///
    /// Returns an empty batch (with the bound schema) when `rows` is empty,
    /// matching the legacy free-function semantics.
    pub fn build(&self, rows: &[Row]) -> IcebergResult<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.arrow_schema.clone()));
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.entries.len());
        // The DML plan is the identity full-schema plan: `entries[col_idx]`
        // describes Iceberg field `col_idx`, with `src_col == col_idx` and
        // `dest == col_idx`, so the row column index and the output array
        // index coincide — identical to the pre-projection positional build.
        for (col_idx, entry) in self.plan.entries.iter().enumerate() {
            debug_assert_eq!(entry.src_col, col_idx);
            arrays.push(entry.field.field_type.build(rows, col_idx)?);
        }
        RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(IcebergError::from)
    }
}

// ---------------------------------------------------------------------------
// Tests — ColumnPlan + read_row (Properties 1, 2, 3)
//
// Split by execution environment (see `docs/testing.md`):
//   * `column_plan_tests` (host `#[cfg(test)]`): pure `ColumnPlan` position
//     arithmetic — `from_full_schema` / `from_projection` — which never touch
//     a PG backend.
//   * `column_plan_pg_test` (`#[pgrx::pg_test]`): anything that calls
//     `RecordBatchRowReader::read_row`. `read_row` dispatches through
//     `field_type.extract(..)`, whose `Decimal` arm runs
//     `Decimal128NumericCodec::decode` -> `AnyNumeric` -> `numeric_recv`. The
//     linker retains that arm even for an int-only fixture, so the whole read
//     path links against PG backend symbols and cannot run in a host
//     `#[test]`.
//   * `column_plan_fixtures`: builders shared by both modules.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "pg_test"))]
mod column_plan_fixtures {
    use std::sync::Arc;

    use iceberg_lite::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Type,
    };

    use super::LiveColumn;

    /// Build an Iceberg schema with `names.len()` required `int` fields,
    /// assigning sequential field ids `1..=n`. The field *names* model the
    /// relation's live (non-dropped) columns in attno order.
    pub(super) fn int_schema(names: &[&str]) -> IcebergSchema {
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
    pub(super) fn live_cols(cols: &[(i16, &str)]) -> Vec<LiveColumn> {
        cols.iter()
            .map(|(attno, name)| LiveColumn::new(*attno, (*name).to_string()))
            .collect()
    }
}

#[cfg(test)]
mod column_plan_tests {
    //! Host (`#[cfg(test)]`) tests for the pure position arithmetic in
    //! [`ColumnPlan`]: `entries` order, `dest`, and
    //! `src_col`, with and without dropped columns. No PG backend.

    use super::column_plan_fixtures::{int_schema, live_cols};
    use super::*;

    // --- from_full_schema -------------------------------------------------

    #[test]
    fn from_full_schema_no_dropped_columns_is_identity() {
        let schema = int_schema(&["a", "b", "c"]);
        let plan = ColumnPlan::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "b"), (3, "c")]),
            3,
        )
        .unwrap();

        assert_eq!(plan.slot_width, 3);
        assert_eq!(plan.entries.len(), 3);
        for (j, entry) in plan.entries.iter().enumerate() {
            assert_eq!(entry.dest, j, "identity dest at entry {j}");
            assert_eq!(entry.src_col, j, "identity src_col at entry {j}");
        }
    }

    #[test]
    fn from_full_schema_with_dropped_column_leaves_gap() {
        let schema = int_schema(&["a", "b", "d"]);
        let plan = ColumnPlan::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "b"), (4, "d")]),
            4,
        )
        .unwrap();

        assert_eq!(plan.slot_width, 4);
        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![0, 1, 3]);
        let srcs: Vec<usize> = plan.entries.iter().map(|e| e.src_col).collect();
        assert_eq!(srcs, vec![0, 1, 2]);
    }

    #[test]
    fn from_full_schema_iceberg_wider_than_live_columns_resolves_by_name() {
        let schema = int_schema(&["a", "b", "c"]);
        let plan = ColumnPlan::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (3, "c")]),
            3,
        )
        .unwrap();

        assert_eq!(plan.slot_width, 3);
        assert_eq!(plan.entries.len(), 2);
        assert_eq!(plan.entries[0].src_col, 0);
        assert_eq!(plan.entries[0].dest, 0);
        assert_eq!(plan.entries[1].src_col, 2);
        assert_eq!(plan.entries[1].dest, 2);
    }

    #[test]
    fn from_full_schema_errors_on_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let err = ColumnPlan::from_full_schema(
            &schema,
            &live_cols(&[(1, "a"), (2, "z")]),
            2,
        );
        assert!(matches!(err, Err(IcebergError::ColumnNotFound(_))));
    }

    // --- from_projection --------------------------------------------------

    #[test]
    fn from_projection_orders_entries_by_passed_order() {
        let schema = int_schema(&["a", "b", "c", "d", "e"]);
        let pairs = vec![
            ProjectedName::new(5, "e".to_string()),
            ProjectedName::new(2, "b".to_string()),
        ];
        let plan = ColumnPlan::from_projection(&schema, &pairs, 5).unwrap();

        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![4, 1]);
    }

    #[test]
    fn from_projection_with_dropped_column_uses_attno_minus_one() {
        let schema = int_schema(&["a", "b", "e"]);
        let pairs = vec![
            ProjectedName::new(2, "b".to_string()),
            ProjectedName::new(4, "e".to_string()),
        ];
        let plan = ColumnPlan::from_projection(&schema, &pairs, 4).unwrap();

        let dests: Vec<usize> = plan.entries.iter().map(|e| e.dest).collect();
        assert_eq!(dests, vec![1, 3]);
    }

    #[test]
    fn from_projection_errors_on_unresolved_name() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(1, "does_not_exist".to_string())];
        let err = ColumnPlan::from_projection(&schema, &pairs, 2);
        assert!(matches!(err, Err(IcebergError::ColumnNotFound(_))));
    }

    #[test]
    fn from_projection_errors_on_attno_below_one() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(0, "a".to_string())];
        let err = ColumnPlan::from_projection(&schema, &pairs, 2);
        assert!(matches!(err, Err(IcebergError::InvariantViolated(_))));
    }

    #[test]
    fn from_projection_errors_on_dest_out_of_range() {
        let schema = int_schema(&["a", "b"]);
        let pairs = vec![ProjectedName::new(5, "b".to_string())];
        let err = ColumnPlan::from_projection(&schema, &pairs, 2);
        assert!(matches!(err, Err(IcebergError::InvariantViolated(_))));
    }
}

#[cfg(feature = "pg_test")]
mod column_plan_pg_test {
    //! Backend (`#[pgrx::pg_test]`) tests for [`RecordBatchRowReader::read_row`]
    //! (Properties 1, 2, 3). `read_row` links against the per-`Type`
    //! `extract` dispatch whose `Decimal` arm reaches `numeric_recv`, so these
    //! must run inside PostgreSQL even though every fixture is int-only.

    #[pgrx::pg_schema]
    mod tests {}

    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use pg_lakebase_core::tuple::Cell;

    use super::column_plan_fixtures::{int_schema, live_cols};
    use super::*;

    /// Build a one-row Arrow `RecordBatch` whose columns (in order) carry the
    /// given `i32` values under the given names.
    fn int_batch(cols: &[(&str, i32)]) -> RecordBatch {
        let fields: Vec<ArrowField> = cols
            .iter()
            .map(|(name, _)| ArrowField::new(*name, DataType::Int32, false))
            .collect();
        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|(_, v)| Arc::new(Int32Array::from(vec![*v])) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays)
            .expect("failed to build test record batch")
    }

    /// Extract the `i32` cell at `slot`, or `None` if the slot is SQL NULL.
    fn cell_i32(row: &Row, slot: usize) -> Option<i32> {
        match row.get(slot).and_then(|c| c.as_ref()) {
            Some(Cell::I32(v)) => Some(*v),
            Some(other) => panic!("expected I32 at slot {slot}, got {other:?}"),
            None => None,
        }
    }

    fn proptest_config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    // --- read_row ---------------------------------------------------------

    #[pgrx::pg_test(schema = "tests")]
    fn read_row_full_schema_writes_each_column_to_its_slot() {
        let schema = Arc::new(int_schema(&["a", "b", "c"]));
        let reader = RecordBatchRowReader::new(
            schema,
            &live_cols(&[(1, "a"), (2, "b"), (3, "c")]),
            3,
        )
        .unwrap();
        let batch = int_batch(&[("a", 10), ("b", 20), ("c", 30)]);

        let mut row = Row::with_capacity(3);
        reader.read_row(&batch, 0, &mut row).unwrap();

        assert_eq!(cell_i32(&row, 0), Some(10));
        assert_eq!(cell_i32(&row, 1), Some(20));
        assert_eq!(cell_i32(&row, 2), Some(30));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn read_row_full_schema_dropped_column_slot_stays_null() {
        let schema = Arc::new(int_schema(&["a", "b", "d"]));
        let reader = RecordBatchRowReader::new(
            schema,
            &live_cols(&[(1, "a"), (2, "b"), (4, "d")]),
            4,
        )
        .unwrap();
        let batch = int_batch(&[("a", 10), ("b", 20), ("d", 40)]);

        let mut row = Row::with_capacity(4);
        reader.read_row(&batch, 0, &mut row).unwrap();

        assert_eq!(cell_i32(&row, 0), Some(10));
        assert_eq!(cell_i32(&row, 1), Some(20));
        assert_eq!(cell_i32(&row, 2), None, "dropped-column slot must be NULL");
        assert_eq!(cell_i32(&row, 3), Some(40));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn read_row_full_schema_iceberg_wider_than_live_columns() {
        let schema = Arc::new(int_schema(&["a", "b", "c"]));
        let reader =
            RecordBatchRowReader::new(schema, &live_cols(&[(1, "a"), (3, "c")]), 3)
                .unwrap();
        let batch = int_batch(&[("a", 10), ("b", 20), ("c", 30)]);

        let mut row = Row::with_capacity(3);
        reader.read_row(&batch, 0, &mut row).unwrap();

        assert_eq!(cell_i32(&row, 0), Some(10), "a -> slot 0");
        assert_eq!(cell_i32(&row, 1), None, "dropped b slot stays NULL");
        assert_eq!(cell_i32(&row, 2), Some(30), "c read from src 2 -> slot 2");
    }

    #[pgrx::pg_test(schema = "tests")]
    fn read_row_projected_writes_selected_and_nulls_rest() {
        let schema = Arc::new(int_schema(&["a", "b", "c", "d", "e"]));
        let pairs = vec![
            ProjectedName::new(2, "b".to_string()),
            ProjectedName::new(5, "e".to_string()),
        ];
        let reader =
            RecordBatchRowReader::with_projection(schema, &pairs, 5).unwrap();
        let batch = int_batch(&[("b", 20), ("e", 50)]);

        let mut row = Row::with_capacity(5);
        reader.read_row(&batch, 0, &mut row).unwrap();

        assert_eq!(cell_i32(&row, 0), None);
        assert_eq!(cell_i32(&row, 1), Some(20));
        assert_eq!(cell_i32(&row, 2), None);
        assert_eq!(cell_i32(&row, 3), None);
        assert_eq!(cell_i32(&row, 4), Some(50));
    }

    #[pgrx::pg_test(schema = "tests")]
    fn read_row_drained_reuse_keeps_non_selected_null() {
        let schema = Arc::new(int_schema(&["a", "b", "c"]));
        let pairs = vec![ProjectedName::new(2, "b".to_string())];
        let reader =
            RecordBatchRowReader::with_projection(schema, &pairs, 3).unwrap();

        let mut row = Row::with_capacity(3);

        reader
            .read_row(&int_batch(&[("b", 99)]), 0, &mut row)
            .unwrap();
        assert_eq!(cell_i32(&row, 1), Some(99));
        for i in 0..3 {
            let _ = row.take_cell(i);
        }

        reader
            .read_row(&int_batch(&[("b", 7)]), 0, &mut row)
            .unwrap();
        assert_eq!(cell_i32(&row, 0), None);
        assert_eq!(cell_i32(&row, 1), Some(7));
        assert_eq!(cell_i32(&row, 2), None);
    }

    /// A live-relation fixture: `live_attnos` are the 1-based attnos of the
    /// non-dropped columns (ascending), `natts` is the full tuple width.
    #[derive(Debug, Clone)]
    struct RelFixture {
        live_attnos: Vec<i32>,
        natts: usize,
    }

    /// Generate a relation with random width and random dropped-column
    /// positions, plus the live-attno list it induces.
    fn rel_fixture() -> impl Strategy<Value = RelFixture> {
        (1usize..=8).prop_flat_map(|natts| {
            proptest::collection::vec(any::<bool>(), natts).prop_map(
                move |mut keep| {
                    if !keep.iter().any(|&k| k) {
                        keep[0] = true;
                    }
                    let live_attnos: Vec<i32> = keep
                        .iter()
                        .enumerate()
                        .filter_map(|(i, &k)| k.then_some((i + 1) as i32))
                        .collect();
                    RelFixture { live_attnos, natts }
                },
            )
        })
    }

    /// (Projection-position correctness): for any relation and any
    /// referenced-attno subset, every selected column's value lands at slot
    /// `attno-1` and every other slot is SQL NULL.
    #[pgrx::pg_test(schema = "tests")]
    fn prop1_projection_position_correctness() {
        let mut runner = TestRunner::new(proptest_config());
        runner
            .run(&(rel_fixture(), any::<u64>()), |(fixture, subset_seed)| {
                let mut subset: Vec<i32> = fixture
                    .live_attnos
                    .iter()
                    .copied()
                    .enumerate()
                    .filter(|(i, _)| (subset_seed >> (i % 64)) & 1 == 1)
                    .map(|(_, a)| a)
                    .collect();
                if subset.is_empty() {
                    subset.push(fixture.live_attnos[0]);
                }

                let live_names: Vec<String> = fixture
                    .live_attnos
                    .iter()
                    .map(|a| format!("c{a}"))
                    .collect();
                let name_refs: Vec<&str> =
                    live_names.iter().map(String::as_str).collect();
                let schema = Arc::new(int_schema(&name_refs));

                let pairs: Vec<ProjectedName> = subset
                    .iter()
                    .map(|&a| ProjectedName::new(a as i16, format!("c{a}")))
                    .collect();
                let reader = RecordBatchRowReader::with_projection(
                    schema,
                    &pairs,
                    fixture.natts,
                )
                .unwrap();

                let owned_names: Vec<String> =
                    subset.iter().map(|a| format!("c{a}")).collect();
                let batch_cols: Vec<(&str, i32)> = owned_names
                    .iter()
                    .zip(subset.iter())
                    .map(|(n, &a)| (n.as_str(), a))
                    .collect();
                let batch = int_batch(&batch_cols);

                let mut row = Row::with_capacity(fixture.natts);
                reader.read_row(&batch, 0, &mut row).unwrap();

                for slot in 0..fixture.natts {
                    let attno = (slot + 1) as i32;
                    if subset.contains(&attno) {
                        prop_assert_eq!(cell_i32(&row, slot), Some(attno));
                    } else {
                        prop_assert_eq!(cell_i32(&row, slot), None);
                    }
                }
                Ok(())
            })
            .expect("projection-position correctness property failed");
    }

    /// (Full-table equivalence): select-all over a relation with no
    /// dropped columns yields `dest == j == attno-1` for every entry — the
    /// degenerate identity case a positional reader produces.
    #[pgrx::pg_test(schema = "tests")]
    fn prop_full_table_no_dropped_is_positional_identity() {
        let mut runner = TestRunner::new(proptest_config());
        runner
            .run(&(1usize..=8), |natts| {
                let names: Vec<String> =
                    (1..=natts).map(|a| format!("c{a}")).collect();
                let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
                let schema = Arc::new(int_schema(&name_refs));
                let live_columns: Vec<LiveColumn> = (1..=natts)
                    .map(|a| LiveColumn::new(a as i16, format!("c{a}")))
                    .collect();

                let reader =
                    RecordBatchRowReader::new(schema, &live_columns, natts).unwrap();

                for (j, entry) in reader.plan.entries.iter().enumerate() {
                    prop_assert_eq!(entry.dest, j);
                    prop_assert_eq!(entry.src_col, j);
                }
                prop_assert_eq!(reader.plan.slot_width, natts);

                let cols: Vec<(&str, i32)> = names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| (n.as_str(), (i + 1) as i32))
                    .collect();
                let batch = int_batch(&cols);
                let mut row = Row::with_capacity(natts);
                reader.read_row(&batch, 0, &mut row).unwrap();
                for slot in 0..natts {
                    prop_assert_eq!(cell_i32(&row, slot), Some((slot + 1) as i32));
                }
                Ok(())
            })
            .expect("full-table positional-identity property failed");
    }
}
