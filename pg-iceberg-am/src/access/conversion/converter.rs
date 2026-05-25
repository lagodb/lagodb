//! Schema-bound converter objects between Arrow `RecordBatch`es and
//! PostgreSQL `Row`s.
//!
//! Both directions are wrapped in an object that resolves per-column
//! descriptors once, at the point the schema is known, and then exposes a
//! tight inner loop the hot paths can call repeatedly.
//!
//! - [`RecordBatchRowReader`] for scan: holds the bound `IcebergSchema` and
//!   the cached column plan; produces a `Row` from a `(batch, row_idx)`.
//! - [`RowRecordBatchBuilder`] for DML: holds the resolved Arrow schema and
//!   the same cached column plan; produces a `RecordBatch` from a slice of
//!   buffered `Row`s.
//!
//! Both share the per-column dispatch path implemented in
//! [`super::traits`]; the converters are the place those traits get *bound*
//! to a specific schema rather than re-resolved per call.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use iceberg_lite::spec::{NestedFieldRef, Schema as IcebergSchema};
use pg_lakebase_core::tuple::Row;

use super::schema::{ValidateSupported, iceberg_schema_to_arrow_schema};
use super::traits::{ArrowToCell, RowsToArrow};
use crate::error::{IcebergError, IcebergResult};

/// Per-column field references resolved once from the schema.
///
/// Cached as an `Arc<[NestedFieldRef]>` so the hot loop iterates over a flat
/// slice without re-walking `Schema -> StructType -> fields()` on every call.
#[derive(Clone)]
struct ColumnPlan {
    fields: Arc<[NestedFieldRef]>,
}

impl ColumnPlan {
    fn from_schema(schema: &IcebergSchema) -> Self {
        let fields: Arc<[NestedFieldRef]> = schema.as_struct().fields().into();
        Self { fields }
    }

    fn len(&self) -> usize {
        self.fields.len()
    }

    fn fields(&self) -> &[NestedFieldRef] {
        &self.fields
    }
}

// ---------------------------------------------------------------------------
// Arrow -> Row
// ---------------------------------------------------------------------------

/// Reads rows out of `RecordBatch`es produced by an Iceberg scan.
///
/// Constructed once per scan from the bound Iceberg schema and reused for
/// every batch / row. The bound schema is also exposed so callers that need
/// it for adjacent work (e.g. translating PostgreSQL `ScanKey`s into Iceberg
/// `Predicate`s) do not need to keep a second reference.
pub struct RecordBatchRowReader {
    schema: Arc<IcebergSchema>,
    plan: ColumnPlan,
}

impl RecordBatchRowReader {
    pub fn new(schema: Arc<IcebergSchema>) -> IcebergResult<Self> {
        // Same boundary check the DML side runs in `RowRecordBatchBuilder::new`.
        // Without this, an externally-defined Iceberg table whose schema
        // contains shapes the per-column dispatch can't materialize (Struct,
        // Map, oversized `Fixed(len > i32::MAX)`, unsupported list-element
        // types, ...) would surface as an opaque `UnsupportedColumnType` /
        // `ArrowTypeMismatch` deep inside the scan's per-row `extract` loop.
        // Failing fast at scan construction makes that the same loud error
        // both directions.
        schema.validate_supported()?;
        let plan = ColumnPlan::from_schema(&schema);
        Ok(Self { schema, plan })
    }

    /// Bound Iceberg schema. Cheap (no allocation): exposes the inner `Arc`'s
    /// referent.
    pub fn schema(&self) -> &IcebergSchema {
        self.schema.as_ref()
    }

    /// Materialize the row at `row_idx` of `batch` into `row`.
    ///
    /// Resizes `row` to match the schema width on the first call so the scan
    /// hot path never reallocates after the first batch.
    pub fn read_row(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        row: &mut Row,
    ) -> IcebergResult<()> {
        row.ensure_len(self.plan.len());
        for (col_idx, field) in self.plan.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let cell = if column.is_null(row_idx) {
                None
            } else {
                field.field_type.extract(column.as_ref(), row_idx)?
            };
            row.set_cell(col_idx, cell);
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
/// writer a stable `Arc`) and the per-column field references (so every
/// `build` call is a tight loop over already-resolved descriptors).
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
        let plan = ColumnPlan::from_schema(schema);
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

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.len());
        for (col_idx, field) in self.plan.fields().iter().enumerate() {
            arrays.push(field.field_type.build(rows, col_idx)?);
        }
        RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(IcebergError::from)
    }
}
