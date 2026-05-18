//! Arrow RecordBatch <=> PostgreSQL Row conversion.
//!
//! This module provides functionality to:
//! 1. Extract PostgreSQL Rows and Cells from Arrow RecordBatches (Extraction)
//! 2. Convert PostgreSQL Rows into Arrow RecordBatches (Conversion)

use arrow_array::{Array, ArrayRef, RecordBatch};
use iceberg_lite::spec::{Schema as IcebergSchema, Type};
use pg_lakebase_core::tuple::{Cell, Row};
use std::sync::Arc;

use crate::access::traits::{ArrowToCell, RowsToArrow};
use crate::error::{IcebergError, IcebergResult};

pub mod complex;
pub mod primitive;
pub mod schema;

#[cfg(test)]
mod tests;

pub use schema::iceberg_schema_to_arrow_schema;

pub use pg_lakebase_core::tuple::{PG_EPOCH_DAYS_DIFF, PG_EPOCH_USECS_DIFF};

// --- Extraction (Arrow -> Row) ---

/// Extract a single row from a RecordBatch at the given row index.
pub fn extract_row_from_batch(
    batch: &RecordBatch,
    row_idx: usize,
    schema: &IcebergSchema,
    row: &mut Row,
) -> IcebergResult<()> {
    let fields = schema.as_struct().fields();
    let num_columns = fields.len();

    // Ensure row has enough slots without reallocating on the scan hot path
    // after the first batch.
    row.ensure_len(num_columns);

    for (col_idx, field) in fields.iter().enumerate() {
        let column = batch.column(col_idx);
        let cell = extract_cell_from_column(column, row_idx, &field.field_type)?;
        row.set_cell(col_idx, cell);
    }

    Ok(())
}

/// Extract a single cell value from an Arrow column at the given row index.
pub fn extract_cell_from_column(
    column: &dyn Array,
    row_idx: usize,
    iceberg_type: &Type,
) -> IcebergResult<Option<Cell>> {
    if column.is_null(row_idx) {
        return Ok(None);
    }

    iceberg_type.extract(column, row_idx)
}

// --- Conversion (Row -> Arrow) ---
#[allow(dead_code)]
pub fn rows_to_record_batch(
    rows: &[Row],
    schema: &IcebergSchema,
) -> IcebergResult<RecordBatch> {
    let arrow_schema = Arc::new(iceberg_schema_to_arrow_schema(schema)?);
    rows_to_record_batch_with_schema(rows, schema, arrow_schema)
}

/// Convert a batch of PostgreSQL Rows into an Arrow RecordBatch using a pre-converted Arrow schema.
pub fn rows_to_record_batch_with_schema(
    rows: &[Row],
    iceberg_schema: &IcebergSchema,
    arrow_schema: Arc<arrow_schema::Schema>,
) -> IcebergResult<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    let fields = iceberg_schema.as_struct().fields();
    let num_columns = fields.len();

    // Build arrays for each column
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_columns);

    for (col_idx, field) in fields.iter().enumerate() {
        let array = build_arrow_array(rows, col_idx, &field.field_type)?;
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema, arrays).map_err(IcebergError::from)
}

/// Build an Arrow array from a column of Row cells.
fn build_arrow_array(
    rows: &[Row],
    col_idx: usize,
    iceberg_type: &Type,
) -> IcebergResult<ArrayRef> {
    iceberg_type.build(rows, col_idx)
}
