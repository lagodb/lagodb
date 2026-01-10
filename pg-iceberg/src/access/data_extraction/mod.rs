//! Arrow RecordBatch to PostgreSQL Row extraction.
//!
//! This module provides functionality to extract PostgreSQL Rows and Cells
//! from Arrow RecordBatches read from Iceberg tables.

use arrow_array::{Array, RecordBatch};
use iceberg_lite::spec::{Schema as IcebergSchema, Type};
use pg_tam::data::{Cell, Row};
use pgrx::JsonB;

use crate::error::IcebergResult;

pub mod complex;
pub mod json;
pub mod primitive;

/// Extract a single row from a RecordBatch at the given row index.
pub fn extract_row_from_batch(
    batch: &RecordBatch,
    row_idx: usize,
    schema: &IcebergSchema,
    row: &mut Row,
) -> IcebergResult<()> {
    let fields = schema.as_struct().fields();
    let num_columns = fields.len();

    // Ensure row has correct capacity
    if row.cells.len() < num_columns {
        row.cells.resize_with(num_columns, || None);
    }

    for (col_idx, field) in fields.iter().enumerate() {
        let column = batch.column(col_idx);
        let cell = extract_cell_from_column(column, row_idx, &field.field_type)?;
        row.cells[col_idx] = cell;
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

    match iceberg_type {
        Type::Primitive(p) => primitive::extract_primitive_cell(column, row_idx, p),
        Type::List(_) => complex::extract_list_cell(column, row_idx),
        Type::Struct(_) => {
            let json_value = json::extract_complex_type_as_json(column, row_idx)?;
            Ok(Some(Cell::Composite(JsonB(json_value))))
        }
        Type::Map(_) => {
            let json_value = json::extract_complex_type_as_json(column, row_idx)?;
            Ok(Some(Cell::Json(JsonB(json_value))))
        }
    }
}
