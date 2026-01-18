//! PostgreSQL Row to Arrow RecordBatch conversion.
//!
//! This module provides functionality to convert PostgreSQL Rows (containing Cells)
//! into Arrow RecordBatches for writing to Iceberg tables.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use iceberg_lite::spec::{Schema as IcebergSchema, Type};
use pg_lakehouse_core::data::Row;

use crate::error::{IcebergError, IcebergResult};

pub mod complex;
pub mod primitive;
pub mod schema;

#[cfg(test)]
mod tests;

pub use schema::iceberg_schema_to_arrow_schema;

/// Convert a batch of PostgreSQL Rows into an Arrow RecordBatch.
///
/// # Arguments
/// * `rows` - The rows to convert
/// * `schema` - The Iceberg schema defining the column types
///
/// # Returns
/// An Arrow RecordBatch containing the converted data
pub fn rows_to_record_batch(
    rows: &[Row],
    schema: &IcebergSchema,
) -> IcebergResult<RecordBatch> {
    if rows.is_empty() {
        let arrow_schema = iceberg_schema_to_arrow_schema(schema)?;
        return Ok(RecordBatch::new_empty(Arc::new(arrow_schema)));
    }

    let fields = schema.as_struct().fields();
    let num_columns = fields.len();

    // Build arrays for each column
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_columns);

    for (col_idx, field) in fields.iter().enumerate() {
        let array = build_arrow_array(rows, col_idx, &field.field_type)?;
        arrays.push(array);
    }

    let arrow_schema = iceberg_schema_to_arrow_schema(schema)?;
    RecordBatch::try_new(Arc::new(arrow_schema), arrays).map_err(IcebergError::from)
}

/// Build an Arrow array from a column of Row cells.
fn build_arrow_array(
    rows: &[Row],
    col_idx: usize,
    iceberg_type: &Type,
) -> IcebergResult<ArrayRef> {
    match iceberg_type {
        Type::Primitive(p) => primitive::build_primitive_array(rows, col_idx, p),
        Type::Struct(s) => complex::build_struct_array(rows, col_idx, s),
        Type::List(l) => complex::build_list_array(rows, col_idx, l),
        Type::Map(m) => complex::build_map_array(rows, col_idx, m),
    }
}
