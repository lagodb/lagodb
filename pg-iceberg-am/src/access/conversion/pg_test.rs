use std::sync::Arc;

use iceberg_lite::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use pg_lakebase_core::tuple::Row;

use super::RowRecordBatchBuilder;

// pgrx-tests calls SELECT tests.<function>(); manual schema placement requires
// an explicit schema entity in the generated SQL graph.
#[pgrx::pg_schema]
mod tests {}

/// Helper function to create an Iceberg schema with given fields
fn create_test_iceberg_schema(fields: Vec<NestedField>) -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .expect("Failed to build test schema")
}

#[pgrx::pg_test(schema = "tests")]
fn test_rows_to_record_batch_empty() {
    let iceberg_schema = create_test_iceberg_schema(vec![NestedField::required(
        1,
        "int_col",
        Type::Primitive(PrimitiveType::Int),
    )]);

    let rows: Vec<Row> = vec![];
    let batch = RowRecordBatchBuilder::new(&iceberg_schema)
        .unwrap()
        .build(&rows)
        .unwrap();

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1);
}

#[pgrx::pg_test(schema = "tests")]
fn test_rows_to_record_batch_primitives() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use pg_lakebase_core::tuple::Cell;

    let iceberg_schema = create_test_iceberg_schema(vec![
        NestedField::required(1, "int_col", Type::Primitive(PrimitiveType::Int)),
        NestedField::optional(
            2,
            "string_col",
            Type::Primitive(PrimitiveType::String),
        ),
    ]);

    let mut row1 = Row::with_capacity(2);
    row1.set_cell(0, Some(Cell::I32(42)));
    row1.set_cell(1, Some(Cell::String("hello".to_string())));

    let mut row2 = Row::with_capacity(2);
    row2.set_cell(0, Some(Cell::I32(100)));

    let rows = vec![row1, row2];
    let batch = RowRecordBatchBuilder::new(&iceberg_schema)
        .unwrap()
        .build(&rows)
        .unwrap();

    assert_eq!(batch.num_rows(), 2);

    // Check Int column
    let int_col = batch.column(0).as_primitive::<Int32Type>();
    assert_eq!(int_col.value(0), 42);
    assert_eq!(int_col.value(1), 100);

    // Check String column
    let str_col = batch.column(1).as_string::<i32>();
    assert_eq!(str_col.value(0), "hello");
    assert!(str_col.is_null(1));
}
