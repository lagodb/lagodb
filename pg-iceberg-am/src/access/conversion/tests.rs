use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use iceberg_lite::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use pg_lakebase_core::tuple::Row;

use super::RowRecordBatchBuilder;
use super::schema::iceberg_schema_to_arrow_schema;

/// Helper function to create an Iceberg schema with given fields
fn create_test_iceberg_schema(fields: Vec<NestedField>) -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(fields.into_iter().map(Arc::new))
        .build()
        .expect("Failed to build test schema")
}

#[test]
fn test_iceberg_schema_to_arrow_schema_primitive_types() {
    let iceberg_schema = create_test_iceberg_schema(vec![
        NestedField::required(1, "bool_col", Type::Primitive(PrimitiveType::Boolean)),
        NestedField::required(2, "int_col", Type::Primitive(PrimitiveType::Int)),
        NestedField::required(3, "long_col", Type::Primitive(PrimitiveType::Long)),
        NestedField::optional(4, "float_col", Type::Primitive(PrimitiveType::Float)),
        NestedField::optional(
            5,
            "double_col",
            Type::Primitive(PrimitiveType::Double),
        ),
        NestedField::required(
            6,
            "string_col",
            Type::Primitive(PrimitiveType::String),
        ),
    ]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    assert_eq!(arrow_schema.fields().len(), 6);

    // Check field names and types
    assert_eq!(arrow_schema.field(0).name(), "bool_col");
    assert_eq!(arrow_schema.field(0).data_type(), &DataType::Boolean);
    assert!(!arrow_schema.field(0).is_nullable()); // required

    assert_eq!(arrow_schema.field(1).name(), "int_col");
    assert_eq!(arrow_schema.field(1).data_type(), &DataType::Int32);

    assert_eq!(arrow_schema.field(2).name(), "long_col");
    assert_eq!(arrow_schema.field(2).data_type(), &DataType::Int64);

    assert_eq!(arrow_schema.field(3).name(), "float_col");
    assert_eq!(arrow_schema.field(3).data_type(), &DataType::Float32);
    assert!(arrow_schema.field(3).is_nullable()); // optional

    assert_eq!(arrow_schema.field(4).name(), "double_col");
    assert_eq!(arrow_schema.field(4).data_type(), &DataType::Float64);

    assert_eq!(arrow_schema.field(5).name(), "string_col");
    assert_eq!(arrow_schema.field(5).data_type(), &DataType::Utf8);
}

#[test]
fn test_iceberg_schema_to_arrow_schema_temporal_types() {
    let iceberg_schema = create_test_iceberg_schema(vec![
        NestedField::required(1, "date_col", Type::Primitive(PrimitiveType::Date)),
        NestedField::required(2, "time_col", Type::Primitive(PrimitiveType::Time)),
        NestedField::required(
            3,
            "timestamp_col",
            Type::Primitive(PrimitiveType::Timestamp),
        ),
        NestedField::required(
            4,
            "timestamptz_col",
            Type::Primitive(PrimitiveType::Timestamptz),
        ),
    ]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    assert_eq!(arrow_schema.field(0).data_type(), &DataType::Date32);
    assert_eq!(
        arrow_schema.field(1).data_type(),
        &DataType::Time64(TimeUnit::Microsecond)
    );
    assert_eq!(
        arrow_schema.field(2).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        arrow_schema.field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
    );
}

#[test]
fn test_iceberg_schema_to_arrow_schema_binary_types() {
    let iceberg_schema = create_test_iceberg_schema(vec![
        NestedField::required(
            1,
            "binary_col",
            Type::Primitive(PrimitiveType::Binary),
        ),
        NestedField::required(
            2,
            "fixed_col",
            Type::Primitive(PrimitiveType::Fixed(16)),
        ),
        NestedField::required(3, "uuid_col", Type::Primitive(PrimitiveType::Uuid)),
    ]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    // pg-iceberg-am delegates the Iceberg → Arrow type table to
    // `iceberg_lite::arrow`, which maps Iceberg `Binary` to Arrow
    // `LargeBinary`. The `ArrowToCell` read path accepts both `Binary` and
    // `LargeBinary` so external producers using the narrow variant are still
    // readable.
    assert_eq!(arrow_schema.field(0).data_type(), &DataType::LargeBinary);
    assert_eq!(
        arrow_schema.field(1).data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        arrow_schema.field(2).data_type(),
        &DataType::FixedSizeBinary(16)
    );
}

#[test]
fn test_iceberg_schema_to_arrow_schema_decimal() {
    let iceberg_schema = create_test_iceberg_schema(vec![NestedField::required(
        1,
        "decimal_col",
        Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        }),
    )]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    assert_eq!(
        arrow_schema.field(0).data_type(),
        &DataType::Decimal128(10, 2)
    );
}

#[test]
fn test_iceberg_schema_to_arrow_schema_list() {
    let iceberg_schema = create_test_iceberg_schema(vec![NestedField::required(
        1,
        "list_col",
        Type::List(iceberg_lite::spec::ListType {
            element_field: NestedField::list_element(
                2,
                Type::Primitive(PrimitiveType::Int),
                true,
            )
            .into(),
        }),
    )]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    match arrow_schema.field(0).data_type() {
        DataType::List(element_field) => {
            assert_eq!(element_field.data_type(), &DataType::Int32);
        }
        _ => panic!("Expected List type"),
    }
}

#[test]
fn test_iceberg_schema_to_arrow_schema_field_ids_in_metadata() {
    let iceberg_schema = create_test_iceberg_schema(vec![
        NestedField::required(42, "col1", Type::Primitive(PrimitiveType::Int)),
        NestedField::required(99, "col2", Type::Primitive(PrimitiveType::String)),
    ]);

    let arrow_schema = iceberg_schema_to_arrow_schema(&iceberg_schema).unwrap();

    // Check field ID metadata
    let col1_meta = arrow_schema.field(0).metadata();
    assert_eq!(
        col1_meta.get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
        Some("42")
    );

    let col2_meta = arrow_schema.field(1).metadata();
    assert_eq!(
        col2_meta.get(PARQUET_FIELD_ID_META_KEY).map(String::as_str),
        Some("99")
    );
}

#[test]
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

#[test]
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
