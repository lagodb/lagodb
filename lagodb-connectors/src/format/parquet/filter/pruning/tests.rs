use super::*;

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{Array, Int32Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

fn row_group_file() -> ParquetRecordBatchReaderBuilder<Bytes> {
    let schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from_iter_values(0..12))],
    )
    .expect("test batch is valid");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties))
        .expect("test writer can be created");
    writer.write(&batch).expect("test batch can be written");
    writer.close().expect("test file can be closed");

    ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output))
        .expect("test file can be opened")
}

fn comparison_predicate(
    operator: ComparisonOperator,
    value: i32,
) -> ParquetBoundPredicate {
    ParquetBoundPredicate::new(BoundNode::Comparison {
        operator,
        column: PlannedColumn {
            attno: 1,
            name: "id".into(),
        },
        value: BoundValue::I32(value),
    })
}

#[test]
fn non_primitive_null_filter_falls_back_to_exact_filter() {
    let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some([Some(1)]),
        None,
        Some([Some(2)]),
    ]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "items",
        lists.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(lists)])
        .expect("test batch is valid");
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, None)
        .expect("test writer can be created");
    writer.write(&batch).expect("test batch can be written");
    writer.close().expect("test file can be closed");

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(output))
        .expect("test file can be opened");
    let filters = [ParquetBoundPredicate::new(BoundNode::IsNull(
        PlannedColumn {
            attno: 1,
            name: "items".into(),
        },
    ))];
    let predicate = ParquetFilePredicate::try_new(
        &filters,
        builder.parquet_schema(),
        builder.schema(),
    )
    .expect("list null checks remain executable without metadata pruning");
    assert!(matches!(&predicate.pruning, PruningNode::Unprunable));
    assert_eq!(predicate.selected_row_groups(builder.metadata()), vec![0]);

    let batches = builder
        .with_row_filter(predicate.into_row_filter())
        .build()
        .expect("test reader can be built")
        .collect::<Result<Vec<_>, _>>()
        .expect("test file can be read");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[test]
fn range_pruning_obeys_inclusive_and_exclusive_bounds() {
    let below = RangeOrdering {
        min: Some(Ordering::Less),
        max: Some(Ordering::Less),
    };
    let equal = RangeOrdering {
        min: Some(Ordering::Equal),
        max: Some(Ordering::Equal),
    };
    let above = RangeOrdering {
        min: Some(Ordering::Greater),
        max: Some(Ordering::Greater),
    };

    assert!(!ComparisonOperator::Eq.might_match(below));
    assert!(ComparisonOperator::Eq.might_match(equal));
    assert!(!ComparisonOperator::Eq.might_match(above));
    assert!(!ComparisonOperator::Lt.might_match(equal));
    assert!(ComparisonOperator::Le.might_match(equal));
    assert!(!ComparisonOperator::Gt.might_match(equal));
    assert!(ComparisonOperator::Ge.might_match(equal));
    assert!(ComparisonOperator::NotEq.might_match(equal));
}

#[test]
fn missing_bounds_never_prune() {
    let missing = RangeOrdering::default();
    for operator in [
        ComparisonOperator::Eq,
        ComparisonOperator::NotEq,
        ComparisonOperator::Lt,
        ComparisonOperator::Le,
        ComparisonOperator::Gt,
        ComparisonOperator::Ge,
    ] {
        assert!(operator.might_match(missing));
    }
}

#[test]
fn row_group_and_exact_filters_compose() {
    let builder = row_group_file();
    let filters = [
        comparison_predicate(ComparisonOperator::Eq, 6),
        comparison_predicate(ComparisonOperator::Ge, 6),
    ];
    let predicate = ParquetFilePredicate::try_new(
        &filters,
        builder.parquet_schema(),
        builder.schema(),
    )
    .expect("test predicate matches the file schema");
    let row_groups = predicate.selected_row_groups(builder.metadata());
    assert_eq!(row_groups, vec![1]);

    let batches = builder
        .with_row_groups(row_groups)
        .with_row_filter(predicate.into_row_filter())
        .build()
        .expect("test reader can be built")
        .collect::<Result<Vec<_>, _>>()
        .expect("test file can be read");
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id remains Int32")
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![6]);
}
