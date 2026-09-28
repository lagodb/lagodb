//! Conservative row-group pruning for bound Parquet predicates.

use std::cmp::Ordering;

use arrow_schema::Schema;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::PlannedColumn;
use super::runtime::{BoundNode, ParquetBoundPredicate};
use super::value::{BoundValue, ComparisonOperator};

/// Per-file compilation of one exact row filter and its conservative metadata filter.
pub(crate) struct ParquetFilePredicate<'a> {
    row_filter: RowFilter,
    pruning: PruningNode<'a>,
}

impl<'a> ParquetFilePredicate<'a> {
    pub(crate) fn try_new(
        filters: &'a [ParquetBoundPredicate],
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        let exact = ParquetBoundPredicate::arrow_predicate(
            filters,
            parquet_schema,
            arrow_schema,
        )?;
        let mut roots = filters
            .iter()
            .map(|filter| {
                PruningNode::bind(&filter.root, parquet_schema, arrow_schema)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let pruning = if roots.len() == 1 {
            roots
                .pop()
                .expect("one Parquet pruning predicate was bound")
        } else {
            PruningNode::And(roots.into_boxed_slice())
        };
        Ok(Self {
            row_filter: RowFilter::new(vec![exact]),
            pruning,
        })
    }

    pub(crate) fn selected_row_groups(
        &self,
        metadata: &ParquetMetaData,
    ) -> Vec<usize> {
        metadata
            .row_groups()
            .iter()
            .enumerate()
            .filter_map(|(index, row_group)| {
                (row_group.num_rows() > 0
                    && self.pruning.row_group_might_match(row_group))
                .then_some(index)
            })
            .collect()
    }

    pub(crate) fn into_row_filter(self) -> RowFilter {
        self.row_filter
    }
}

enum PruningNode<'a> {
    Comparison {
        operator: ComparisonOperator,
        column: PruningColumn,
        value: &'a BoundValue,
    },
    IsNull(PruningColumn),
    IsNotNull(PruningColumn),
    And(Box<[Self]>),
    Or(Box<[Self]>),
    NeverTrue,
    Unprunable,
}

impl<'a> PruningNode<'a> {
    fn bind(
        node: &'a BoundNode,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        Ok(match node {
            BoundNode::Comparison {
                operator,
                column,
                value,
            } => match PruningColumn::bind(column, parquet_schema, arrow_schema)? {
                Some(column) => Self::Comparison {
                    operator: *operator,
                    column,
                    value,
                },
                None => Self::Unprunable,
            },
            BoundNode::IsNull(column) => {
                match PruningColumn::bind(column, parquet_schema, arrow_schema)? {
                    Some(column) => Self::IsNull(column),
                    None => Self::Unprunable,
                }
            }
            BoundNode::IsNotNull(column) => {
                match PruningColumn::bind(column, parquet_schema, arrow_schema)? {
                    Some(column) => Self::IsNotNull(column),
                    None => Self::Unprunable,
                }
            }
            BoundNode::And(children) => Self::And(
                children
                    .iter()
                    .map(|child| Self::bind(child, parquet_schema, arrow_schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice(),
            ),
            BoundNode::Or(children) => Self::Or(
                children
                    .iter()
                    .map(|child| Self::bind(child, parquet_schema, arrow_schema))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice(),
            ),
            BoundNode::NeverTrue => Self::NeverTrue,
        })
    }

    fn row_group_might_match(&self, row_group: &RowGroupMetaData) -> bool {
        match self {
            Self::Comparison {
                operator,
                column,
                value,
            } => column.comparison_might_match(row_group, *operator, value),
            Self::IsNull(column) => column.nulls_might_match(row_group, true),
            Self::IsNotNull(column) => column.nulls_might_match(row_group, false),
            Self::And(children) => children
                .iter()
                .all(|child| child.row_group_might_match(row_group)),
            Self::Or(children) => children
                .iter()
                .any(|child| child.row_group_might_match(row_group)),
            Self::NeverTrue => false,
            Self::Unprunable => true,
        }
    }
}

#[derive(Clone, Copy)]
struct PruningColumn {
    leaf: usize,
}

impl PruningColumn {
    fn bind(
        column: &PlannedColumn,
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &Schema,
    ) -> Result<Option<Self>, ConnectorError> {
        let root = arrow_schema.index_of(&column.name).map_err(|_| {
            ConnectorError::invalid_object_schema(
                FormatKind::Parquet,
                format!(
                    "filter column {:?} is missing from the Parquet schema",
                    column.name
                ),
            )
        })?;
        let root_type = parquet_schema
            .root_schema()
            .get_fields()
            .get(root)
            .ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    "Arrow and Parquet root schemas are inconsistent",
                )
            })?;
        if !root_type.is_primitive() {
            return Ok(None);
        }
        let leaf = parquet_schema
            .columns()
            .iter()
            .enumerate()
            .find_map(|(leaf, _)| {
                (parquet_schema.get_column_root_idx(leaf) == root).then_some(leaf)
            })
            .ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!(
                        "filter column {:?} has no Parquet leaf column",
                        column.name
                    ),
                )
            })?;
        Ok(Some(Self { leaf }))
    }

    fn statistics(self, row_group: &RowGroupMetaData) -> Option<&Statistics> {
        row_group.column(self.leaf).statistics()
    }

    fn comparison_might_match(
        self,
        row_group: &RowGroupMetaData,
        operator: ComparisonOperator,
        value: &BoundValue,
    ) -> bool {
        let Some(statistics) = self.statistics(row_group) else {
            return true;
        };
        if statistics.null_count_opt() == u64::try_from(row_group.num_rows()).ok() {
            return false;
        }
        operator.might_match(value.row_group_range(statistics))
    }

    fn nulls_might_match(
        self,
        row_group: &RowGroupMetaData,
        want_null: bool,
    ) -> bool {
        let Some(statistics) = self.statistics(row_group) else {
            return true;
        };
        match statistics.null_count_opt() {
            Some(0) if want_null => false,
            Some(nulls)
                if !want_null
                    && Some(nulls) == u64::try_from(row_group.num_rows()).ok() =>
            {
                false
            }
            _ => true,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct RangeOrdering {
    min: Option<Ordering>,
    max: Option<Ordering>,
}

impl ComparisonOperator {
    fn might_match(self, range: RangeOrdering) -> bool {
        match self {
            Self::Eq => {
                range.min != Some(Ordering::Greater)
                    && range.max != Some(Ordering::Less)
            }
            // Iceberg also keeps NotEq conservatively: min/max do not encode
            // value membership unless additional guarantees are available.
            Self::NotEq => true,
            Self::Lt => {
                range.min != Some(Ordering::Equal)
                    && range.min != Some(Ordering::Greater)
            }
            Self::Le => range.min != Some(Ordering::Greater),
            Self::Gt => {
                range.max != Some(Ordering::Equal)
                    && range.max != Some(Ordering::Less)
            }
            Self::Ge => range.max != Some(Ordering::Less),
        }
    }
}

impl BoundValue {
    fn row_group_range(&self, statistics: &Statistics) -> RangeOrdering {
        match (self, statistics) {
            (Self::Bool(value), Statistics::Boolean(typed)) => RangeOrdering {
                min: typed
                    .min_is_exact()
                    .then(|| typed.min_opt().map(|min| min.cmp(value)))
                    .flatten(),
                max: typed
                    .max_is_exact()
                    .then(|| typed.max_opt().map(|max| max.cmp(value)))
                    .flatten(),
            },
            (Self::I32(value), Statistics::Int32(typed)) => RangeOrdering {
                min: typed
                    .min_is_exact()
                    .then(|| typed.min_opt().map(|min| min.cmp(value)))
                    .flatten(),
                max: typed
                    .max_is_exact()
                    .then(|| typed.max_opt().map(|max| max.cmp(value)))
                    .flatten(),
            },
            (Self::I64(value), Statistics::Int64(typed)) => RangeOrdering {
                min: typed
                    .min_is_exact()
                    .then(|| typed.min_opt().map(|min| min.cmp(value)))
                    .flatten(),
                max: typed
                    .max_is_exact()
                    .then(|| typed.max_opt().map(|max| max.cmp(value)))
                    .flatten(),
            },
            (Self::String(value), Statistics::ByteArray(typed)) => {
                let value = value.as_bytes();
                RangeOrdering {
                    min: typed
                        .min_is_exact()
                        .then(|| typed.min_opt().map(|min| min.data().cmp(value)))
                        .flatten(),
                    max: typed
                        .max_is_exact()
                        .then(|| typed.max_opt().map(|max| max.data().cmp(value)))
                        .flatten(),
                }
            }
            _ => RangeOrdering::default(),
        }
    }
}

#[cfg(test)]
mod tests {
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
}
