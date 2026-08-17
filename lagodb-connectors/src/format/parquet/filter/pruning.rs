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
mod tests;
