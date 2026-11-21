//! Per-file compilation of bound predicates: an exact row filter plus
//! conservative row-group pruning.

use std::cmp::Ordering;

use arrow_schema::Schema;
use lagodb_core::expr::BooleanTestKind;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::{Statistics, ValueStatistics};
use parquet::schema::types::SchemaDescriptor;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::bound::{LoweredPredicate, ParquetBoundPredicate};
use super::execute::ExecutableNode;
use super::lower::{FileNode, FileSchema, FileValue};
use super::operator::ComparisonOperator;

/// One file's row filter and its conservative row-group selector.
pub(crate) struct ParquetFilePredicate {
    row_filter: Option<RowFilter>,
    pruning: PruningNode,
}

impl ParquetFilePredicate {
    pub(crate) fn try_new(
        filters: &[ParquetBoundPredicate],
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &Schema,
    ) -> Result<Self, ConnectorError> {
        let schema = FileSchema::new(arrow_schema);
        let lowered = filters
            .iter()
            .map(|filter| filter.lower(schema))
            .collect::<Result<Vec<_>, _>>()?;
        let row_filters = lowered
            .iter()
            .filter_map(LoweredPredicate::row_filter)
            .collect::<Vec<_>>();
        let row_filter = if row_filters.is_empty() {
            None
        } else {
            let predicate = ExecutableNode::arrow_predicate(
                &row_filters,
                parquet_schema,
                schema,
            )?;
            Some(RowFilter::new(vec![predicate]))
        };
        let mut roots = lowered
            .into_iter()
            .map(|lowered| PruningNode::bind(lowered.pruning, parquet_schema, schema))
            .collect::<Result<Vec<_>, _>>()?;
        let pruning = if roots.len() == 1 {
            roots
                .pop()
                .expect("one Parquet pruning predicate was bound")
        } else {
            PruningNode::And(roots.into_boxed_slice())
        };
        Ok(Self {
            row_filter,
            pruning,
        })
    }

    pub(crate) fn row_group_might_match(&self, row_group: &RowGroupMetaData) -> bool {
        row_group.num_rows() > 0 && self.pruning.row_group_might_match(row_group)
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
                self.row_group_might_match(row_group).then_some(index)
            })
            .collect()
    }

    pub(crate) fn into_row_filter(self) -> Option<RowFilter> {
        self.row_filter
    }
}

enum PruningNode {
    Comparison {
        operator: ComparisonOperator,
        column: PruningColumn,
        value: FileValue,
    },
    InSet {
        column: PruningColumn,
        values: Box<[FileValue]>,
    },
    StartsWith {
        column: PruningColumn,
        prefix: Box<[u8]>,
    },
    IsNull(PruningColumn),
    IsNotNull(PruningColumn),
    /// A leaf false for every NULL row and otherwise not prunable.
    NonNull(PruningColumn),
    And(Box<[Self]>),
    Or(Box<[Self]>),
    NeverTrue,
    Unprunable,
}

impl PruningNode {
    fn bind(
        node: FileNode,
        parquet_schema: &SchemaDescriptor,
        schema: FileSchema<'_>,
    ) -> Result<Self, ConnectorError> {
        let column = |name: &str| PruningColumn::bind(name, parquet_schema, schema);
        Ok(match node {
            FileNode::AlwaysTrue => Self::Unprunable,
            FileNode::NeverTrue => Self::NeverTrue,
            FileNode::Comparison {
                operator,
                column: name,
                value,
            } => match column(&name)? {
                Some(column) => Self::Comparison {
                    operator,
                    column,
                    value,
                },
                None => Self::Unprunable,
            },
            FileNode::InSet {
                column: name,
                values,
            } => match column(&name)? {
                Some(column) => Self::InSet { column, values },
                None => Self::Unprunable,
            },
            FileNode::StartsWith {
                column: name,
                prefix: FileValue::Utf8(prefix) | FileValue::LargeUtf8(prefix),
            } => match column(&name)? {
                Some(column) => Self::StartsWith {
                    column,
                    prefix: prefix.into_boxed_bytes(),
                },
                None => Self::Unprunable,
            },
            FileNode::StartsWith { .. } => Self::Unprunable,
            FileNode::IsNull(name)
            | FileNode::BooleanTest {
                kind: BooleanTestKind::IsUnknown,
                column: name,
            } => column(&name)?.map_or(Self::Unprunable, Self::IsNull),
            FileNode::IsNotNull(name)
            | FileNode::BooleanTest {
                kind: BooleanTestKind::IsNotUnknown,
                column: name,
            } => column(&name)?.map_or(Self::Unprunable, Self::IsNotNull),
            FileNode::BooleanTest {
                kind: kind @ (BooleanTestKind::IsTrue | BooleanTestKind::IsFalse),
                column: name,
            } => match column(&name)? {
                Some(column) => Self::Comparison {
                    operator: ComparisonOperator::Eq,
                    column,
                    value: FileValue::Bool(kind == BooleanTestKind::IsTrue),
                },
                None => Self::Unprunable,
            },
            // NOT TRUE / NOT FALSE accept NULL rows.
            FileNode::BooleanTest { .. } => Self::Unprunable,
            FileNode::IsNan(name) | FileNode::IsNotNan(name) => {
                column(&name)?.map_or(Self::Unprunable, Self::NonNull)
            }
            FileNode::And(children) => {
                Self::And(Self::bind_children(children, parquet_schema, schema)?)
            }
            FileNode::Or(children) => {
                Self::Or(Self::bind_children(children, parquet_schema, schema)?)
            }
        })
    }

    fn bind_children(
        children: Box<[FileNode]>,
        parquet_schema: &SchemaDescriptor,
        schema: FileSchema<'_>,
    ) -> Result<Box<[Self]>, ConnectorError> {
        children
            .into_vec()
            .into_iter()
            .map(|child| Self::bind(child, parquet_schema, schema))
            .collect()
    }

    fn row_group_might_match(&self, row_group: &RowGroupMetaData) -> bool {
        match self {
            Self::Comparison {
                operator,
                column,
                value,
            } => column.non_null_might_match(row_group, |statistics| {
                operator.might_match(value.row_group_range(statistics))
            }),
            Self::InSet { column, values } => {
                column.non_null_might_match(row_group, |statistics| {
                    values.iter().any(|value| {
                        ComparisonOperator::Eq
                            .might_match(value.row_group_range(statistics))
                    })
                })
            }
            Self::StartsWith { column, prefix } => column
                .non_null_might_match(row_group, |statistics| {
                    PrefixRange::of(statistics).might_start_with(prefix)
                }),
            Self::IsNull(column) => column.nulls_might_match(row_group, true),
            Self::IsNotNull(column) => column.nulls_might_match(row_group, false),
            Self::NonNull(column) => column.non_null_might_match(row_group, |_| true),
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
    /// The leaf of a primitive root column; nested roots are not pruned.
    fn bind(
        name: &str,
        parquet_schema: &SchemaDescriptor,
        schema: FileSchema<'_>,
    ) -> Result<Option<Self>, ConnectorError> {
        let root = schema.root(name)?;
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
        let leaf = (0..parquet_schema.num_columns())
            .find(|leaf| parquet_schema.get_column_root_idx(*leaf) == root)
            .ok_or_else(|| {
                ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!("filter column {name:?} has no Parquet leaf column"),
                )
            })?;
        Ok(Some(Self { leaf }))
    }

    fn statistics(self, row_group: &RowGroupMetaData) -> Option<&Statistics> {
        row_group.column(self.leaf).statistics()
    }

    /// A leaf false for NULL: an all-NULL row group cannot match it.
    fn non_null_might_match(
        self,
        row_group: &RowGroupMetaData,
        might_match: impl FnOnce(&Statistics) -> bool,
    ) -> bool {
        let Some(statistics) = self.statistics(row_group) else {
            return true;
        };
        if statistics.null_count_opt() == u64::try_from(row_group.num_rows()).ok() {
            return false;
        }
        might_match(statistics)
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

/// Exact row-group bounds compared with one value; `None` is unknown.
#[derive(Clone, Copy, Default)]
struct RangeOrdering {
    min: Option<Ordering>,
    max: Option<Ordering>,
}

impl RangeOrdering {
    fn of<T: Ord + ?Sized, S>(
        statistics: &ValueStatistics<S>,
        value: &T,
        project: impl Fn(&S) -> &T,
    ) -> Self {
        Self {
            min: statistics
                .min_is_exact()
                .then(|| statistics.min_opt().map(|min| project(min).cmp(value)))
                .flatten(),
            max: statistics
                .max_is_exact()
                .then(|| statistics.max_opt().map(|max| project(max).cmp(value)))
                .flatten(),
        }
    }
}

impl ComparisonOperator {
    fn might_match(self, range: RangeOrdering) -> bool {
        match self {
            Self::Eq => {
                range.min != Some(Ordering::Greater)
                    && range.max != Some(Ordering::Less)
            }
            // min/max do not encode value membership.
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

impl FileValue {
    /// Bounds use Parquet's physical sort order: signed for INT32/INT64
    /// (including Date32, Int16 and timestamps) and unsigned bytewise for
    /// strings and binary. Floats and decimals are never pruned.
    fn row_group_range(&self, statistics: &Statistics) -> RangeOrdering {
        match (self, statistics) {
            (Self::Bool(value), Statistics::Boolean(typed)) => {
                RangeOrdering::of(typed, value, |bound| bound)
            }
            (Self::I16(value), Statistics::Int32(typed)) => {
                RangeOrdering::of(typed, &i32::from(*value), |bound| bound)
            }
            (Self::I32(value) | Self::Date32(value), Statistics::Int32(typed)) => {
                RangeOrdering::of(typed, value, |bound| bound)
            }
            (
                Self::I64(value) | Self::Timestamp { value, .. },
                Statistics::Int64(typed),
            ) => RangeOrdering::of(typed, value, |bound| bound),
            (
                Self::Utf8(value) | Self::LargeUtf8(value),
                Statistics::ByteArray(typed),
            ) => RangeOrdering::of(typed, value.as_bytes(), |bound| bound.data()),
            (
                Self::Binary(value) | Self::LargeBinary(value),
                Statistics::ByteArray(typed),
            ) => RangeOrdering::of(typed, value.as_ref(), |bound| bound.data()),
            (Self::FixedSizeBinary(value), Statistics::FixedLenByteArray(typed)) => {
                RangeOrdering::of(typed, value.as_ref(), |bound| bound.data())
            }
            _ => RangeOrdering::default(),
        }
    }
}

/// Exact string bounds of a row group for prefix pruning.
struct PrefixRange<'a> {
    min: Option<&'a [u8]>,
    max: Option<&'a [u8]>,
}

impl<'a> PrefixRange<'a> {
    fn of(statistics: &'a Statistics) -> Self {
        let Statistics::ByteArray(typed) = statistics else {
            return Self {
                min: None,
                max: None,
            };
        };
        Self {
            min: typed
                .min_is_exact()
                .then(|| typed.min_opt().map(|min| min.data()))
                .flatten(),
            max: typed
                .max_is_exact()
                .then(|| typed.max_opt().map(|max| max.data()))
                .flatten(),
        }
    }

    /// Values starting with `prefix` lie in `[prefix, successor(prefix))`.
    fn might_start_with(&self, prefix: &[u8]) -> bool {
        if self.max.is_some_and(|max| max < prefix) {
            return false;
        }
        !self
            .min
            .is_some_and(|min| min > prefix && !min.starts_with(prefix))
    }
}
