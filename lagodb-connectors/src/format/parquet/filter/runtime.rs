//! Bound predicate execution against projected Arrow record batches.

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, or_kleene};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, Scalar};
use arrow_schema::{ArrowError, Schema};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};
use parquet::schema::types::SchemaDescriptor;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::PlannedColumn;
use super::value::{BoundValue, ComparisonOperator};

#[derive(Clone)]
pub(super) enum BoundNode {
    Comparison {
        operator: ComparisonOperator,
        column: PlannedColumn,
        value: BoundValue,
    },
    IsNull(PlannedColumn),
    IsNotNull(PlannedColumn),
    And(Box<[Self]>),
    Or(Box<[Self]>),
    NeverTrue,
}

#[derive(Clone)]
pub(crate) struct ParquetBoundPredicate {
    pub(super) root: BoundNode,
}

impl ParquetBoundPredicate {
    pub(super) const fn new(root: BoundNode) -> Self {
        Self { root }
    }

    pub(crate) fn arrow_predicate(
        filters: &[Self],
        parquet_schema: &SchemaDescriptor,
        arrow_schema: &Schema,
    ) -> Result<Box<dyn ArrowPredicate>, ConnectorError> {
        let mut roots = Vec::new();
        for filter in filters {
            filter.root.collect_roots(arrow_schema, &mut roots)?;
        }
        roots.sort_unstable();
        roots.dedup();
        let mut executable = filters
            .iter()
            .map(|filter| filter.root.bind_schema(arrow_schema, &roots))
            .collect::<Result<Vec<_>, _>>()?;
        let executable = if executable.len() == 1 {
            executable.pop().expect("one Parquet filter was bound")
        } else {
            ExecutableNode::And(executable.into_boxed_slice())
        };
        let projection = ProjectionMask::roots(parquet_schema, roots);
        Ok(Box::new(ArrowPredicateFn::new(projection, move |batch| {
            executable.evaluate(&batch)
        })))
    }
}

impl BoundNode {
    fn collect_roots(
        &self,
        schema: &Schema,
        roots: &mut Vec<usize>,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::Comparison { column, .. }
            | Self::IsNull(column)
            | Self::IsNotNull(column) => {
                roots.push(schema.index_of(&column.name).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "filter column {:?} is missing from the Parquet schema",
                            column.name
                        ),
                    )
                })?)
            }
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.collect_roots(schema, roots)?;
                }
            }
            Self::NeverTrue => {}
        }
        Ok(())
    }

    fn bind_schema(
        &self,
        schema: &Schema,
        roots: &[usize],
    ) -> Result<ExecutableNode, ConnectorError> {
        let column_index =
            |column: &PlannedColumn| -> Result<(usize, usize), ConnectorError> {
                let root = schema.index_of(&column.name).map_err(|_| {
                    ConnectorError::invalid_object_schema(
                        FormatKind::Parquet,
                        format!(
                            "filter column {:?} is missing from the Parquet schema",
                            column.name
                        ),
                    )
                })?;
                let batch = roots.binary_search(&root).map_err(|_| {
                    ConnectorError::invalid_filter_plan(FormatKind::Parquet)
                })?;
                Ok((root, batch))
            };
        Ok(match self {
            Self::Comparison {
                operator,
                column,
                value,
            } => {
                let (root, batch) = column_index(column)?;
                ExecutableNode::Comparison {
                    operator: *operator,
                    column: batch,
                    scalar: value.scalar(schema.field(root).data_type())?,
                }
            }
            Self::IsNull(column) => ExecutableNode::IsNull(column_index(column)?.1),
            Self::IsNotNull(column) => {
                ExecutableNode::IsNotNull(column_index(column)?.1)
            }
            Self::And(children) => ExecutableNode::And(
                children
                    .iter()
                    .map(|child| child.bind_schema(schema, roots))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice(),
            ),
            Self::Or(children) => ExecutableNode::Or(
                children
                    .iter()
                    .map(|child| child.bind_schema(schema, roots))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_boxed_slice(),
            ),
            Self::NeverTrue => ExecutableNode::NeverTrue,
        })
    }
}

enum ExecutableNode {
    Comparison {
        operator: ComparisonOperator,
        column: usize,
        scalar: Scalar<ArrayRef>,
    },
    IsNull(usize),
    IsNotNull(usize),
    And(Box<[Self]>),
    Or(Box<[Self]>),
    NeverTrue,
}

impl ExecutableNode {
    fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::Comparison {
                operator,
                column,
                scalar,
            } => operator.evaluate(batch.column(*column).as_ref(), scalar),
            Self::IsNull(column) => is_null(batch.column(*column).as_ref()),
            Self::IsNotNull(column) => is_not_null(batch.column(*column).as_ref()),
            Self::And(children) => {
                Self::evaluate_children(children, batch, and_kleene)
            }
            Self::Or(children) => Self::evaluate_children(children, batch, or_kleene),
            Self::NeverTrue => Ok(BooleanArray::new_null(batch.num_rows())),
        }
    }

    fn evaluate_children(
        children: &[Self],
        batch: &RecordBatch,
        combine: fn(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>,
    ) -> Result<BooleanArray, ArrowError> {
        let mut children = children.iter();
        let mut result = children
            .next()
            .expect("planned logical Parquet filters are non-empty")
            .evaluate(batch)?;
        for child in children {
            result = combine(&result, &child.evaluate(batch)?)?;
        }
        Ok(result)
    }
}
