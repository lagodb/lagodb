//! Row-filter evaluation of lowered predicates against projected batches.

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, or_kleene};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{ArrowError, DataType};
use arrow_string::like::starts_with;
use lagodb_core::expr::BooleanTestKind;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};
use parquet::schema::types::SchemaDescriptor;

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::lower::{FileNode, FileSchema};
use super::operator::ComparisonOperator;

pub(super) enum ExecutableNode {
    AlwaysTrue,
    NeverTrue,
    Comparison {
        operator: ComparisonOperator,
        column: usize,
        scalar: Scalar<ArrayRef>,
    },
    IsNull(usize),
    IsNotNull(usize),
    IsNan(usize),
    IsNotNan(usize),
    BooleanTest {
        kind: BooleanTestKind,
        column: usize,
    },
    StartsWith {
        column: usize,
        prefix: Scalar<ArrayRef>,
    },
    And(Box<[Self]>),
    Or(Box<[Self]>),
}

impl ExecutableNode {
    /// Compile row-filter trees into one Parquet `ArrowPredicate` over the
    /// root columns they reference.
    pub(super) fn arrow_predicate(
        filters: &[&FileNode],
        parquet_schema: &SchemaDescriptor,
        schema: FileSchema<'_>,
    ) -> Result<Box<dyn ArrowPredicate>, ConnectorError> {
        let mut roots = Vec::new();
        for filter in filters {
            filter.collect_roots(schema, &mut roots)?;
        }
        roots.sort_unstable();
        roots.dedup();
        let mut executable = filters
            .iter()
            .map(|filter| Self::compile(filter, schema, &roots))
            .collect::<Result<Vec<_>, _>>()?;
        let executable = if executable.len() == 1 {
            executable
                .pop()
                .expect("one Parquet row filter was compiled")
        } else {
            Self::And(executable.into_boxed_slice())
        };
        let projection = ProjectionMask::roots(parquet_schema, roots);
        Ok(Box::new(ArrowPredicateFn::new(projection, move |batch| {
            executable.evaluate(&batch)
        })))
    }

    fn compile(
        node: &FileNode,
        schema: FileSchema<'_>,
        roots: &[usize],
    ) -> Result<Self, ConnectorError> {
        let column = |name: &str| -> Result<usize, ConnectorError> {
            let root = schema.root(name)?;
            roots
                .binary_search(&root)
                .map_err(|_| ConnectorError::invalid_filter_plan(FormatKind::Parquet))
        };
        Ok(match node {
            FileNode::AlwaysTrue => Self::AlwaysTrue,
            FileNode::NeverTrue => Self::NeverTrue,
            FileNode::Comparison {
                operator,
                column: name,
                value,
            } => Self::Comparison {
                operator: *operator,
                column: column(name)?,
                scalar: value.scalar()?,
            },
            FileNode::IsNull(name) => Self::IsNull(column(name)?),
            FileNode::IsNotNull(name) => Self::IsNotNull(column(name)?),
            FileNode::IsNan(name) => Self::IsNan(column(name)?),
            FileNode::IsNotNan(name) => Self::IsNotNan(column(name)?),
            FileNode::BooleanTest { kind, column: name } => Self::BooleanTest {
                kind: *kind,
                column: column(name)?,
            },
            FileNode::StartsWith {
                column: name,
                prefix,
            } => Self::StartsWith {
                column: column(name)?,
                prefix: prefix.scalar()?,
            },
            FileNode::InSet { .. } => {
                // `ParquetNode::row_filter` projects sets out before lowering.
                return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
            }
            FileNode::And(children) => {
                Self::And(Self::compile_children(children, schema, roots)?)
            }
            FileNode::Or(children) => {
                Self::Or(Self::compile_children(children, schema, roots)?)
            }
        })
    }

    fn compile_children(
        children: &[FileNode],
        schema: FileSchema<'_>,
        roots: &[usize],
    ) -> Result<Box<[Self]>, ConnectorError> {
        children
            .iter()
            .map(|child| Self::compile(child, schema, roots))
            .collect()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::AlwaysTrue => Ok(BooleanArray::new(
                BooleanBuffer::new_set(batch.num_rows()),
                None,
            )),
            Self::NeverTrue => Ok(BooleanArray::new(
                BooleanBuffer::new_unset(batch.num_rows()),
                None,
            )),
            Self::Comparison {
                operator,
                column,
                scalar,
            } => operator.evaluate(batch.column(*column), scalar),
            Self::IsNull(column) => is_null(batch.column(*column).as_ref()),
            Self::IsNotNull(column) => is_not_null(batch.column(*column).as_ref()),
            Self::IsNan(column) => {
                Self::nan_mask(batch.column(*column).as_ref(), true)
            }
            Self::IsNotNan(column) => {
                Self::nan_mask(batch.column(*column).as_ref(), false)
            }
            Self::BooleanTest { kind, column } => {
                Self::boolean_test(*kind, batch.column(*column).as_ref())
            }
            Self::StartsWith { column, prefix } => {
                starts_with(batch.column(*column), prefix)
            }
            Self::And(children) => {
                Self::evaluate_children(children, batch, and_kleene)
            }
            Self::Or(children) => Self::evaluate_children(children, batch, or_kleene),
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
            .expect("bound logical Parquet filters are non-empty")
            .evaluate(batch)?;
        for child in children {
            result = combine(&result, &child.evaluate(batch)?)?;
        }
        Ok(result)
    }

    /// Non-NULL values that are (or are not) NaN; NULL rows are false.
    fn nan_mask(array: &dyn Array, nan: bool) -> Result<BooleanArray, ArrowError> {
        let values = match array.data_type() {
            DataType::Float32 => {
                let values = array.as_primitive::<Float32Type>().values();
                BooleanBuffer::collect_bool(values.len(), |row| {
                    values[row].is_nan() == nan
                })
            }
            DataType::Float64 => {
                let values = array.as_primitive::<Float64Type>().values();
                BooleanBuffer::collect_bool(values.len(), |row| {
                    values[row].is_nan() == nan
                })
            }
            other => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "NaN test on non-float Parquet column type {other}"
                )));
            }
        };
        Ok(Self::non_null(values, array))
    }

    /// SQL `IS [NOT] TRUE/FALSE/UNKNOWN`, which never yields NULL.
    fn boolean_test(
        kind: BooleanTestKind,
        array: &dyn Array,
    ) -> Result<BooleanArray, ArrowError> {
        let Some(array) = array.as_boolean_opt() else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "boolean test on non-boolean Parquet column type {}",
                array.data_type()
            )));
        };
        let valid = match array.logical_nulls() {
            Some(nulls) => nulls.inner().clone(),
            None => BooleanBuffer::new_set(array.len()),
        };
        let values = array.values();
        let mask = match kind {
            BooleanTestKind::IsTrue => values & &valid,
            BooleanTestKind::IsNotTrue => !&(values & &valid),
            BooleanTestKind::IsFalse => &!values & &valid,
            BooleanTestKind::IsNotFalse => values | &!&valid,
            BooleanTestKind::IsUnknown => !&valid,
            BooleanTestKind::IsNotUnknown => valid,
        };
        Ok(BooleanArray::new(mask, None))
    }

    fn non_null(values: BooleanBuffer, array: &dyn Array) -> BooleanArray {
        let values = match array.logical_nulls() {
            Some(nulls) => &values & nulls.inner(),
            None => values,
        };
        BooleanArray::new(values, None)
    }
}

impl FileNode {
    fn collect_roots(
        &self,
        schema: FileSchema<'_>,
        roots: &mut Vec<usize>,
    ) -> Result<(), ConnectorError> {
        match self {
            Self::AlwaysTrue | Self::NeverTrue => {}
            Self::Comparison { column, .. }
            | Self::IsNull(column)
            | Self::IsNotNull(column)
            | Self::IsNan(column)
            | Self::IsNotNan(column)
            | Self::BooleanTest { column, .. }
            | Self::StartsWith { column, .. }
            | Self::InSet { column, .. } => roots.push(schema.root(column)?),
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.collect_roots(schema, roots)?;
                }
            }
        }
        Ok(())
    }
}
