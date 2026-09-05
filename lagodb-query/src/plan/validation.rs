//! One-time semantic validation for decoded query plans.

use std::mem;

use lagodb_core::expr::{ColumnRef, ExprType, RuntimeValueLayout};
use pgrx::pg_sys;

use super::{
    ExecutionExpr, ExecutionScalarRepr, QueryNode, QueryPlanError, ScalarSemantics,
};

struct NodeFacts {
    columns: Vec<ColumnRef>,
    outputs: Vec<Option<OutputFact>>,
}

#[derive(Clone, Copy)]
struct OutputFact {
    value_type: ExprType,
    supports_having: bool,
}

pub(super) struct PlanExpressionValidator<'a> {
    runtime_values: &'a RuntimeValueLayout,
    output_count: usize,
}

impl<'a> PlanExpressionValidator<'a> {
    pub(super) const fn new(
        runtime_values: &'a RuntimeValueLayout,
        output_count: usize,
    ) -> Self {
        Self {
            runtime_values,
            output_count,
        }
    }

    pub(super) fn validate(&self, node: &QueryNode) -> Result<(), QueryPlanError> {
        if self
            .runtime_values
            .values()
            .iter()
            .any(|value| !value.source_kind.is_rescan_stable())
        {
            return Err(QueryPlanError::UnsupportedRuntimeValueSource);
        }
        self.validate_node(node).map(|_| ())
    }

    fn validate_node(&self, node: &QueryNode) -> Result<NodeFacts, QueryPlanError> {
        match node {
            QueryNode::Scan(scan) => {
                let mut seen_attnos = Vec::new();
                for column in scan.columns() {
                    if column.scan != scan.scan()
                        || !(*column).has_binary_compatible_value()
                    {
                        return Err(QueryPlanError::MismatchedScanColumn);
                    }
                    let index = usize::try_from(column.attno)
                        .ok()
                        .and_then(|attno| attno.checked_sub(1))
                        .ok_or(QueryPlanError::MissingProjectedColumn)?;
                    if seen_attnos.len() <= index {
                        seen_attnos.resize(index + 1, false);
                    }
                    if mem::replace(&mut seen_attnos[index], true) {
                        return Err(QueryPlanError::DuplicateProjectedColumn);
                    }
                }
                let facts = NodeFacts {
                    columns: scan.columns().to_vec(),
                    outputs: vec![None; self.output_count],
                };
                if let Some(filter) = scan.filter() {
                    self.validate_boolean(filter, &facts, false)?;
                }
                Ok(facts)
            }
            QueryNode::Aggregate(aggregate) => {
                let input = self.validate_node(aggregate.input())?;
                let mut outputs = vec![None; self.output_count];
                for group in aggregate.groups() {
                    let ty =
                        self.expression_type(group.expression(), &input, false)?;
                    if ty != group.result_type() {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    outputs[group.output().index()] = Some(OutputFact {
                        value_type: group.result_type(),
                        supports_having: true,
                    });
                }
                for aggregate in aggregate.aggregates() {
                    let argument_type = aggregate
                        .argument()
                        .map(|argument| self.expression_type(argument, &input, false))
                        .transpose()?;
                    aggregate.validate_argument_type(argument_type)?;
                    for order in aggregate.order_by() {
                        let _ =
                            self.expression_type(order.expression(), &input, false)?;
                    }
                    if let Some(filter) = aggregate.filter() {
                        self.validate_boolean(filter, &input, false)?;
                    }
                    outputs[aggregate.output().index()] = Some(OutputFact {
                        value_type: aggregate.result_type(),
                        supports_having: aggregate.supports_having_result(),
                    });
                }
                Ok(NodeFacts {
                    columns: Vec::new(),
                    outputs,
                })
            }
            QueryNode::Distinct(distinct) => {
                let input = self.validate_node(distinct.input())?;
                let mut outputs = vec![None; self.output_count];
                for key in distinct.keys() {
                    let value_type =
                        self.expression_type(key.expression(), &input, false)?;
                    if value_type != key.result_type() {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    outputs[key.output().index()] = Some(OutputFact {
                        value_type,
                        supports_having: false,
                    });
                }
                Ok(NodeFacts {
                    columns: Vec::new(),
                    outputs,
                })
            }
            QueryNode::Filter(filter) => {
                let input = self.validate_node(filter.input())?;
                self.validate_boolean(filter.predicate(), &input, true)?;
                Ok(input)
            }
            QueryNode::Project(project) => self.validate_node(project.input()),
        }
    }

    fn validate_boolean(
        &self,
        expression: &ExecutionExpr,
        facts: &NodeFacts,
        allow_outputs: bool,
    ) -> Result<(), QueryPlanError> {
        if self
            .expression_type(expression, facts, allow_outputs)?
            .type_oid
            != pg_sys::BOOLOID
        {
            return Err(QueryPlanError::UnsupportedPredicate);
        }
        Ok(())
    }

    fn expression_type(
        &self,
        expression: &ExecutionExpr,
        facts: &NodeFacts,
        allow_outputs: bool,
    ) -> Result<ExprType, QueryPlanError> {
        match expression {
            ExecutionExpr::Column(column) => facts
                .columns
                .iter()
                .find(|candidate| {
                    (**candidate).same_storage_column(*column)
                        && (*column).has_binary_compatible_value()
                })
                .map(|_| column.value_type)
                .ok_or(QueryPlanError::MissingProjectedColumn),
            ExecutionExpr::Value(value) => {
                let value_type = self
                    .runtime_values
                    .values()
                    .get(value.index())
                    .map(|spec| spec.value_type)
                    .ok_or(QueryPlanError::RuntimeValueOutOfBounds)?;
                ExecutionScalarRepr::for_runtime_value(value_type)
                    .ok_or(QueryPlanError::UnsupportedRuntimeValueType)?;
                Ok(value_type)
            }
            ExecutionExpr::Output(output) if allow_outputs => {
                let fact = facts
                    .outputs
                    .get(output.index())
                    .and_then(|fact| *fact)
                    .ok_or(QueryPlanError::OutputOutOfBounds {
                    index: output.index(),
                })?;
                if !fact.supports_having {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(fact.value_type)
            }
            ExecutionExpr::Output(output) => Err(QueryPlanError::OutputOutOfBounds {
                index: output.index(),
            }),
            ExecutionExpr::Comparison {
                operator,
                left,
                right,
            } => {
                let left = self.expression_type(left, facts, allow_outputs)?;
                let right = self.expression_type(right, facts, allow_outputs)?;
                let comparison = if allow_outputs {
                    ScalarSemantics::Having.comparison(*operator, left, right)
                } else {
                    ScalarSemantics::Exact.comparison(*operator, left, right)
                };
                comparison.ok_or(QueryPlanError::UnsupportedPredicate)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::IsNull(value) | ExecutionExpr::IsNotNull(value) => {
                let _ = self.expression_type(value, facts, allow_outputs)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::BooleanTest { value, .. } => {
                self.validate_boolean(value, facts, allow_outputs)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::And(children) | ExecutionExpr::Or(children) => {
                if children.is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for child in children {
                    self.validate_boolean(child, facts, allow_outputs)?;
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::Not(child) => {
                self.validate_boolean(child, facts, allow_outputs)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::Relabel { value, result_type } => {
                let input_type = self.expression_type(value, facts, allow_outputs)?;
                if unsafe {
                    !pg_sys::IsBinaryCoercible(
                        input_type.type_oid,
                        result_type.type_oid,
                    )
                } {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(*result_type)
            }
            ExecutionExpr::Case {
                when_then,
                else_expr,
                result_type,
            } => {
                if when_then.is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for branch in when_then {
                    self.validate_boolean(branch.when(), facts, allow_outputs)?;
                    if self.expression_type(branch.then(), facts, allow_outputs)?
                        != *result_type
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                }
                if let Some(else_expr) = else_expr
                    && self.expression_type(else_expr, facts, allow_outputs)?
                        != *result_type
                {
                    return Err(QueryPlanError::TupleLayoutTypeMismatch);
                }
                Ok(*result_type)
            }
            ExecutionExpr::InList { value, list, .. } => {
                if list.is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                let value_type = self.expression_type(value, facts, allow_outputs)?;
                if !ScalarSemantics::Integer.supports_type(value_type) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for item in list {
                    if self.expression_type(item, facts, allow_outputs)? != value_type
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::Function {
                kind,
                arguments,
                input_collation,
                result_type,
            } => {
                let mut argument_types = Vec::with_capacity(arguments.len());
                for argument in arguments {
                    argument_types.push(self.expression_type(
                        argument,
                        facts,
                        allow_outputs,
                    )?);
                }
                if !kind.supports_signature(
                    &argument_types,
                    *input_collation,
                    *result_type,
                ) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(*result_type)
            }
            ExecutionExpr::Postgres(postgres) => {
                if postgres.serialized().to_bytes().is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                if postgres.inputs().len() > pg_sys::MaxTupleAttributeNumber as usize
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                if ExecutionScalarRepr::for_postgres_eval(postgres.result_type())
                    .is_none()
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for input in postgres.inputs() {
                    if ExecutionScalarRepr::for_postgres_eval(input.value_type())
                        .is_none()
                    {
                        return Err(QueryPlanError::UnsupportedPredicate);
                    }
                    if !matches!(
                        input.expression(),
                        ExecutionExpr::Column(_)
                            | ExecutionExpr::Value(_)
                            | ExecutionExpr::Output(_)
                    ) {
                        return Err(QueryPlanError::UnsupportedPredicate);
                    }
                    if self.expression_type(
                        input.expression(),
                        facts,
                        allow_outputs,
                    )? != input.value_type()
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                }
                Ok(postgres.result_type())
            }
        }
    }

    const fn boolean_type() -> ExprType {
        ExprType {
            type_oid: pg_sys::BOOLOID,
            typmod: -1,
            collation: pg_sys::InvalidOid,
        }
    }
}
