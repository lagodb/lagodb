//! One-time semantic validation for decoded query plans.

use std::mem;

use lagodb_core::expr::{ColumnRef, ExprType, PgIntegerWidening, RuntimeValueLayout};
use pgrx::pg_sys;

use super::{
    Decimal128Semantics, ExecutionExpr, ExecutionScalarRepr, QueryNode,
    QueryPlanError, ScalarSemantics,
};

struct NodeFacts {
    columns: Vec<ColumnRef>,
    outputs: Vec<Option<OutputFact>>,
}

#[derive(Clone, Copy)]
struct OutputFact {
    value_type: ExprType,
    execution_type: ExprType,
    supports_having: bool,
    nullable: bool,
}

impl OutputFact {
    /// HAVING normally follows PostgreSQL's declared aggregate type. Decimal
    /// MIN/MAX are the one current exception: PostgreSQL erases their typmod,
    /// while DataFusion retains the bounded Decimal128 input representation.
    fn predicate_type(self) -> ExprType {
        if Decimal128Semantics::for_type(self.execution_type).is_some() {
            self.execution_type
        } else {
            self.value_type
        }
    }
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
                        execution_type: group.result_type(),
                        supports_having: true,
                        nullable: true,
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
                        execution_type: aggregate.execution_result_type(),
                        supports_having: aggregate.supports_having_result(),
                        nullable: aggregate.nullable(),
                    });
                }
                Ok(NodeFacts {
                    columns: Vec::new(),
                    outputs,
                })
            }
            QueryNode::Join(join) => {
                let left = self.validate_node(join.left())?;
                let right = self.validate_node(join.right())?;
                for key in join.keys() {
                    let left_type = Self::column_type(key.left(), &left)?;
                    let right_type = Self::column_type(key.right(), &right)?;
                    if left_type != right_type
                        || key.validate_semantics()? != left_type
                    {
                        return Err(QueryPlanError::MismatchedJoinKey);
                    }
                }
                let mut joined_columns = left.columns.clone();
                joined_columns.extend_from_slice(&right.columns);
                let joined = NodeFacts {
                    columns: joined_columns,
                    outputs: vec![None; self.output_count],
                };
                if let Some(filter) = join.on_filter() {
                    self.validate_boolean(filter, &joined, false)?;
                }
                if let Some(filter) = join.mark_filter() {
                    let _ = Self::column_type(filter.null_test(), &left)?;
                }
                if join.join_type().emits_right() {
                    Ok(joined)
                } else {
                    Ok(NodeFacts {
                        columns: left.columns,
                        outputs: vec![None; self.output_count],
                    })
                }
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
                        execution_type: value_type,
                        supports_having: false,
                        nullable: true,
                    });
                }
                Ok(NodeFacts {
                    columns: Vec::new(),
                    outputs,
                })
            }
            QueryNode::Filter(filter) => {
                let input = self.validate_node(filter.input())?;
                let allow_outputs = input.columns.is_empty();
                self.validate_boolean(filter.predicate(), &input, allow_outputs)?;
                Ok(input)
            }
            QueryNode::Project(project) => {
                let input = self.validate_node(project.input())?;
                let mut outputs = vec![None; self.output_count];
                let projects_semantic_outputs = matches!(
                    project.input(),
                    QueryNode::Aggregate(_) | QueryNode::Distinct(_)
                ) || matches!(
                    project.input(),
                    QueryNode::Filter(filter)
                        if matches!(filter.input(), QueryNode::Aggregate(_))
                );
                for expression in project.expressions() {
                    let supported = if projects_semantic_outputs {
                        matches!(expression.expression(), ExecutionExpr::Output(_))
                    } else {
                        !matches!(expression.expression(), ExecutionExpr::Output(_))
                    };
                    if !supported {
                        return Err(QueryPlanError::UnsupportedTopology);
                    }
                    let (result_type, nullable) = match expression.expression() {
                        ExecutionExpr::Output(output) => {
                            let fact = input
                                .outputs
                                .get(output.index())
                                .and_then(|fact| *fact)
                                .ok_or(QueryPlanError::OutputOutOfBounds {
                                    index: output.index(),
                                })?;
                            (fact.value_type, fact.nullable)
                        }
                        expression => {
                            (self.expression_type(expression, &input, false)?, true)
                        }
                    };
                    if result_type != expression.result_type() {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    if nullable != expression.nullable() {
                        return Err(QueryPlanError::TupleLayoutNullabilityMismatch);
                    }
                    outputs[expression.output().index()] = Some(OutputFact {
                        value_type: result_type,
                        execution_type: result_type,
                        supports_having: false,
                        nullable,
                    });
                }
                Ok(NodeFacts {
                    columns: Vec::new(),
                    outputs,
                })
            }
            QueryNode::Sort(sort) => {
                let input = self.validate_node(sort.input())?;
                for key in sort.keys() {
                    let fact = input
                        .outputs
                        .get(key.output().index())
                        .and_then(|fact| *fact)
                        .ok_or(QueryPlanError::OutputOutOfBounds {
                            index: key.output().index(),
                        })?;
                    if fact.value_type != key.result_type() {
                        return Err(QueryPlanError::UnsupportedSortKey);
                    }
                }
                Ok(input)
            }
            QueryNode::Limit(limit) => {
                let input = self.validate_node(limit.input())?;
                for value in [limit.offset(), limit.count()].into_iter().flatten() {
                    let spec = self
                        .runtime_values
                        .values()
                        .get(value.index())
                        .ok_or(QueryPlanError::RuntimeValueOutOfBounds)?;
                    if spec.value_type.type_oid != pg_sys::INT8OID
                        || spec.value_type.typmod != -1
                        || spec.value_type.collation != pg_sys::InvalidOid
                    {
                        return Err(QueryPlanError::UnsupportedRuntimeValueType);
                    }
                }
                Ok(input)
            }
        }
    }

    fn column_type(
        column: ColumnRef,
        facts: &NodeFacts,
    ) -> Result<ExprType, QueryPlanError> {
        facts
            .columns
            .iter()
            .find(|candidate| candidate.same_storage_column(column))
            .map(|_| column.value_type)
            .ok_or(QueryPlanError::MismatchedJoinKey)
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
            ExecutionExpr::DecimalValue { value, semantics } => {
                let spec = self
                    .runtime_values
                    .values()
                    .get(value.index())
                    .ok_or(QueryPlanError::RuntimeValueOutOfBounds)?;
                if spec.value_type.type_oid != pg_sys::NUMERICOID
                    || spec.value_type.collation != pg_sys::InvalidOid
                    || !spec.source_kind.is_static()
                {
                    return Err(QueryPlanError::UnsupportedRuntimeValueType);
                }
                Ok(semantics.value_type())
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
                Ok(fact.predicate_type())
            }
            ExecutionExpr::Output(output) => Err(QueryPlanError::OutputOutOfBounds {
                index: output.index(),
            }),
            ExecutionExpr::Comparison {
                operator,
                left: left_expression,
                right: right_expression,
            } => {
                let left =
                    self.expression_type(left_expression, facts, allow_outputs)?;
                let right =
                    self.expression_type(right_expression, facts, allow_outputs)?;
                let semantics = if allow_outputs {
                    ScalarSemantics::Having
                } else {
                    ScalarSemantics::Exact
                };
                semantics
                    .comparison(*operator, left, right)
                    .ok_or(QueryPlanError::UnsupportedPredicate)?;
                if let Some((_, decimal)) =
                    semantics.decimal_comparison(*operator, left, right)
                    && (!Self::has_decimal128_representation(
                        left_expression,
                        decimal,
                    ) || !Self::has_decimal128_representation(
                        right_expression,
                        decimal,
                    ))
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::IsNull(value) | ExecutionExpr::IsNotNull(value) => {
                let _ = self.expression_type(value, facts, allow_outputs)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::IsNan(value) | ExecutionExpr::IsNotNan(value) => {
                let value_type = self.expression_type(value, facts, allow_outputs)?;
                if !matches!(
                    value_type.type_oid,
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                ) || value_type.typmod != -1
                    || value_type.collation != pg_sys::InvalidOid
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::StrictTrue(value) | ExecutionExpr::StrictFalse(value) => {
                if !matches!(value.as_ref(), ExecutionExpr::Column(_)) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                let value_type = self.expression_type(value, facts, allow_outputs)?;
                if !matches!(
                    value_type.type_oid,
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
                ) || value_type.typmod != -1
                    || value_type.collation != pg_sys::InvalidOid
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
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
            ExecutionExpr::WidenInteger { value, result_type } => {
                let input_type = self.expression_type(value, facts, allow_outputs)?;
                if input_type.typmod != -1
                    || result_type.typmod != -1
                    || input_type.collation != pg_sys::InvalidOid
                    || result_type.collation != pg_sys::InvalidOid
                    || PgIntegerWidening::for_types(
                        input_type.type_oid,
                        result_type.type_oid,
                    )
                    .is_none()
                {
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
                if !ScalarSemantics::Exact.supports_type(value_type) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                let decimal = Decimal128Semantics::for_type(value_type);
                if decimal.is_some_and(|decimal| {
                    !Self::has_decimal128_representation(value, decimal)
                }) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for item in list {
                    let item_type =
                        self.expression_type(item, facts, allow_outputs)?;
                    if item_type != value_type
                        && !(value_type.type_oid == pg_sys::TEXTOID
                            && item_type.type_oid == pg_sys::TEXTOID
                            && item_type.typmod == value_type.typmod)
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    if decimal.is_some_and(|decimal| {
                        !Self::has_decimal128_representation(item, decimal)
                    }) {
                        return Err(QueryPlanError::UnsupportedPredicate);
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

    fn has_decimal128_representation(
        expression: &ExecutionExpr,
        semantics: Decimal128Semantics,
    ) -> bool {
        matches!(expression, ExecutionExpr::Output(_))
            || expression.decimal128_semantics() == Some(semantics)
    }
}
