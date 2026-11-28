//! One-time expression and output validation for decoded query plans.

mod decimal;

use std::mem;

use lagodb_core::expr::{
    ColumnRef, ExprType, PgBpcharSemantics, PgComparisonOp, PgIntegerWidening,
    PgTextComparisonSemantics, RuntimeValueLayout,
};
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

use super::{
    CaseWhen, Decimal128Semantics, ExecutionExpr, ExecutionScalarRepr,
    FixedDecimalType, NumericRepr, QueryNode, QueryPlanError, RelationValueRef,
    ScalarFunctionKind, ScalarSemantics,
};

struct NodeFacts {
    columns: Vec<ColumnRef>,
    outputs: Vec<Option<OutputFact>>,
}

#[derive(Clone, Copy)]
struct OutputFact {
    value_type: ExprType,
    execution_type: ExprType,
    /// Whether a predicate over the defining node's rows may read the
    /// output: aggregate groups and results, not DISTINCT keys or projected
    /// results.
    predicate_readable: bool,
    nullable: bool,
}

impl OutputFact {
    /// HAVING normally follows PostgreSQL's declared output type. NUMERIC
    /// group keys and decimal MIN/MAX, SUM and AVG(numeric) are the
    /// exceptions: PostgreSQL erases their typmod, while DataFusion emits a
    /// fixed decimal representation.
    fn predicate_type(self) -> ExprType {
        if FixedDecimalType::for_type(self.execution_type).is_some() {
            self.execution_type
        } else {
            self.value_type
        }
    }
}

pub(in crate::plan) struct PlanExpressionValidator<'a> {
    runtime_values: &'a RuntimeValueLayout,
    output_count: usize,
}

impl<'a> PlanExpressionValidator<'a> {
    pub(in crate::plan) const fn new(
        runtime_values: &'a RuntimeValueLayout,
        output_count: usize,
    ) -> Self {
        Self {
            runtime_values,
            output_count,
        }
    }

    pub(in crate::plan) fn validate(
        &self,
        node: &QueryNode,
    ) -> Result<(), QueryPlanError> {
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
                    self.validate_boolean(filter, &facts, ScalarSemantics::Exact)?;
                }
                Ok(facts)
            }
            QueryNode::Aggregate(aggregate) => {
                let input = self.validate_node(aggregate.input())?;
                let mut outputs = vec![None; self.output_count];
                for group in aggregate.groups() {
                    let ty = self.expression_type(
                        group.expression(),
                        &input,
                        ScalarSemantics::Exact,
                    )?;
                    if ty != group.result_type() {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    outputs[group.output().index()] = Some(OutputFact {
                        value_type: group.result_type(),
                        execution_type: group.execution_type(),
                        predicate_readable: true,
                        nullable: true,
                    });
                }
                for aggregate in aggregate.aggregates() {
                    let argument_type = aggregate
                        .argument()
                        .map(|argument| {
                            self.aggregate_argument_type(argument, &input)
                        })
                        .transpose()?;
                    aggregate.validate_argument_type(argument_type)?;
                    for order in aggregate.order_by() {
                        let _ = self.expression_type(
                            order.expression(),
                            &input,
                            ScalarSemantics::Exact,
                        )?;
                    }
                    if let Some(filter) = aggregate.filter() {
                        self.validate_boolean(
                            filter,
                            &input,
                            ScalarSemantics::Exact,
                        )?;
                    }
                    outputs[aggregate.output().index()] = Some(OutputFact {
                        value_type: aggregate.result_type(),
                        execution_type: aggregate.execution_result_type(),
                        predicate_readable: true,
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
                    let left_type = Self::relation_value_type(key.left(), &left)?;
                    let right_type = Self::relation_value_type(key.right(), &right)?;
                    if left_type != right_type
                        || key.validate_semantics()? != left_type
                    {
                        return Err(QueryPlanError::MismatchedJoinKey);
                    }
                }
                let mut joined_columns = left.columns.clone();
                joined_columns.extend_from_slice(&right.columns);
                // Topology validation proves the inputs' outputs disjoint.
                let joined = NodeFacts {
                    columns: joined_columns,
                    outputs: left
                        .outputs
                        .iter()
                        .zip(&right.outputs)
                        .map(|(left, right)| left.or(*right))
                        .collect(),
                };
                if let Some(filter) = join.on_filter() {
                    self.validate_boolean(filter, &joined, ScalarSemantics::Exact)?;
                }
                if let Some(filter) = join.mark_filter() {
                    let _ = Self::column_type(filter.null_test(), &left)?;
                }
                if join.join_type().emits_right() {
                    Ok(joined)
                } else {
                    Ok(left)
                }
            }
            QueryNode::Distinct(distinct) => {
                let input = self.validate_node(distinct.input())?;
                let mut outputs = vec![None; self.output_count];
                for key in distinct.keys() {
                    let value_type = self.expression_type(
                        key.expression(),
                        &input,
                        ScalarSemantics::Exact,
                    )?;
                    if value_type != key.result_type() {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    outputs[key.output().index()] = Some(OutputFact {
                        value_type,
                        execution_type: value_type,
                        predicate_readable: false,
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
                // HAVING compares aggregate results in PostgreSQL's declared
                // types; every other filter compares row values.
                let domain = if matches!(filter.input(), QueryNode::Aggregate(_)) {
                    ScalarSemantics::Having
                } else {
                    ScalarSemantics::Exact
                };
                self.validate_boolean(filter.predicate(), &input, domain)?;
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
                        expression => (
                            self.expression_type(
                                expression,
                                &input,
                                ScalarSemantics::Exact,
                            )?,
                            true,
                        ),
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
                        predicate_readable: false,
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

    fn relation_value_type(
        value: RelationValueRef,
        facts: &NodeFacts,
    ) -> Result<ExprType, QueryPlanError> {
        match value {
            RelationValueRef::Column(column) => Self::column_type(column, facts),
            RelationValueRef::Output { output, value_type } => facts
                .outputs
                .get(output.index())
                .and_then(|fact| *fact)
                .filter(|fact| fact.value_type == value_type)
                .map(|_| value_type)
                .ok_or(QueryPlanError::MismatchedJoinKey),
        }
    }

    fn validate_boolean(
        &self,
        expression: &ExecutionExpr,
        facts: &NodeFacts,
        domain: ScalarSemantics,
    ) -> Result<(), QueryPlanError> {
        if self.expression_type(expression, facts, domain)?.type_oid
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
        domain: ScalarSemantics,
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
            ExecutionExpr::DecimalValue {
                value,
                decimal_type,
            } => {
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
                Ok(decimal_type.value_type())
            }
            ExecutionExpr::DecimalArithmetic {
                operator,
                left,
                right,
                result_type,
            } => self.validate_decimal_arithmetic(
                *operator,
                [left, right],
                *result_type,
                facts,
                domain,
            ),
            ExecutionExpr::IntegerToDecimal { value, result_type } => {
                self.validate_integer_to_decimal(value, *result_type, facts, domain)
            }
            ExecutionExpr::DecimalBoundComparison {
                operand,
                bound,
                operand_type,
                ..
            } => self.validate_decimal_bound_comparison(
                operand,
                *bound,
                *operand_type,
                facts,
                domain,
            ),
            ExecutionExpr::BpcharValue { value, semantics } => {
                let spec = self
                    .runtime_values
                    .values()
                    .get(value.index())
                    .ok_or(QueryPlanError::RuntimeValueOutOfBounds)?;
                if spec.value_type.type_oid != pg_sys::BPCHAROID
                    || !spec.source_kind.is_static()
                {
                    return Err(QueryPlanError::UnsupportedRuntimeValueType);
                }
                Ok(ExprType {
                    typmod: semantics.typmod(),
                    ..spec.value_type
                })
            }
            ExecutionExpr::Output(output) => {
                let fact = self.predicate_output(*output, facts)?;
                Ok(fact.predicate_type())
            }
            ExecutionExpr::DecimalOutput {
                output,
                decimal_type,
            } => {
                let fact = self.predicate_output(*output, facts)?;
                if FixedDecimalType::for_type(fact.execution_type)
                    != Some(*decimal_type)
                {
                    return Err(QueryPlanError::TupleLayoutTypeMismatch);
                }
                Ok(fact.predicate_type())
            }
            ExecutionExpr::Comparison {
                operator,
                left: left_expression,
                right: right_expression,
            } => {
                let left = self.expression_type(left_expression, facts, domain)?;
                let right = self.expression_type(right_expression, facts, domain)?;
                // A built-in NUMERIC operator compares the operands' engine
                // representations, which PostgreSQL's value types leave
                // unbounded.
                if domain.numeric_comparison(*operator).is_some() {
                    self.numeric_repr(left_expression, facts)
                        .zip(self.numeric_repr(right_expression, facts))
                        .and_then(|(left, right)| {
                            domain.numeric_repr_comparison(*operator, left, right)
                        })
                        .ok_or(QueryPlanError::UnsupportedPredicate)?;
                    return Ok(Self::boolean_type());
                }
                if left.type_oid == pg_sys::BPCHAROID {
                    Self::validate_bpchar_comparison(
                        domain,
                        *operator,
                        (left_expression, left),
                        (right_expression, right),
                    )?;
                    return Ok(Self::boolean_type());
                }
                domain
                    .comparison(*operator, left, right)
                    .ok_or(QueryPlanError::UnsupportedPredicate)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::IsNull(value) | ExecutionExpr::IsNotNull(value) => {
                let _ = self.expression_type(value, facts, domain)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::IsNan(value) | ExecutionExpr::IsNotNan(value) => {
                let value_type = self.expression_type(value, facts, domain)?;
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
                let value_type = self.expression_type(value, facts, domain)?;
                // Exactly the planner's producers: NaN comparisons over floats,
                // date/timestamp comparisons that no date can satisfy, and
                // character(n) equality with a constant wider than the column.
                let produced = match value_type.type_oid {
                    pg_sys::FLOAT4OID | pg_sys::FLOAT8OID | pg_sys::DATEOID => {
                        value_type.typmod == -1
                            && value_type.collation == pg_sys::InvalidOid
                    }
                    pg_sys::BPCHAROID => {
                        PgBpcharSemantics::for_type(value_type).is_some()
                    }
                    _ => false,
                };
                if !produced {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::BooleanTest { value, .. } => {
                self.validate_boolean(value, facts, domain)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::And(children) | ExecutionExpr::Or(children) => {
                if children.is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for child in children {
                    self.validate_boolean(child, facts, domain)?;
                }
                Ok(Self::boolean_type())
            }
            ExecutionExpr::Not(child) => {
                self.validate_boolean(child, facts, domain)?;
                Ok(Self::boolean_type())
            }
            ExecutionExpr::Relabel { value, result_type } => {
                let input_type = self.expression_type(value, facts, domain)?;
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
                let input_type = self.expression_type(value, facts, domain)?;
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
                // JSONBOID does not establish one physical Arrow type for
                // all branches: a struct and a map are both valid JSONB
                // outputs. Materializing either is supported, but combining
                // them inside DataFusion requires a separate semantic rule.
                if when_then.is_empty() || result_type.type_oid == pg_sys::JSONBOID {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                if let Some(common) = expression.decimal_type() {
                    for branch in when_then {
                        self.validate_boolean(branch.when(), facts, domain)?;
                    }
                    self.validate_decimal_branches(
                        when_then
                            .iter()
                            .map(CaseWhen::then)
                            .chain(else_expr.as_deref()),
                        common,
                        *result_type,
                        facts,
                        domain,
                    )?;
                    return Ok(*result_type);
                }
                for branch in when_then {
                    self.validate_boolean(branch.when(), facts, domain)?;
                    if self.expression_type(branch.then(), facts, domain)?
                        != *result_type
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                }
                if let Some(else_expr) = else_expr
                    && self.expression_type(else_expr, facts, domain)? != *result_type
                {
                    return Err(QueryPlanError::TupleLayoutTypeMismatch);
                }
                Ok(*result_type)
            }
            ExecutionExpr::InList { value, list, .. } => {
                if list.is_empty() {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                let value_type = self.expression_type(value, facts, domain)?;
                if value_type.type_oid == pg_sys::BPCHAROID {
                    for item in list {
                        self.expression_type(item, facts, domain)?;
                    }
                    Self::validate_bpchar_in_list((value, value_type), list)?;
                    return Ok(Self::boolean_type());
                }
                // An integer NUMERIC value matches integers: int4 values bound
                // from the NUMERIC list, or the integers under its casts.
                let is_integer = |repr| matches!(repr, Some(NumericRepr::Integer(_)));
                if value_type.type_oid == pg_sys::NUMERICOID
                    && is_integer(self.numeric_repr(value, facts))
                {
                    for item in list {
                        self.expression_type(item, facts, domain)?;
                        if !is_integer(self.numeric_repr(item, facts)) {
                            return Err(QueryPlanError::UnsupportedPredicate);
                        }
                    }
                    return Ok(Self::boolean_type());
                }
                if !ScalarSemantics::Exact.supports_type(value_type) {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                let decimal = Decimal128Semantics::for_type(value_type)
                    .map(FixedDecimalType::from);
                if decimal
                    .is_some_and(|decimal| value.decimal_type() != Some(decimal))
                {
                    return Err(QueryPlanError::UnsupportedPredicate);
                }
                for item in list {
                    let item_type = self.expression_type(item, facts, domain)?;
                    if item_type != value_type
                        && !(value_type.type_oid == pg_sys::TEXTOID
                            && item_type.type_oid == pg_sys::TEXTOID
                            && item_type.typmod == value_type.typmod)
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                    if decimal
                        .is_some_and(|decimal| item.decimal_type() != Some(decimal))
                    {
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
                if matches!(
                    kind,
                    ScalarFunctionKind::Add
                        | ScalarFunctionKind::Subtract
                        | ScalarFunctionKind::Multiply
                        | ScalarFunctionKind::Divide
                ) && result_type.type_oid == pg_sys::NUMERICOID
                {
                    return self.validate_float_numeric_arithmetic(
                        *kind,
                        arguments,
                        *input_collation,
                        *result_type,
                        facts,
                        domain,
                    );
                }
                if *kind == ScalarFunctionKind::Coalesce
                    && let Some(common) = expression.decimal_type()
                {
                    self.validate_decimal_branches(
                        arguments.iter(),
                        common,
                        *result_type,
                        facts,
                        domain,
                    )?;
                    return Ok(*result_type);
                }
                let mut argument_types = Vec::with_capacity(arguments.len());
                for argument in arguments {
                    argument_types
                        .push(self.expression_type(argument, facts, domain)?);
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
                    if self.expression_type(input.expression(), facts, domain)?
                        != input.value_type()
                    {
                        return Err(QueryPlanError::TupleLayoutTypeMismatch);
                    }
                }
                Ok(postgres.result_type())
            }
        }
    }

    /// The fact of an output a predicate reads. Only outputs the input node
    /// emits are visible, and only aggregate outputs may be read.
    fn predicate_output(
        &self,
        output: OutputId,
        facts: &NodeFacts,
    ) -> Result<OutputFact, QueryPlanError> {
        let fact = facts
            .outputs
            .get(output.index())
            .and_then(|fact| *fact)
            .ok_or(QueryPlanError::OutputOutOfBounds {
                index: output.index(),
            })?;
        if !fact.predicate_readable {
            return Err(QueryPlanError::UnsupportedPredicate);
        }
        Ok(fact)
    }

    const fn boolean_type() -> ExprType {
        ExprType {
            type_oid: pg_sys::BOOLOID,
            typmod: -1,
            collation: pg_sys::InvalidOid,
        }
    }

    /// A `bpchar` comparison holds only between a `character(n)` column and a
    /// value padded to that column's width.
    fn validate_bpchar_comparison(
        semantics: ScalarSemantics,
        operator: PgComparisonOp,
        left: (&ExecutionExpr, ExprType),
        right: (&ExecutionExpr, ExprType),
    ) -> Result<(), QueryPlanError> {
        let ((column, padded), value) = match (left.0, right.0) {
            (
                ExecutionExpr::Column(_),
                ExecutionExpr::BpcharValue { semantics, .. },
            ) => ((left.1, *semantics), right.1),
            (
                ExecutionExpr::BpcharValue { semantics, .. },
                ExecutionExpr::Column(_),
            ) => ((right.1, *semantics), left.1),
            _ => return Err(QueryPlanError::UnsupportedPredicate),
        };
        match semantics.bpchar_comparison(operator, column, value) {
            Some((_, proven)) if proven == padded => Ok(()),
            _ => Err(QueryPlanError::UnsupportedPredicate),
        }
    }

    /// A `bpchar` membership holds only between a `character(n)` column under
    /// a deterministic collation and values padded to that column's width.
    fn validate_bpchar_in_list(
        (value, value_type): (&ExecutionExpr, ExprType),
        list: &[ExecutionExpr],
    ) -> Result<(), QueryPlanError> {
        let column = PgBpcharSemantics::for_type(value_type)
            .filter(|_| {
                matches!(value, ExecutionExpr::Column(_))
                    && unsafe {
                        PgTextComparisonSemantics::for_equality_collation(
                            value_type.collation,
                        )
                    }
                    .is_some()
            })
            .ok_or(QueryPlanError::UnsupportedPredicate)?;
        if list.iter().all(|item| {
            matches!(
                item,
                ExecutionExpr::BpcharValue { semantics, .. } if *semantics == column
            )
        }) {
            Ok(())
        } else {
            Err(QueryPlanError::UnsupportedPredicate)
        }
    }
}
