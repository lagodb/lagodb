//! User-facing formatting for expressions in the logical EXPLAIN tree.

use std::ffi::CStr;
use std::fmt::{self, Write};

use lagodb_core::expr::{ColumnRef, PgComparisonSignature};
use lagodb_query::plan::{
    AggCall, AggregateArguments, AggregateKind, ComparisonKind, ExecutionExpr,
    OutputId, PostgresExprVolatility, QueryNode, SortDirection,
};
use pgrx::pg_sys;

use super::plan::ScanExplainMetadata;

pub(super) struct ExpressionFormatter<'a> {
    scans: &'a [ScanExplainMetadata],
    verbose: bool,
}

impl<'a> ExpressionFormatter<'a> {
    pub(super) const fn new(scans: &'a [ScanExplainMetadata], verbose: bool) -> Self {
        Self { scans, verbose }
    }

    pub(super) fn column(&self, column: ColumnRef) -> ColumnExplain<'_> {
        ColumnExplain {
            column,
            metadata: &self.scans[column.scan.index()],
            qualify: self.scans.len() != 1,
        }
    }

    pub(super) fn expression<'expression>(
        &self,
        expression: &'expression ExecutionExpr,
    ) -> ExpressionExplain<'_, 'a, 'expression> {
        ExpressionExplain {
            formatter: self,
            expression,
            output_source: None,
        }
    }

    pub(super) fn expression_from<'expression>(
        &self,
        expression: &'expression ExecutionExpr,
        output_source: &'expression QueryNode,
    ) -> ExpressionExplain<'_, 'a, 'expression> {
        ExpressionExplain {
            formatter: self,
            expression,
            output_source: Some(output_source),
        }
    }

    pub(super) fn output<'node>(
        &self,
        node: &'node QueryNode,
        output: OutputId,
    ) -> OutputExplain<'_, 'a, 'node> {
        OutputExplain {
            formatter: self,
            node,
            output,
        }
    }

    pub(super) fn aggregate(&self, aggregate: &AggCall) -> String {
        let name = match aggregate.kind() {
            AggregateKind::Count => "COUNT",
            AggregateKind::Min => "MIN",
            AggregateKind::Max => "MAX",
            AggregateKind::Sum | AggregateKind::NumericSum => "SUM",
            AggregateKind::Average | AggregateKind::NumericAverage => "AVG",
            AggregateKind::VarianceSample => "VAR_SAMP",
            AggregateKind::VariancePopulation => "VAR_POP",
            AggregateKind::StddevSample => "STDDEV_SAMP",
            AggregateKind::StddevPopulation => "STDDEV_POP",
            AggregateKind::BoolAnd => "BOOL_AND",
            AggregateKind::BoolOr => "BOOL_OR",
            AggregateKind::ArrayAgg => "ARRAY_AGG",
            AggregateKind::StringAgg => "STRING_AGG",
        };
        let arguments = match aggregate.arguments() {
            AggregateArguments::None => "*".to_owned(),
            AggregateArguments::Unary(value) => self.expression(value).to_string(),
            AggregateArguments::StringAgg { value, delimiter } => {
                // SAFETY: `delimiter` is a live C string. PostgreSQL returns a
                // current-context SQL literal which is copied into this value.
                let quoted =
                    unsafe { pg_sys::quote_literal_cstr(delimiter.as_ptr()) };
                let quoted = unsafe { CStr::from_ptr(quoted) }.to_string_lossy();
                format!("{}, {quoted}", self.expression(value))
            }
        };
        let distinct = if aggregate.is_distinct() {
            "DISTINCT "
        } else {
            ""
        };
        let order_by = if aggregate.order_by().is_empty() {
            String::new()
        } else {
            let expressions = aggregate
                .order_by()
                .iter()
                .map(|order| {
                    let direction = match order.direction() {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    let nulls = if order.nulls_first() {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    };
                    format!(
                        "{} {direction} {nulls}",
                        self.expression(order.expression())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {expressions}")
        };
        let filter = aggregate.filter().map_or_else(String::new, |predicate| {
            format!(" FILTER (WHERE {})", self.expression(predicate))
        });
        format!("{name}({distinct}{arguments}{order_by}){filter}")
    }
}

pub(super) struct OutputExplain<'formatter, 'plan, 'node> {
    formatter: &'formatter ExpressionFormatter<'plan>,
    node: &'node QueryNode,
    output: OutputId,
}

impl fmt::Display for OutputExplain<'_, '_, '_> {
    fn fmt(&self, target: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.formatter.write_output(target, self.node, self.output)
    }
}

pub(super) struct ColumnExplain<'a> {
    column: ColumnRef,
    metadata: &'a ScanExplainMetadata,
    qualify: bool,
}

impl fmt::Display for ColumnExplain<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.qualify {
            Self::write_identifier(output, self.metadata.alias())?;
            output.write_char('.')?;
        }
        Self::write_identifier(output, self.metadata.column_name(self.column.attno))
    }
}

impl ColumnExplain<'_> {
    fn write_identifier(
        output: &mut fmt::Formatter<'_>,
        identifier: &CStr,
    ) -> fmt::Result {
        // SAFETY: catalog and RTE identifiers are live C strings. PostgreSQL
        // returns either the same pointer or a current-context quoted copy.
        let quoted = unsafe { pg_sys::quote_identifier(identifier.as_ptr()) };
        output.write_str(&unsafe { CStr::from_ptr(quoted) }.to_string_lossy())
    }
}

pub(super) struct ExpressionExplain<'formatter, 'plan, 'expression> {
    formatter: &'formatter ExpressionFormatter<'plan>,
    expression: &'expression ExecutionExpr,
    output_source: Option<&'expression QueryNode>,
}

impl fmt::Display for ExpressionExplain<'_, '_, '_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nested = |expression| ExpressionExplain {
            formatter: self.formatter,
            expression,
            output_source: self.output_source,
        };
        match self.expression {
            ExecutionExpr::StrictTrue(value) => {
                write!(output, "({0} = {0})", nested(value))
            }
            ExecutionExpr::StrictFalse(value) => {
                write!(output, "({0} > {0})", nested(value))
            }
            ExecutionExpr::Column(column) => {
                write!(output, "{}", self.formatter.column(*column))
            }
            ExecutionExpr::Value(value) if self.formatter.verbose => {
                write!(output, "Runtime Binding {}", value.index() + 1)
            }
            ExecutionExpr::Value(_) => output.write_str("runtime value"),
            ExecutionExpr::DecimalValue { value, semantics }
                if self.formatter.verbose =>
            {
                write!(
                    output,
                    "Runtime Binding {}::decimal({}, {})",
                    value.index() + 1,
                    semantics.precision(),
                    semantics.scale()
                )
            }
            ExecutionExpr::DecimalValue { semantics, .. } => write!(
                output,
                "runtime value::decimal({}, {})",
                semantics.precision(),
                semantics.scale()
            ),
            ExecutionExpr::Output(value) => self.formatter.write_output(
                output,
                self.output_source
                    .expect("validated output expression has an input producer"),
                *value,
            ),
            ExecutionExpr::Comparison {
                operator,
                left,
                right,
            } => {
                let signature = PgComparisonSignature::for_operator(operator.opno)
                    .expect(
                        "validated comparison has a supported PostgreSQL operator",
                    );
                let symbol = match signature.kind() {
                    ComparisonKind::Equal => "=",
                    ComparisonKind::NotEqual => "<>",
                    ComparisonKind::Less => "<",
                    ComparisonKind::LessEqual => "<=",
                    ComparisonKind::Greater => ">",
                    ComparisonKind::GreaterEqual => ">=",
                };
                write!(output, "({} {symbol} {})", nested(left), nested(right))
            }
            ExecutionExpr::IsNull(value) => {
                write!(output, "({} IS NULL)", nested(value))
            }
            ExecutionExpr::IsNotNull(value) => {
                write!(output, "({} IS NOT NULL)", nested(value))
            }
            ExecutionExpr::IsNan(value) => {
                write!(output, "({} IS NAN)", nested(value))
            }
            ExecutionExpr::IsNotNan(value) => {
                write!(output, "({} IS NOT NAN)", nested(value))
            }
            ExecutionExpr::BooleanTest { kind, value } => {
                write!(output, "({} IS {kind})", nested(value))
            }
            ExecutionExpr::And(children) => self.list(output, children, " AND "),
            ExecutionExpr::Or(children) => self.list(output, children, " OR "),
            ExecutionExpr::Not(value) => write!(output, "NOT ({})", nested(value)),
            ExecutionExpr::Relabel { value, result_type }
            | ExecutionExpr::WidenInteger { value, result_type } => {
                let type_name = unsafe {
                    pg_sys::format_type_with_typemod(
                        result_type.type_oid,
                        result_type.typmod,
                    )
                };
                let type_name =
                    unsafe { CStr::from_ptr(type_name) }.to_string_lossy();
                write!(output, "({})::{type_name}", nested(value))
            }
            ExecutionExpr::Case {
                when_then,
                else_expr,
                ..
            } => {
                output.write_str("CASE")?;
                for branch in when_then {
                    write!(
                        output,
                        " WHEN {} THEN {}",
                        nested(branch.when()),
                        nested(branch.then())
                    )?;
                }
                if let Some(value) = else_expr {
                    write!(output, " ELSE {}", nested(value))?;
                }
                output.write_str(" END")
            }
            ExecutionExpr::InList {
                value,
                list,
                negated,
            } => {
                write!(
                    output,
                    "{} {}IN (",
                    nested(value),
                    if *negated { "NOT " } else { "" }
                )?;
                for (index, item) in list.iter().enumerate() {
                    if index != 0 {
                        output.write_str(", ")?;
                    }
                    write!(output, "{}", nested(item))?;
                }
                output.write_char(')')
            }
            ExecutionExpr::Function {
                kind, arguments, ..
            } => {
                write!(output, "{kind}(")?;
                for (index, argument) in arguments.iter().enumerate() {
                    if index != 0 {
                        output.write_str(", ")?;
                    }
                    write!(output, "{}", nested(argument))?;
                }
                output.write_char(')')
            }
            ExecutionExpr::Postgres(_) if !self.formatter.verbose => {
                output.write_str("PostgreSQL expression")
            }
            ExecutionExpr::Postgres(expression) => {
                let volatility = match expression.volatility() {
                    PostgresExprVolatility::Immutable => "immutable",
                    PostgresExprVolatility::Stable => "stable",
                    PostgresExprVolatility::Volatile => "volatile",
                };
                write!(output, "PostgreSQL {volatility} expression")?;
                if expression.inputs().is_empty() {
                    return Ok(());
                }
                output.write_str(" over ")?;
                for (index, input) in expression.inputs().iter().enumerate() {
                    if index != 0 {
                        output.write_str(", ")?;
                    }
                    write!(output, "{}", nested(input.expression()))?;
                }
                Ok(())
            }
        }
    }
}

impl ExpressionExplain<'_, '_, '_> {
    fn list(
        &self,
        output: &mut fmt::Formatter<'_>,
        children: &[ExecutionExpr],
        separator: &str,
    ) -> fmt::Result {
        output.write_char('(')?;
        for (index, child) in children.iter().enumerate() {
            if index != 0 {
                output.write_str(separator)?;
            }
            write!(
                output,
                "{}",
                ExpressionExplain {
                    formatter: self.formatter,
                    expression: child,
                    output_source: self.output_source,
                }
            )?;
        }
        output.write_char(')')
    }
}

impl ExpressionFormatter<'_> {
    fn write_output(
        &self,
        target: &mut fmt::Formatter<'_>,
        node: &QueryNode,
        output: OutputId,
    ) -> fmt::Result {
        match node {
            QueryNode::Aggregate(aggregate) => {
                if let Some(group) = aggregate
                    .groups()
                    .iter()
                    .find(|group| group.output() == output)
                {
                    return write!(target, "{}", self.expression(group.expression()));
                }
                let aggregate = aggregate
                    .aggregates()
                    .iter()
                    .find(|aggregate| aggregate.output() == output)
                    .expect("validated aggregate output has a producer");
                target.write_str(&self.aggregate(aggregate))
            }
            QueryNode::Distinct(distinct) => {
                let key = distinct
                    .keys()
                    .iter()
                    .find(|key| key.output() == output)
                    .expect("validated distinct output has a producer");
                write!(target, "{}", self.expression(key.expression()))
            }
            QueryNode::Project(project) => {
                let expression = project
                    .expressions()
                    .iter()
                    .find(|expression| expression.output() == output)
                    .expect("validated projection output has a producer");
                write!(
                    target,
                    "{}",
                    self.expression_from(expression.expression(), project.input())
                )
            }
            QueryNode::Filter(filter) => {
                self.write_output(target, filter.input(), output)
            }
            QueryNode::Sort(sort) => self.write_output(target, sort.input(), output),
            QueryNode::Limit(limit) => {
                self.write_output(target, limit.input(), output)
            }
            QueryNode::Scan(_) | QueryNode::Join(_) => {
                unreachable!("validated output reference has no semantic producer")
            }
        }
    }
}
