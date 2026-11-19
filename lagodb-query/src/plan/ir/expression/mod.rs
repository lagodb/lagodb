//! Exact expression IR executed by the query engine.
//!
//! Provider predicate fragments deliberately use the smaller pushdown IR in
//! `lagodb-core`.  This IR is the exact PostgreSQL-result contract: expressions
//! that do not have a proven native DataFusion lowering are represented by a
//! PostgreSQL evaluator node instead of weakening scan semantics.

mod kinds;
mod physical;
mod postgres_eval;
mod scalar_function;

pub use kinds::BooleanTestKind;
pub use physical::ExecutionScalarRepr;
pub use postgres_eval::{
    PostgresEvalExpr, PostgresEvalInput, PostgresExprVolatility,
};
pub use scalar_function::ScalarFunctionKind;

use lagodb_core::expr::{
    ColumnRef, ExprType, PgBpcharSemantics, PgComparisonOp, RuntimeValueId,
};
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

use super::decimal::{DecimalOperator, FixedDecimalType};
use super::semantics::{ComparisonKind, Decimal128Semantics};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseWhen {
    when: ExecutionExpr,
    then: ExecutionExpr,
}

impl CaseWhen {
    pub fn new(when: ExecutionExpr, then: ExecutionExpr) -> Self {
        Self { when, then }
    }

    pub fn when(&self) -> &ExecutionExpr {
        &self.when
    }

    pub fn then(&self) -> &ExecutionExpr {
        &self.then
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionExpr {
    /// TRUE for non-NULL input and UNKNOWN for NULL input.
    StrictTrue(Box<ExecutionExpr>),
    /// FALSE for non-NULL input and UNKNOWN for NULL input.
    StrictFalse(Box<ExecutionExpr>),
    Column(ColumnRef),
    Value(RuntimeValueId),
    /// A constant NUMERIC bound exactly in its fixed decimal type: a
    /// comparison operand's type, or an arithmetic constant's own digits and
    /// display scale.
    DecimalValue {
        value: RuntimeValueId,
        decimal_type: FixedDecimalType,
    },
    /// PostgreSQL NUMERIC `+`, `-` or `*` over fixed decimal operands. The
    /// PostgreSQL value type is unconstrained `numeric`; `result_type` is the
    /// proven execution type.
    DecimalArithmetic {
        operator: DecimalOperator,
        left: Box<ExecutionExpr>,
        right: Box<ExecutionExpr>,
        result_type: FixedDecimalType,
    },
    /// PostgreSQL's implicit `int2`/`int4`/`int8` to `numeric` cast.
    IntegerToDecimal {
        value: Box<ExecutionExpr>,
        result_type: FixedDecimalType,
    },
    /// `operand <kind> bound`, where `bound` is a NUMERIC value set by an
    /// InitPlan and known only at execution. Execution places the value in
    /// `operand_type` with the rounding that preserves the comparison, or
    /// folds the comparison when no operand value can equal or pass it.
    DecimalBoundComparison {
        kind: ComparisonKind,
        operand: Box<ExecutionExpr>,
        bound: RuntimeValueId,
        operand_type: FixedDecimalType,
    },
    /// A constant `bpchar` padded at execution to the width of the
    /// `character(n)` column it is compared with.
    BpcharValue {
        value: RuntimeValueId,
        semantics: PgBpcharSemantics,
    },
    Output(OutputId),
    /// A NUMERIC output in its fixed decimal execution type, the plan node's
    /// fact for that output. A decimal operation reads its operands' types
    /// from the expression structure, so an output it reads names its type.
    DecimalOutput {
        output: OutputId,
        decimal_type: FixedDecimalType,
    },
    Comparison {
        operator: PgComparisonOp,
        left: Box<ExecutionExpr>,
        right: Box<ExecutionExpr>,
    },
    IsNull(Box<ExecutionExpr>),
    IsNotNull(Box<ExecutionExpr>),
    IsNan(Box<ExecutionExpr>),
    IsNotNan(Box<ExecutionExpr>),
    BooleanTest {
        kind: BooleanTestKind,
        value: Box<ExecutionExpr>,
    },
    And(Box<[ExecutionExpr]>),
    Or(Box<[ExecutionExpr]>),
    Not(Box<ExecutionExpr>),
    Relabel {
        value: Box<ExecutionExpr>,
        result_type: ExprType,
    },
    WidenInteger {
        value: Box<ExecutionExpr>,
        result_type: ExprType,
    },
    Case {
        when_then: Box<[CaseWhen]>,
        else_expr: Option<Box<ExecutionExpr>>,
        result_type: ExprType,
    },
    InList {
        value: Box<ExecutionExpr>,
        list: Box<[ExecutionExpr]>,
        negated: bool,
    },
    Function {
        kind: ScalarFunctionKind,
        arguments: Box<[ExecutionExpr]>,
        input_collation: pg_sys::Oid,
        result_type: ExprType,
    },
    Postgres(PostgresEvalExpr),
}

impl ExecutionExpr {
    /// Fixed decimal execution type carried by this expression's structure.
    /// Storage columns are Decimal128; a DecimalOutput's type is checked
    /// against its plan-node fact by validation. CASE and COALESCE take
    /// DataFusion's common type of their branches.
    pub fn decimal_type(&self) -> Option<FixedDecimalType> {
        match self {
            Self::Column(column) => {
                Decimal128Semantics::for_type(column.value_type).map(Into::into)
            }
            Self::DecimalValue { decimal_type, .. }
            | Self::DecimalOutput { decimal_type, .. } => Some(*decimal_type),
            Self::DecimalArithmetic { result_type, .. }
            | Self::IntegerToDecimal { result_type, .. } => Some(*result_type),
            Self::Relabel { value, result_type } => {
                let result = FixedDecimalType::for_type(*result_type)?;
                (value.decimal_type() == Some(result)).then_some(result)
            }
            Self::Case {
                when_then,
                else_expr,
                result_type,
            } if result_type.type_oid == pg_sys::NUMERICOID => Self::common_decimal(
                when_then
                    .iter()
                    .map(CaseWhen::then)
                    .chain(else_expr.as_deref()),
            ),
            Self::Function {
                kind: ScalarFunctionKind::Coalesce,
                arguments,
                result_type,
                ..
            } if result_type.type_oid == pg_sys::NUMERICOID => {
                Self::common_decimal(arguments.iter())
            }
            _ => None,
        }
    }

    /// Common fixed type of conditional branches, all of which must have one.
    fn common_decimal<'a>(
        mut branches: impl Iterator<Item = &'a ExecutionExpr>,
    ) -> Option<FixedDecimalType> {
        let first = branches.next()?.decimal_type()?;
        branches.try_fold(first, |common, branch| {
            common.common(branch.decimal_type()?)
        })
    }

    /// The type that selects an aggregate's input semantics: the fixed
    /// decimal execution type for NUMERIC, otherwise the PostgreSQL type.
    pub fn execution_type_hint(&self) -> Option<ExprType> {
        self.decimal_type()
            .map(FixedDecimalType::value_type)
            .or_else(|| self.result_type_hint())
    }

    /// PostgreSQL-style per-tuple operator units used by the central query
    /// cost estimator. Leaf references are free; calls, comparisons, tests,
    /// and IN-list comparisons each contribute one operator unit.
    pub(crate) fn cost_units(&self) -> usize {
        match self {
            Self::Column(_)
            | Self::Value(_)
            | Self::DecimalValue { .. }
            | Self::BpcharValue { .. }
            | Self::Output(_)
            | Self::DecimalOutput { .. } => 0,
            Self::Comparison { left, right, .. }
            | Self::DecimalArithmetic { left, right, .. } => {
                1 + left.cost_units() + right.cost_units()
            }
            Self::IntegerToDecimal { value, .. }
            | Self::DecimalBoundComparison { operand: value, .. } => {
                1 + value.cost_units()
            }
            Self::IsNull(value)
            | Self::IsNotNull(value)
            | Self::IsNan(value)
            | Self::IsNotNan(value)
            | Self::StrictTrue(value)
            | Self::StrictFalse(value)
            | Self::Not(value)
            | Self::BooleanTest { value, .. } => 1 + value.cost_units(),
            Self::Relabel { value, .. } => value.cost_units(),
            Self::WidenInteger { value, .. } => 1 + value.cost_units(),
            Self::And(children) | Self::Or(children) => {
                children.iter().map(Self::cost_units).sum()
            }
            Self::Case {
                when_then,
                else_expr,
                ..
            } => {
                when_then
                    .iter()
                    .map(|branch| {
                        branch.when().cost_units() + branch.then().cost_units()
                    })
                    .sum::<usize>()
                    + else_expr.as_deref().map(Self::cost_units).unwrap_or(0)
            }
            Self::InList { value, list, .. } => {
                value.cost_units()
                    + list.iter().map(Self::cost_units).sum::<usize>()
                    + list.len()
            }
            Self::Function { arguments, .. } => {
                1 + arguments.iter().map(Self::cost_units).sum::<usize>()
            }
            Self::Postgres(expression) => {
                1 + expression
                    .inputs()
                    .iter()
                    .map(|input| input.expression().cost_units())
                    .sum::<usize>()
            }
        }
    }

    pub fn result_type_hint(&self) -> Option<ExprType> {
        match self {
            Self::Column(column) => Some(column.value_type),
            Self::DecimalValue { decimal_type, .. } => {
                Some(decimal_type.value_type())
            }
            Self::DecimalArithmetic { .. } | Self::IntegerToDecimal { .. } => {
                Some(ExprType {
                    type_oid: pg_sys::NUMERICOID,
                    typmod: -1,
                    collation: pg_sys::InvalidOid,
                })
            }
            Self::Comparison { .. }
            | Self::DecimalBoundComparison { .. }
            | Self::IsNull(_)
            | Self::IsNotNull(_)
            | Self::IsNan(_)
            | Self::IsNotNan(_)
            | Self::StrictTrue(_)
            | Self::StrictFalse(_)
            | Self::BooleanTest { .. }
            | Self::And(_)
            | Self::Or(_)
            | Self::Not(_)
            | Self::InList { .. } => Some(ExprType {
                type_oid: pg_sys::BOOLOID,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            }),
            Self::Relabel { result_type, .. }
            | Self::WidenInteger { result_type, .. }
            | Self::Case { result_type, .. }
            | Self::Function { result_type, .. } => Some(*result_type),
            Self::Postgres(expression) => Some(expression.result_type()),
            Self::Value(_)
            | Self::BpcharValue { .. }
            | Self::Output(_)
            | Self::DecimalOutput { .. } => None,
        }
    }

    pub fn has_postgres_fallback(&self) -> bool {
        self.postgres_fallback_count() != 0
    }

    pub fn postgres_fallback_count(&self) -> usize {
        match self {
            Self::Comparison { left, right, .. }
            | Self::DecimalArithmetic { left, right, .. } => {
                left.postgres_fallback_count() + right.postgres_fallback_count()
            }
            Self::IntegerToDecimal { value, .. }
            | Self::DecimalBoundComparison { operand: value, .. } => {
                value.postgres_fallback_count()
            }
            Self::IsNull(value)
            | Self::IsNotNull(value)
            | Self::IsNan(value)
            | Self::IsNotNan(value)
            | Self::StrictTrue(value)
            | Self::StrictFalse(value)
            | Self::Not(value)
            | Self::BooleanTest { value, .. }
            | Self::Relabel { value, .. } => value.postgres_fallback_count(),
            Self::WidenInteger { value, .. } => value.postgres_fallback_count(),
            Self::And(children) | Self::Or(children) => {
                children.iter().map(Self::postgres_fallback_count).sum()
            }
            Self::Function { arguments, .. } => {
                arguments.iter().map(Self::postgres_fallback_count).sum()
            }
            Self::Case {
                when_then,
                else_expr,
                ..
            } => {
                when_then
                    .iter()
                    .map(|branch| {
                        branch.when().postgres_fallback_count()
                            + branch.then().postgres_fallback_count()
                    })
                    .sum::<usize>()
                    + else_expr
                        .as_deref()
                        .map(Self::postgres_fallback_count)
                        .unwrap_or(0)
            }
            Self::InList { value, list, .. } => {
                value.postgres_fallback_count()
                    + list
                        .iter()
                        .map(Self::postgres_fallback_count)
                        .sum::<usize>()
            }
            Self::Postgres(expression) => {
                1 + expression
                    .inputs()
                    .iter()
                    .map(|input| input.expression().postgres_fallback_count())
                    .sum::<usize>()
            }
            Self::Column(_)
            | Self::Value(_)
            | Self::DecimalValue { .. }
            | Self::BpcharValue { .. }
            | Self::Output(_)
            | Self::DecimalOutput { .. } => 0,
        }
    }
}
