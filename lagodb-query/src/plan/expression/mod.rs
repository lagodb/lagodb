//! Exact expression IR executed by the query engine.
//!
//! Provider predicate fragments deliberately use the smaller pushdown IR in
//! `lagodb-core`.  This IR is the exact PostgreSQL-result contract: expressions
//! that do not have a proven native DataFusion lowering are represented by a
//! PostgreSQL evaluator node instead of weakening scan semantics.

mod codec;
mod kinds;
mod leaf_codec;
mod physical;
mod postgres_eval;
mod scalar_function;

pub(super) use codec::ExecutionExprCodec;
pub use kinds::BooleanTestKind;
pub use physical::ExecutionScalarRepr;
pub use postgres_eval::{
    PostgresEvalExpr, PostgresEvalInput, PostgresExprVolatility,
};
pub use scalar_function::ScalarFunctionKind;

use lagodb_core::expr::{ColumnRef, ExprType, PgComparisonOp, RuntimeValueId};
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

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
    Column(ColumnRef),
    Value(RuntimeValueId),
    Output(OutputId),
    Comparison {
        operator: PgComparisonOp,
        left: Box<ExecutionExpr>,
        right: Box<ExecutionExpr>,
    },
    IsNull(Box<ExecutionExpr>),
    IsNotNull(Box<ExecutionExpr>),
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
    pub fn result_type_hint(&self) -> Option<ExprType> {
        match self {
            Self::Column(column) => Some(column.value_type),
            Self::Comparison { .. }
            | Self::IsNull(_)
            | Self::IsNotNull(_)
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
            | Self::Case { result_type, .. }
            | Self::Function { result_type, .. } => Some(*result_type),
            Self::Postgres(expression) => Some(expression.result_type()),
            Self::Value(_) | Self::Output(_) => None,
        }
    }

    pub fn has_postgres_fallback(&self) -> bool {
        self.postgres_fallback_count() != 0
    }

    pub fn postgres_fallback_count(&self) -> usize {
        match self {
            Self::Comparison { left, right, .. } => {
                left.postgres_fallback_count() + right.postgres_fallback_count()
            }
            Self::IsNull(value)
            | Self::IsNotNull(value)
            | Self::Not(value)
            | Self::BooleanTest { value, .. }
            | Self::Relabel { value, .. } => value.postgres_fallback_count(),
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
            Self::Column(_) | Self::Value(_) | Self::Output(_) => 0,
        }
    }
}
