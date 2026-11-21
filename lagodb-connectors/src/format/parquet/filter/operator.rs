//! Comparison operators shared by planned, bound and per-file predicates.

use arrow_array::{ArrayRef, BooleanArray, Datum, Scalar};
use arrow_ord::cmp;
use arrow_schema::ArrowError;
use lagodb_core::expr::PgComparisonKind;
use lagodb_core::runtime_api::RuntimeComparisonOperator;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorError;
use crate::format::FormatKind;

const OP_EQ: i32 = 0;
const OP_NOT_EQ: i32 = 1;
const OP_LT: i32 = 2;
const OP_LE: i32 = 3;
const OP_GT: i32 = 4;
const OP_GE: i32 = 5;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum ComparisonOperator {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl ComparisonOperator {
    pub(super) const fn kind(self) -> PgComparisonKind {
        match self {
            Self::Eq => PgComparisonKind::Equal,
            Self::NotEq => PgComparisonKind::NotEqual,
            Self::Lt => PgComparisonKind::Less,
            Self::Le => PgComparisonKind::LessEqual,
            Self::Gt => PgComparisonKind::Greater,
            Self::Ge => PgComparisonKind::GreaterEqual,
        }
    }

    pub(super) const fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }

    pub(super) const fn mirrored(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
            Self::Eq | Self::NotEq => self,
        }
    }

    /// The complement over a total order; callers own NULL semantics.
    pub(super) const fn negated(self) -> Self {
        match self {
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::Lt => Self::Ge,
            Self::Le => Self::Gt,
            Self::Gt => Self::Le,
            Self::Ge => Self::Lt,
        }
    }

    pub(super) const fn tag(self) -> i32 {
        match self {
            Self::Eq => OP_EQ,
            Self::NotEq => OP_NOT_EQ,
            Self::Lt => OP_LT,
            Self::Le => OP_LE,
            Self::Gt => OP_GT,
            Self::Ge => OP_GE,
        }
    }

    pub(super) fn from_tag(tag: i32) -> Result<Self, ConnectorError> {
        match tag {
            OP_EQ => Ok(Self::Eq),
            OP_NOT_EQ => Ok(Self::NotEq),
            OP_LT => Ok(Self::Lt),
            OP_LE => Ok(Self::Le),
            OP_GT => Ok(Self::Gt),
            OP_GE => Ok(Self::Ge),
            _ => Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet)),
        }
    }

    pub(super) fn evaluate(
        self,
        column: &dyn Datum,
        scalar: &Scalar<ArrayRef>,
    ) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::Eq => cmp::eq(column, scalar),
            Self::NotEq => cmp::neq(column, scalar),
            Self::Lt => cmp::lt(column, scalar),
            Self::Le => cmp::lt_eq(column, scalar),
            Self::Gt => cmp::gt(column, scalar),
            Self::Ge => cmp::gt_eq(column, scalar),
        }
    }
}

impl From<PgComparisonKind> for ComparisonOperator {
    fn from(kind: PgComparisonKind) -> Self {
        match kind {
            PgComparisonKind::Equal => Self::Eq,
            PgComparisonKind::NotEqual => Self::NotEq,
            PgComparisonKind::Less => Self::Lt,
            PgComparisonKind::LessEqual => Self::Le,
            PgComparisonKind::Greater => Self::Gt,
            PgComparisonKind::GreaterEqual => Self::Ge,
        }
    }
}

impl From<RuntimeComparisonOperator> for ComparisonOperator {
    fn from(operator: RuntimeComparisonOperator) -> Self {
        match operator {
            RuntimeComparisonOperator::Equal => Self::Eq,
            RuntimeComparisonOperator::NotEqual => Self::NotEq,
            RuntimeComparisonOperator::LessThan => Self::Lt,
            RuntimeComparisonOperator::LessThanOrEqual => Self::Le,
            RuntimeComparisonOperator::GreaterThan => Self::Gt,
            RuntimeComparisonOperator::GreaterThanOrEqual => Self::Ge,
        }
    }
}
