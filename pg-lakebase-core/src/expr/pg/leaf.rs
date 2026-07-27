//! Shared structural parsing for predicate leaves.

use pgrx::pg_sys;
use thiserror::Error;

use crate::expr::contract::PgComparisonOp;

use super::view::{PgConst, PgExprRef, PgNullTest, PgOpExpr, PgParam, PgVar};

#[derive(Clone, Copy, Debug)]
pub enum PgScalarExprRef<'a> {
    Var(PgVar<'a>),
    Const(PgConst<'a>),
    Param(PgParam<'a>),
}

impl<'a> PgScalarExprRef<'a> {
    pub fn parse(expr: PgExprRef<'a>) -> Result<Self, PgStructuralError> {
        let expr = expr.without_relabels();
        match expr.node_tag() {
            pg_sys::NodeTag::T_Var => Ok(Self::Var(
                PgVar::try_from_expr(expr).expect("NodeTag established a Var"),
            )),
            pg_sys::NodeTag::T_Const => Ok(Self::Const(
                PgConst::try_from_expr(expr).expect("NodeTag established a Const"),
            )),
            pg_sys::NodeTag::T_Param => Ok(Self::Param(
                PgParam::try_from_expr(expr).expect("NodeTag established a Param"),
            )),
            _ => Err(PgStructuralError::UnsupportedScalar),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PgPredicateLeafRef<'a> {
    Comparison {
        op: PgComparisonOp,
        left: PgScalarExprRef<'a>,
        right: PgScalarExprRef<'a>,
    },
    NullTest {
        kind: PgNullTestKind,
        value: PgScalarExprRef<'a>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PgNullTestKind {
    IsNull,
    IsNotNull,
}

impl<'a> PgPredicateLeafRef<'a> {
    pub fn parse(expr: PgExprRef<'a>) -> Result<Self, PgStructuralError> {
        let expr = expr.without_relabels();
        match expr.node_tag() {
            pg_sys::NodeTag::T_OpExpr => {
                let op = PgOpExpr::try_from_expr(expr)
                    .expect("NodeTag established an OpExpr");
                let (left, right) = op
                    .binary_operands()
                    .ok_or(PgStructuralError::NonBinaryComparison)?;
                Ok(Self::Comparison {
                    op: op.comparison_op(),
                    left: PgScalarExprRef::parse(left)?,
                    right: PgScalarExprRef::parse(right)?,
                })
            }
            pg_sys::NodeTag::T_NullTest => {
                let test = PgNullTest::try_from_expr(expr)
                    .expect("NodeTag established a NullTest");
                if test.argisrow() {
                    return Err(PgStructuralError::RowNullTest);
                }
                let value = test.arg().ok_or(PgStructuralError::NullChild)?;
                let kind = match test.nulltesttype() {
                    pg_sys::NullTestType::IS_NULL => PgNullTestKind::IsNull,
                    pg_sys::NullTestType::IS_NOT_NULL => PgNullTestKind::IsNotNull,
                    kind => {
                        return Err(PgStructuralError::UnsupportedNullTest { kind });
                    }
                };
                Ok(Self::NullTest {
                    kind,
                    value: PgScalarExprRef::parse(value)?,
                })
            }
            tag => Err(PgStructuralError::UnsupportedNodeTag { tag }),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PgStructuralError {
    #[error("unsupported predicate NodeTag {tag:?}")]
    UnsupportedNodeTag { tag: pg_sys::NodeTag },
    #[error("comparison is not binary")]
    NonBinaryComparison,
    #[error("unsupported scalar operand")]
    UnsupportedScalar,
    #[error("row-valued null test is not supported")]
    RowNullTest,
    #[error("null test has a null child")]
    NullChild,
    #[error("unsupported NullTestType {kind:?}")]
    UnsupportedNullTest { kind: pg_sys::NullTestType::Type },
}
