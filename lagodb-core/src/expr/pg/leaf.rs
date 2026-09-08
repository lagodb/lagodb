//! Shared structural parsing for predicate leaves.

use pgrx::pg_sys;
use thiserror::Error;

use crate::expr::contract::PgComparisonOp;

use super::view::{
    PgConst, PgExprRef, PgFuncExpr, PgNullTest, PgOpExpr, PgParam, PgVar,
};

#[derive(Clone, Copy, Debug)]
pub enum PgScalarExprRef<'a> {
    Var {
        node: PgVar<'a>,
        expression: PgExprRef<'a>,
    },
    Const {
        node: PgConst<'a>,
        expression: PgExprRef<'a>,
    },
    Param {
        node: PgParam<'a>,
        expression: PgExprRef<'a>,
    },
    WidenedIntegerVar {
        node: PgVar<'a>,
        expression: PgExprRef<'a>,
    },
}

impl<'a> PgScalarExprRef<'a> {
    pub fn parse(expr: PgExprRef<'a>) -> Result<Self, PgStructuralError> {
        let node = expr.without_relabels();
        match node.node_tag() {
            pg_sys::NodeTag::T_Var => Ok(Self::Var {
                node: PgVar::try_from_expr(node).expect("NodeTag established a Var"),
                expression: expr,
            }),
            pg_sys::NodeTag::T_Const => Ok(Self::Const {
                node: PgConst::try_from_expr(node)
                    .expect("NodeTag established a Const"),
                expression: expr,
            }),
            pg_sys::NodeTag::T_Param => Ok(Self::Param {
                node: PgParam::try_from_expr(node)
                    .expect("NodeTag established a Param"),
                expression: expr,
            }),
            pg_sys::NodeTag::T_FuncExpr => {
                let function = PgFuncExpr::try_from_expr(node)
                    .expect("NodeTag established a FuncExpr");
                Ok(Self::WidenedIntegerVar {
                    node: function
                        .widened_integer_var()
                        .ok_or(PgStructuralError::UnsupportedScalar)?,
                    expression: expr,
                })
            }
            _ => Err(PgStructuralError::UnsupportedScalar),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PgPredicateLeafRef<'a> {
    Comparison {
        op: PgComparisonOp,
        left: PgExprRef<'a>,
        right: PgExprRef<'a>,
    },
    NullTest {
        kind: PgNullTestKind,
        value: PgExprRef<'a>,
    },
    StartsWith {
        value: PgExprRef<'a>,
        prefix: PgExprRef<'a>,
        input_collation: pg_sys::Oid,
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
                if let Some((value, prefix, input_collation)) =
                    op.builtin_starts_with_operands()
                {
                    return Ok(Self::StartsWith {
                        value,
                        prefix,
                        input_collation,
                    });
                }
                let (left, right) = op
                    .binary_operands()
                    .ok_or(PgStructuralError::NonBinaryComparison)?;
                Ok(Self::Comparison {
                    op: op.comparison_op(),
                    left,
                    right,
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
                Ok(Self::NullTest { kind, value })
            }
            pg_sys::NodeTag::T_FuncExpr => {
                let function = PgFuncExpr::try_from_expr(expr)
                    .expect("NodeTag established a FuncExpr");
                let (value, prefix, input_collation) = function
                    .builtin_starts_with_operands()
                    .ok_or(PgStructuralError::UnsupportedScalarFunction)?;
                Ok(Self::StartsWith {
                    value,
                    prefix,
                    input_collation,
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
    #[error("unsupported scalar function predicate")]
    UnsupportedScalarFunction,
    #[error("row-valued null test is not supported")]
    RowNullTest,
    #[error("null test has a null child")]
    NullChild,
    #[error("unsupported NullTestType {kind:?}")]
    UnsupportedNullTest { kind: pg_sys::NullTestType::Type },
}
