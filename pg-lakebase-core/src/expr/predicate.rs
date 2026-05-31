//! Plan-time typed predicate view over PG `Expr` leaves (not persisted in `custom_private`).

use core::ffi::c_int;

use pgrx::pg_sys;
use thiserror::Error;

use crate::expr::nodes::{
    ParamKey, PgComparisonOp, PgConst, PgExprRef, PgNullTest, PgOpExpr, PgParam,
    PgVar,
};

/// Plan-stage context for parsing leaf predicates from PG `Expr` nodes.
#[derive(Debug, Clone, Copy)]
pub struct PlanPredicateContext {
    pub rel_oid: pg_sys::Oid,
    pub scan_relid: c_int,
}

impl PlanPredicateContext {
    /// Parse one leaf `Expr` into a [`PlanPredicate`] (comparison or null test).
    ///
    /// Structural parse only: does not apply provider pushability rules.
    ///
    /// # Safety
    ///
    /// `expr` must be valid for `'a` in the planner per-query memory context.
    pub unsafe fn parse_leaf<'a>(
        &self,
        expr: PgExprRef<'a>,
    ) -> Result<PlanPredicate<'a>, PredicateParseError> {
        let leaf = unsafe { expr.without_relabels() };
        let tag = unsafe { leaf.node_tag() };

        match tag {
            pg_sys::NodeTag::T_OpExpr => {
                let op = unsafe { PgOpExpr::try_from_expr(leaf) }
                    .ok_or(PredicateParseError::UnsupportedNodeTag { tag })?;
                unsafe { self.parse_comparison(op) }
            }
            pg_sys::NodeTag::T_NullTest => {
                let nt = unsafe { PgNullTest::try_from_expr(leaf) }
                    .ok_or(PredicateParseError::UnsupportedNodeTag { tag })?;
                unsafe { self.parse_null_test(nt) }
            }
            _ => Err(PredicateParseError::UnsupportedNodeTag { tag }),
        }
    }

    unsafe fn parse_comparison<'a>(
        &self,
        op: PgOpExpr<'a>,
    ) -> Result<PlanPredicate<'a>, PredicateParseError> {
        let (lhs_raw, rhs_raw) = unsafe { op.binary_operands() }
            .ok_or(PredicateParseError::NonBinaryOpExpr)?;
        let lhs = unsafe { lhs_raw.without_relabels() };
        let rhs = unsafe { rhs_raw.without_relabels() };

        let lhs_scalar = unsafe { self.parse_scalar_operand(lhs) }?;
        let rhs_scalar = unsafe { self.parse_scalar_operand(rhs) }?;

        let key = unsafe { op.comparison_op() };
        Ok(PlanPredicate::Comparison {
            op: key,
            left: lhs_scalar,
            right: rhs_scalar,
        })
    }

    unsafe fn parse_null_test<'a>(
        &self,
        nt: PgNullTest<'a>,
    ) -> Result<PlanPredicate<'a>, PredicateParseError> {
        if unsafe { nt.argisrow() } {
            return Err(PredicateParseError::RowNullTest);
        }
        let arg = unsafe { nt.arg() }
            .ok_or(PredicateParseError::UnsupportedScalarOperand)?;
        let arg = unsafe { arg.without_relabels() };
        let value = unsafe { self.parse_scalar_operand(arg) }?;

        match unsafe { nt.nulltesttype() } {
            pg_sys::NullTestType::IS_NULL => Ok(PlanPredicate::IsNull { value }),
            pg_sys::NullTestType::IS_NOT_NULL => {
                Ok(PlanPredicate::IsNotNull { value })
            }
            other => Err(PredicateParseError::UnsupportedNullTestType {
                nulltesttype: other,
            }),
        }
    }

    unsafe fn parse_scalar_operand<'a>(
        &self,
        expr: PgExprRef<'a>,
    ) -> Result<PlanScalar<'a>, PredicateParseError> {
        let tag = unsafe { expr.node_tag() };
        match tag {
            pg_sys::NodeTag::T_Var => {
                let var = unsafe { PgVar::try_from_expr(expr) }
                    .ok_or(PredicateParseError::UnsupportedScalarOperand)?;
                let attno = unsafe { var.varattno() };
                let varno = unsafe { var.varno() };
                let atttypid = unsafe { var.vartype() };
                let attcollation = unsafe { var.varcollid() };
                if varno == self.scan_relid {
                    if attno <= 0 {
                        return Err(PredicateParseError::UnsupportedScalarOperand);
                    }
                    Ok(PlanScalar::Column(PlanColumnRef {
                        rel_oid: self.rel_oid,
                        attno,
                        atttypid,
                        attcollation,
                    }))
                } else {
                    Ok(PlanScalar::Dynamic(PlanDynamicRef::OuterVar(
                        PlanOuterVarRef {
                            varno: varno as pg_sys::Index,
                            attno,
                            atttypid,
                            attcollation,
                        },
                    )))
                }
            }
            pg_sys::NodeTag::T_Const => {
                let c = unsafe { PgConst::try_from_expr(expr) }
                    .ok_or(PredicateParseError::UnsupportedScalarOperand)?;
                let (consttypid, constcollid, _, is_null) = unsafe { c.parts() };
                Ok(PlanScalar::Literal(PlanLiteralRef {
                    consttypid,
                    constcollid,
                    is_null,
                    _marker: core::marker::PhantomData,
                }))
            }
            pg_sys::NodeTag::T_Param => {
                let p = unsafe { PgParam::try_from_expr(expr) }
                    .ok_or(PredicateParseError::UnsupportedScalarOperand)?;
                Ok(PlanScalar::Dynamic(PlanDynamicRef::Param(PlanParamRef {
                    key: unsafe { p.key() },
                    paramtype: unsafe { p.paramtype() },
                    paramcollid: unsafe { p.paramcollid() },
                })))
            }
            _ => Err(PredicateParseError::UnsupportedScalarOperand),
        }
    }
}

/// Typed plan-time predicate (temporary; not stored in `custom_private`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanPredicate<'a> {
    Comparison {
        op: PgComparisonOp,
        left: PlanScalar<'a>,
        right: PlanScalar<'a>,
    },
    IsNull {
        value: PlanScalar<'a>,
    },
    IsNotNull {
        value: PlanScalar<'a>,
    },
}

impl PlanPredicate<'_> {
    /// Returns the scan-column type OID when one side references the scan relation.
    pub fn scan_column_type(&self) -> Option<pg_sys::Oid> {
        match self {
            PlanPredicate::Comparison { left, right, .. } => {
                left.column_type().or_else(|| right.column_type())
            }
            PlanPredicate::IsNull { value } | PlanPredicate::IsNotNull { value } => {
                value.column_type()
            }
        }
    }
}

/// Operand of a plan-time predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanScalar<'a> {
    Column(PlanColumnRef),
    Literal(PlanLiteralRef<'a>),
    Dynamic(PlanDynamicRef),
}

impl PlanScalar<'_> {
    /// Type OID when this scalar is a scan-relation column.
    pub fn column_type(&self) -> Option<pg_sys::Oid> {
        match self {
            PlanScalar::Column(col) => Some(col.atttypid),
            _ => None,
        }
    }
}

/// Scan-relation column metadata extracted from a `Var` at plan time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanColumnRef {
    pub rel_oid: pg_sys::Oid,
    pub attno: pg_sys::AttrNumber,
    pub atttypid: pg_sys::Oid,
    pub attcollation: pg_sys::Oid,
}

/// Literal operand metadata from a `Const` (no datum value; plan-time classification only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanLiteralRef<'a> {
    pub consttypid: pg_sys::Oid,
    pub constcollid: pg_sys::Oid,
    pub is_null: bool,
    _marker: core::marker::PhantomData<&'a ()>,
}

/// `Param` operand metadata at plan time (before runtime resolution).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanParamRef {
    pub key: ParamKey,
    pub paramtype: pg_sys::Oid,
    pub paramcollid: pg_sys::Oid,
}

/// Outer-relation `Var` operand before `replace_nestloop_params`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanOuterVarRef {
    pub varno: pg_sys::Index,
    pub attno: pg_sys::AttrNumber,
    pub atttypid: pg_sys::Oid,
    pub attcollation: pg_sys::Oid,
}

/// Dynamic operand: plan-time `Param` or outer-relation column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlanDynamicRef {
    Param(PlanParamRef),
    OuterVar(PlanOuterVarRef),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PredicateParseError {
    #[error("unsupported NodeTag {tag:?} for plan predicate leaf")]
    UnsupportedNodeTag { tag: pg_sys::NodeTag },
    #[error("OpExpr is not binary")]
    NonBinaryOpExpr,
    #[error("unsupported scalar operand (not Var/Const/Param)")]
    UnsupportedScalarOperand,
    #[error("row-level NullTest is not supported")]
    RowNullTest,
    #[error("unsupported NullTestType {nulltesttype:?}")]
    UnsupportedNullTestType {
        nulltesttype: pg_sys::NullTestType::Type,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_operand_metadata_fields_are_copy() {
        let col = PlanColumnRef {
            rel_oid: pg_sys::Oid::from(1),
            attno: 1,
            atttypid: pg_sys::INT4OID,
            attcollation: pg_sys::Oid::INVALID,
        };
        let lit = PlanLiteralRef {
            consttypid: pg_sys::INT4OID,
            constcollid: pg_sys::Oid::INVALID,
            is_null: false,
            _marker: core::marker::PhantomData,
        };
        let param = PlanParamRef {
            key: ParamKey {
                paramkind: pg_sys::ParamKind::PARAM_EXTERN,
                param_id: 1,
            },
            paramtype: pg_sys::INT4OID,
            paramcollid: pg_sys::Oid::INVALID,
        };
        let _ = (col, lit, param);
    }
}
