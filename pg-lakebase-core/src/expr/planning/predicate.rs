//! Typed plan-stage predicate view over PG `Expr` leaves (not persisted in `custom_private`).

use core::ffi::c_int;

use pgrx::pg_sys;
use thiserror::Error;

use crate::expr::contract::{ParamKey, PgComparisonOp};
use crate::expr::pg::{
    PgExprRef, PgNullTestKind, PgPredicateLeafRef, PgScalarExprRef, PgStructuralError,
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
    pub fn parse_leaf(
        &self,
        expr: PgExprRef<'_>,
    ) -> Result<PlanPredicate, PredicateParseError> {
        match PgPredicateLeafRef::parse(expr).map_err(PredicateParseError::from)? {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                Ok(PlanPredicate::Comparison {
                    op,
                    left: self.parse_scalar_operand(left)?,
                    right: self.parse_scalar_operand(right)?,
                })
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                let value = self.parse_scalar_operand(value)?;
                match kind {
                    PgNullTestKind::IsNull => Ok(PlanPredicate::IsNull { value }),
                    PgNullTestKind::IsNotNull => {
                        Ok(PlanPredicate::IsNotNull { value })
                    }
                }
            }
        }
    }

    fn parse_scalar_operand(
        &self,
        expr: PgScalarExprRef<'_>,
    ) -> Result<PlanScalar, PredicateParseError> {
        match expr {
            PgScalarExprRef::Var(var) => {
                let attno = var.varattno();
                let varno = var.varno();
                let atttypid = var.vartype();
                let attcollation = var.varcollid();
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
            PgScalarExprRef::Const(value) => {
                let (consttypid, constcollid, _, is_null) = value.parts();
                Ok(PlanScalar::Literal(PlanLiteralRef {
                    consttypid,
                    constcollid,
                    is_null,
                }))
            }
            PgScalarExprRef::Param(param) => {
                Ok(PlanScalar::Dynamic(PlanDynamicRef::Param(PlanParamRef {
                    key: param.key(),
                    paramtype: param.paramtype(),
                    paramcollid: param.paramcollid(),
                })))
            }
        }
    }
}

impl From<PgStructuralError> for PredicateParseError {
    fn from(error: PgStructuralError) -> Self {
        match error {
            PgStructuralError::UnsupportedNodeTag { tag } => {
                Self::UnsupportedNodeTag { tag }
            }
            PgStructuralError::NonBinaryComparison => Self::NonBinaryOpExpr,
            PgStructuralError::RowNullTest => Self::RowNullTest,
            PgStructuralError::UnsupportedNullTest { kind } => {
                Self::UnsupportedNullTestType { nulltesttype: kind }
            }
            PgStructuralError::UnsupportedScalar | PgStructuralError::NullChild => {
                Self::UnsupportedScalarOperand
            }
        }
    }
}

/// Typed plan-time predicate (temporary; not stored in `custom_private`).
#[derive(Debug, Clone)]
pub enum PlanPredicate {
    Comparison {
        op: PgComparisonOp,
        left: PlanScalar,
        right: PlanScalar,
    },
    IsNull {
        value: PlanScalar,
    },
    IsNotNull {
        value: PlanScalar,
    },
}

impl PlanPredicate {
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
pub enum PlanScalar {
    Column(PlanColumnRef),
    Literal(PlanLiteralRef),
    Dynamic(PlanDynamicRef),
}

impl PlanScalar {
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
pub struct PlanLiteralRef {
    pub consttypid: pg_sys::Oid,
    pub constcollid: pg_sys::Oid,
    pub is_null: bool,
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
