//! Exact lowering for PostgreSQL predicate leaves.

use lagodb_core::expr::pg::{PgConst, PgExprRef, PgNullTestKind, PgPredicateLeafRef};
use lagodb_core::expr::{
    ExprType, PgComparisonOp, PgNanComparison, PgTextComparisonSemantics,
};
use lagodb_core::tuple::Utf8ServerEncoding;
use lagodb_query::plan::{ExecutionExpr, ScalarFunctionKind};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, PredicateDomain,
    QueryExpressionPlanner,
};

impl QueryExpressionPlanner {
    pub(super) unsafe fn lower_predicate_leaf(
        &mut self,
        expression: *mut pg_sys::Node,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let expression_ref = unsafe { PgExprRef::from_raw(expression.cast()) };
        match PgPredicateLeafRef::parse(expression_ref).map_err(|_| {
            ExpressionDecline::UnsupportedNode(expression_ref.node_tag())
        })? {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                if op.opno == pg_sys::Oid::from(pg_sys::OID_TEXT_LIKE_OP) {
                    return unsafe { self.lower_like_prefix(op, left, right, scope) };
                }
                if let Some(result) =
                    unsafe { self.lower_nan_comparison(op, left, right, scope) }
                {
                    return result;
                }
                let left_type = unsafe { Self::predicate_type(left.as_ptr(), scope) };
                let right_type =
                    unsafe { Self::predicate_type(right.as_ptr(), scope) };
                let decimal = domain
                    .decimal_comparison(op, left_type, right_type)
                    .map(|(_, decimal)| decimal);
                if decimal.is_none()
                    && domain.comparison(op, left_type, right_type).is_none()
                {
                    return Err(ExpressionDecline::UnsupportedSemantics);
                }
                let left = if let Some(decimal) = decimal {
                    unsafe {
                        self.lower_decimal_operand(
                            left.as_ptr(),
                            scope.as_scalar(),
                            decimal,
                        )
                    }?
                } else {
                    unsafe { self.lower(left.as_ptr().cast(), scope.as_scalar()) }?
                };
                let right = if let Some(decimal) = decimal {
                    unsafe {
                        self.lower_decimal_operand(
                            right.as_ptr(),
                            scope.as_scalar(),
                            decimal,
                        )
                    }?
                } else {
                    unsafe { self.lower(right.as_ptr().cast(), scope.as_scalar()) }?
                };
                Ok(ExecutionExpr::Comparison {
                    operator: op,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                // NULL tests inspect validity, not value ordering/equality. Lowering
                // the operand is the complete execution-capability check here.
                let value = Box::new(unsafe {
                    self.lower(value.as_ptr().cast(), scope.as_scalar())
                }?);
                match kind {
                    PgNullTestKind::IsNull => Ok(ExecutionExpr::IsNull(value)),
                    PgNullTestKind::IsNotNull => Ok(ExecutionExpr::IsNotNull(value)),
                }
            }
            PgPredicateLeafRef::StartsWith {
                value,
                prefix,
                input_collation,
            } => unsafe {
                self.lower_starts_with(value, prefix, input_collation, scope)
            },
        }
    }

    unsafe fn lower_starts_with(
        &mut self,
        value: PgExprRef<'_>,
        prefix: PgExprRef<'_>,
        input_collation: pg_sys::Oid,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if Utf8ServerEncoding::resolve().is_err()
            || unsafe {
                PgTextComparisonSemantics::for_equality_collation(input_collation)
            }
            .is_none()
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let value = unsafe { self.lower(value.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&value, ExecutionExpr::Column(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let prefix =
            unsafe { self.lower(prefix.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&prefix, ExecutionExpr::Value(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(ExecutionExpr::Function {
            kind: ScalarFunctionKind::StartsWith,
            arguments: vec![value, prefix].into_boxed_slice(),
            input_collation,
            result_type: ExprType {
                type_oid: pg_sys::BOOLOID,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            },
        })
    }

    unsafe fn lower_like_prefix(
        &mut self,
        op: PgComparisonOp,
        value: PgExprRef<'_>,
        pattern: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if op.opfuncid != pg_sys::Oid::from(pg_sys::F_TEXTLIKE)
            || op.opresulttype != pg_sys::BOOLOID
            || op.opcollid != pg_sys::Oid::INVALID
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let pattern = PgConst::try_from_expr(pattern.without_relabels())
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let prefix = unsafe { pattern.pure_like_prefix() }
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        unsafe { self.lower_starts_with(value, prefix, op.inputcollid, scope) }
    }

    unsafe fn lower_nan_comparison(
        &mut self,
        op: PgComparisonOp,
        left: PgExprRef<'_>,
        right: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
    ) -> Option<ExpressionPlanResult<ExecutionExpr>> {
        let signature = op.builtin_signature()?;
        if !matches!(
            (signature.left_type(), signature.right_type()),
            (
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID,
                pg_sys::FLOAT4OID | pg_sys::FLOAT8OID
            )
        ) {
            return None;
        }
        let left_nan = PgConst::try_from_expr(left.without_relabels())
            .is_some_and(|constant| unsafe { constant.is_float_nan() });
        let right_nan = PgConst::try_from_expr(right.without_relabels())
            .is_some_and(|constant| unsafe { constant.is_float_nan() });
        let (column, nan_on_left) = match (left_nan, right_nan) {
            (true, false) => (right, true),
            (false, true) => (left, false),
            _ => return None,
        };
        Some(
            unsafe { self.lower(column.as_ptr().cast(), scope.as_scalar()) }
                .and_then(|column| {
                    if !matches!(&column, ExecutionExpr::Column(_)) {
                        return Err(ExpressionDecline::UnsupportedSemantics);
                    }
                    let column = Box::new(column);
                    Ok(match signature.kind().with_nan_on(nan_on_left) {
                        PgNanComparison::IsNan => ExecutionExpr::IsNan(column),
                        PgNanComparison::IsNotNan => ExecutionExpr::IsNotNan(column),
                        PgNanComparison::StrictTrue => {
                            ExecutionExpr::StrictTrue(column)
                        }
                        PgNanComparison::StrictFalse => {
                            ExecutionExpr::StrictFalse(column)
                        }
                    })
                }),
        )
    }
}
