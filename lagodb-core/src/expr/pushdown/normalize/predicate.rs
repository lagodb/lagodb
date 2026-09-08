//! Operation-family normalization kept on the shared expression normalizer.

use pgrx::pg_sys;

use crate::expr::pg::{PgConst, PgExprRef};
use crate::expr::{
    PgComparisonOp, PgNanComparison, PgTextComparisonSemantics, RuntimeValueExpr,
};

use super::{ExpressionNormalizer, PredicateExpr, ScalarExpr};

impl ExpressionNormalizer<'_> {
    pub(super) unsafe fn normalize_starts_with(
        &self,
        value: PgExprRef<'_>,
        prefix: PgExprRef<'_>,
        input_collation: pg_sys::Oid,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
        unsafe {
            PgTextComparisonSemantics::for_equality_collation(input_collation)?;
        }
        let value = self.normalize_scalar(value, bindings)?;
        if !matches!(value, ScalarExpr::Column(_)) {
            return None;
        }
        let prefix = self.normalize_scalar(prefix, bindings)?;
        if !matches!(prefix, ScalarExpr::Value(_)) {
            return None;
        }
        Some(PredicateExpr::StartsWith { value, prefix })
    }

    pub(super) unsafe fn normalize_like_prefix(
        &self,
        op: PgComparisonOp,
        value: PgExprRef<'_>,
        pattern: PgExprRef<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
        if op.opfuncid != pg_sys::Oid::from(pg_sys::F_TEXTLIKE)
            || op.opresulttype != pg_sys::BOOLOID
            || op.opcollid != pg_sys::Oid::INVALID
        {
            return None;
        }
        let constant = PgConst::try_from_expr(pattern.without_relabels())?;
        let prefix = unsafe { constant.pure_like_prefix() }?;
        unsafe { self.normalize_starts_with(value, prefix, op.inputcollid, bindings) }
    }

    pub(super) unsafe fn normalize_nan_comparison(
        &self,
        op: PgComparisonOp,
        left: PgExprRef<'_>,
        right: PgExprRef<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
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
        let checkpoint = bindings.len();
        let normalized = match (left_nan, right_nan) {
            (true, false) => self
                .normalize_scalar(right, bindings)
                .map(|column| (column, true)),
            (false, true) => self
                .normalize_scalar(left, bindings)
                .map(|column| (column, false)),
            _ => None,
        };
        let Some((column, nan_on_left)) = normalized else {
            bindings.truncate(checkpoint);
            return None;
        };
        if !matches!(column, ScalarExpr::Column(_)) {
            bindings.truncate(checkpoint);
            return None;
        }
        Some(match signature.kind().with_nan_on(nan_on_left) {
            PgNanComparison::IsNan => PredicateExpr::IsNan(column),
            PgNanComparison::IsNotNan => PredicateExpr::IsNotNan(column),
            PgNanComparison::StrictTrue => PredicateExpr::StrictTrue(column),
            PgNanComparison::StrictFalse => PredicateExpr::StrictFalse(column),
        })
    }
}
