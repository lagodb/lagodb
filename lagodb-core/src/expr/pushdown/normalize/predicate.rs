//! Operation-family normalization kept on the shared expression normalizer.

use pgrx::pg_sys;

use crate::expr::pg::{PgConst, PgExprRef, PgScalarArrayOpExpr};
use crate::expr::{
    PgComparisonKind, PgComparisonOp, PgComparisonSignature, PgNanComparison,
    PgTextComparisonSemantics, RuntimeValueExpr,
};

use super::{ExpressionNormalizer, ExpressionScope, PredicateExpr, ScalarExpr};

impl ExpressionNormalizer<'_> {
    /// Normalize `column op ANY (array)` with the array in one value slot.
    ///
    /// Query scope declines the leaf. PostgreSQL negotiation there only sets
    /// costing and EXPLAIN identity; DataFusion negotiates the filter it
    /// executes, and that filter carries no set. Accepting it here would credit
    /// file selection that execution never performs.
    pub(super) fn normalize_in_set(
        &self,
        membership: PgScalarArrayOpExpr<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
        if matches!(self.scope, ExpressionScope::Query(_)) || !membership.use_or() {
            return None;
        }
        let (value, set) = membership.operands();
        let value = self.normalize_scalar(value, bindings)?;
        if !matches!(value, ScalarExpr::Column(_)) {
            return None;
        }
        let set = self.normalize_scalar(set, bindings)?;
        if !matches!(set, ScalarExpr::Value(_)) {
            return None;
        }
        Some(PredicateExpr::InSet {
            operator: membership.comparison_op(),
            value,
            set,
        })
    }

    pub(super) unsafe fn normalize_boolean_column(
        &self,
        value: PgExprRef<'_>,
        bindings: &mut Vec<RuntimeValueExpr>,
    ) -> Option<PredicateExpr> {
        let left = self.normalize_scalar(value, bindings)?;
        let constant = unsafe { pg_sys::makeBoolConst(true, false) };
        let right = self.normalize_scalar(
            unsafe { PgExprRef::from_raw(constant.cast()) },
            bindings,
        )?;
        let signature = PgComparisonSignature::for_types(
            pg_sys::BOOLOID,
            pg_sys::BOOLOID,
            PgComparisonKind::Equal,
        )
        .expect("built-in boolean equality is registered");
        Some(PredicateExpr::Comparison {
            operator: PgComparisonOp {
                opno: signature.operator_oid(),
                opfuncid: signature.function_oid(),
                opresulttype: pg_sys::BOOLOID,
                opcollid: pg_sys::InvalidOid,
                inputcollid: pg_sys::InvalidOid,
            },
            left,
            right,
        })
    }

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
