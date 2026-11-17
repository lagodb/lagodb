//! Exact `character(n)` equality against constants padded to the column width.

use lagodb_core::expr::pg::{PgConst, PgExprRef};
use lagodb_core::expr::{PgComparisonOp, RuntimeValueSource, RuntimeValueSpec};
use lagodb_query::plan::{ComparisonKind, ExecutionExpr};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, PredicateDomain,
    QueryExpressionPlanner,
};

impl QueryExpressionPlanner {
    /// Lower a built-in `bpchar` comparison, or return `None` for any other
    /// operator family.
    ///
    /// Only a direct `character(n)` column compared by `=` / `<>` with a
    /// direct constant is admitted: the column's stored values are padded to
    /// its width, and execution pads the constant identically
    /// ([`ExecutionExpr::BpcharValue`]). No stored value equals a constant
    /// with more characters than the width, so that comparison folds to
    /// FALSE (`=`) or TRUE (`<>`) for every non-NULL value.
    pub(super) unsafe fn lower_bpchar_comparison(
        &mut self,
        operator: PgComparisonOp,
        left: PgExprRef<'_>,
        right: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> Option<ExpressionPlanResult<ExecutionExpr>> {
        let signature = operator.builtin_signature()?;
        if signature.left_type() != pg_sys::BPCHAROID
            || signature.right_type() != pg_sys::BPCHAROID
        {
            return None;
        }
        Some(unsafe {
            self.lower_bpchar_operands(operator, (left, right), scope, domain)
        })
    }

    unsafe fn lower_bpchar_operands(
        &mut self,
        operator: PgComparisonOp,
        (left, right): (PgExprRef<'_>, PgExprRef<'_>),
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let left_constant = PgConst::try_from_expr(left.without_relabels());
        let right_constant = PgConst::try_from_expr(right.without_relabels());
        let (column, value, constant, value_on_left) =
            match (left_constant, right_constant) {
                (None, Some(constant)) => (left, right, constant, false),
                (Some(constant), None) => (right, left, constant, true),
                _ => return Err(ExpressionDecline::UnsupportedSemantics),
            };
        let value_type = Self::expr_type(value.as_ptr());
        let (kind, semantics) = domain
            .bpchar_comparison(operator, Self::expr_type(column.as_ptr()), value_type)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let lowered_column =
            unsafe { self.lower(column.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&lowered_column, ExecutionExpr::Column(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let (_, _, datum, is_null) = constant.parts();
        // SAFETY: a non-NULL PostgreSQL Const owns a live bpchar datum for
        // the duration of planning.
        if !is_null && !unsafe { semantics.admits(datum) } {
            let column = Box::new(lowered_column);
            // The bpchar proof admits only `=` and `<>`.
            return Ok(match kind {
                ComparisonKind::NotEqual => ExecutionExpr::StrictTrue(column),
                _ => ExecutionExpr::StrictFalse(column),
            });
        }
        let padded = ExecutionExpr::BpcharValue {
            value: self.push_runtime(
                scope.source_root(),
                value.as_ptr(),
                RuntimeValueSpec {
                    value_type,
                    source_kind: RuntimeValueSource::Constant,
                },
            ),
            semantics,
        };
        let (left, right) = if value_on_left {
            (padded, lowered_column)
        } else {
            (lowered_column, padded)
        };
        Ok(ExecutionExpr::Comparison {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        })
    }
}
