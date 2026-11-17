//! Exact `character(n)` equality and membership against constants padded to
//! the column width.

use lagodb_core::expr::pg::{PgConst, PgExprRef, PgScalarArrayOpExpr};
use lagodb_core::expr::{
    ExprType, PgBpcharSemantics, PgComparisonOp, PgTextComparisonSemantics,
    RuntimeValueSource, RuntimeValueSpec,
};
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
        let (kind, semantics) = domain
            .bpchar_comparison(
                operator,
                Self::expr_type(column.as_ptr()),
                Self::expr_type(value.as_ptr()),
            )
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let lowered_column = unsafe { self.lower_bpchar_column(column, scope) }?;
        let Some(padded) =
            (unsafe { self.lower_bpchar_value(value, constant, semantics, scope) })
        else {
            let column = Box::new(lowered_column);
            // The bpchar proof admits only `=` and `<>`.
            return Ok(match kind {
                ComparisonKind::NotEqual => ExecutionExpr::StrictTrue(column),
                _ => ExecutionExpr::StrictFalse(column),
            });
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

    /// Lower `character(n)` membership in a list of direct constants
    /// (`= ANY` / `<> ALL`).
    ///
    /// An element wider than the column equals no stored value, so it is
    /// dropped from the OR (`IN`) or the AND (`NOT IN`); when none remain the
    /// membership folds exactly as a single over-width comparison does.
    pub(super) unsafe fn lower_bpchar_in_list(
        &mut self,
        membership: PgScalarArrayOpExpr<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let (column, array) = membership.operands();
        let column_type = Self::expr_type(column.as_ptr());
        // A PostgreSQL array has one element type, so one proof covers every
        // element.
        let element_type = ExprType {
            type_oid: unsafe { pg_sys::get_element_type(array.type_oid()) },
            typmod: -1,
            collation: array.collation(),
        };
        let (kind, semantics) = domain
            .bpchar_comparison(membership.comparison_op(), column_type, element_type)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        // InList keeps no operator, so its validator proves byte equality from
        // the column's collation, as for text; require it here as well.
        if unsafe {
            PgTextComparisonSemantics::for_equality_collation(column_type.collation)
        }
        .is_none()
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let negated = membership
            .membership_negation(kind)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let column = Box::new(unsafe { self.lower_bpchar_column(column, scope) }?);
        let elements = unsafe { Self::scalar_array_elements(array) }?;
        let mut list = Vec::with_capacity(elements.len());
        for element in elements {
            let value = unsafe { PgExprRef::from_raw(element.cast()) };
            let constant = PgConst::try_from_expr(value.without_relabels())
                .ok_or(ExpressionDecline::UnsupportedSemantics)?;
            list.extend(unsafe {
                self.lower_bpchar_value(value, constant, semantics, scope)
            });
        }
        Ok(match (list.is_empty(), negated) {
            (true, false) => ExecutionExpr::StrictFalse(column),
            (true, true) => ExecutionExpr::StrictTrue(column),
            (false, _) => ExecutionExpr::InList {
                value: column,
                list: list.into_boxed_slice(),
                negated,
            },
        })
    }

    /// The compared value must be a direct `character(n)` column, whose
    /// stored values are padded to its width.
    unsafe fn lower_bpchar_column(
        &mut self,
        column: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let lowered =
            unsafe { self.lower(column.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&lowered, ExecutionExpr::Column(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(lowered)
    }

    /// Lower a direct constant padded to the column width at execution, or
    /// return `None` for a non-NULL constant wider than the column.
    unsafe fn lower_bpchar_value(
        &mut self,
        value: PgExprRef<'_>,
        constant: PgConst<'_>,
        semantics: PgBpcharSemantics,
        scope: ExpressionScope<'_>,
    ) -> Option<ExecutionExpr> {
        let (_, _, datum, is_null) = constant.parts();
        // SAFETY: a non-NULL PostgreSQL Const owns a live bpchar datum for
        // the duration of planning.
        if !is_null && !unsafe { semantics.admits(datum) } {
            return None;
        }
        Some(ExecutionExpr::BpcharValue {
            value: self.push_runtime(
                scope.source_root(),
                value.as_ptr(),
                RuntimeValueSpec {
                    value_type: Self::expr_type(value.as_ptr()),
                    source_kind: RuntimeValueSource::Constant,
                },
            ),
            semantics,
        })
    }
}
