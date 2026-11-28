//! Exact lowering for PostgreSQL predicate leaves.

use lagodb_core::expr::pg::{PgConst, PgExprRef, PgNullTestKind, PgPredicateLeafRef};
use lagodb_core::expr::{
    ExprType, PgComparisonOp, PgDateComparisonOutcome, PgDateTimestampComparison,
    PgNanComparison, PgTextComparisonSemantics,
};
use lagodb_query::plan::{ExecutionExpr, FixedDecimalType, ScalarFunctionKind};
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
            PgPredicateLeafRef::BooleanTest { kind, value } => {
                Ok(ExecutionExpr::BooleanTest {
                    kind,
                    value: Box::new(unsafe {
                        self.lower(value.as_ptr().cast(), scope.as_scalar())
                    }?),
                })
            }
            PgPredicateLeafRef::Comparison { op, left, right } => {
                if matches!(
                    u32::from(op.opfuncid),
                    pg_sys::F_TEXTLIKE | pg_sys::F_TEXTNLIKE
                ) {
                    return unsafe { self.lower_like(op, left, right, scope) };
                }
                if let Some(result) = unsafe {
                    self.lower_bpchar_comparison(op, left, right, scope, domain)
                } {
                    return result;
                }
                if let Some(result) = unsafe {
                    self.lower_date_timestamp_comparison(
                        op, left, right, scope, domain,
                    )
                } {
                    return result;
                }
                if let Some(result) =
                    unsafe { self.lower_nan_comparison(op, left, right, scope) }
                {
                    return result;
                }
                if let Some(comparison) = unsafe {
                    self.lower_decimal_bound_comparison(
                        op, left, right, scope, domain,
                    )
                } {
                    return Ok(comparison);
                }
                let left_type = unsafe { Self::predicate_type(left.as_ptr(), scope) };
                let right_type =
                    unsafe { Self::predicate_type(right.as_ptr(), scope) };
                if let Some(result) = unsafe {
                    self.lower_decimal_expression_comparison(
                        op,
                        left.as_ptr(),
                        right.as_ptr(),
                        [left_type, right_type],
                        scope,
                        domain,
                    )
                } {
                    return result;
                }
                let decimal = domain
                    .decimal_comparison(op, left_type, right_type)
                    .map(|(_, left, right)| (left, right));
                // The decimal path binds an operand of no fixed type only as a
                // direct Const. Any other NUMERIC comparison without two fixed
                // decimal operands compares in its operands' engine
                // representations.
                let binds_as_decimal =
                    |expression: PgExprRef<'_>, value_type: ExprType| {
                        FixedDecimalType::for_type(value_type).is_some()
                            || unsafe { Self::is_direct_const(expression.as_ptr()) }
                    };
                if domain.numeric_comparison(op).is_some()
                    && (decimal.is_none()
                        || !binds_as_decimal(left, left_type)
                        || !binds_as_decimal(right, right_type))
                {
                    return unsafe {
                        self.lower_numeric_repr_comparison(
                            op,
                            left.as_ptr(),
                            right.as_ptr(),
                            scope,
                            domain,
                        )
                    };
                }
                if decimal.is_none()
                    && domain.comparison(op, left_type, right_type).is_none()
                {
                    return Err(ExpressionDecline::UnsupportedSemantics);
                }
                let left = if let Some((decimal, _)) = decimal {
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
                let right = if let Some((_, decimal)) = decimal {
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
                self.lower_text_match(
                    ScalarFunctionKind::StartsWith,
                    value,
                    prefix,
                    input_collation,
                    scope,
                )
            },
        }
    }

    unsafe fn lower_date_timestamp_comparison(
        &mut self,
        operator: PgComparisonOp,
        left: PgExprRef<'_>,
        right: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> Option<ExpressionPlanResult<ExecutionExpr>> {
        let normalized =
            unsafe { PgDateTimestampComparison::normalize(operator, left, right) }?;
        Some(unsafe {
            self.lower_normalized_date_timestamp(normalized, scope, domain)
        })
    }

    unsafe fn lower_normalized_date_timestamp(
        &mut self,
        normalized: PgDateTimestampComparison<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let column = normalized.column();
        let column_type = unsafe { Self::predicate_type(column.as_ptr(), scope) };
        if !domain.supports_type(column_type) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let column =
            unsafe { self.lower(column.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&column, ExecutionExpr::Column(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let column = Box::new(column);
        Ok(match normalized.outcome() {
            PgDateComparisonOutcome::Compare { operator, bound } => {
                let bound_type =
                    unsafe { Self::predicate_type(bound.as_ptr(), scope) };
                domain
                    .comparison(operator, column_type, bound_type)
                    .ok_or(ExpressionDecline::UnsupportedSemantics)?;
                // A derived date Const always lowers to a runtime value; an
                // infinite bound has no Arrow date and declines there.
                let bound =
                    unsafe { self.lower(bound.as_ptr().cast(), scope.as_scalar()) }?;
                ExecutionExpr::Comparison {
                    operator,
                    left: column,
                    right: Box::new(bound),
                }
            }
            PgDateComparisonOutcome::StrictTrue => ExecutionExpr::StrictTrue(column),
            PgDateComparisonOutcome::StrictFalse => {
                ExecutionExpr::StrictFalse(column)
            }
        })
    }

    /// A deterministic-collation text column matched against a constant:
    /// `starts_with` or LIKE.
    unsafe fn lower_text_match(
        &mut self,
        kind: ScalarFunctionKind,
        value: PgExprRef<'_>,
        pattern: PgExprRef<'_>,
        input_collation: pg_sys::Oid,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if unsafe {
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
        let pattern =
            unsafe { self.lower(pattern.as_ptr().cast(), scope.as_scalar()) }?;
        if !matches!(&pattern, ExecutionExpr::Value(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(ExecutionExpr::Function {
            kind,
            arguments: vec![value, pattern].into_boxed_slice(),
            input_collation,
            result_type: ExprType {
                type_oid: pg_sys::BOOLOID,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            },
        })
    }

    /// Text LIKE and NOT LIKE over a constant pattern. A LIKE with a pure
    /// `prefix%` pattern becomes `starts_with`, which scan pruning
    /// understands; any other pattern PostgreSQL accepts runs in Arrow's LIKE
    /// kernel. NOT LIKE is the negated match, which keeps a NULL operand NULL.
    /// It always keeps the LIKE kernel, which folds the negation into its one
    /// pass, and does not prune the scan.
    unsafe fn lower_like(
        &mut self,
        op: PgComparisonOp,
        value: PgExprRef<'_>,
        pattern: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if op.opresulttype != pg_sys::BOOLOID || op.opcollid != pg_sys::Oid::INVALID {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let pattern = pattern.without_relabels();
        let constant = PgConst::try_from_expr(pattern)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let negated = u32::from(op.opfuncid) == pg_sys::F_TEXTNLIKE;
        let prefix = if negated {
            None
        } else {
            unsafe { constant.pure_like_prefix() }
        };
        let matched = if let Some(prefix) = prefix {
            unsafe {
                self.lower_text_match(
                    ScalarFunctionKind::StartsWith,
                    value,
                    prefix,
                    op.inputcollid,
                    scope,
                )
            }?
        } else if constant.is_complete_like_pattern() {
            unsafe {
                self.lower_text_match(
                    ScalarFunctionKind::Like,
                    value,
                    pattern,
                    op.inputcollid,
                    scope,
                )
            }?
        } else {
            return Err(ExpressionDecline::UnsupportedSemantics);
        };
        Ok(if negated {
            ExecutionExpr::Not(Box::new(matched))
        } else {
            matched
        })
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
