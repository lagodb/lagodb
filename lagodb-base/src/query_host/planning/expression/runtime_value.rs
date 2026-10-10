//! Planner-owned runtime expressions and temporal constant admission.

use lagodb_core::expr::pg::{PgConst, PgExprRef};
use lagodb_core::expr::{RuntimeValueSource, RuntimeValueSpec};
use lagodb_query::plan::{ExecutionExpr, ExecutionScalarRepr};
use pgrx::{FromDatum, pg_sys};

use super::{ExpressionDecline, ExpressionPlanResult, QueryExpressionPlanner};

impl QueryExpressionPlanner {
    pub(super) fn lower_runtime_value(
        &mut self,
        root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Expr,
        source_kind: RuntimeValueSource,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let value_type = Self::expr_type(expression);
        let representation = ExecutionScalarRepr::for_runtime_value(value_type)
            .ok_or(ExpressionDecline::UnsupportedType(value_type.type_oid))?;
        // SAFETY: runtime expressions are borrowed from the current planner tree.
        if !unsafe {
            Self::supports_runtime_expression(
                representation,
                PgExprRef::from_raw(expression),
                source_kind,
            )
        } {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(ExecutionExpr::Value(self.push_runtime(
            root,
            expression,
            RuntimeValueSpec {
                value_type,
                source_kind,
            },
        )))
    }

    /// Temporal values must be known and representable before selecting native
    /// execution. Parameters retain PG evaluation; their values are not known
    /// during planning. This follows the existing Decimal128 operand boundary.
    ///
    /// # Safety
    /// The expression and its constant datum remain live in the planner context.
    unsafe fn supports_runtime_expression(
        representation: ExecutionScalarRepr,
        expression: PgExprRef<'_>,
        source: RuntimeValueSource,
    ) -> bool {
        if !matches!(
            representation,
            ExecutionScalarRepr::Date
                | ExecutionScalarRepr::Timestamp
                | ExecutionScalarRepr::Timestamptz
        ) {
            return true;
        }
        if source != RuntimeValueSource::Constant {
            return false;
        }
        let Some(constant) = PgConst::try_from_expr(expression.without_relabels())
        else {
            return false;
        };
        let (_, _, datum, is_null) = constant.parts();
        if is_null {
            return true;
        }
        match representation {
            ExecutionScalarRepr::Date => unsafe { i32::from_datum(datum, false) }
                .is_some_and(ExecutionScalarRepr::supports_date_value),
            ExecutionScalarRepr::Timestamp | ExecutionScalarRepr::Timestamptz => {
                unsafe { i64::from_datum(datum, false) }
                    .is_some_and(ExecutionScalarRepr::supports_timestamp_value)
            }
            _ => unreachable!("temporal representation selected above"),
        }
    }
}
