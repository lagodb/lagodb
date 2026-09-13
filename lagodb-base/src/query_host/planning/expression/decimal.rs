//! Exact finite Decimal128 operands for native query comparisons.

use lagodb_core::expr::{RuntimeValueSource, RuntimeValueSpec};
use lagodb_query::plan::{Decimal128Semantics, ExecutionExpr};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, QueryExpressionPlanner,
};

impl QueryExpressionPlanner {
    pub(super) unsafe fn lower_decimal_operand(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        semantics: Decimal128Semantics,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if let Some(output) = scope
            .outputs()
            .and_then(|catalog| unsafe { catalog.resolve_output(expression) })
            && Decimal128Semantics::for_type(output.execution_type) == Some(semantics)
        {
            return Ok(ExecutionExpr::Output(output.output));
        }
        let direct = unsafe { Self::unwrap_relabel(expression) };
        if unsafe { (*direct).type_ } == pg_sys::NodeTag::T_Const {
            let constant = direct.cast::<pg_sys::Const>();
            if unsafe { (*constant).consttype } != pg_sys::NUMERICOID {
                return Err(ExpressionDecline::UnsupportedType(unsafe {
                    (*constant).consttype
                }));
            }
            if !unsafe { (*constant).constisnull } {
                let codec = semantics.codec();
                // SAFETY: a non-NULL PostgreSQL Const owns a live datum for the
                // duration of planning. Encoding is a capability proof only;
                // the executor binds the plan-owned expression again.
                unsafe { codec.encode_bound_datum((*constant).constvalue) }
                    .map_err(|_| ExpressionDecline::UnsupportedSemantics)?;
            }
            let value_type = Self::expr_type(expression);
            return Ok(ExecutionExpr::DecimalValue {
                value: self.push_runtime(
                    expression,
                    RuntimeValueSpec {
                        value_type,
                        source_kind: RuntimeValueSource::Constant,
                    },
                ),
                semantics,
            });
        }
        if Decimal128Semantics::for_type(Self::expr_type(expression))
            != Some(semantics)
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let value = unsafe { self.lower(expression.cast(), scope) }?;
        if value.decimal128_semantics() != Some(semantics) {
            return Err(ExpressionDecline::UnsupportedRuntimeSource);
        }
        Ok(value)
    }
}
