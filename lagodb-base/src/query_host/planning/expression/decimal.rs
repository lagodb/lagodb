//! Fixed decimal lowering of PostgreSQL NUMERIC expressions.
//!
//! A NUMERIC engine value always has a fixed Arrow decimal execution type
//! proven from its operands ([`FixedDecimalType`]): a bounded column, a
//! constant in its own digits and display scale, an integer cast, `+ - *`
//! over such values, and CASE/COALESCE over them in their common type. A
//! NUMERIC leaf outside a decimal expression keeps the scalar path, where an
//! integer AVG compares in Float64.

use lagodb_core::datum::{NumericRounding, NumericScaledDomain, NumericScaledValue};
use lagodb_core::expr::pg::PgExprRef;
use lagodb_core::expr::{PgComparisonOp, RuntimeValueSource, RuntimeValueSpec};
use lagodb_query::plan::{
    CaseWhen, DecimalOperator, ExecutionExpr, FixedDecimalType, ScalarFunctionKind,
};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, PredicateDomain,
    QueryExpressionPlanner,
};

impl QueryExpressionPlanner {
    /// Lower a NUMERIC operand of a comparison in the comparison's fixed type.
    pub(super) unsafe fn lower_decimal_operand(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        decimal: FixedDecimalType,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if let Some(output) = scope
            .outputs()
            .and_then(|catalog| unsafe { catalog.resolve_output(expression) })
            && FixedDecimalType::for_type(output.execution_type) == Some(decimal)
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
            // SAFETY: a non-NULL PostgreSQL Const owns a live datum for the
            // duration of planning. Binding is a capability proof only; the
            // executor binds the plan-owned expression again.
            if !unsafe { (*constant).constisnull }
                && !matches!(
                    unsafe {
                        decimal.domain().bind_datum(
                            (*constant).constvalue,
                            NumericRounding::Floor,
                        )
                    },
                    Ok(NumericScaledValue::Finite { exact: true, .. })
                )
            {
                return Err(ExpressionDecline::UnsupportedSemantics);
            }
            return Ok(self.push_decimal_constant(expression, scope, decimal));
        }
        if FixedDecimalType::for_type(Self::expr_type(expression)) != Some(decimal) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let value = unsafe { self.lower(expression.cast(), scope) }?;
        if value.decimal_type() != Some(decimal) {
            return Err(ExpressionDecline::UnsupportedRuntimeSource);
        }
        Ok(value)
    }

    /// Lower a NUMERIC expression to an engine value with a proven fixed
    /// type, or decline.
    pub(super) unsafe fn lower_decimal_value(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        // A NUMERIC-to-NUMERIC RelabelType (a domain) keeps the value.
        let expression = unsafe { Self::unwrap_relabel(expression) };
        if Self::expr_type(expression).type_oid != pg_sys::NUMERICOID {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let value = match unsafe { (*expression).type_ } {
            pg_sys::NodeTag::T_Const => {
                let constant = expression.cast::<pg_sys::Const>();
                let decimal = if unsafe { (*constant).constisnull } {
                    FixedDecimalType::new(1, 0)
                } else {
                    // SAFETY: a non-NULL planner Const owns a live datum.
                    unsafe { NumericScaledDomain::of_datum((*constant).constvalue) }
                        .and_then(|domain| {
                            FixedDecimalType::new(domain.precision(), domain.scale())
                        })
                }
                .ok_or(ExpressionDecline::UnsupportedSemantics)?;
                self.push_decimal_constant(expression, scope, decimal)
            }
            pg_sys::NodeTag::T_OpExpr => {
                unsafe { self.lower_decimal_arithmetic(expression.cast(), scope) }?
            }
            pg_sys::NodeTag::T_FuncExpr => {
                unsafe { self.lower_integer_to_decimal(expression.cast(), scope) }?
            }
            pg_sys::NodeTag::T_CaseExpr => {
                unsafe { self.lower_decimal_case(expression.cast(), scope) }?
            }
            pg_sys::NodeTag::T_CoalesceExpr => {
                let coalesce = expression.cast::<pg_sys::CoalesceExpr>();
                let arguments = unsafe { Self::list_nodes((*coalesce).args) }
                    .into_iter()
                    .map(|argument| unsafe {
                        self.lower_decimal_value(argument.cast(), scope)
                    })
                    .collect::<ExpressionPlanResult<Vec<_>>>()?;
                ExecutionExpr::Function {
                    kind: ScalarFunctionKind::Coalesce,
                    arguments: arguments.into_boxed_slice(),
                    input_collation: unsafe { (*coalesce).coalescecollid },
                    result_type: Self::expr_type(expression),
                }
            }
            _ => unsafe { self.lower_leaf(expression, scope) }?,
        };
        if value.decimal_type().is_none() {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(value)
    }

    /// `operand <op> $initplan` over a NUMERIC InitPlan result. The value is
    /// placed in the operand's fixed type when the plan is bound, so the
    /// comparison stays native; any other NUMERIC parameter has no value
    /// during planning and stays on the scalar path. An operand without a
    /// fixed type leaves the comparison to the other comparison paths.
    pub(super) unsafe fn lower_decimal_bound_comparison(
        &mut self,
        operator: PgComparisonOp,
        left: PgExprRef<'_>,
        right: PgExprRef<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> Option<ExecutionExpr> {
        let kind = domain.numeric_comparison(operator)?;
        let root = scope.source_root();
        let left_bound = unsafe { Self::is_initplan_numeric(root, left.as_ptr()) };
        let right_bound = unsafe { Self::is_initplan_numeric(root, right.as_ptr()) };
        let (operand, bound, kind) = match (left_bound, right_bound) {
            (false, true) => (left, right, kind),
            (true, false) => (right, left, kind.commuted()),
            _ => return None,
        };
        let operand = operand.as_ptr();
        let output = scope
            .outputs()
            .and_then(|catalog| unsafe { catalog.resolve_output(operand) });
        // An integer AVG keeps its Float64 comparison on the scalar path.
        let output = match output {
            Some(output) => Some((
                output.output,
                FixedDecimalType::for_type(output.execution_type)?,
            )),
            None => None,
        };
        self.attempt(|planner| {
            let (operand, operand_type) = match output {
                Some((output, operand_type)) => {
                    (ExecutionExpr::Output(output), operand_type)
                }
                None => {
                    let operand = unsafe {
                        planner.lower_decimal_value(operand, scope.as_scalar())
                    }?;
                    let operand_type = operand
                        .decimal_type()
                        .expect("lowered decimal values have a fixed type");
                    (operand, operand_type)
                }
            };
            let bound = bound.as_ptr();
            Ok(ExecutionExpr::DecimalBoundComparison {
                kind,
                operand: Box::new(operand),
                bound: planner.push_runtime(
                    root,
                    bound,
                    RuntimeValueSpec {
                        value_type: Self::expr_type(bound),
                        source_kind: RuntimeValueSource::ExecParam,
                    },
                ),
                operand_type,
            })
        })
        .ok()
    }

    unsafe fn lower_decimal_arithmetic(
        &mut self,
        expression: *mut pg_sys::OpExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        // eval_const_expressions has set opfuncid on every planned OpExpr.
        let operator =
            DecimalOperator::for_function(unsafe { (*expression).opfuncid }.to_u32())
                .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let [left, right] = unsafe { Self::list_nodes((*expression).args) }[..]
        else {
            return Err(ExpressionDecline::InvalidShape);
        };
        let left = unsafe { self.lower_decimal_value(left.cast(), scope) }?;
        let right = unsafe { self.lower_decimal_value(right.cast(), scope) }?;
        let result_type = left
            .decimal_type()
            .zip(right.decimal_type())
            .and_then(|(left, right)| left.arithmetic(operator, right))
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        Ok(ExecutionExpr::DecimalArithmetic {
            operator,
            left: Box::new(left),
            right: Box::new(right),
            result_type,
        })
    }

    /// The built-in `numeric(int2/int4/int8)` casts, exact in the integer's
    /// own digits.
    unsafe fn lower_integer_to_decimal(
        &mut self,
        expression: *mut pg_sys::FuncExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let input_type = match unsafe { (*expression).funcid }.to_u32() {
            pg_sys::F_NUMERIC_INT2 => pg_sys::INT2OID,
            pg_sys::F_NUMERIC_INT4 => pg_sys::INT4OID,
            pg_sys::F_NUMERIC_INT8 => pg_sys::INT8OID,
            _ => return Err(ExpressionDecline::UnsupportedSemantics),
        };
        let [value] = unsafe { Self::list_nodes((*expression).args) }[..] else {
            return Err(ExpressionDecline::InvalidShape);
        };
        let result_type = FixedDecimalType::for_integer(input_type)
            .expect("PostgreSQL integer types have a decimal image");
        Ok(ExecutionExpr::IntegerToDecimal {
            value: Box::new(unsafe { self.lower(value, scope.as_scalar()) }?),
            result_type,
        })
    }

    /// A searched CASE whose branches all have fixed types.
    unsafe fn lower_decimal_case(
        &mut self,
        expression: *mut pg_sys::CaseExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        if !unsafe { (*expression).arg }.is_null() {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let branches = unsafe { Self::list_nodes((*expression).args) };
        if branches.is_empty() {
            return Err(ExpressionDecline::InvalidShape);
        }
        let mut when_then = Vec::with_capacity(branches.len());
        for branch in branches {
            let branch = branch.cast::<pg_sys::CaseWhen>();
            let when = unsafe {
                self.lower(
                    (*branch).expr.cast(),
                    scope.with_predicate(scope.scalar_domain()),
                )
            }?;
            let then = unsafe { self.lower_decimal_value((*branch).result, scope) }?;
            when_then.push(CaseWhen::new(when, then));
        }
        let else_node = unsafe { (*expression).defresult };
        let else_expr = if else_node.is_null() {
            None
        } else {
            Some(Box::new(unsafe {
                self.lower_decimal_value(else_node, scope)
            }?))
        };
        Ok(ExecutionExpr::Case {
            when_then: when_then.into_boxed_slice(),
            else_expr,
            result_type: Self::expr_type(expression.cast()),
        })
    }

    fn push_decimal_constant(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        decimal_type: FixedDecimalType,
    ) -> ExecutionExpr {
        let value_type = Self::expr_type(expression);
        ExecutionExpr::DecimalValue {
            value: self.push_runtime(
                scope.source_root(),
                expression,
                RuntimeValueSpec {
                    value_type,
                    source_kind: RuntimeValueSource::Constant,
                },
            ),
            decimal_type,
        }
    }

    /// Whether `expression` is a NUMERIC PARAM_EXEC set by an InitPlan of
    /// this query level or an enclosing one. PostgreSQL has registered every
    /// InitPlan in `init_plans` while preprocessing expressions, before any
    /// path is generated, and evaluates it once per (re)scan of its level.
    unsafe fn is_initplan_numeric(
        mut root: *mut pg_sys::PlannerInfo,
        expression: *mut pg_sys::Expr,
    ) -> bool {
        let expression = unsafe { Self::unwrap_relabel(expression) };
        if unsafe { (*expression).type_ } != pg_sys::NodeTag::T_Param {
            return false;
        }
        let parameter = unsafe { &*expression.cast::<pg_sys::Param>() };
        if parameter.paramkind != pg_sys::ParamKind::PARAM_EXEC
            || parameter.paramtype != pg_sys::NUMERICOID
        {
            return false;
        }
        while !root.is_null() {
            let init_plans = unsafe { (*root).init_plans };
            for subplan in unsafe { Self::list_nodes(init_plans) } {
                let subplan = subplan.cast::<pg_sys::SubPlan>();
                if unsafe {
                    pg_sys::list_member_int((*subplan).setParam, parameter.paramid)
                } {
                    return true;
                }
            }
            root = unsafe { (*root).parent_root };
        }
        false
    }
}
