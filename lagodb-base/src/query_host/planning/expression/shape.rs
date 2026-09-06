//! Native lowering for PostgreSQL expression shapes beyond scalar functions.

use lagodb_core::expr::{ExprType, PgComparisonKind, PgComparisonOp};
use lagodb_query::plan::{
    BooleanTestKind, CaseWhen, ExecutionExpr, ScalarFunctionKind,
};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, PredicateDomain,
    QueryExpressionPlanner,
};

impl QueryExpressionPlanner {
    pub(super) unsafe fn lower_native_shape(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        match unsafe { (*expression).type_ } {
            pg_sys::NodeTag::T_RelabelType => unsafe {
                self.lower_relabel(expression.cast(), scope)
            },
            pg_sys::NodeTag::T_BooleanTest => unsafe {
                self.lower_boolean_test(expression.cast(), scope, domain)
            },
            pg_sys::NodeTag::T_CaseExpr => unsafe {
                self.lower_case(expression.cast(), scope, domain)
            },
            pg_sys::NodeTag::T_CoalesceExpr => unsafe {
                self.lower_coalesce(expression.cast(), scope)
            },
            pg_sys::NodeTag::T_NullIfExpr => unsafe {
                self.lower_nullif(expression.cast(), scope)
            },
            pg_sys::NodeTag::T_ScalarArrayOpExpr => unsafe {
                self.lower_scalar_array(expression.cast(), scope)
            },
            // CoerceViaIO is deliberately handled by PostgreSQL fallback.
            // NAME input/output can truncate at NAMEDATALEN, so the text-family
            // cast is not generally a value-preserving Arrow no-op.
            tag => Err(ExpressionDecline::UnsupportedNode(tag)),
        }
    }

    unsafe fn lower_relabel(
        &mut self,
        expression: *mut pg_sys::RelabelType,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let argument = unsafe { (*expression).arg };
        let source_type = unsafe { pg_sys::exprType(argument.cast()) };
        let result_type = unsafe { (*expression).resulttype };
        if !unsafe { pg_sys::IsBinaryCoercible(source_type, result_type) } {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(ExecutionExpr::Relabel {
            value: Box::new(unsafe {
                self.lower(argument.cast(), scope.as_scalar())
            }?),
            result_type: Self::expr_type(expression.cast()),
        })
    }

    unsafe fn lower_boolean_test(
        &mut self,
        expression: *mut pg_sys::BooleanTest,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let kind = match unsafe { (*expression).booltesttype } {
            pg_sys::BoolTestType::IS_TRUE => BooleanTestKind::IsTrue,
            pg_sys::BoolTestType::IS_NOT_TRUE => BooleanTestKind::IsNotTrue,
            pg_sys::BoolTestType::IS_FALSE => BooleanTestKind::IsFalse,
            pg_sys::BoolTestType::IS_NOT_FALSE => BooleanTestKind::IsNotFalse,
            pg_sys::BoolTestType::IS_UNKNOWN => BooleanTestKind::IsUnknown,
            pg_sys::BoolTestType::IS_NOT_UNKNOWN => BooleanTestKind::IsNotUnknown,
            _ => return Err(ExpressionDecline::InvalidShape),
        };
        Ok(ExecutionExpr::BooleanTest {
            kind,
            value: Box::new(unsafe {
                self.lower((*expression).arg.cast(), scope.with_predicate(domain))
            }?),
        })
    }

    unsafe fn lower_case(
        &mut self,
        expression: *mut pg_sys::CaseExpr,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        // A simple CASE contains CaseTestExpr placeholders and requires a
        // separate comparison-semantic proof. PostgreSQL evaluates that form
        // as one fallback subtree for now.
        if !unsafe { (*expression).arg }.is_null() {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let result_type = Self::expr_type(expression.cast());
        let args = unsafe { (*expression).args };
        let count = unsafe { pg_sys::list_length(args) };
        if count == 0 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let mut when_then = Vec::with_capacity(count as usize);
        for index in 0..count {
            let branch =
                unsafe { pg_sys::list_nth(args, index) }.cast::<pg_sys::CaseWhen>();
            let when = unsafe {
                self.lower((*branch).expr.cast(), scope.with_predicate(domain))
            }?;
            let then_type = Self::expr_type(unsafe { (*branch).result });
            if then_type != result_type {
                return Err(ExpressionDecline::UnsupportedSemantics);
            }
            let then =
                unsafe { self.lower((*branch).result.cast(), scope.as_scalar()) }?;
            when_then.push(CaseWhen::new(when, then));
        }
        let else_node = unsafe { (*expression).defresult };
        let else_expr = if else_node.is_null() {
            None
        } else {
            if Self::expr_type(else_node) != result_type {
                return Err(ExpressionDecline::UnsupportedSemantics);
            }
            Some(Box::new(unsafe {
                self.lower(else_node.cast(), scope.as_scalar())
            }?))
        };
        Ok(ExecutionExpr::Case {
            when_then: when_then.into_boxed_slice(),
            else_expr,
            result_type,
        })
    }

    unsafe fn lower_coalesce(
        &mut self,
        expression: *mut pg_sys::CoalesceExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let result_type = Self::expr_type(expression.cast());
        let arguments = unsafe {
            self.lower_arguments((*expression).args, scope, Some(result_type))
        }?;
        Ok(ExecutionExpr::Function {
            kind: ScalarFunctionKind::Coalesce,
            arguments: arguments.into_boxed_slice(),
            input_collation: unsafe { (*expression).coalescecollid },
            result_type,
        })
    }

    unsafe fn lower_nullif(
        &mut self,
        expression: *mut pg_sys::OpExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let nodes = unsafe { Self::list_nodes((*expression).args) };
        if nodes.len() != 2 {
            return Err(ExpressionDecline::InvalidShape);
        }
        let left_type = Self::expr_type(nodes[0].cast());
        let right_type = Self::expr_type(nodes[1].cast());
        let operator = PgComparisonOp {
            opno: unsafe { (*expression).opno },
            opfuncid: unsafe { (*expression).opfuncid },
            opresulttype: pg_sys::BOOLOID,
            opcollid: unsafe { (*expression).opcollid },
            inputcollid: unsafe { (*expression).inputcollid },
        };
        if PredicateDomain::Integer.comparison(operator, left_type, right_type)
            != Some(PgComparisonKind::Equal)
            || left_type != right_type
            || Self::expr_type(expression.cast()) != left_type
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let arguments = nodes
            .into_iter()
            .map(|node| unsafe { self.lower(node, scope.as_scalar()) })
            .collect::<ExpressionPlanResult<Vec<_>>>()?;
        Ok(ExecutionExpr::Function {
            kind: ScalarFunctionKind::NullIf,
            arguments: arguments.into_boxed_slice(),
            input_collation: unsafe { (*expression).inputcollid },
            result_type: left_type,
        })
    }

    unsafe fn lower_scalar_array(
        &mut self,
        expression: *mut pg_sys::ScalarArrayOpExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let args = unsafe { Self::list_nodes((*expression).args) };
        if args.len() != 2
            || unsafe { (*args[1]).type_ } != pg_sys::NodeTag::T_ArrayExpr
        {
            return Err(ExpressionDecline::InvalidShape);
        }
        let array = args[1].cast::<pg_sys::ArrayExpr>();
        if unsafe { (*array).multidims } {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let elements = unsafe { Self::list_nodes((*array).elements) };
        if elements.is_empty() {
            return Err(ExpressionDecline::InvalidShape);
        }
        let value_type = Self::expr_type(args[0].cast());
        if !PredicateDomain::Integer.supports_type(value_type)
            || elements
                .iter()
                .any(|element| Self::expr_type((*element).cast()) != value_type)
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let operator = PgComparisonOp {
            opno: unsafe { (*expression).opno },
            opfuncid: unsafe { (*expression).opfuncid },
            opresulttype: pg_sys::BOOLOID,
            opcollid: pg_sys::InvalidOid,
            inputcollid: unsafe { (*expression).inputcollid },
        };
        let comparison = PredicateDomain::Integer
            .comparison(operator, value_type, value_type)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let negated = match (unsafe { (*expression).useOr }, comparison) {
            (true, PgComparisonKind::Equal) => false,
            (false, PgComparisonKind::NotEqual) => true,
            _ => return Err(ExpressionDecline::UnsupportedSemantics),
        };
        Ok(ExecutionExpr::InList {
            value: Box::new(unsafe { self.lower(args[0], scope.as_scalar()) }?),
            list: elements
                .into_iter()
                .map(|element| unsafe { self.lower(element, scope.as_scalar()) })
                .collect::<ExpressionPlanResult<Vec<_>>>()?
                .into_boxed_slice(),
            negated,
        })
    }

    unsafe fn lower_arguments(
        &mut self,
        list: *mut pg_sys::List,
        scope: ExpressionScope<'_>,
        required_type: Option<ExprType>,
    ) -> ExpressionPlanResult<Vec<ExecutionExpr>> {
        let nodes = unsafe { Self::list_nodes(list) };
        if nodes.is_empty()
            || required_type.is_some_and(|required| {
                nodes
                    .iter()
                    .any(|node| Self::expr_type((*node).cast()) != required)
            })
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        nodes
            .into_iter()
            .map(|node| unsafe { self.lower(node, scope.as_scalar()) })
            .collect()
    }

    unsafe fn list_nodes(list: *mut pg_sys::List) -> Vec<*mut pg_sys::Node> {
        let count = unsafe { pg_sys::list_length(list) };
        (0..count)
            .map(|index| unsafe { pg_sys::list_nth(list, index) }.cast())
            .collect()
    }
}
