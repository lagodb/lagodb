//! Native lowering for PostgreSQL expression shapes other than classified
//! scalar calls: relabels, integer widening casts, boolean tests,
//! conditionals and scalar-array comparisons.

use lagodb_core::expr::pg::{
    PgBooleanTest, PgConst, PgExprRef, PgFuncExpr, PgScalarArrayOpExpr,
};
use lagodb_core::expr::{
    ExprType, PgComparisonKind, PgComparisonOp, PgIntegerWidening,
};
use lagodb_query::plan::{
    CaseWhen, Decimal128Semantics, ExecutionExpr, FixedDecimalType,
    ScalarFunctionKind,
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
            pg_sys::NodeTag::T_FuncExpr => unsafe {
                self.lower_integer_widening(expression.cast(), scope)
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
                self.lower_scalar_array(expression.cast(), scope, domain)
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

    /// One of PostgreSQL's lossless integer widening casts over any integer
    /// expression. A cast of a scan column is already lowered as a leaf
    /// before this shape is tried.
    unsafe fn lower_integer_widening(
        &mut self,
        expression: *mut pg_sys::FuncExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let function = PgFuncExpr::try_from_expr(unsafe {
            PgExprRef::from_raw(expression.cast())
        })
        .expect("FuncExpr dispatch established the node kind");
        let argument = function
            .argument(0)
            .ok_or(ExpressionDecline::InvalidShape)?;
        PgIntegerWidening::for_function(
            function.function_oid(),
            argument.type_oid(),
            function.result_type(),
        )
        .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        Ok(ExecutionExpr::WidenInteger {
            value: Box::new(unsafe {
                self.lower(argument.as_ptr().cast(), scope.as_scalar())
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
        let test = PgBooleanTest::try_from_expr(unsafe {
            PgExprRef::from_raw(expression.cast())
        })
        .expect("BooleanTest dispatch established the node kind");
        let kind = test.kind();
        Ok(ExecutionExpr::BooleanTest {
            kind,
            value: Box::new(unsafe {
                self.lower(test.arg().as_ptr().cast(), scope.with_predicate(domain))
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
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let expression = unsafe { PgExprRef::from_raw(expression) };
        let membership = PgScalarArrayOpExpr::try_from_expr(expression)
            .ok_or(ExpressionDecline::InvalidShape)?;
        let (value, array) = membership.operands();
        let value_type = unsafe { Self::predicate_type(value.as_ptr(), scope) };
        if value_type.type_oid == pg_sys::BPCHAROID {
            return unsafe { self.lower_bpchar_in_list(membership, scope, domain) };
        }
        // A NUMERIC value without a fixed decimal type matches in its own
        // engine representation.
        if value_type.type_oid == pg_sys::NUMERICOID
            && FixedDecimalType::for_type(value_type).is_none()
        {
            return unsafe { self.lower_numeric_in_list(membership, scope, domain) };
        }
        // Prove the operator over the value and the array's element type
        // before materializing any element.
        if !domain.supports_type(value_type)
            || !PredicateDomain::Exact.supports_type(value_type)
            || unsafe { pg_sys::get_element_type(array.type_oid()) }
                != value_type.type_oid
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let operator = membership.comparison_op();
        // An IN-list operand is a storage column or a Decimal128 aggregate.
        let decimal =
            Decimal128Semantics::for_type(value_type).map(FixedDecimalType::from);
        let comparison = if decimal.is_some() {
            domain
                .decimal_comparison(operator, value_type, value_type)
                .map(|(comparison, ..)| comparison)
        } else {
            domain.comparison(operator, value_type, value_type)
        }
        .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let negated = membership
            .membership_negation(comparison)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let elements = unsafe { Self::scalar_array_elements(array) }?;
        let mut element_types = elements
            .iter()
            .map(|element| Self::expr_type((*element).cast()));
        if let Some(decimal) = decimal {
            // A Const element carries no typmod; each must be exactly
            // encodable in the column's representation.
            if element_types.any(|element_type| {
                domain.decimal_comparison(operator, value_type, element_type)
                    != Some((comparison, decimal, decimal))
            }) {
                return Err(ExpressionDecline::UnsupportedSemantics);
            }
        } else if element_types.any(|element_type| {
            // ScalarArrayOpExpr carries the resolved comparison collation in
            // inputcollid; its text elements can retain their original
            // expression collations while sharing the same Utf8 representation.
            element_type != value_type
                && !(value_type.type_oid == pg_sys::TEXTOID
                    && element_type.type_oid == pg_sys::TEXTOID
                    && element_type.typmod == value_type.typmod)
        }) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let value = if let Some(decimal) = decimal {
            unsafe {
                self.lower_decimal_operand(value.as_ptr(), scope.as_scalar(), decimal)
            }?
        } else {
            unsafe { self.lower(value.as_ptr().cast(), scope.as_scalar()) }?
        };
        let list = if let Some(decimal) = decimal {
            elements
                .into_iter()
                .map(|element| unsafe {
                    self.lower_decimal_operand(
                        element.cast(),
                        scope.as_scalar(),
                        decimal,
                    )
                })
                .collect::<ExpressionPlanResult<Vec<_>>>()?
        } else {
            elements
                .into_iter()
                .map(|element| unsafe { self.lower(element, scope.as_scalar()) })
                .collect::<ExpressionPlanResult<Vec<_>>>()?
        };
        Ok(ExecutionExpr::InList {
            value: Box::new(value),
            list: list.into_boxed_slice(),
            negated,
        })
    }

    /// The elements of a scalar-array operand. PostgreSQL folds an
    /// all-constant IN list or ARRAY[...] into one array Const; ANY/ALL visit
    /// every physical element whatever the dimensions, so its flattened
    /// elements are the same list. An empty array is declined: PostgreSQL
    /// answers FALSE (ANY) or TRUE (ALL) even for a NULL operand, which no
    /// strict fold represents.
    pub(super) unsafe fn scalar_array_elements(
        array: PgExprRef<'_>,
    ) -> ExpressionPlanResult<Vec<*mut pg_sys::Node>> {
        let elements = match array.node_tag() {
            pg_sys::NodeTag::T_ArrayExpr => {
                let array = array.as_ptr().cast::<pg_sys::ArrayExpr>();
                if unsafe { (*array).multidims } {
                    return Err(ExpressionDecline::UnsupportedSemantics);
                }
                unsafe { Self::list_nodes((*array).elements) }
            }
            pg_sys::NodeTag::T_Const => PgConst::try_from_expr(array)
                .and_then(|array| unsafe { array.array_elements() })
                .ok_or(ExpressionDecline::UnsupportedSemantics)?
                .into_iter()
                .map(|element| element.as_ptr().cast())
                .collect(),
            _ => return Err(ExpressionDecline::InvalidShape),
        };
        if elements.is_empty() {
            return Err(ExpressionDecline::InvalidShape);
        }
        Ok(elements)
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

    pub(super) unsafe fn list_nodes(
        list: *mut pg_sys::List,
    ) -> Vec<*mut pg_sys::Node> {
        let count = unsafe { pg_sys::list_length(list) };
        (0..count)
            .map(|index| unsafe { pg_sys::list_nth(list, index) }.cast())
            .collect()
    }
}
