//! NUMERIC comparisons and HAVING arithmetic over engine representations
//! other than a fixed decimal.
//!
//! PostgreSQL types `extract(year ...)` and integer AVG, variance and
//! standard deviation as unbounded `numeric`, while the engine computes the
//! first as an integer and the others as Float64 ([`NumericRepr`]). A
//! comparison is proven over its operands' representations
//! ([`PredicateDomain::numeric_repr_comparison`]); a direct Const takes the
//! other operand's: an exact int4 against an integer, a runtime value, which
//! the engine reads as Float64, against Float64. Each pair compares with the
//! conversions DataFusion itself applies to it: an integer under
//! PostgreSQL's integer-to-NUMERIC cast compares as the integer, and an
//! integer compared with a fixed decimal converts to its decimal image.

use lagodb_core::expr::pg::{PgConst, PgExprRef, PgScalarArrayOpExpr};
use lagodb_core::expr::{ExprType, PgComparisonOp};
use lagodb_query::plan::{
    ExecutionExpr, FixedDecimalType, NumericRepr, ScalarFunctionKind,
};
use pgrx::pg_sys;

use super::{
    ExpressionDecline, ExpressionPlanResult, ExpressionScope, PredicateDomain,
    QueryExpressionPlanner,
};

/// A lowered NUMERIC operand and the representation it executes in.
type NumericOperand = (ExecutionExpr, NumericRepr);

impl QueryExpressionPlanner {
    /// `left <op> right` under a built-in NUMERIC operator that the fixed
    /// decimal paths do not prove: an operand is in another representation.
    pub(super) unsafe fn lower_numeric_repr_comparison(
        &mut self,
        operator: PgComparisonOp,
        left: *mut pg_sys::Expr,
        right: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let scope = scope.as_scalar();
        let constants =
            unsafe { [Self::is_direct_const(left), Self::is_direct_const(right)] };
        let ((left, left_repr), (right, right_repr)) = match constants {
            [false, true] => {
                let (left, repr) =
                    unsafe { self.lower_numeric_operand(left, scope) }?;
                let right =
                    unsafe { self.lower_numeric_constant(right, scope, repr) }?;
                ((left, repr), (right, repr))
            }
            [true, false] => {
                let (right, repr) =
                    unsafe { self.lower_numeric_operand(right, scope) }?;
                let left = unsafe { self.lower_numeric_constant(left, scope, repr) }?;
                ((left, repr), (right, repr))
            }
            [false, false] => Self::align_numeric_operands(
                unsafe { self.lower_numeric_operand(left, scope) }?,
                unsafe { self.lower_numeric_operand(right, scope) }?,
            ),
            [true, true] => return Err(ExpressionDecline::InvalidShape),
        };
        domain
            .numeric_repr_comparison(operator, left_repr, right_repr)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        Ok(ExecutionExpr::Comparison {
            operator,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    /// `value IN (...)` or `NOT IN` over a NUMERIC value without a fixed
    /// decimal type. Only an integer value is matched natively: each element
    /// is a constant bound as an exact int4, or another integer value.
    pub(super) unsafe fn lower_numeric_in_list(
        &mut self,
        membership: PgScalarArrayOpExpr<'_>,
        scope: ExpressionScope<'_>,
        domain: PredicateDomain,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        let (value, array) = membership.operands();
        if unsafe { pg_sys::get_element_type(array.type_oid()) } != pg_sys::NUMERICOID
        {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let scope = scope.as_scalar();
        let (value, repr) =
            unsafe { self.lower_numeric_operand(value.as_ptr(), scope) }?;
        if !matches!(repr, NumericRepr::Integer(_)) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        let comparison = domain
            .numeric_repr_comparison(membership.comparison_op(), repr, repr)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let negated = membership
            .membership_negation(comparison)
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        let list = unsafe { Self::scalar_array_elements(array) }?
            .into_iter()
            .map(|element| {
                let element = element.cast::<pg_sys::Expr>();
                if unsafe { Self::is_direct_const(element) } {
                    return unsafe {
                        self.lower_numeric_constant(element, scope, repr)
                    };
                }
                let operand = unsafe { self.lower_numeric_operand(element, scope) }?;
                match Self::uncast_integer(operand) {
                    (element, NumericRepr::Integer(_)) => Ok(element),
                    _ => Err(ExpressionDecline::UnsupportedSemantics),
                }
            })
            .collect::<ExpressionPlanResult<Vec<_>>>()?;
        Ok(ExecutionExpr::InList {
            value: Box::new(value),
            list: list.into_boxed_slice(),
            negated,
        })
    }

    /// HAVING NUMERIC `+`, `-` or `*` with an approximate Float64 operand,
    /// an integer AVG, variance or standard deviation result. The engine
    /// computes it in Float64 as DataFusion computes decimal and float
    /// operands, converting the other operand; a constant is read as float8.
    /// Division declines: PostgreSQL raises on a zero divisor, where Float64
    /// returns an infinity or NaN.
    pub(super) unsafe fn lower_float_numeric_arithmetic(
        &mut self,
        expression: *mut pg_sys::OpExpr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        // eval_const_expressions has set opfuncid on every planned OpExpr.
        let kind = match unsafe { (*expression).opfuncid }.to_u32() {
            pg_sys::F_NUMERIC_ADD => ScalarFunctionKind::Add,
            pg_sys::F_NUMERIC_SUB => ScalarFunctionKind::Subtract,
            pg_sys::F_NUMERIC_MUL => ScalarFunctionKind::Multiply,
            _ => return Err(ExpressionDecline::UnsupportedSemantics),
        };
        let [left, right] = unsafe { Self::list_nodes((*expression).args) }[..]
        else {
            return Err(ExpressionDecline::InvalidShape);
        };
        let scope = scope.as_scalar();
        let (left, left_repr) = Self::uncast_integer(unsafe {
            self.lower_numeric_operand(left.cast(), scope)
        }?);
        let (right, right_repr) = Self::uncast_integer(unsafe {
            self.lower_numeric_operand(right.cast(), scope)
        }?);
        if ![left_repr, right_repr].contains(&NumericRepr::Float64) {
            return Err(ExpressionDecline::UnsupportedSemantics);
        }
        Ok(ExecutionExpr::Function {
            kind,
            arguments: vec![left, right].into_boxed_slice(),
            input_collation: pg_sys::InvalidOid,
            result_type: ExprType {
                type_oid: pg_sys::NUMERICOID,
                typmod: -1,
                collation: pg_sys::InvalidOid,
            },
        })
    }

    /// Lower a NUMERIC operand other than a direct Const comparison operand,
    /// with the engine representation it executes in.
    unsafe fn lower_numeric_operand(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
    ) -> ExpressionPlanResult<NumericOperand> {
        let value = unsafe { self.lower(expression.cast(), scope) }?;
        let repr = value
            .numeric_repr(|leaf| match *leaf {
                ExecutionExpr::Output(output) => {
                    scope.outputs()?.output_execution_type(output)
                }
                ExecutionExpr::Value(value) => self
                    .runtime_specs
                    .get(value.index())
                    .map(|spec| spec.value_type),
                _ => None,
            })
            .ok_or(ExpressionDecline::UnsupportedSemantics)?;
        Ok((value, repr))
    }

    /// Two non-constant comparison operands in representations that compare
    /// with DataFusion's own conversions: against an operand of no fixed
    /// decimal type, an integer under PostgreSQL's integer-to-NUMERIC cast
    /// compares as the integer; an integer compared with a fixed decimal
    /// converts to its decimal image.
    fn align_numeric_operands(
        left: NumericOperand,
        right: NumericOperand,
    ) -> (NumericOperand, NumericOperand) {
        let is_decimal =
            |operand: &NumericOperand| matches!(operand.1, NumericRepr::Decimal(_));
        let (left_decimal, right_decimal) = (is_decimal(&left), is_decimal(&right));
        let left = if right_decimal {
            left
        } else {
            Self::uncast_integer(left)
        };
        let right = if left_decimal {
            right
        } else {
            Self::uncast_integer(right)
        };
        match (left.1, right.1) {
            (NumericRepr::Integer(image), NumericRepr::Decimal(_)) => {
                (Self::integer_image(left.0, image), right)
            }
            (NumericRepr::Decimal(_), NumericRepr::Integer(image)) => {
                (left, Self::integer_image(right.0, image))
            }
            _ => (left, right),
        }
    }

    /// The integer under PostgreSQL's integer-to-NUMERIC cast, or the operand
    /// itself.
    fn uncast_integer(operand: NumericOperand) -> NumericOperand {
        match operand {
            (ExecutionExpr::IntegerToDecimal { value, result_type }, _) => {
                (*value, NumericRepr::Integer(result_type))
            }
            operand => operand,
        }
    }

    /// An integer operand converted to its lossless decimal image.
    fn integer_image(
        value: ExecutionExpr,
        image: FixedDecimalType,
    ) -> NumericOperand {
        (
            ExecutionExpr::IntegerToDecimal {
                value: Box::new(value),
                result_type: image,
            },
            NumericRepr::Decimal(image),
        )
    }

    /// Lower a direct NUMERIC Const compared with an operand in `repr`.
    unsafe fn lower_numeric_constant(
        &mut self,
        expression: *mut pg_sys::Expr,
        scope: ExpressionScope<'_>,
        repr: NumericRepr,
    ) -> ExpressionPlanResult<ExecutionExpr> {
        match repr {
            NumericRepr::Integer(_) => {
                let constant = PgConst::try_from_expr(
                    unsafe { PgExprRef::from_raw(expression) }.without_relabels(),
                )
                .ok_or(ExpressionDecline::InvalidShape)?;
                let integer = unsafe { constant.exact_int4() }
                    .ok_or(ExpressionDecline::UnsupportedSemantics)?;
                unsafe { self.lower(integer.as_ptr().cast(), scope) }
            }
            NumericRepr::Float64 | NumericRepr::Float64Value => unsafe {
                self.lower(expression.cast(), scope)
            },
            // A fixed decimal operand binds its constants on the decimal
            // paths.
            NumericRepr::Decimal(_) => Err(ExpressionDecline::UnsupportedSemantics),
        }
    }

    pub(super) unsafe fn is_direct_const(expression: *mut pg_sys::Expr) -> bool {
        unsafe {
            (*Self::unwrap_relabel(expression)).type_ == pg_sys::NodeTag::T_Const
        }
    }
}
