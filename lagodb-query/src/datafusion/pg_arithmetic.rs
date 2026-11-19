//! PostgreSQL integer and floating-point `+ - * / %`.
//!
//! DataFusion's integer `+ - *` wraps on overflow unless a physical
//! `BinaryExpr` opts into checking, which a logical plan cannot request.
//! PostgreSQL raises instead (`int.c`, `int8.c`), so integer operators run
//! Arrow's checked kernels and report PostgreSQL's errors. Floating-point
//! operators keep DataFusion's IEEE arithmetic, a documented engine
//! semantic. Both operands arrive in the operator's result type, the type a
//! cross-type operator such as `int48pl` computes in.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{Array, ArrayRef, Datum, Int32Array};
use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::compute::try_binary;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_expr_common::datum::apply;
use lagodb_core::diag::PgReportError;
use pgrx::prelude::PgSqlErrorCode;

use super::pg_integer::PgIntegerKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum PgArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    /// Integer only; PostgreSQL has no floating-point `%`.
    Modulo,
}

impl PgArithmeticOperator {
    const ALL: [Self; 5] = [
        Self::Add,
        Self::Subtract,
        Self::Multiply,
        Self::Divide,
        Self::Modulo,
    ];

    /// DataFusion's floating-point operator.
    pub(super) const fn float_operator(self) -> Option<Operator> {
        match self {
            Self::Add => Some(Operator::Plus),
            Self::Subtract => Some(Operator::Minus),
            Self::Multiply => Some(Operator::Multiply),
            Self::Divide => Some(Operator::Divide),
            Self::Modulo => None,
        }
    }

    /// The integer UDF name, which identifies the operator in the physical
    /// codec.
    const fn integer_name(self) -> &'static str {
        match self {
            Self::Add => "lagodb_int_add",
            Self::Subtract => "lagodb_int_subtract",
            Self::Multiply => "lagodb_int_multiply",
            Self::Divide => "lagodb_int_divide",
            Self::Modulo => "lagodb_int_modulo",
        }
    }

    pub(super) fn from_integer_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operator| operator.integer_name() == name)
    }

    /// Arrow's checked integer kernel. Overflow and division by zero are
    /// errors, `MIN / -1` overflows and `MIN % -1` is 0, as in PostgreSQL.
    fn checked(
        self,
        left: &dyn Datum,
        right: &dyn Datum,
    ) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Add => numeric::add(left, right),
            Self::Subtract => numeric::sub(left, right),
            Self::Multiply => numeric::mul(left, right),
            Self::Divide => numeric::div(left, right),
            Self::Modulo => numeric::rem(left, right),
        }
    }

    /// The int2 operation over Int32-held values. The operation is chosen
    /// once, outside the loop, as Arrow's own kernels do. Operands in the
    /// int2 domain cannot overflow i32, so only the int2 result is checked.
    fn smallint_datum(
        self,
        left: &dyn Datum,
        right: &dyn Datum,
    ) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Add => smallint_kernel(left, right, |l, r| smallint_result(l + r)),
            Self::Subtract => {
                smallint_kernel(left, right, |l, r| smallint_result(l - r))
            }
            Self::Multiply => {
                smallint_kernel(left, right, |l, r| smallint_result(l * r))
            }
            Self::Divide => smallint_kernel(left, right, |l, r| {
                smallint_result(l / nonzero_divisor(r)?)
            }),
            Self::Modulo => smallint_kernel(left, right, |l, r| {
                smallint_result(l % nonzero_divisor(r)?)
            }),
        }
    }
}

fn nonzero_divisor(divisor: i32) -> Result<i32, ArrowError> {
    if divisor == 0 {
        Err(ArrowError::DivideByZero)
    } else {
        Ok(divisor)
    }
}

fn smallint_result(value: i32) -> Result<i32, ArrowError> {
    i16::try_from(value)
        .map(i32::from)
        .map_err(|_| ArrowError::ArithmeticOverflow(String::new()))
}

/// One pass over the valid values, keeping a scalar operand unbroadcast as
/// Arrow's own kernels do.
fn smallint_kernel(
    left: &dyn Datum,
    right: &dyn Datum,
    operation: impl Fn(i32, i32) -> Result<i32, ArrowError>,
) -> Result<ArrayRef, ArrowError> {
    let (left, left_is_scalar) = left.get();
    let (right, right_is_scalar) = right.get();
    let left = left.as_primitive::<Int32Type>();
    let right = right.as_primitive::<Int32Type>();
    let result: Int32Array = match (left_is_scalar, right_is_scalar) {
        (true, true) | (false, false) => try_binary(left, right, operation)?,
        (true, false) => match left.is_valid(0).then(|| left.value(0)) {
            Some(left) => right.try_unary(|right| operation(left, right))?,
            None => Int32Array::new_null(right.len()),
        },
        (false, true) => match right.is_valid(0).then(|| right.value(0)) {
            Some(right) => left.try_unary(|left| operation(left, right))?,
            None => Int32Array::new_null(left.len()),
        },
    };
    Ok(Arc::new(result))
}

/// A PostgreSQL integer operator over its result type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct PgIntegerArithmeticUdf {
    operator: PgArithmeticOperator,
    kind: PgIntegerKind,
    signature: Signature,
}

impl PgIntegerArithmeticUdf {
    /// The UDF of `operator` over `kind`.
    pub(super) fn for_operator(
        operator: PgArithmeticOperator,
        kind: PgIntegerKind,
    ) -> ScalarUDF {
        ScalarUDF::from(Self {
            operator,
            kind,
            signature: Signature::exact(
                vec![kind.data_type(), kind.data_type()],
                Volatility::Immutable,
            ),
        })
    }

    pub(super) fn codec_tag(udf: &ScalarUDF) -> Option<u8> {
        udf.inner()
            .downcast_ref::<Self>()
            .map(|implementation| implementation.kind.codec_tag())
    }

    pub(super) fn from_codec_tag(
        operator: PgArithmeticOperator,
        tag: u8,
    ) -> Option<ScalarUDF> {
        PgIntegerKind::from_codec_tag(tag)
            .map(|kind| Self::for_operator(operator, kind))
    }

    /// PostgreSQL's error for an Arrow arithmetic error.
    #[cold]
    #[inline(never)]
    fn postgres_error(&self, error: DataFusionError) -> DataFusionError {
        let report = match &error {
            DataFusionError::ArrowError(source, _) => match source.as_ref() {
                ArrowError::DivideByZero => Some(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_DIVISION_BY_ZERO,
                    "division by zero",
                )),
                ArrowError::ArithmeticOverflow(_) => Some(self.kind.out_of_range()),
                _ => None,
            },
            _ => None,
        };
        report.map_or(error, |report| DataFusionError::External(Box::new(report)))
    }
}

impl ScalarUDFImpl for PgIntegerArithmeticUdf {
    fn name(&self) -> &str {
        self.operator.integer_name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _argument_types: &[DataType],
    ) -> DataFusionResult<DataType> {
        Ok(self.kind.data_type())
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(
        &self,
        arguments: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        // `Signature::exact` admits exactly two operands of the result type.
        let (left, right) = (&arguments.args[0], &arguments.args[1]);
        match self.kind {
            PgIntegerKind::SmallInt => apply(left, right, |left, right| {
                self.operator.smallint_datum(left, right)
            }),
            PgIntegerKind::Integer | PgIntegerKind::BigInt => {
                apply(left, right, |left, right| {
                    self.operator.checked(left, right)
                })
            }
        }
        .map_err(|error| self.postgres_error(error))
    }
}
