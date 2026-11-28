//! PostgreSQL integer and floating-point `+ - * / %`.
//!
//! DataFusion's integer `+ - *` wraps on overflow unless a physical
//! `BinaryExpr` opts into checking, which a logical plan cannot request.
//! PostgreSQL raises instead (`int.c`, `int8.c`), so integer operators report
//! PostgreSQL's errors: `+ - *` compute every wrapping result in one pass and
//! raise when an overflow flag of a valid slot is set, and division runs
//! Arrow's checked kernels.
//! Floating-point operators keep DataFusion's IEEE arithmetic, a documented
//! engine semantic. Both operands arrive in the operator's result type, the
//! type a cross-type operator such as `int48pl` computes in.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, Datum, Int32Array, PrimitiveArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::compute::try_binary;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_expr_common::datum::apply;

use super::error::QueryExecutionError;
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

    /// The operation over operands of `kind`, held in its Arrow type. The
    /// operation is chosen once, outside the loop, as Arrow's own kernels do.
    fn evaluate(
        self,
        kind: PgIntegerKind,
        left: &dyn Datum,
        right: &dyn Datum,
    ) -> Result<ArrayRef, ArrowError> {
        match (kind, self) {
            // int2 operands are exact in i32, so only the int2 result is
            // checked. A NULL slot may hold any i32, which wraps.
            (PgIntegerKind::SmallInt, Self::Add) => {
                flagged_kernel::<Int32Type>(left, right, |l, r| {
                    smallint(l.wrapping_add(r))
                })
            }
            (PgIntegerKind::SmallInt, Self::Subtract) => {
                flagged_kernel::<Int32Type>(left, right, |l, r| {
                    smallint(l.wrapping_sub(r))
                })
            }
            (PgIntegerKind::SmallInt, Self::Multiply) => {
                flagged_kernel::<Int32Type>(left, right, |l, r| {
                    smallint(l.wrapping_mul(r))
                })
            }
            (PgIntegerKind::SmallInt, Self::Divide) => {
                smallint_kernel(left, right, |l, r| {
                    smallint_result(l / nonzero_divisor(r)?)
                })
            }
            (PgIntegerKind::SmallInt, Self::Modulo) => {
                smallint_kernel(left, right, |l, r| {
                    smallint_result(l % nonzero_divisor(r)?)
                })
            }
            (PgIntegerKind::Integer, Self::Add) => {
                flagged_kernel::<Int32Type>(left, right, i32::flagged_add)
            }
            (PgIntegerKind::Integer, Self::Subtract) => {
                flagged_kernel::<Int32Type>(left, right, i32::flagged_sub)
            }
            (PgIntegerKind::Integer, Self::Multiply) => {
                flagged_kernel::<Int32Type>(left, right, i32::flagged_mul)
            }
            (PgIntegerKind::BigInt, Self::Add) => {
                flagged_kernel::<Int64Type>(left, right, i64::flagged_add)
            }
            (PgIntegerKind::BigInt, Self::Subtract) => {
                flagged_kernel::<Int64Type>(left, right, i64::flagged_sub)
            }
            (PgIntegerKind::BigInt, Self::Multiply) => {
                flagged_kernel::<Int64Type>(left, right, i64::flagged_mul)
            }
            // Arrow's checked division: division by zero is an error,
            // `MIN / -1` overflows and `MIN % -1` is 0, as in PostgreSQL.
            (PgIntegerKind::Integer | PgIntegerKind::BigInt, Self::Divide) => {
                numeric::div(left, right)
            }
            (PgIntegerKind::Integer | PgIntegerKind::BigInt, Self::Modulo) => {
                numeric::rem(left, right)
            }
        }
    }
}

/// Two's-complement `+ - *` and whether the result left the type, computed
/// without a branch so that a loop over values vectorizes.
trait FlaggedArithmetic: Sized {
    fn flagged_add(self, right: Self) -> (Self, bool);
    fn flagged_sub(self, right: Self) -> (Self, bool);
    fn flagged_mul(self, right: Self) -> (Self, bool);
}

impl FlaggedArithmetic for i32 {
    fn flagged_add(self, right: Self) -> (Self, bool) {
        let sum = self.wrapping_add(right);
        // Overflow gives the sum the sign both operands lack.
        (sum, (self ^ sum) & (right ^ sum) < 0)
    }

    fn flagged_sub(self, right: Self) -> (Self, bool) {
        let difference = self.wrapping_sub(right);
        // Overflow needs operands of opposite signs and flips the left sign.
        (difference, (self ^ right) & (self ^ difference) < 0)
    }

    fn flagged_mul(self, right: Self) -> (Self, bool) {
        let product = i64::from(self) * i64::from(right);
        let wrapped = product as i32;
        (wrapped, i64::from(wrapped) != product)
    }
}

impl FlaggedArithmetic for i64 {
    fn flagged_add(self, right: Self) -> (Self, bool) {
        let sum = self.wrapping_add(right);
        (sum, (self ^ sum) & (right ^ sum) < 0)
    }

    fn flagged_sub(self, right: Self) -> (Self, bool) {
        let difference = self.wrapping_sub(right);
        (difference, (self ^ right) & (self ^ difference) < 0)
    }

    /// No vector unit multiplies 64-bit lanes into their high half; this
    /// still removes the branch.
    fn flagged_mul(self, right: Self) -> (Self, bool) {
        self.overflowing_mul(right)
    }
}

/// An int2 result held in i32 and whether it left int2.
const fn smallint(value: i32) -> (i32, bool) {
    (value, value as i16 as i32 != value)
}

/// `+ - *` without a branch per value: every slot takes its wrapping result
/// and one flag decides the batch, as the checked SUM accumulates
/// (`decimal_sum.rs`). A scalar operand stays unbroadcast, as in Arrow's own
/// kernels.
fn flagged_kernel<T: ArrowPrimitiveType>(
    left: &dyn Datum,
    right: &dyn Datum,
    operation: impl Fn(T::Native, T::Native) -> (T::Native, bool),
) -> Result<ArrayRef, ArrowError> {
    let (left, left_is_scalar) = left.get();
    let (right, right_is_scalar) = right.get();
    let left = left.as_primitive::<T>();
    let right = right.as_primitive::<T>();
    let scalar =
        |array: &PrimitiveArray<T>| array.is_valid(0).then(|| array.value(0));
    match (left_is_scalar, right_is_scalar) {
        (true, true) | (false, false) => flagged_values::<T, _>(
            left.values()
                .chunks(VALIDITY_WORD_BITS)
                .zip(right.values().chunks(VALIDITY_WORD_BITS))
                .map(|(left, right)| left.iter().copied().zip(right.iter().copied())),
            left.len(),
            NullBuffer::union(left.nulls(), right.nulls()),
            operation,
        ),
        (true, false) => match scalar(left) {
            Some(l) => flagged_values::<T, _>(
                right
                    .values()
                    .chunks(VALIDITY_WORD_BITS)
                    .map(|right| right.iter().map(move |&r| (l, r))),
                right.len(),
                right.nulls().cloned(),
                operation,
            ),
            None => Ok(Arc::new(PrimitiveArray::<T>::new_null(right.len()))),
        },
        (false, true) => match scalar(right) {
            Some(r) => flagged_values::<T, _>(
                left.values()
                    .chunks(VALIDITY_WORD_BITS)
                    .map(|left| left.iter().map(move |&l| (l, r))),
                left.len(),
                left.nulls().cloned(),
                operation,
            ),
            None => Ok(Arc::new(PrimitiveArray::<T>::new_null(left.len()))),
        },
    }
}

/// Slots per validity bitmap word.
const VALIDITY_WORD_BITS: usize = 64;

/// The results of `operation` over operand chunks of one validity word each,
/// raising when a valid slot overflowed. A NULL slot holds an arbitrary
/// value, so each chunk packs its overflow flags into a word that its
/// validity word masks, in the same pass.
fn flagged_values<
    T: ArrowPrimitiveType,
    C: Iterator<Item = (T::Native, T::Native)>,
>(
    chunks: impl Iterator<Item = C>,
    len: usize,
    nulls: Option<NullBuffer>,
    operation: impl Fn(T::Native, T::Native) -> (T::Native, bool),
) -> Result<ArrayRef, ArrowError> {
    let mut values = Vec::with_capacity(len);
    let mut overflow = false;
    match &nulls {
        None => {
            for chunk in chunks {
                values.extend(chunk.map(|(left, right)| {
                    let (value, overflowed) = operation(left, right);
                    overflow |= overflowed;
                    value
                }));
            }
        }
        Some(nulls) => {
            for (chunk, valid) in chunks.zip(nulls.inner().bit_chunks().iter_padded())
            {
                let mut flags = 0_u64;
                values.extend(chunk.enumerate().map(|(lane, (left, right))| {
                    let (value, overflowed) = operation(left, right);
                    flags |= u64::from(overflowed) << lane;
                    value
                }));
                overflow |= flags & valid != 0;
            }
        }
    }
    if overflow {
        return Err(ArrowError::ArithmeticOverflow(String::new()));
    }
    Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), nulls)))
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
                ArrowError::DivideByZero => {
                    Some(QueryExecutionError::division_by_zero())
                }
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
        apply(left, right, |left, right| {
            self.operator.evaluate(self.kind, left, right)
        })
        .map_err(|error| self.postgres_error(error))
    }
}
