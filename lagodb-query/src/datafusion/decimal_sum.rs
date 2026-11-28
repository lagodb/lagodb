//! Checked 128-bit SUM for int8 and wide NUMERIC inputs.
//!
//! PostgreSQL accumulates sum(int8) in int128 (`int8_avg_accum`) and never
//! overflows a NUMERIC sum. DataFusion's native SUM adds with `add_wrapping`,
//! which stays exact only while the input precision leaves room for every row
//! count ([`FixedDecimalType::sum`](crate::plan::FixedDecimalType::sum)). This
//! SUM reads int8 and Decimal128 inputs as they are, without a widening cast,
//! accumulates them in i128 and raises PostgreSQL's NUMERIC overflow error
//! when a sum leaves i128. Its result is `Decimal128(38, s)`: Arrow never validates a
//! decimal precision, so a sum beyond 38 digits that fits i128 is exact.
//!
//! A batch accumulates without branching on overflow and checks one flag at
//! its end. Partial states are sums of the result type and merge through the
//! same checked addition, so no state carries a wrapped value.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Array,
    PrimitiveArray,
};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow_schema::{DECIMAL128_MAX_PRECISION, DataType, Field, FieldRef};
use datafusion::common::{
    DataFusionError, Result, ScalarValue, internal_err, plan_err,
};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator,
    ReversedUDAF, Signature, Volatility,
};
use datafusion::physical_expr::NullState;

use super::error::QueryExecutionError;

pub(super) const NAME: &str = "lagodb_decimal_sum";

static DECIMAL_SUM: LazyLock<Arc<AggregateUDF>> = LazyLock::new(|| {
    Arc::new(AggregateUDF::from(DecimalSum {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

pub(super) fn sum(argument: Expr) -> Expr {
    DECIMAL_SUM.call(vec![argument])
}

pub(super) fn udaf() -> Arc<AggregateUDF> {
    Arc::clone(&DECIMAL_SUM)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DecimalSum {
    signature: Signature,
}

/// The two inputs this SUM accumulates: int8 and Decimal128 coefficients.
trait SumInput: ArrowPrimitiveType<Native: Into<i128>> + Debug + Send + Sync {}

impl SumInput for Int64Type {}

impl SumInput for Decimal128Type {}

impl DecimalSum {
    /// The validated input type of a non-DISTINCT call. Compilation routes
    /// DISTINCT SUM(int8) to the native SUM, whose distinct state holds it.
    fn input_type<'a>(args: &'a AccumulatorArgs<'_>) -> Result<&'a DataType> {
        match args.expr_fields {
            [input] if !args.is_distinct => Ok(input.data_type()),
            _ => internal_err!("{NAME} requires one non-DISTINCT argument"),
        }
    }

    #[cold]
    fn overflow() -> DataFusionError {
        DataFusionError::External(Box::new(QueryExecutionError::numeric_overflow()))
    }
}

impl AggregateUDFImpl for DecimalSum {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let scale = match arg_types {
            [DataType::Int64] => 0,
            [DataType::Decimal128(_, scale)] => *scale,
            _ => return plan_err!("{NAME} requires one int8 or Decimal128 argument"),
        };
        Ok(DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let result_type = args.return_type().clone();
        match Self::input_type(&args)? {
            DataType::Int64 => {
                Ok(Box::new(CheckedSum::<Int64Type>::new(result_type)))
            }
            DataType::Decimal128(..) => {
                Ok(Box::new(CheckedSum::<Decimal128Type>::new(result_type)))
            }
            input => internal_err!("{NAME} cannot accumulate {input}"),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "sum"),
                args.return_type().clone(),
                true,
            )
            .into(),
        ])
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let result_type = args.return_type().clone();
        match Self::input_type(&args)? {
            DataType::Int64 => {
                Ok(Box::new(CheckedGroupSums::<Int64Type>::new(result_type)))
            }
            DataType::Decimal128(..) => Ok(Box::new(CheckedGroupSums::<
                Decimal128Type,
            >::new(result_type))),
            input => internal_err!("{NAME} cannot accumulate {input}"),
        }
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

/// Slots per validity bitmap word.
const VALIDITY_WORD_BITS: usize = 64;

/// Add every value to `sum`, reporting whether any step left i128.
fn add_all<V: Into<i128>>(
    sum: i128,
    values: impl Iterator<Item = V>,
) -> (i128, bool) {
    values.fold((sum, false), |(sum, overflow), value| {
        let (sum, overflowed) = sum.overflowing_add(value.into());
        (sum, overflow | overflowed)
    })
}

/// The SUM of an aggregate without groups.
#[derive(Debug)]
struct CheckedSum<T> {
    sum: Option<i128>,
    result_type: DataType,
    input: PhantomData<T>,
}

impl<T: SumInput> CheckedSum<T> {
    const fn new(result_type: DataType) -> Self {
        Self {
            sum: None,
            result_type,
            input: PhantomData,
        }
    }

    fn add<A: SumInput>(&mut self, values: &PrimitiveArray<A>) -> Result<()> {
        if values.null_count() == values.len() {
            return Ok(());
        }
        let start = self.sum.unwrap_or(0);
        let (sum, overflow) = match values.nulls() {
            None => add_all(start, values.values().iter().copied()),
            // One validity word masks each 64 values, as Arrow's nullable
            // `sum` lanes do: a valid bit widens to an all-ones mask and a
            // NULL slot adds zero, so its arbitrary value never sets the
            // overflow flag.
            Some(nulls) => add_all(
                start,
                values
                    .values()
                    .chunks(VALIDITY_WORD_BITS)
                    .zip(nulls.inner().bit_chunks().iter_padded())
                    .flat_map(|(chunk, valid)| {
                        chunk.iter().enumerate().map(move |(lane, &value)| {
                            Into::<i128>::into(value) & -i128::from(valid >> lane & 1)
                        })
                    }),
            ),
        };
        if overflow {
            return Err(DecimalSum::overflow());
        }
        self.sum = Some(sum);
        Ok(())
    }
}

impl<T: SumInput> Accumulator for CheckedSum<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add(values[0].as_primitive::<T>())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.add(states[0].as_primitive::<Decimal128Type>())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<Decimal128Type>(self.sum, &self.result_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// Per-group sums, following DataFusion's `PrimitiveGroupsAccumulator`.
struct CheckedGroupSums<T> {
    sums: Vec<i128>,
    /// Groups that have seen a non-NULL, unfiltered value.
    null_state: NullState,
    result_type: DataType,
    input: PhantomData<T>,
}

impl<T: SumInput> CheckedGroupSums<T> {
    fn new(result_type: DataType) -> Self {
        Self {
            sums: Vec::new(),
            null_state: NullState::new(),
            result_type,
            input: PhantomData,
        }
    }

    fn add<A: SumInput>(
        &mut self,
        values: &PrimitiveArray<A>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.sums.resize(total_num_groups, 0);
        let sums = &mut self.sums;
        let mut overflow = false;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group, value| {
                let (sum, overflowed) = sums[group].overflowing_add(value.into());
                sums[group] = sum;
                overflow |= overflowed;
            },
        );
        if overflow {
            Err(DecimalSum::overflow())
        } else {
            Ok(())
        }
    }

    fn sum_array(
        &self,
        sums: ScalarBuffer<i128>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        Arc::new(
            Decimal128Array::new(sums, nulls)
                .with_data_type(self.result_type.clone()),
        )
    }
}

impl<T: SumInput> GroupsAccumulator for CheckedGroupSums<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(
            values[0].as_primitive::<T>(),
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);
        Ok(self.sum_array(sums.into(), nulls))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.evaluate(emit_to).map(|sums| vec![sums])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(
            values[0].as_primitive::<Decimal128Type>(),
            group_indices,
            None,
            total_num_groups,
        )
    }

    /// Each input row becomes the state of a one-row sum, NULL where the
    /// value is NULL or filtered out.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let (_, sums, nulls) = values[0]
            .as_primitive::<T>()
            .unary::<_, Decimal128Type>(|value| value.into())
            .into_parts();
        let filter = opt_filter.map(|filter| {
            NullBuffer::new(match filter.nulls() {
                Some(valid) => valid.inner() & filter.values(),
                None => filter.values().clone(),
            })
        });
        let nulls = NullBuffer::union(nulls.as_ref(), filter.as_ref());
        Ok(vec![self.sum_array(sums, nulls)])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<i128>() + self.null_state.size()
    }
}
