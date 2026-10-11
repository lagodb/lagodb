//! Final representation of the native SUM(real) aggregate.
//!
//! Keep DataFusion's Float64 transition state and optimized grouped/parallel
//! aggregation. Only completed groups cross this Float32 result boundary,
//! before HAVING or any consumer of the PostgreSQL real result. This retains
//! the native aggregate's accumulation/rounding differences from PG float4pl;
//! the final conversion implements PG's real overflow semantics.

use std::sync::{Arc, LazyLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use lagodb_core::diag::PgReportError;
use pgrx::prelude::PgSqlErrorCode;

pub(super) const NAME: &str = "lagodb_real_sum_result";

static RESULT: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(RealSumResult {
        signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
    }))
});

pub(super) fn result() -> Arc<ScalarUDF> {
    Arc::clone(&RESULT)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RealSumResult {
    signature: Signature,
}

impl RealSumResult {
    fn convert(value: f64) -> Result<f32> {
        let result = value as f32;
        // Match PG17 dtof's overflow rule. Infinity/NaN remain valid; a finite
        // aggregate overflowing real is an SQL error, not a storage invariant.
        if result.is_infinite() && !value.is_infinite() {
            return Err(Self::overflow());
        }
        // Real inputs and their Float64 sums retain integer multiples of the
        // smallest real subnormal, so this aggregate needs no underflow check.
        Ok(result)
    }

    #[cold]
    fn overflow() -> DataFusionError {
        DataFusionError::External(Box::new(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            "value out of range: overflow",
        )))
    }
}

impl ScalarUDFImpl for RealSumResult {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return internal_err!("SUM(real) result requires one Float64 argument");
        };
        match input {
            ColumnarValue::Array(array) => {
                let output = array
                    .as_primitive::<Float64Type>()
                    .try_unary::<_, Float32Type, DataFusionError>(Self::convert)?;
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                    value.map(Self::convert).transpose()?,
                )))
            }
            _ => internal_err!("SUM(real) result requires Float64 input"),
        }
    }
}
