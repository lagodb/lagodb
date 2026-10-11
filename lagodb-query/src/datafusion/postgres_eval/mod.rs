//! DataFusion scalar expression fallback backed by PostgreSQL `ExecEvalExpr`.
//!
//! The PostgreSQL expression state and virtual input slot are initialized once,
//! while Arrow conversion is bound once per input batch. The per-tuple
//! expression context is reset for every row, and PostgreSQL's volatility
//! classification is preserved in the DataFusion UDF signature.
//!
//! Query auto mode declines plans containing this fallback. Force mode accepts
//! its deliberate per-row PostgreSQL evaluation cost. Optimization therefore
//! expands centrally owned native expression coverage first; fallback subtree
//! granularity changes require profile data showing that they reduce total
//! boundary and conversion cost.

mod signature;
mod state;

use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::sync::OnceLock;

use arrow_array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use lagodb_arrow::{PgDatumArrayBuilder, PgDatumArrayReader};
use lagodb_core::diag::PgReportError;
use pgrx::{PgTryBuilder, pg_sys};

use crate::plan::PostgresEvalExpr;
use state::PgExprState;

pub(super) use signature::PgExprSignature;
pub(super) use state::{PgExprRuntime, without_pg_cleanup};

pub(super) struct PgExprUdf {
    contract: PgExprSignature,
    expression: PostgresEvalExpr,
    state: OnceLock<Result<PgExprState, String>>,
    runtime: PgExprRuntime,
}

impl std::fmt::Debug for PgExprUdf {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgExprUdf")
            .field("name", &self.contract.name())
            .field("result_type", &self.expression.result_type())
            .finish()
    }
}

impl PartialEq for PgExprUdf {
    fn eq(&self, other: &Self) -> bool {
        self.expression.serialized() == other.expression.serialized()
            && self.expression.result_type() == other.expression.result_type()
            && self.expression.volatility() == other.expression.volatility()
            && self
                .expression
                .inputs()
                .iter()
                .map(|input| input.value_type())
                .eq(other
                    .expression
                    .inputs()
                    .iter()
                    .map(|input| input.value_type()))
    }
}

impl Eq for PgExprUdf {}

impl Hash for PgExprUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expression.serialized().to_bytes().hash(state);
        self.expression.result_type().hash(state);
        self.expression.volatility().hash(state);
        for input in self.expression.inputs() {
            input.value_type().hash(state);
        }
    }
}

impl PgExprUdf {
    pub(super) fn try_new(
        expression: PostgresEvalExpr,
        runtime: PgExprRuntime,
    ) -> Option<ScalarUDF> {
        let contract = PgExprSignature::try_new(&expression)?;
        Some(ScalarUDF::from(Self {
            contract,
            expression,
            state: OnceLock::new(),
            runtime,
        }))
    }

    fn execution_error(error: impl std::fmt::Display) -> DataFusionError {
        DataFusionError::Execution(format!(
            "PostgreSQL expression evaluation failed: {error}"
        ))
    }

    fn evaluate_batch(
        &self,
        arguments: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        pg_sys::check_for_interrupts!();
        let state = self
            .state
            .get_or_init(|| unsafe { self.runtime.initialize(&self.expression) })
            .as_ref()
            .map_err(Self::execution_error)?;

        let arrays = arguments
            .args
            .iter()
            .map(|argument| argument.to_array(arguments.number_rows))
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
        let readers = arrays
            .iter()
            .zip(self.expression.inputs())
            .map(|(array, input)| {
                PgDatumArrayReader::bind(array.as_ref(), input.value_type().type_oid)
                    .map_err(Self::execution_error)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let mut output = PgDatumArrayBuilder::bind(
            self.contract.result_type(),
            self.expression.result_type().type_oid,
            arguments.number_rows,
        )
        .map_err(Self::execution_error)?;

        // Resetting the per-tuple context is required for PostgreSQL expression
        // lifecycle semantics; it is intentionally isolated to this force-mode
        // fallback rather than added to the native vectorized path.
        for row in 0..arguments.number_rows {
            unsafe {
                pg_sys::ExecClearTuple(state.slot);
                pg_sys::MemoryContextReset((*state.econtext).ecxt_per_tuple_memory);
                let _tuple_context = state.enter_tuple_context();
                for (index, reader) in readers.iter().enumerate() {
                    match reader
                        .datum_unchecked(row)
                        .map_err(Self::execution_error)?
                    {
                        Some(datum) => {
                            (*state.slot).tts_values.add(index).write(datum);
                            (*state.slot).tts_isnull.add(index).write(false);
                        }
                        None => {
                            (*state.slot)
                                .tts_values
                                .add(index)
                                .write(pg_sys::Datum::from(0usize));
                            (*state.slot).tts_isnull.add(index).write(true);
                        }
                    }
                }
                (*state.slot).tts_nvalid = readers.len() as i16;
                pg_sys::ExecStoreVirtualTuple(state.slot);
                let mut is_null = false;
                let datum =
                    pg_sys::ExecEvalExpr(state.expr, state.econtext, &mut is_null);
                output
                    .append((!is_null).then_some(datum))
                    .map_err(Self::execution_error)?;
            }
        }
        Ok(ColumnarValue::Array(
            output.finish().map_err(Self::execution_error)?,
        ))
    }
}

impl ScalarUDFImpl for PgExprUdf {
    fn name(&self) -> &str {
        self.contract.name()
    }

    fn signature(&self) -> &Signature {
        self.contract.signature()
    }

    fn return_type(
        &self,
        _argument_types: &[DataType],
    ) -> DataFusionResult<DataType> {
        Ok(self.contract.result_type().clone())
    }

    fn invoke_with_args(
        &self,
        arguments: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        PgTryBuilder::new(AssertUnwindSafe(|| self.evaluate_batch(arguments)))
            .catch_others(|error| {
                Err(DataFusionError::External(Box::new(
                    PgReportError::from_caught(error),
                )))
            })
            .execute()
    }
}
