//! PostgreSQL finalization for fixed-width exact NUMERIC aggregate state.

use std::panic::AssertUnwindSafe;

use arrow_buffer::i256;
use datafusion::common::{DataFusionError, Result};
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::DetoastedVarlena;
use pgrx::prelude::PgSqlErrorCode;
use pgrx::{AnyNumeric, IntoDatum, PgTryBuilder, pg_sys};

use crate::plan::AggregateKind;

/// Cold finalization of one exact fixed-width numeric aggregate state.
/// Per-input-row accumulation never formats or enters PostgreSQL; only one
/// value per output group crosses this boundary.
pub(super) struct NumericOutput;

impl NumericOutput {
    pub(super) fn finalize(
        sum: i256,
        count: u64,
        scale: u32,
        kind: AggregateKind,
    ) -> Result<Vec<u8>> {
        // This output-boundary conversion keeps accumulation fixed-width and
        // performs PostgreSQL materialization once per output group. If grouped
        // benchmarks prove the text conversion material, extend lagodb-core's
        // shared base-10000 NUMERIC
        // codec with an unbounded-typmod i256 coefficient encoder. That codec
        // must independently prove sign, weight, dscale, zero/carry and
        // numeric_recv error semantics; it must not become a private aggregate
        // fast path or apply the input typmod to a SUM result.
        let text = Self::scaled_text(sum, scale);
        let count = i64::try_from(count).map_err(|_| {
            DataFusionError::External(Box::new(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "aggregate count is out of range for bigint",
            )))
        })?;
        PgTryBuilder::new(AssertUnwindSafe(move || {
            let sum = AnyNumeric::try_from(text.as_str()).map_err(|error| {
                DataFusionError::Internal(format!(
                    "failed to materialize exact numeric aggregate state: {error}"
                ))
            })?;
            let value = match kind {
                AggregateKind::NumericSum => sum,
                AggregateKind::NumericAverage => sum / AnyNumeric::from(count),
                _ => return Err(Self::invalid_kind()),
            };
            let datum = value.into_datum().ok_or_else(|| {
                DataFusionError::Internal(
                    "PostgreSQL numeric finalization returned SQL NULL".to_owned(),
                )
            })?;
            let bytes = {
                // SAFETY: into_datum produced a live NUMERIC varlena and the
                // Arrow value copies its bytes before this vector is released.
                let numeric = unsafe { DetoastedVarlena::from_datum(datum) };
                numeric.full_varlena_bytes().to_vec()
            };
            // SAFETY: into_datum returned a fresh palloc'd value; a distinct
            // detoast allocation, if any, was released by the guard above.
            unsafe { pg_sys::pfree(datum.cast_mut_ptr()) };
            Ok(bytes)
        }))
        .catch_others(|error| {
            Err(DataFusionError::External(Box::new(
                PgReportError::from_caught(error),
            )))
        })
        .execute()
    }

    fn scaled_text(value: i256, scale: u32) -> String {
        let integer = value.to_string();
        if scale == 0 {
            return integer;
        }
        let (negative, digits) = integer
            .strip_prefix('-')
            .map_or((false, integer.as_str()), |digits| (true, digits));
        let scale = scale as usize;
        let leading_zeroes = scale.saturating_sub(digits.len());
        let mut output = String::with_capacity(
            usize::from(negative) + digits.len().max(scale + 1) + 1,
        );
        if negative {
            output.push('-');
        }
        if digits.len() <= scale {
            output.push_str("0.");
            output.extend(std::iter::repeat_n('0', leading_zeroes));
            output.push_str(digits);
        } else {
            let point = digits.len() - scale;
            output.push_str(&digits[..point]);
            output.push('.');
            output.push_str(&digits[point..]);
        }
        output
    }

    fn invalid_kind() -> DataFusionError {
        DataFusionError::Internal(
            "non-NUMERIC aggregate reached numeric finalization".to_owned(),
        )
    }
}
