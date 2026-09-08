//! PostgreSQL finalization for fixed-width exact NUMERIC aggregate state.

use std::panic::AssertUnwindSafe;

use arrow_buffer::i256;
use datafusion::common::{DataFusionError, Result};
use lagodb_core::diag::PgReportError;
use lagodb_core::tuple::{DetoastedVarlena, PostgresNumericCodec};
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
        if kind == AggregateKind::NumericSum {
            return PostgresNumericCodec::varlena_from_i256_be_bytes(
                sum.to_be_bytes(),
                scale,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)));
        }
        if kind != AggregateKind::NumericAverage {
            return Err(Self::invalid_kind());
        }
        let average_count = i64::try_from(count).map_err(|_| {
            DataFusionError::External(Box::new(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                "aggregate count is out of range for bigint",
            )))
        })?;
        let sum = PostgresNumericCodec::numeric_from_i256_be_bytes(
            sum.to_be_bytes(),
            scale,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        PgTryBuilder::new(AssertUnwindSafe(move || {
            let value = sum / AnyNumeric::from(average_count);
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

    fn invalid_kind() -> DataFusionError {
        DataFusionError::Internal(
            "non-NUMERIC aggregate reached numeric finalization".to_owned(),
        )
    }
}
