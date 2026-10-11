//! Execution-time materialization of PostgreSQL scalar values.

use datafusion::common::ScalarValue;
use lagodb_arrow::PgDatumArrayBuilder;
use lagodb_core::expr::{RuntimeValue, RuntimeValueId, RuntimeValueSource};
use pgrx::{AnyNumeric, FromDatum, pg_sys};

use crate::plan::{Decimal128Semantics, ExecutionScalarRepr, LimitNode};

use super::DataFusionPlanError;
use super::expression::DataFusionExpressionCompiler;

impl DataFusionExpressionCompiler<'_> {
    pub(super) fn runtime_value(
        value: RuntimeValue,
    ) -> Result<ScalarValue, DataFusionPlanError> {
        let oid = value.metadata().value_type.type_oid;
        let invalid = || DataFusionPlanError::InvalidRuntimeValue { oid };
        let value_type = value.metadata().value_type;
        let representation = ExecutionScalarRepr::for_runtime_value(value_type)
            .ok_or(DataFusionPlanError::UnsupportedRuntimeType { oid })?;
        if oid != pg_sys::NUMERICOID {
            let data_type = representation.data_type();
            let mut builder = PgDatumArrayBuilder::bind(&data_type, oid, 1)
                .map_err(|_| invalid())?;
            let datum = if value.is_null() {
                None
            } else {
                Some(unsafe { value.datum() })
            };
            unsafe { builder.append(datum) }.map_err(|_| invalid())?;
            let array = builder.finish().map_err(|_| invalid())?;
            let scalar = ScalarValue::try_from_array(&array, 0)?;
            // The storage encoder uses large offsets. A scalar owns only bytes,
            // so selecting the query's Binary representation moves that buffer
            // without another copy and also permits fixed-binary coercion.
            return Ok(match (representation, scalar) {
                (ExecutionScalarRepr::Binary, ScalarValue::LargeBinary(bytes)) => {
                    ScalarValue::Binary(bytes)
                }
                (_, scalar) => scalar,
            });
        }
        debug_assert_eq!(representation, ExecutionScalarRepr::Float64);
        if value.is_null() {
            return Ok(ScalarValue::Float64(None));
        }
        let datum = unsafe { value.datum() };
        unsafe {
            AnyNumeric::from_datum(datum, false)
                .ok_or_else(invalid)
                .and_then(|value| {
                    f64::try_from(value)
                        .map(|value| ScalarValue::Float64(Some(value)))
                        .map_err(|_| invalid())
                })
        }
    }

    pub(super) fn decimal_runtime_value(
        value: RuntimeValue,
        semantics: Decimal128Semantics,
    ) -> Result<ScalarValue, DataFusionPlanError> {
        let metadata = value.metadata();
        let oid = metadata.value_type.type_oid;
        let invalid = || DataFusionPlanError::InvalidRuntimeValue { oid };
        if oid != pg_sys::NUMERICOID
            || metadata.source_kind != RuntimeValueSource::Constant
        {
            return Err(invalid());
        }
        if value.is_null() {
            return Ok(ScalarValue::Decimal128(
                None,
                semantics.precision(),
                semantics.scale(),
            ));
        }
        let codec = semantics.codec();
        // SAFETY: RuntimeValueState evaluated this NUMERIC expression in the
        // current executor context and retains the datum for plan compilation.
        let coefficient = unsafe { codec.encode_bound_datum(value.datum()) }
            .map_err(|_| invalid())?;
        Ok(ScalarValue::Decimal128(
            Some(coefficient),
            semantics.precision(),
            semantics.scale(),
        ))
    }
}

pub(super) struct LimitWindow {
    pub(super) offset: usize,
    pub(super) count: Option<usize>,
}

impl LimitWindow {
    pub(super) fn bind(
        limit: &LimitNode,
        values: &[RuntimeValue],
    ) -> Result<Self, DataFusionPlanError> {
        Ok(Self {
            offset: Self::value(limit.offset(), values)?.unwrap_or(0),
            count: Self::value(limit.count(), values)?,
        })
    }

    fn value(
        id: Option<RuntimeValueId>,
        values: &[RuntimeValue],
    ) -> Result<Option<usize>, DataFusionPlanError> {
        let Some(id) = id else {
            return Ok(None);
        };
        let value = values
            .get(id.index())
            .copied()
            .ok_or(DataFusionPlanError::MissingRuntimeValue { index: id.index() })?;
        if value.is_null() {
            return Ok(None);
        }
        let value = unsafe { i64::from_datum(value.datum(), false) }
            .ok_or(DataFusionPlanError::InvalidLimit)?;
        usize::try_from(value)
            .map(Some)
            .map_err(|_| DataFusionPlanError::InvalidLimit)
    }

    pub(super) fn supports_bounded_top_k(&self) -> bool {
        self.count.is_none_or(|count| {
            self.offset
                .checked_add(count)
                .is_some_and(|top_k| top_k <= i64::MAX as usize)
        })
    }
}
