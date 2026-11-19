//! PostgreSQL integer ABS semantics over DataFusion's native implementation.

use std::sync::Arc;

use arrow_array::Int32Array;
use arrow_array::types::Int32Type;
use arrow_schema::{ArrowError, DataType};
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use pgrx::pg_sys;

use super::pg_integer::PgIntegerKind;

/// Integer ABS retaining DataFusion's batch execution while restoring the
/// PostgreSQL overflow domain and structured SQL error.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct PgIntegerAbsUdf {
    kind: PgIntegerKind,
    signature: Signature,
    native: Arc<ScalarUDF>,
}

impl PgIntegerAbsUdf {
    pub(super) fn for_type(type_oid: pg_sys::Oid) -> Option<ScalarUDF> {
        let kind = PgIntegerKind::from_oid(type_oid)?;
        Some(ScalarUDF::from(Self {
            kind,
            signature: Signature::exact(
                vec![kind.data_type()],
                Volatility::Immutable,
            ),
            native: datafusion::functions::math::abs(),
        }))
    }

    pub(super) fn codec_tag(udf: &ScalarUDF) -> Option<u8> {
        udf.inner()
            .downcast_ref::<Self>()
            .map(|implementation| implementation.kind.codec_tag())
    }

    pub(super) fn from_codec_tag(tag: u8) -> Option<ScalarUDF> {
        let kind = PgIntegerKind::from_codec_tag(tag)?;
        Some(ScalarUDF::from(Self {
            kind,
            signature: Signature::exact(
                vec![kind.data_type()],
                Volatility::Immutable,
            ),
            native: datafusion::functions::math::abs(),
        }))
    }

    fn overflow(&self) -> DataFusionError {
        DataFusionError::External(Box::new(self.kind.out_of_range()))
    }

    fn smallint_abs(
        &self,
        arguments: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&arguments.args)?;
        let [input] = arrays.as_slice() else {
            return Err(DataFusionError::Internal(
                "PostgreSQL smallint ABS received an invalid argument layout"
                    .to_owned(),
            ));
        };
        let input = input.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
            DataFusionError::Internal(
                "PostgreSQL smallint ABS received a non-Int32 array".to_owned(),
            )
        })?;
        let output = input.try_unary::<_, Int32Type, DataFusionError>(|value| {
            if value == i32::from(i16::MIN) {
                Err(self.overflow())
            } else {
                // The PostgreSQL int2 type is the value-domain invariant even
                // though its Iceberg/Arrow physical representation is Int32.
                Ok(value.wrapping_abs())
            }
        })?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }

    fn map_native_error(&self, error: DataFusionError) -> DataFusionError {
        let is_overflow = matches!(
            &error,
            DataFusionError::ArrowError(source, _)
                if matches!(
                    source.as_ref(),
                    ArrowError::ComputeError(_)
                        | ArrowError::ArithmeticOverflow(_)
                )
        );
        if is_overflow { self.overflow() } else { error }
    }
}

impl ScalarUDFImpl for PgIntegerAbsUdf {
    fn name(&self) -> &str {
        "abs"
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
        match self.kind {
            PgIntegerKind::SmallInt => self.smallint_abs(arguments),
            PgIntegerKind::Integer | PgIntegerKind::BigInt => self
                .native
                .invoke_with_args(arguments)
                .map_err(|error| self.map_native_error(error)),
        }
    }

    fn output_ordering(
        &self,
        inputs: &[ExprProperties],
    ) -> DataFusionResult<SortProperties> {
        self.native.output_ordering(inputs)
    }
}
