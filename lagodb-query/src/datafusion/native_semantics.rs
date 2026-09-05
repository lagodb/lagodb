//! PostgreSQL semantic adapters around otherwise native DataFusion functions.

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
use lagodb_core::diag::PgReportError;
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PgIntegerKind {
    SmallInt,
    Integer,
    BigInt,
}

impl PgIntegerKind {
    const fn from_oid(type_oid: pg_sys::Oid) -> Option<Self> {
        match type_oid {
            pg_sys::INT2OID => Some(Self::SmallInt),
            pg_sys::INT4OID => Some(Self::Integer),
            pg_sys::INT8OID => Some(Self::BigInt),
            _ => None,
        }
    }

    const fn overflow_message(self) -> &'static str {
        match self {
            Self::SmallInt => "smallint out of range",
            Self::Integer => "integer out of range",
            Self::BigInt => "bigint out of range",
        }
    }

    const fn data_type(self) -> DataType {
        match self {
            // Iceberg has no distinct smallint physical type. LagoDB widens
            // PostgreSQL int2 columns to Arrow Int32 at the scan boundary.
            Self::SmallInt | Self::Integer => DataType::Int32,
            Self::BigInt => DataType::Int64,
        }
    }
}

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

    #[cold]
    #[inline(never)]
    fn overflow(&self) -> DataFusionError {
        DataFusionError::External(Box::new(PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            self.kind.overflow_message(),
        )))
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
