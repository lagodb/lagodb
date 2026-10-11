//! Shared PostgreSQL UDF type contract, independent of executor state.

use arrow_schema::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::plan::{ExecutionScalarRepr, PostgresEvalExpr, PostgresExprVolatility};

/// Admission keeps the real argument signature in the logical expression tree.
/// Only type coercion runs on this UDF; PostgreSQL state belongs to PgExprUdf.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(in crate::datafusion) struct PgExprSignature {
    name: String,
    signature: Signature,
    return_type: DataType,
}

impl PgExprSignature {
    pub(in crate::datafusion) fn try_new(
        expression: &PostgresEvalExpr,
    ) -> Option<Self> {
        let return_type =
            ExecutionScalarRepr::for_postgres_eval(expression.result_type())?
                .data_type();
        let input_types = expression
            .inputs()
            .iter()
            .map(|input| {
                ExecutionScalarRepr::for_postgres_eval(input.value_type())
                    .map(ExecutionScalarRepr::data_type)
            })
            .collect::<Option<Vec<_>>>()?;
        let volatility = match expression.volatility() {
            PostgresExprVolatility::Immutable => Volatility::Immutable,
            PostgresExprVolatility::Stable => Volatility::Stable,
            PostgresExprVolatility::Volatile => Volatility::Volatile,
        };
        Some(Self {
            name: Self::stable_name(expression.serialized().to_bytes()),
            signature: Signature::exact(input_types, volatility),
            return_type,
        })
    }

    pub(super) fn result_type(&self) -> &DataType {
        &self.return_type
    }

    pub(in crate::datafusion) fn into_udf(self) -> ScalarUDF {
        ScalarUDF::from(self)
    }

    fn stable_name(serialized: &[u8]) -> String {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in serialized {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("lagodb_pg_expr_{hash:016x}")
    }
}

impl ScalarUDFImpl for PgExprSignature {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _argument_types: &[DataType],
    ) -> DataFusionResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> DataFusionResult<ColumnarValue> {
        unreachable!("admission only analyzes PostgreSQL UDF signatures")
    }
}
