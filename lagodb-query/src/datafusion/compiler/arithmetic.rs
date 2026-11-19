//! PostgreSQL integer and floating-point arithmetic and the casts PostgreSQL
//! inserts between them.
//!
//! Each operand is cast to the operator's result type, the type PostgreSQL's
//! cross-type operators compute in (`int48pl` widens its int4 operand). A
//! cast to the type an operand already has is removed by physical planning.

use arrow_schema::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::{Expr, binary_expr};
use lagodb_core::expr::ExprType;

use crate::datafusion::pg_arithmetic::{
    PgArithmeticOperator, PgIntegerArithmeticUdf,
};
use crate::datafusion::pg_integer::PgIntegerKind;
use crate::plan::ExecutionScalarRepr;

use super::DataFusionPlanError;
use super::expression::DataFusionExpressionCompiler;

impl DataFusionExpressionCompiler<'_> {
    pub(super) fn arithmetic(
        operator: PgArithmeticOperator,
        left: Expr,
        right: Expr,
        result_type: ExprType,
    ) -> Result<Expr, DataFusionPlanError> {
        let data_type = Self::scalar_data_type(result_type)?;
        let left = cast(left, data_type.clone());
        let right = cast(right, data_type);
        if let Some(kind) = PgIntegerKind::from_oid(result_type.type_oid) {
            return Ok(PgIntegerArithmeticUdf::for_operator(operator, kind)
                .call(vec![left, right]));
        }
        let operator = operator.float_operator().ok_or_else(|| {
            DataFusionError::Plan(
                "PostgreSQL has no floating-point modulo operator".to_owned(),
            )
        })?;
        Ok(binary_expr(left, operator, right))
    }

    /// The DataFusion type of a scalar PostgreSQL result.
    pub(super) fn scalar_data_type(
        result_type: ExprType,
    ) -> Result<DataType, DataFusionPlanError> {
        ExecutionScalarRepr::for_runtime_value(result_type)
            .map(ExecutionScalarRepr::data_type)
            .ok_or(DataFusionPlanError::UnsupportedRuntimeType {
                oid: result_type.type_oid,
            })
    }
}
