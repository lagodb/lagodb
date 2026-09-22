//! Opaque PostgreSQL expression fallback payload.

use std::ffi::CStr;
use std::sync::Arc;

use super::{ExecutionExpr, ExecutionScalarRepr};
use lagodb_core::expr::ExprType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PostgresExprVolatility {
    Immutable,
    Stable,
    Volatile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresEvalInput {
    expression: ExecutionExpr,
    value_type: ExprType,
}

impl PostgresEvalInput {
    pub fn new(expression: ExecutionExpr, value_type: ExprType) -> Self {
        Self {
            expression,
            value_type,
        }
    }

    #[inline]
    pub const fn expression(&self) -> &ExecutionExpr {
        &self.expression
    }

    #[inline]
    pub const fn value_type(&self) -> ExprType {
        self.value_type
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresEvalExpr {
    serialized: Arc<CStr>,
    inputs: Box<[PostgresEvalInput]>,
    result_type: ExprType,
    volatility: PostgresExprVolatility,
}

impl PostgresEvalExpr {
    #[inline]
    pub const fn supports_type(value_type: ExprType) -> bool {
        ExecutionScalarRepr::for_postgres_eval(value_type).is_some()
    }

    pub fn new(
        serialized: Arc<CStr>,
        inputs: Box<[PostgresEvalInput]>,
        result_type: ExprType,
        volatility: PostgresExprVolatility,
    ) -> Self {
        Self {
            serialized,
            inputs,
            result_type,
            volatility,
        }
    }

    #[inline]
    pub fn serialized(&self) -> &CStr {
        &self.serialized
    }

    #[inline]
    pub fn inputs(&self) -> &[PostgresEvalInput] {
        &self.inputs
    }

    #[inline]
    pub const fn result_type(&self) -> ExprType {
        self.result_type
    }

    #[inline]
    pub const fn volatility(&self) -> PostgresExprVolatility {
        self.volatility
    }
}
