//! Query-level DISTINCT semantics and IR node.

use lagodb_core::expr::ExprType;
use lagodb_core::query_contract::OutputId;
use pgrx::pg_sys;

use super::ir::{QueryNode, QueryPlanError, RowEstimate};
use super::{Decimal128Semantics, ExecutionExpr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctExpr {
    expression: ExecutionExpr,
    result_type: ExprType,
    output: OutputId,
}

impl DistinctExpr {
    pub fn try_new(
        expression: ExecutionExpr,
        result_type: ExprType,
        output: OutputId,
    ) -> Result<Self, QueryPlanError> {
        let expression_type = expression
            .result_type_hint()
            .ok_or(QueryPlanError::UnsupportedDistinctKey)?;
        if !matches!(&expression, ExecutionExpr::Column(_))
            || expression_type != result_type
            || !Self::supports_type(result_type)
        {
            return Err(QueryPlanError::UnsupportedDistinctKey);
        }
        Ok(Self {
            expression,
            result_type,
            output,
        })
    }

    fn supports_type(value_type: ExprType) -> bool {
        // Query DISTINCT deliberately inherits DataFusion's native grouping
        // keys. Float keys use bit equality, so -0/+0 and distinct NaN payloads
        // do not follow PostgreSQL equality. Text-family keys use Arrow byte
        // equality; nondeterministic collations are rejected below, but this
        // gate does not provide locale-aware equality normalization.
        match value_type.type_oid {
            pg_sys::BOOLOID
            | pg_sys::INT2OID
            | pg_sys::INT4OID
            | pg_sys::INT8OID
            | pg_sys::FLOAT4OID
            | pg_sys::FLOAT8OID
            | pg_sys::BYTEAOID
            | pg_sys::UUIDOID
            | pg_sys::DATEOID
            | pg_sys::TIMEOID
            | pg_sys::TIMESTAMPOID
            | pg_sys::TIMESTAMPTZOID => {
                value_type.typmod == -1 && value_type.collation == pg_sys::InvalidOid
            }
            pg_sys::TEXTOID
            | pg_sys::VARCHAROID
            | pg_sys::BPCHAROID
            | pg_sys::NAMEOID => {
                value_type.collation != pg_sys::InvalidOid
                    && unsafe {
                        pg_sys::get_collation_isdeterministic(value_type.collation)
                    }
            }
            pg_sys::NUMERICOID => Decimal128Semantics::for_type(value_type).is_some(),
            _ => false,
        }
    }

    #[inline]
    pub const fn expression(&self) -> &ExecutionExpr {
        &self.expression
    }

    #[inline]
    pub const fn result_type(&self) -> ExprType {
        self.result_type
    }

    #[inline]
    pub const fn output(&self) -> OutputId {
        self.output
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctNode {
    input: Box<QueryNode>,
    keys: Box<[DistinctExpr]>,
    estimated_rows: RowEstimate,
}

impl DistinctNode {
    pub fn new(
        input: QueryNode,
        keys: Box<[DistinctExpr]>,
        estimated_rows: f64,
    ) -> Result<Self, QueryPlanError> {
        if keys.is_empty() {
            return Err(QueryPlanError::EmptyDistinct);
        }
        Ok(Self {
            input: Box::new(input),
            keys,
            estimated_rows: RowEstimate::try_new(estimated_rows)?,
        })
    }

    #[inline]
    pub fn input(&self) -> &QueryNode {
        &self.input
    }

    #[inline]
    pub fn keys(&self) -> &[DistinctExpr] {
        &self.keys
    }

    #[inline]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows.get()
    }
}
