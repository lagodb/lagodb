//! PostgreSQL scalar types with a materialized DataFusion representation.
//!
//! This is the single capability contract shared by planner admission,
//! decoded-plan validation, runtime-literal construction, and PostgreSQL UDF
//! binding. It deliberately describes physical representation only; aggregate
//! and DISTINCT semantic allowlists remain responsible for their own operator
//! semantics.

use arrow_schema::DataType;
use lagodb_core::expr::ExprType;
use pgrx::pg_sys;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutionScalarRepr {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
}

impl ExecutionScalarRepr {
    /// Representation accepted for a value evaluated by PostgreSQL before the
    /// DataFusion plan is built.
    pub const fn for_runtime_value(value_type: ExprType) -> Option<Self> {
        match value_type.type_oid {
            pg_sys::BOOLOID => Some(Self::Boolean),
            pg_sys::INT2OID | pg_sys::INT4OID => Some(Self::Int32),
            pg_sys::INT8OID => Some(Self::Int64),
            pg_sys::FLOAT4OID => Some(Self::Float32),
            pg_sys::FLOAT8OID | pg_sys::NUMERICOID => Some(Self::Float64),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::NAMEOID => {
                Some(Self::Utf8)
            }
            _ => None,
        }
    }

    /// Representation accepted at an ExecEvalExpr UDF input or result
    /// boundary. NUMERIC is intentionally absent until its Datum/Arrow
    /// semantics are implemented for the fallback evaluator.
    pub const fn for_postgres_eval(value_type: ExprType) -> Option<Self> {
        match value_type.type_oid {
            pg_sys::BOOLOID => Some(Self::Boolean),
            pg_sys::INT2OID | pg_sys::INT4OID => Some(Self::Int32),
            pg_sys::INT8OID => Some(Self::Int64),
            pg_sys::FLOAT4OID => Some(Self::Float32),
            pg_sys::FLOAT8OID => Some(Self::Float64),
            pg_sys::TEXTOID | pg_sys::VARCHAROID | pg_sys::NAMEOID => {
                Some(Self::Utf8)
            }
            _ => None,
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
        }
    }
}
