//! PostgreSQL scalar types with a materialized DataFusion representation.
//!
//! This is the single capability contract shared by planner admission,
//! decoded-plan validation, runtime-literal construction, and PostgreSQL UDF
//! binding. It deliberately describes physical representation only; aggregate
//! and DISTINCT semantic allowlists remain responsible for their own operator
//! semantics.

use arrow_schema::{DataType, TimeUnit};
use lagodb_arrow::{pg_epoch_days_to_unix_days, pg_epoch_micros_to_unix_micros};
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
    Binary,
    Uuid,
    Date,
    Timestamp,
    Timestamptz,
}

impl ExecutionScalarRepr {
    /// Representation accepted for a value evaluated by PostgreSQL before the
    /// DataFusion plan is built.
    pub const fn for_runtime_value(value_type: ExprType) -> Option<Self> {
        match value_type.type_oid {
            pg_sys::BYTEAOID => Some(Self::Binary),
            pg_sys::UUIDOID => Some(Self::Uuid),
            pg_sys::DATEOID => Some(Self::Date),
            pg_sys::TIMESTAMPOID => Some(Self::Timestamp),
            pg_sys::TIMESTAMPTZOID => Some(Self::Timestamptz),
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

    /// Whether a non-NULL PG-epoch date has a finite Arrow Date32 value.
    pub fn supports_date_value(days: i32) -> bool {
        !matches!(days, i32::MIN | i32::MAX)
            && pg_epoch_days_to_unix_days(days).is_some()
    }

    /// Whether a non-NULL PG-epoch timestamp has a finite Arrow microsecond value.
    pub fn supports_timestamp_value(micros: i64) -> bool {
        !matches!(micros, i64::MIN | i64::MAX)
            && pg_epoch_micros_to_unix_micros(micros).is_some()
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
            Self::Binary => DataType::Binary,
            Self::Uuid => DataType::FixedSizeBinary(16),
            Self::Date => DataType::Date32,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
            }
        }
    }
}
