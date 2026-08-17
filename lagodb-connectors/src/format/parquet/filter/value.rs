//! PostgreSQL comparison semantics and bound scalar values.
//!
//! Exact pushdown is intentionally limited to boolean equality, integer
//! comparisons, and collation-compatible strings. Arrow's floating comparison
//! uses IEEE total ordering (including distinct signed zero), which is not
//! PostgreSQL's equality semantics; temporal values also require PostgreSQL
//! epoch/infinity normalization. Comparisons on those types remain local quals
//! until an exact representation is implemented.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, LargeStringArray, Scalar,
    StringArray,
};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, DataType};
use pg_lakebase_core::expr::pushdown::{FilterColumn, FilterValue, FilterValueSlot};
use pg_lakebase_core::tuple::ColumnDatumTarget;
use pgrx::{FromDatum, PgBuiltInOids, PgOid, pg_sys};

use crate::error::ConnectorError;
use crate::format::FormatKind;

const OP_EQ: i32 = 0;
const OP_NOT_EQ: i32 = 1;
const OP_LT: i32 = 2;
const OP_LE: i32 = 3;
const OP_GT: i32 = 4;
const OP_GE: i32 = 5;

const VALUE_BOOL: i32 = 0;
const VALUE_I32: i32 = 1;
const VALUE_I64: i32 = 2;
const VALUE_STRING: i32 = 3;

mod operator_oid {
    use pgrx::pg_sys;

    pub const BOOL_EQ: u32 = pg_sys::BooleanEqualOperator;
    pub const BOOL_NE: u32 = pg_sys::BooleanNotEqualOperator;
    pub const INT2_EQ: u32 = 94;
    pub const INT2_NE: u32 = 519;
    pub const INT2_LT: u32 = 95;
    pub const INT2_LE: u32 = 522;
    pub const INT2_GT: u32 = 520;
    pub const INT2_GE: u32 = 524;
    pub const INT4_EQ: u32 = pg_sys::Int4EqualOperator;
    pub const INT4_NE: u32 = 518;
    pub const INT4_LT: u32 = pg_sys::Int4LessOperator;
    pub const INT4_LE: u32 = 523;
    pub const INT4_GT: u32 = 521;
    pub const INT4_GE: u32 = 525;
    pub const INT8_EQ: u32 = 410;
    pub const INT8_NE: u32 = 411;
    pub const INT8_LT: u32 = pg_sys::Int8LessOperator;
    pub const INT8_LE: u32 = 414;
    pub const INT8_GT: u32 = 413;
    pub const INT8_GE: u32 = 415;
    pub const TEXT_EQ: u32 = pg_sys::TextEqualOperator;
    pub const TEXT_NE: u32 = 531;
    pub const TEXT_LT: u32 = pg_sys::TextLessOperator;
    pub const TEXT_LE: u32 = 665;
    pub const TEXT_GT: u32 = 666;
    pub const TEXT_GE: u32 = pg_sys::TextGreaterEqualOperator;
}

#[derive(Clone, Copy)]
pub(super) enum ComparisonOperator {
    Eq,
    NotEq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl ComparisonOperator {
    pub(super) const fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
        }
    }

    pub(super) fn from_oid(oid: pg_sys::Oid) -> Option<Self> {
        use operator_oid as op;
        Some(match u32::from(oid) {
            op::BOOL_EQ | op::INT2_EQ | op::INT4_EQ | op::INT8_EQ | op::TEXT_EQ => {
                Self::Eq
            }
            op::BOOL_NE | op::INT2_NE | op::INT4_NE | op::INT8_NE | op::TEXT_NE => {
                Self::NotEq
            }
            op::INT2_LT | op::INT4_LT | op::INT8_LT | op::TEXT_LT => Self::Lt,
            op::INT2_LE | op::INT4_LE | op::INT8_LE | op::TEXT_LE => Self::Le,
            op::INT2_GT | op::INT4_GT | op::INT8_GT | op::TEXT_GT => Self::Gt,
            op::INT2_GE | op::INT4_GE | op::INT8_GE | op::TEXT_GE => Self::Ge,
            _ => return None,
        })
    }

    pub(super) const fn mirrored(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
            Self::Eq | Self::NotEq => self,
        }
    }

    pub(super) const fn negated(self) -> Self {
        match self {
            Self::Eq => Self::NotEq,
            Self::NotEq => Self::Eq,
            Self::Lt => Self::Ge,
            Self::Le => Self::Gt,
            Self::Gt => Self::Le,
            Self::Ge => Self::Lt,
        }
    }

    pub(super) const fn tag(self) -> i32 {
        match self {
            Self::Eq => OP_EQ,
            Self::NotEq => OP_NOT_EQ,
            Self::Lt => OP_LT,
            Self::Le => OP_LE,
            Self::Gt => OP_GT,
            Self::Ge => OP_GE,
        }
    }

    pub(super) fn from_tag(tag: i32) -> Result<Self, ConnectorError> {
        match tag {
            OP_EQ => Ok(Self::Eq),
            OP_NOT_EQ => Ok(Self::NotEq),
            OP_LT => Ok(Self::Lt),
            OP_LE => Ok(Self::Le),
            OP_GT => Ok(Self::Gt),
            OP_GE => Ok(Self::Ge),
            _ => Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet)),
        }
    }

    pub(super) fn evaluate(
        self,
        column: &dyn Array,
        scalar: &Scalar<ArrayRef>,
    ) -> Result<BooleanArray, ArrowError> {
        match self {
            Self::Eq => cmp::eq(column, scalar),
            Self::NotEq => cmp::neq(column, scalar),
            Self::Lt => cmp::lt(column, scalar),
            Self::Le => cmp::lt_eq(column, scalar),
            Self::Gt => cmp::gt(column, scalar),
            Self::Ge => cmp::gt_eq(column, scalar),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ValueType {
    Bool,
    I32,
    I64,
    String,
}

impl ValueType {
    pub(super) const fn accepts_operator(self, operator: ComparisonOperator) -> bool {
        !matches!(self, Self::Bool)
            || matches!(operator, ComparisonOperator::Eq | ComparisonOperator::NotEq)
    }

    pub(super) fn for_comparison(
        column: &FilterColumn,
        value: &FilterValueSlot,
        opno: pg_sys::Oid,
        opcollid: pg_sys::Oid,
        inputcollid: pg_sys::Oid,
    ) -> Option<Self> {
        let declared = PgOid::from(column.declared_type.type_oid);
        let effective = PgOid::from(column.value_type.type_oid);
        let value_oid = PgOid::from(value.value_type.type_oid);
        let operator = ComparisonOperator::from_oid(opno)?;
        let value_type = match (declared, effective, value_oid) {
            (
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
                PgOid::BuiltIn(PgBuiltInOids::BOOLOID),
            ) if matches!(
                operator,
                ComparisonOperator::Eq | ComparisonOperator::NotEq
            ) =>
            {
                Self::Bool
            }
            (
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
                PgOid::BuiltIn(PgBuiltInOids::INT2OID),
            )
            | (
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
                PgOid::BuiltIn(PgBuiltInOids::INT4OID),
            ) => Self::I32,
            (
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                PgOid::BuiltIn(PgBuiltInOids::INT8OID),
            ) => Self::I64,
            (
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
            ) => Self::String,
            _ => return None,
        };

        match value_type {
            Self::Bool | Self::I32 | Self::I64
                if opcollid == pg_sys::Oid::INVALID
                    && inputcollid == pg_sys::Oid::INVALID =>
            {
                Some(value_type)
            }
            Self::String => {
                // pgrx String datums and Arrow UTF-8 scalars both require a
                // UTF-8 PostgreSQL database encoding.
                if ColumnDatumTarget::validate_utf8_server_encoding().is_err() {
                    return None;
                }
                let c_order = inputcollid == pg_sys::C_COLLATION_OID
                    || inputcollid == pg_sys::POSIX_COLLATION_OID;
                let deterministic = c_order
                    || (inputcollid != pg_sys::Oid::INVALID
                        && unsafe {
                            pg_sys::get_collation_isdeterministic(inputcollid)
                        });
                match operator {
                    ComparisonOperator::Eq | ComparisonOperator::NotEq
                        if deterministic =>
                    {
                        Some(value_type)
                    }
                    ComparisonOperator::Lt
                    | ComparisonOperator::Le
                    | ComparisonOperator::Gt
                    | ComparisonOperator::Ge
                        if c_order =>
                    {
                        Some(value_type)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub(super) const fn tag(self) -> i32 {
        match self {
            Self::Bool => VALUE_BOOL,
            Self::I32 => VALUE_I32,
            Self::I64 => VALUE_I64,
            Self::String => VALUE_STRING,
        }
    }

    pub(super) fn from_tag(tag: i32) -> Result<Self, ConnectorError> {
        match tag {
            VALUE_BOOL => Ok(Self::Bool),
            VALUE_I32 => Ok(Self::I32),
            VALUE_I64 => Ok(Self::I64),
            VALUE_STRING => Ok(Self::String),
            _ => Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet)),
        }
    }

    /// # Safety
    ///
    /// `value` must be a live, non-NULL PostgreSQL datum for the binding
    /// callback. This method validates its catalog OID before interpreting the
    /// datum and copies pass-by-reference strings into Rust-owned storage.
    pub(super) unsafe fn decode(
        self,
        value: FilterValue,
    ) -> Result<BoundValue, ConnectorError> {
        let datum = unsafe { value.datum() };
        let type_oid = value.metadata().value_type.type_oid;
        let decoded = match (self, type_oid) {
            (Self::Bool, pg_sys::BOOLOID) => BoundValue::Bool(
                unsafe { bool::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            ),
            (Self::I32, pg_sys::INT2OID) => BoundValue::I32(
                unsafe { i16::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?
                    as i32,
            ),
            (Self::I32, pg_sys::INT4OID) => BoundValue::I32(
                unsafe { i32::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            ),
            (Self::I64, pg_sys::INT8OID) => BoundValue::I64(
                unsafe { i64::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            ),
            (Self::String, pg_sys::TEXTOID | pg_sys::VARCHAROID) => {
                BoundValue::String(
                    unsafe { String::from_datum(datum, false) }
                        .ok_or_else(|| {
                            ConnectorError::invalid_filter_datum(type_oid)
                        })?
                        .into_boxed_str(),
                )
            }
            _ => {
                return Err(ConnectorError::invalid_filter_plan(FormatKind::Parquet));
            }
        };
        Ok(decoded)
    }
}

#[derive(Clone)]
pub(super) enum BoundValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    String(Box<str>),
}

impl BoundValue {
    pub(super) fn scalar(
        &self,
        data_type: &DataType,
    ) -> Result<Scalar<ArrayRef>, ConnectorError> {
        let array: ArrayRef = match (self, data_type) {
            (Self::Bool(value), DataType::Boolean) => {
                Arc::new(BooleanArray::from(vec![*value]))
            }
            (Self::I32(value), DataType::Int32) => {
                Arc::new(Int32Array::from(vec![*value]))
            }
            (Self::I64(value), DataType::Int64) => {
                Arc::new(Int64Array::from(vec![*value]))
            }
            (Self::String(value), DataType::Utf8) => {
                Arc::new(StringArray::from(vec![value.as_ref()]))
            }
            (Self::String(value), DataType::LargeUtf8) => {
                Arc::new(LargeStringArray::from(vec![value.as_ref()]))
            }
            _ => {
                return Err(ConnectorError::invalid_object_schema(
                    FormatKind::Parquet,
                    format!(
                        "a pushed predicate value is incompatible with Parquet type {data_type}"
                    ),
                ));
            }
        };
        Ok(Scalar::new(array))
    }
}
