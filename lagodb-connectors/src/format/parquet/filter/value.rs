//! PostgreSQL comparison semantics and bound scalar values.
//!
//! Exact pushdown is intentionally limited to boolean equality, integer and
//! bounded-decimal comparisons, and collation-compatible strings. Arrow's
//! floating comparison uses IEEE total ordering (including distinct signed
//! zero), which is not PostgreSQL's equality semantics; temporal values also
//! require PostgreSQL epoch/infinity normalization. Comparisons on those types
//! remain local quals until an exact representation is implemented.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int64Array,
    LargeStringArray, Scalar, StringArray,
};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, DataType};
use lagodb_core::expr::{
    ColumnRef, PgComparisonIdentity, PgComparisonKind, PgComparisonSignature,
    PgTextComparisonSemantics, RuntimeValue, RuntimeValueSpec,
};
use lagodb_core::plan_data::{PlanDataReader, PlanDataWriter};
use lagodb_core::tuple::{
    Decimal128ComparisonValue, Decimal128Semantics, Utf8ServerEncoding,
};
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
const VALUE_DECIMAL128: i32 = 4;

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
    pub(super) const fn kind(self) -> PgComparisonKind {
        match self {
            Self::Eq => PgComparisonKind::Equal,
            Self::NotEq => PgComparisonKind::NotEqual,
            Self::Lt => PgComparisonKind::Less,
            Self::Le => PgComparisonKind::LessEqual,
            Self::Gt => PgComparisonKind::Greater,
            Self::Ge => PgComparisonKind::GreaterEqual,
        }
    }

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
        let signature = PgComparisonSignature::for_operator(oid)?;
        // This is Parquet's operator-family allowlist. PostgreSQL normalization
        // already guarantees compatibility between the selected signature and
        // the comparison operands.
        match (signature.left_type(), signature.right_type()) {
            (pg_sys::BOOLOID, pg_sys::BOOLOID)
            | (pg_sys::INT2OID, pg_sys::INT2OID)
            | (pg_sys::INT4OID, pg_sys::INT4OID)
            | (pg_sys::INT8OID, pg_sys::INT8OID)
            | (pg_sys::NUMERICOID, pg_sys::NUMERICOID)
            | (pg_sys::TEXTOID, pg_sys::TEXTOID) => {}
            _ => return None,
        }
        Some(match signature.kind() {
            PgComparisonKind::Equal => Self::Eq,
            PgComparisonKind::NotEqual => Self::NotEq,
            PgComparisonKind::Less => Self::Lt,
            PgComparisonKind::LessEqual => Self::Le,
            PgComparisonKind::Greater => Self::Gt,
            PgComparisonKind::GreaterEqual => Self::Ge,
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
            Self::Eq => cmp::eq(&column, scalar),
            Self::NotEq => cmp::neq(&column, scalar),
            Self::Lt => cmp::lt(&column, scalar),
            Self::Le => cmp::lt_eq(&column, scalar),
            Self::Gt => cmp::gt(&column, scalar),
            Self::Ge => cmp::gt_eq(&column, scalar),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ValueType {
    Bool,
    I32,
    I64,
    String,
    Decimal128(Decimal128Semantics),
}

impl ValueType {
    pub(super) const fn accepts_operator(self, operator: ComparisonOperator) -> bool {
        !matches!(self, Self::Bool)
            || matches!(operator, ComparisonOperator::Eq | ComparisonOperator::NotEq)
    }

    pub(super) fn for_comparison(
        column: &ColumnRef,
        value: &RuntimeValueSpec,
        opno: pg_sys::Oid,
        opcollid: pg_sys::Oid,
        inputcollid: pg_sys::Oid,
        utf8: Option<Utf8ServerEncoding>,
    ) -> Option<(Self, ComparisonOperator)> {
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
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
                PgOid::BuiltIn(PgBuiltInOids::NUMERICOID),
            ) => Self::Decimal128(Decimal128Semantics::for_storage_comparison(
                column.declared_type,
                column.value_type,
                value.value_type,
            )?),
            (
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
                PgOid::BuiltIn(PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID),
            ) => Self::String,
            _ => return None,
        };

        let value_type = match value_type {
            Self::Bool | Self::I32 | Self::I64 | Self::Decimal128(_)
                if opcollid == pg_sys::Oid::INVALID
                    && inputcollid == pg_sys::Oid::INVALID =>
            {
                Some(value_type)
            }
            Self::String => {
                utf8?;
                let kind = operator.kind();
                unsafe {
                    PgTextComparisonSemantics::for_comparison(
                        PgComparisonIdentity {
                            opno,
                            opcollid,
                            inputcollid,
                        },
                        kind,
                    )
                }
                .map(|_| value_type)
            }
            _ => None,
        }?;
        Some((value_type, operator))
    }

    pub(super) fn encode(self, writer: &mut PlanDataWriter) {
        match self {
            Self::Bool => writer.append_i32(VALUE_BOOL),
            Self::I32 => writer.append_i32(VALUE_I32),
            Self::I64 => writer.append_i32(VALUE_I64),
            Self::String => writer.append_i32(VALUE_STRING),
            Self::Decimal128(semantics) => writer
                .append_i32(VALUE_DECIMAL128)
                .append_i32(i32::from(semantics.precision()))
                .append_i32(i32::from(semantics.scale())),
        };
    }

    pub(super) fn decode_plan(
        reader: &mut PlanDataReader<'_>,
    ) -> Result<Self, ConnectorError> {
        match reader.read_i32()? {
            VALUE_BOOL => Ok(Self::Bool),
            VALUE_I32 => Ok(Self::I32),
            VALUE_I64 => Ok(Self::I64),
            VALUE_STRING => Ok(Self::String),
            VALUE_DECIMAL128 => {
                let encoded_precision = reader.read_i32()?;
                let encoded_scale = reader.read_i32()?;
                let semantics = u8::try_from(encoded_precision)
                    .ok()
                    .and_then(|precision| {
                        let scale = i8::try_from(encoded_scale).ok()?;
                        Decimal128Semantics::new(precision, scale)
                    })
                    .ok_or_else(|| {
                        ConnectorError::invalid_filter_plan(FormatKind::Parquet)
                    })?;
                Ok(Self::Decimal128(semantics))
            }
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
        value: RuntimeValue,
    ) -> Result<DecodedValue, ConnectorError> {
        let datum = unsafe { value.datum() };
        let type_oid = value.metadata().value_type.type_oid;
        let decoded = match (self, type_oid) {
            (Self::Bool, pg_sys::BOOLOID) => DecodedValue::Scalar(BoundValue::Bool(
                unsafe { bool::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            )),
            (Self::I32, pg_sys::INT2OID) => DecodedValue::Scalar(BoundValue::I32(
                unsafe { i16::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?
                    as i32,
            )),
            (Self::I32, pg_sys::INT4OID) => DecodedValue::Scalar(BoundValue::I32(
                unsafe { i32::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            )),
            (Self::I64, pg_sys::INT8OID) => DecodedValue::Scalar(BoundValue::I64(
                unsafe { i64::from_datum(datum, false) }
                    .ok_or_else(|| ConnectorError::invalid_filter_datum(type_oid))?,
            )),
            (Self::String, pg_sys::TEXTOID | pg_sys::VARCHAROID) => {
                DecodedValue::Scalar(BoundValue::String(
                    // ValueType::String can only be persisted when the planner
                    // holds Utf8ServerEncoding, so this bind does not repeat
                    // the statement-invariant server-encoding check.
                    unsafe { String::from_datum(datum, false) }
                        .ok_or_else(|| {
                            ConnectorError::invalid_filter_datum(type_oid)
                        })?
                        .into_boxed_str(),
                ))
            }
            (Self::Decimal128(semantics), pg_sys::NUMERICOID) => {
                match unsafe { semantics.codec().encode_comparison_datum(datum) }? {
                    Decimal128ComparisonValue::Finite(coefficient) => {
                        DecodedValue::Scalar(BoundValue::Decimal128 {
                            coefficient,
                            semantics,
                        })
                    }
                    Decimal128ComparisonValue::NegativeInfinity => {
                        DecodedValue::OutsideFinite(
                            Decimal128ComparisonValue::NegativeInfinity,
                        )
                    }
                    special @ (Decimal128ComparisonValue::PositiveInfinity
                    | Decimal128ComparisonValue::NaN) => {
                        DecodedValue::OutsideFinite(special)
                    }
                }
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
    Decimal128 {
        coefficient: i128,
        semantics: Decimal128Semantics,
    },
}

pub(super) enum DecodedValue {
    Scalar(BoundValue),
    OutsideFinite(Decimal128ComparisonValue),
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
            (
                Self::Decimal128 {
                    coefficient,
                    semantics,
                },
                DataType::Decimal128(precision, scale),
            ) if *precision == semantics.precision()
                && *scale == semantics.scale() =>
            {
                Arc::new(
                    Decimal128Array::from(vec![*coefficient])
                        .with_precision_and_scale(*precision, *scale)?,
                )
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
