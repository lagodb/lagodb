//! Lowering of bound predicates to one file's physical representation.
//!
//! The declared contract admits several physical encodings per PostgreSQL
//! type (`Utf8`/`LargeUtf8`, `Binary`/`LargeBinary`/`FixedSizeBinary`,
//! microsecond/nanosecond timestamps with any zone label). Each file lowers
//! the same logical predicate once, at open, so row filtering and row-group
//! pruning compare in the file's own representation.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, Scalar, StringArray,
    TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::error::ConnectorError;
use crate::format::FormatKind;

use super::bound::{BoundNode, ParquetNode};
use super::integer::IntegerOutOfRange;
use super::operator::ComparisonOperator;
use super::value::BoundValue;

/// A comparison value in one file's physical Arrow representation.
#[derive(Clone, Debug)]
pub(super) enum FileValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal128 {
        coefficient: i128,
        precision: u8,
        scale: i8,
    },
    Utf8(Box<str>),
    LargeUtf8(Box<str>),
    Binary(Box<[u8]>),
    LargeBinary(Box<[u8]>),
    FixedSizeBinary(Box<[u8]>),
    Date32(i32),
    Timestamp {
        value: i64,
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
    },
}

pub(super) type FileNode = ParquetNode<FileValue>;

/// Name-resolved view of one file's Arrow schema.
#[derive(Clone, Copy)]
pub(super) struct FileSchema<'a>(&'a Schema);

impl<'a> FileSchema<'a> {
    pub(super) const fn new(schema: &'a Schema) -> Self {
        Self(schema)
    }

    pub(super) fn root(self, column: &str) -> Result<usize, ConnectorError> {
        self.0.index_of(column).map_err(|_| Self::missing(column))
    }

    fn data_type(self, column: &str) -> Result<&'a DataType, ConnectorError> {
        self.0
            .field_with_name(column)
            .map(|field| field.data_type())
            .map_err(|_| Self::missing(column))
    }

    fn expect_float(self, column: &str) -> Result<(), ConnectorError> {
        match self.data_type(column)? {
            DataType::Float32 | DataType::Float64 => Ok(()),
            _ => Err(Self::incompatible(column)),
        }
    }

    fn missing(column: &str) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Parquet,
            format!("filter column {column:?} is missing from the Parquet schema"),
        )
    }

    fn incompatible(column: &str) -> ConnectorError {
        ConnectorError::invalid_object_schema(
            FormatKind::Parquet,
            format!(
                "filter column {column:?} has a Parquet type incompatible with its predicate"
            ),
        )
    }
}

impl BoundNode {
    pub(super) fn lower(
        &self,
        schema: FileSchema<'_>,
    ) -> Result<FileNode, ConnectorError> {
        Ok(match self {
            Self::AlwaysTrue => FileNode::AlwaysTrue,
            Self::NeverTrue => FileNode::NeverTrue,
            Self::Comparison {
                operator,
                column,
                value,
            } => value.lower_comparison(
                *operator,
                column,
                schema.data_type(column)?,
            )?,
            Self::IsNull(column) => {
                schema.data_type(column)?;
                FileNode::IsNull(column.clone())
            }
            Self::IsNotNull(column) => {
                schema.data_type(column)?;
                FileNode::IsNotNull(column.clone())
            }
            Self::IsNan(column) => {
                schema.expect_float(column)?;
                FileNode::IsNan(column.clone())
            }
            Self::IsNotNan(column) => {
                schema.expect_float(column)?;
                FileNode::IsNotNan(column.clone())
            }
            Self::BooleanTest { kind, column } => {
                if schema.data_type(column)? != &DataType::Boolean {
                    return Err(FileSchema::incompatible(column));
                }
                FileNode::BooleanTest {
                    kind: *kind,
                    column: column.clone(),
                }
            }
            Self::StartsWith { column, prefix } => {
                let prefix = match (prefix, schema.data_type(column)?) {
                    (BoundValue::String(prefix), DataType::Utf8) => {
                        FileValue::Utf8(prefix.clone())
                    }
                    (BoundValue::String(prefix), DataType::LargeUtf8) => {
                        FileValue::LargeUtf8(prefix.clone())
                    }
                    _ => return Err(FileSchema::incompatible(column)),
                };
                FileNode::StartsWith {
                    column: column.clone(),
                    prefix,
                }
            }
            Self::InSet { column, values } => {
                let data_type = schema.data_type(column)?;
                let mut lowered = Vec::with_capacity(values.len());
                for value in values {
                    if let Some(value) = value.lower_equality(column, data_type)? {
                        lowered.push(value);
                    }
                }
                FileNode::InSet {
                    column: column.clone(),
                    values: lowered.into_boxed_slice(),
                }
            }
            Self::And(children) => {
                FileNode::And(Self::lower_children(children, schema)?)
            }
            Self::Or(children) => {
                FileNode::Or(Self::lower_children(children, schema)?)
            }
        })
    }

    fn lower_children(
        children: &[Self],
        schema: FileSchema<'_>,
    ) -> Result<Box<[FileNode]>, ConnectorError> {
        children.iter().map(|child| child.lower(schema)).collect()
    }
}

impl BoundValue {
    fn lower_comparison(
        &self,
        operator: ComparisonOperator,
        column: &str,
        data_type: &DataType,
    ) -> Result<FileNode, ConnectorError> {
        let comparison = |operator, value| FileNode::Comparison {
            operator,
            column: column.into(),
            value,
        };
        match (self, data_type) {
            (Self::Binary(value), DataType::FixedSizeBinary(width)) => {
                let width = usize::try_from(*width)
                    .map_err(|_| FileSchema::incompatible(column))?;
                Ok(FixedWidth::new(value, width).lower(operator, column))
            }
            (
                Self::Timestamp(micros),
                DataType::Timestamp(TimeUnit::Nanosecond, tz),
            ) => {
                Ok(NanosecondRange::new(*micros, tz.clone()).lower(operator, column))
            }
            _ => Ok(comparison(operator, self.lower_exact(column, data_type)?)),
        }
    }

    /// The file value equal to this value, or `None` when no value of the
    /// file's fixed width can equal it.
    fn lower_equality(
        &self,
        column: &str,
        data_type: &DataType,
    ) -> Result<Option<FileValue>, ConnectorError> {
        match (self, data_type) {
            (Self::Binary(value), DataType::FixedSizeBinary(width)) => {
                Ok(usize::try_from(*width)
                    .is_ok_and(|width| width == value.len())
                    .then(|| FileValue::FixedSizeBinary(value.clone())))
            }
            _ => self.lower_exact(column, data_type).map(Some),
        }
    }

    fn lower_exact(
        &self,
        column: &str,
        data_type: &DataType,
    ) -> Result<FileValue, ConnectorError> {
        Ok(match (self, data_type) {
            (Self::Bool(value), DataType::Boolean) => FileValue::Bool(*value),
            (Self::I16(value), DataType::Int16) => FileValue::I16(*value),
            (Self::I32(value), DataType::Int32) => FileValue::I32(*value),
            (Self::I64(value), DataType::Int64) => FileValue::I64(*value),
            (Self::F32(value), DataType::Float32) => FileValue::F32(*value),
            (Self::F64(value), DataType::Float64) => FileValue::F64(*value),
            (
                Self::Decimal128 {
                    coefficient,
                    semantics,
                },
                DataType::Decimal128(precision, scale),
            ) if *precision == semantics.precision()
                && *scale == semantics.scale() =>
            {
                FileValue::Decimal128 {
                    coefficient: *coefficient,
                    precision: *precision,
                    scale: *scale,
                }
            }
            (Self::String(value), DataType::Utf8) => FileValue::Utf8(value.clone()),
            (Self::String(value), DataType::LargeUtf8) => {
                FileValue::LargeUtf8(value.clone())
            }
            (Self::Binary(value), DataType::Binary) => {
                FileValue::Binary(value.clone())
            }
            (Self::Binary(value), DataType::LargeBinary) => {
                FileValue::LargeBinary(value.clone())
            }
            (Self::Date32(value), DataType::Date32) => FileValue::Date32(*value),
            (
                Self::Timestamp(value),
                DataType::Timestamp(TimeUnit::Microsecond, tz),
            ) => FileValue::Timestamp {
                value: *value,
                unit: TimeUnit::Microsecond,
                timezone: tz.clone(),
            },
            _ => return Err(FileSchema::incompatible(column)),
        })
    }
}

/// A byte string compared with a fixed-width column. PostgreSQL orders
/// `bytea` (and `uuid`) bytewise, a proper prefix first.
struct FixedWidth<'a> {
    value: &'a [u8],
    width: usize,
}

impl<'a> FixedWidth<'a> {
    const fn new(value: &'a [u8], width: usize) -> Self {
        Self { value, width }
    }

    fn lower(self, operator: ComparisonOperator, column: &str) -> FileNode {
        use ComparisonOperator as Op;
        let comparison = |operator, value: Vec<u8>| FileNode::Comparison {
            operator,
            column: column.into(),
            value: FileValue::FixedSizeBinary(value.into_boxed_slice()),
        };
        if self.value.len() == self.width {
            return comparison(operator, self.value.to_vec());
        }
        match operator {
            Op::Eq => return FileNode::NeverTrue,
            Op::NotEq => return FileNode::IsNotNull(column.into()),
            _ => {}
        }
        if self.value.len() > self.width {
            // A stored value equal to the literal's first `width` bytes is a
            // proper prefix of it, hence smaller.
            let truncated = self.value[..self.width].to_vec();
            let operator = match operator {
                Op::Lt | Op::Le => Op::Le,
                _ => Op::Gt,
            };
            comparison(operator, truncated)
        } else {
            // A stored value sharing the literal's bytes is longer, hence
            // greater; zero padding is the smallest such extension.
            let mut padded = self.value.to_vec();
            padded.resize(self.width, 0);
            let operator = match operator {
                Op::Lt | Op::Le => Op::Lt,
                _ => Op::Ge,
            };
            comparison(operator, padded)
        }
    }
}

/// A microsecond value against a nanosecond column. The value `u` denotes
/// the half-open nanosecond interval `[u*1000, (u+1)*1000)`, including for
/// negative epochs, matching the read path's floor to microseconds.
struct NanosecondRange {
    lower: Result<i64, IntegerOutOfRange>,
    upper: Result<i64, IntegerOutOfRange>,
    timezone: Option<Arc<str>>,
}

impl NanosecondRange {
    fn new(micros: i64, timezone: Option<Arc<str>>) -> Self {
        Self {
            lower: Self::nanos(micros),
            // Only the successor of `i64::MAX` overflows, upward.
            upper: micros
                .checked_add(1)
                .map_or(Err(IntegerOutOfRange::Above), Self::nanos),
            timezone,
        }
    }

    /// The first nanosecond of a microsecond, or the side of the nanosecond
    /// domain it lies beyond.
    fn nanos(micros: i64) -> Result<i64, IntegerOutOfRange> {
        micros.checked_mul(1_000).ok_or(if micros < 0 {
            IntegerOutOfRange::Below
        } else {
            IntegerOutOfRange::Above
        })
    }

    fn lower(self, operator: ComparisonOperator, column: &str) -> FileNode {
        use ComparisonOperator as Op;
        match operator {
            Op::Lt => self.bound(Op::Lt, self.lower, column),
            Op::Ge => self.bound(Op::Ge, self.lower, column),
            Op::Le => self.bound(Op::Lt, self.upper, column),
            Op::Gt => self.bound(Op::Ge, self.upper, column),
            Op::Eq => FileNode::And(
                vec![
                    self.bound(Op::Ge, self.lower, column),
                    self.bound(Op::Lt, self.upper, column),
                ]
                .into_boxed_slice(),
            ),
            Op::NotEq => FileNode::Or(
                vec![
                    self.bound(Op::Lt, self.lower, column),
                    self.bound(Op::Ge, self.upper, column),
                ]
                .into_boxed_slice(),
            ),
        }
    }

    /// `column operator boundary`, where a boundary outside the nanosecond
    /// domain decides the comparison for every non-NULL row.
    fn bound(
        &self,
        operator: ComparisonOperator,
        boundary: Result<i64, IntegerOutOfRange>,
        column: &str,
    ) -> FileNode {
        match boundary {
            Ok(value) => FileNode::Comparison {
                operator,
                column: column.into(),
                value: FileValue::Timestamp {
                    value,
                    unit: TimeUnit::Nanosecond,
                    timezone: self.timezone.clone(),
                },
            },
            Err(outside) if outside.comparison_matches_non_null(operator) => {
                FileNode::IsNotNull(column.into())
            }
            Err(_) => FileNode::NeverTrue,
        }
    }
}

impl FileValue {
    /// A one-element Arrow scalar of exactly the file column's type.
    pub(super) fn scalar(&self) -> Result<Scalar<ArrayRef>, ConnectorError> {
        let array: ArrayRef = match self {
            Self::Bool(value) => Arc::new(BooleanArray::from(vec![*value])),
            Self::I16(value) => Arc::new(Int16Array::from(vec![*value])),
            Self::I32(value) => Arc::new(Int32Array::from(vec![*value])),
            Self::I64(value) => Arc::new(Int64Array::from(vec![*value])),
            Self::F32(value) => Arc::new(Float32Array::from(vec![*value])),
            Self::F64(value) => Arc::new(Float64Array::from(vec![*value])),
            Self::Decimal128 {
                coefficient,
                precision,
                scale,
            } => Arc::new(
                Decimal128Array::from(vec![*coefficient])
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            Self::Utf8(value) => Arc::new(StringArray::from(vec![value.as_ref()])),
            Self::LargeUtf8(value) => {
                Arc::new(LargeStringArray::from(vec![value.as_ref()]))
            }
            Self::Binary(value) => Arc::new(BinaryArray::from(vec![value.as_ref()])),
            Self::LargeBinary(value) => {
                Arc::new(LargeBinaryArray::from(vec![value.as_ref()]))
            }
            Self::FixedSizeBinary(value) => Arc::new(
                FixedSizeBinaryArray::try_from_iter(std::iter::once(value.as_ref()))?,
            ),
            Self::Date32(value) => Arc::new(Date32Array::from(vec![*value])),
            Self::Timestamp {
                value,
                unit: TimeUnit::Nanosecond,
                timezone,
            } => Arc::new(
                TimestampNanosecondArray::from(vec![*value])
                    .with_timezone_opt(timezone.clone()),
            ),
            Self::Timestamp {
                value, timezone, ..
            } => Arc::new(
                TimestampMicrosecondArray::from(vec![*value])
                    .with_timezone_opt(timezone.clone()),
            ),
        };
        Ok(Scalar::new(array))
    }
}
