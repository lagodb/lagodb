//! Provider-neutral concrete predicate specialization for table scans.
//!
//! PostgreSQL planning and DataFusion execution share the generic
//! [`PredicateExpr`] structure. This specialization owns source positions and
//! typed values produced by the DataFusion adapter. Decoding produces an owned
//! representation for provider adapters.
//! Only compact bytes cross the DSO boundary, so Rust enum layout and
//! DataFusion/Iceberg types are not part of the runtime ABI.
//!
//! Set membership is intentionally absent from this scan-pushdown contract.
//! The current Iceberg reader expands `IN` into one full-batch equality and OR
//! per literal. PostgreSQL can select hashed ScalarArrayOp execution, and
//! DataFusion builds a hash-based static filter.
//! Keeping `IN` in the engine also avoids crediting temporal-array pruning that
//! an Iceberg binder can later reject for unrepresentable values.

use std::borrow::Cow;
use std::mem::size_of;
use std::ptr;

use crate::expr::pushdown::PredicateExpr;

const CODEC_VERSION: u8 = 5;

/// Provider decision for one concrete, provider-neutral predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PredicateSupport {
    /// The provider applies a safe superset; the complete engine residual is
    /// still required.
    Conservative,
    /// Every provider reader applies the accepted predicate exactly.
    Exact,
}

impl PredicateSupport {
    const CONSERVATIVE_CODE: u32 = 1;
    const EXACT_CODE: u32 = 2;

    pub const fn code(self) -> u32 {
        match self {
            Self::Conservative => Self::CONSERVATIVE_CODE,
            Self::Exact => Self::EXACT_CODE,
        }
    }

    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            Self::CONSERVATIVE_CODE => Some(Self::Conservative),
            Self::EXACT_CODE => Some(Self::Exact),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Target representation of a lossless integer widening already proved by
/// the query planner. Providers must still verify the source column's physical
/// type before rewriting the predicate to that column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeIntegerWidening {
    /// A source integer column widened to Arrow `Int32`.
    ToInt32,
    /// A source integer column widened to Arrow `Int64`.
    ToInt64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimePredicateValue<'a> {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Date32(i32),
    TimestampMicrosecond(i64),
    TimestampTzMicrosecond(i64),
    Decimal128 {
        coefficient: i128,
        precision: u8,
        scale: i8,
    },
    String(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimePredicateScalar<'a> {
    Column(usize),
    WidenedIntegerColumn {
        column: usize,
        target: RuntimeIntegerWidening,
    },
    Value(RuntimePredicateValue<'a>),
}

pub type RuntimePruningPredicate<'a> =
    PredicateExpr<RuntimePredicateScalar<'a>, RuntimeComparisonOperator>;

#[derive(Debug, thiserror::Error)]
pub enum RuntimePredicateCodecError {
    #[error("runtime pruning predicate has an incompatible ABI layout")]
    IncompatibleLayout,
    #[error("runtime pruning predicate has an unsupported codec version")]
    UnsupportedVersion,
    #[error("runtime pruning predicate payload is truncated")]
    Truncated,
    #[error("runtime pruning predicate payload has a null data pointer")]
    NullData,
    #[error("runtime pruning predicate update contains an unknown action")]
    UnknownUpdateAction,
    #[error("runtime pruning predicate clear update contains an unexpected payload")]
    InvalidClearPayload,
    #[error("runtime pruning predicate operation requires a replacement update")]
    ExpectedReplacement,
    #[error("runtime pruning predicate payload contains an unknown tag")]
    UnknownTag,
    #[error(
        "runtime pruning predicate payload contains a value outside this platform"
    )]
    ValueOutOfRange,
    #[error("runtime pruning predicate payload contains trailing bytes")]
    TrailingBytes,
    #[error("runtime pruning predicate text is not valid UTF-8")]
    InvalidUtf8,
}

impl RuntimePruningPredicate<'_> {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output);
        output
    }

    /// Replace an existing buffer with this encoding while retaining its
    /// allocation for the next evolving-filter generation.
    pub fn encode_into(&self, output: &mut Vec<u8>) {
        output.clear();
        output.push(CODEC_VERSION);
        self.encode_node(output);
    }

    pub fn decode(
        input: &[u8],
    ) -> Result<RuntimePruningPredicate<'static>, RuntimePredicateCodecError> {
        let mut decoder = PredicateDecoder { input, position: 0 };
        if decoder.read_u8()? != CODEC_VERSION {
            return Err(RuntimePredicateCodecError::UnsupportedVersion);
        }
        let predicate = decoder.read_node()?;
        if decoder.position != input.len() {
            return Err(RuntimePredicateCodecError::TrailingBytes);
        }
        Ok(predicate)
    }

    fn encode_node(&self, output: &mut Vec<u8>) {
        match self {
            Self::AlwaysTrue => output.push(1),
            Self::AlwaysFalse => output.push(2),
            Self::StrictTrue(value) => {
                output.push(14);
                value.encode(output);
            }
            Self::StrictFalse(value) => {
                output.push(13);
                value.encode(output);
            }
            Self::Comparison {
                operator,
                left,
                right,
            } => {
                output.push(3);
                output.push(match operator {
                    RuntimeComparisonOperator::Equal => 1,
                    RuntimeComparisonOperator::NotEqual => 2,
                    RuntimeComparisonOperator::LessThan => 3,
                    RuntimeComparisonOperator::LessThanOrEqual => 4,
                    RuntimeComparisonOperator::GreaterThan => 5,
                    RuntimeComparisonOperator::GreaterThanOrEqual => 6,
                });
                left.encode(output);
                right.encode(output);
            }
            Self::IsNull(value) => {
                output.push(4);
                value.encode(output);
            }
            Self::IsNotNull(value) => {
                output.push(5);
                value.encode(output);
            }
            Self::IsNan(value) => {
                output.push(10);
                value.encode(output);
            }
            Self::IsNotNan(value) => {
                output.push(11);
                value.encode(output);
            }
            Self::StartsWith { value, prefix } => {
                output.push(12);
                value.encode(output);
                prefix.encode(output);
            }
            Self::And(children) | Self::Or(children) => {
                output.push(if matches!(self, Self::And(_)) { 7 } else { 8 });
                encode_usize(children.len(), output);
                for child in children {
                    child.encode_node(output);
                }
            }
            Self::Not(child) => {
                output.push(9);
                child.encode_node(output);
            }
        }
    }
}

impl RuntimePredicateScalar<'_> {
    fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Column(column) => {
                output.push(1);
                encode_usize(*column, output);
            }
            Self::WidenedIntegerColumn { column, target } => {
                output.push(3);
                encode_usize(*column, output);
                output.push(match target {
                    RuntimeIntegerWidening::ToInt32 => 1,
                    RuntimeIntegerWidening::ToInt64 => 2,
                });
            }
            Self::Value(value) => {
                output.push(2);
                value.encode(output);
            }
        }
    }
}

impl RuntimePredicateValue<'_> {
    fn encode(&self, output: &mut Vec<u8>) {
        match self {
            Self::Boolean(value) => {
                output.push(1);
                output.push(u8::from(*value));
            }
            Self::Int32(value) => {
                output.push(2);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::Int64(value) => {
                output.push(3);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::Date32(value) => {
                output.push(4);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::TimestampMicrosecond(value) => {
                output.push(5);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::TimestampTzMicrosecond(value) => {
                output.push(6);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Self::Decimal128 {
                coefficient,
                precision,
                scale,
            } => {
                output.push(8);
                output.extend_from_slice(&coefficient.to_le_bytes());
                output.push(*precision);
                output.push(*scale as u8);
            }
            Self::String(value) => {
                output.push(7);
                encode_usize(value.len(), output);
                output.extend_from_slice(value.as_bytes());
            }
        }
    }
}

fn encode_usize(value: usize, output: &mut Vec<u8>) {
    output.extend_from_slice(&(value as u64).to_le_bytes());
}

struct PredicateDecoder<'a> {
    input: &'a [u8],
    position: usize,
}

impl PredicateDecoder<'_> {
    fn read_node(
        &mut self,
    ) -> Result<RuntimePruningPredicate<'static>, RuntimePredicateCodecError> {
        match self.read_u8()? {
            1 => Ok(RuntimePruningPredicate::AlwaysTrue),
            2 => Ok(RuntimePruningPredicate::AlwaysFalse),
            3 => {
                let operator = match self.read_u8()? {
                    1 => RuntimeComparisonOperator::Equal,
                    2 => RuntimeComparisonOperator::NotEqual,
                    3 => RuntimeComparisonOperator::LessThan,
                    4 => RuntimeComparisonOperator::LessThanOrEqual,
                    5 => RuntimeComparisonOperator::GreaterThan,
                    6 => RuntimeComparisonOperator::GreaterThanOrEqual,
                    _ => return Err(RuntimePredicateCodecError::UnknownTag),
                };
                Ok(RuntimePruningPredicate::Comparison {
                    operator,
                    left: self.read_scalar()?,
                    right: self.read_scalar()?,
                })
            }
            4 => Ok(RuntimePruningPredicate::IsNull(self.read_scalar()?)),
            5 => Ok(RuntimePruningPredicate::IsNotNull(self.read_scalar()?)),
            tag @ (7 | 8) => {
                let count = self.read_sequence_len()?;
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(self.read_node()?);
                }
                let children = children.into_boxed_slice();
                Ok(if tag == 7 {
                    RuntimePruningPredicate::And(children)
                } else {
                    RuntimePruningPredicate::Or(children)
                })
            }
            9 => Ok(RuntimePruningPredicate::Not(Box::new(self.read_node()?))),
            10 => Ok(RuntimePruningPredicate::IsNan(self.read_scalar()?)),
            11 => Ok(RuntimePruningPredicate::IsNotNan(self.read_scalar()?)),
            12 => Ok(RuntimePruningPredicate::StartsWith {
                value: self.read_scalar()?,
                prefix: self.read_scalar()?,
            }),
            13 => Ok(RuntimePruningPredicate::StrictFalse(self.read_scalar()?)),
            14 => Ok(RuntimePruningPredicate::StrictTrue(self.read_scalar()?)),
            _ => Err(RuntimePredicateCodecError::UnknownTag),
        }
    }

    fn read_scalar(
        &mut self,
    ) -> Result<RuntimePredicateScalar<'static>, RuntimePredicateCodecError> {
        match self.read_u8()? {
            1 => Ok(RuntimePredicateScalar::Column(self.read_usize()?)),
            2 => Ok(RuntimePredicateScalar::Value(self.read_value()?)),
            3 => {
                let column = self.read_usize()?;
                let target = match self.read_u8()? {
                    1 => RuntimeIntegerWidening::ToInt32,
                    2 => RuntimeIntegerWidening::ToInt64,
                    _ => return Err(RuntimePredicateCodecError::UnknownTag),
                };
                Ok(RuntimePredicateScalar::WidenedIntegerColumn { column, target })
            }
            _ => Err(RuntimePredicateCodecError::UnknownTag),
        }
    }

    fn read_value(
        &mut self,
    ) -> Result<RuntimePredicateValue<'static>, RuntimePredicateCodecError> {
        match self.read_u8()? {
            1 => match self.read_u8()? {
                0 => Ok(RuntimePredicateValue::Boolean(false)),
                1 => Ok(RuntimePredicateValue::Boolean(true)),
                _ => Err(RuntimePredicateCodecError::UnknownTag),
            },
            2 => Ok(RuntimePredicateValue::Int32(i32::from_le_bytes(
                self.read_array()?,
            ))),
            3 => Ok(RuntimePredicateValue::Int64(i64::from_le_bytes(
                self.read_array()?,
            ))),
            4 => Ok(RuntimePredicateValue::Date32(i32::from_le_bytes(
                self.read_array()?,
            ))),
            5 => Ok(RuntimePredicateValue::TimestampMicrosecond(
                i64::from_le_bytes(self.read_array()?),
            )),
            6 => Ok(RuntimePredicateValue::TimestampTzMicrosecond(
                i64::from_le_bytes(self.read_array()?),
            )),
            7 => {
                let length = self.read_usize()?;
                let value = std::str::from_utf8(self.read(length)?)
                    .map_err(|_| RuntimePredicateCodecError::InvalidUtf8)?;
                Ok(RuntimePredicateValue::String(Cow::Owned(value.into())))
            }
            8 => {
                let coefficient = i128::from_le_bytes(self.read_array()?);
                let precision = self.read_u8()?;
                let scale = self.read_u8()? as i8;
                if !(1..=38).contains(&precision)
                    || scale < 0
                    || scale as u8 > precision
                    || coefficient <= -10_i128.pow(u32::from(precision))
                    || coefficient >= 10_i128.pow(u32::from(precision))
                {
                    return Err(RuntimePredicateCodecError::ValueOutOfRange);
                }
                Ok(RuntimePredicateValue::Decimal128 {
                    coefficient,
                    precision,
                    scale,
                })
            }
            _ => Err(RuntimePredicateCodecError::UnknownTag),
        }
    }

    fn read_u8(&mut self) -> Result<u8, RuntimePredicateCodecError> {
        Ok(self.read(1)?[0])
    }

    fn read_usize(&mut self) -> Result<usize, RuntimePredicateCodecError> {
        usize::try_from(u64::from_le_bytes(self.read_array()?))
            .map_err(|_| RuntimePredicateCodecError::ValueOutOfRange)
    }

    fn read_sequence_len(&mut self) -> Result<usize, RuntimePredicateCodecError> {
        let length = self.read_usize()?;
        if length > self.input.len() - self.position {
            return Err(RuntimePredicateCodecError::Truncated);
        }
        Ok(length)
    }

    fn read_array<const N: usize>(
        &mut self,
    ) -> Result<[u8; N], RuntimePredicateCodecError> {
        self.read(N)?
            .try_into()
            .map_err(|_| RuntimePredicateCodecError::Truncated)
    }

    fn read(&mut self, length: usize) -> Result<&[u8], RuntimePredicateCodecError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(RuntimePredicateCodecError::Truncated)?;
        let value = self
            .input
            .get(self.position..end)
            .ok_or(RuntimePredicateCodecError::Truncated)?;
        self.position = end;
        Ok(value)
    }
}

/// Action published by an evolving runtime-predicate slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimePredicateUpdateAction {
    /// Replace the previously consumed supplemental predicate.
    Replace,
    /// Remove the previously consumed supplemental predicate.
    Clear,
}

impl RuntimePredicateUpdateAction {
    const REPLACE_CODE: u32 = 1;
    const CLEAR_CODE: u32 = 2;

    pub const fn code(self) -> u32 {
        match self {
            Self::Replace => Self::REPLACE_CODE,
            Self::Clear => Self::CLEAR_CODE,
        }
    }

    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            Self::REPLACE_CODE => Some(Self::Replace),
            Self::CLEAR_CODE => Some(Self::Clear),
            _ => None,
        }
    }
}

/// Borrowed runtime-predicate update state. The owner updates this only
/// between serialized Arrow stream callbacks.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableScanRuntimePredicate {
    pub struct_size: u32,
    pub action: u32,
    pub generation: u64,
    pub data: *const u8,
    pub data_len: usize,
}

impl TableScanRuntimePredicate {
    #[must_use]
    pub const fn clear(generation: u64) -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            action: RuntimePredicateUpdateAction::Clear.code(),
            generation,
            data: ptr::null(),
            data_len: 0,
        }
    }

    #[must_use]
    pub fn replacement(generation: u64, encoded: &[u8]) -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            action: RuntimePredicateUpdateAction::Replace.code(),
            generation,
            data: encoded.as_ptr(),
            data_len: encoded.len(),
        }
    }

    #[must_use]
    pub const fn action(self) -> Option<RuntimePredicateUpdateAction> {
        RuntimePredicateUpdateAction::from_code(self.action)
    }
}
