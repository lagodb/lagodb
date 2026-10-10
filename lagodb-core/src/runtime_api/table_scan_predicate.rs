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

mod codec;

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
    Binary(Cow<'a, [u8]>),
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
