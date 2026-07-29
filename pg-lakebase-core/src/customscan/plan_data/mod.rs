//! Plan-tree carriers shared by planning and execution.
//!
//! This boundary owns the `custom_private`, `custom_exprs`, and scan-tuple
//! contracts. It must contain only data that PostgreSQL can copy or rewrite as
//! part of a plan tree; provider runtime state belongs in `execution`.

use pgrx::pg_sys;

pub mod custom_exprs;
pub mod custom_private;
mod purpose;
pub(crate) mod tuple_layout;

pub use purpose::ScanPurpose;

/// Errors for the framework-owned `custom_private` envelope and tuple layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EnvelopeError {
    #[error("custom_private payload is NULL")]
    NullPayload,
    #[error(
        "custom_private top-level list has wrong length: found {found}, expected {expected}"
    )]
    WrongTopLevelLength { found: usize, expected: usize },
    #[error("custom_private cell {field} is NULL but a Node* was expected")]
    NullCell { field: i32 },
    #[error(
        "custom_private cell {field} has wrong NodeTag: found {found:?}, expected {expected:?}"
    )]
    WrongNodeTag {
        field: i32,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
    #[error("custom_private provider_id_or_name has NULL sval")]
    NullProviderName,
    #[error(
        "custom_private column_refs[{entry}] has wrong length: found {found}, expected {expected}"
    )]
    MalformedColumnRef {
        entry: usize,
        found: usize,
        expected: usize,
    },
    #[error(
        "custom_private pushed_contracts[{entry}] holds unknown encoding {value}"
    )]
    UnknownContract { entry: usize, value: i32 },
    #[error("custom_private cell {field} encodes negative count {value}")]
    NegativeCount { field: i32, value: i32 },
    #[error(
        "custom_private cross-field invariant violated: pushed_contracts.len() = {pushed_contracts_len}, expected to equal pushed_count = {pushed_count}"
    )]
    PushedContractsLengthMismatch {
        pushed_count: usize,
        pushed_contracts_len: usize,
    },
    #[error(
        "custom_private column_refs[{entry}].expr_index = {expr_index} is out of range for pushed_count = {pushed_count}"
    )]
    ColumnRefExprIndexOutOfRange {
        entry: usize,
        expr_index: usize,
        pushed_count: usize,
    },
    #[error("custom_private cannot encode count {value}: exceeds i32::MAX")]
    CountTooLargeToEncode { value: usize },
    #[error("custom_private tuple layout is malformed: {reason}")]
    MalformedTupleLayout { reason: &'static str },
    #[error("custom_private tuple layout has unknown kind tag {value}")]
    UnknownTupleLayoutKind { value: i32 },
    #[error("custom_private has unknown scan purpose tag {value}")]
    UnknownScanPurpose { value: i32 },
    #[error("custom_private tuple layout attnos[{index}] has invalid value {value}")]
    InvalidTupleLayoutAttno { index: usize, value: i32 },
    #[error("custom_private tuple layout contains duplicate base attno {attno}")]
    DuplicateTupleLayoutAttno { attno: pg_sys::AttrNumber },
    #[error("custom path private data is malformed: {reason}")]
    MalformedPathPrivate { reason: &'static str },
}
