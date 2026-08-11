//! Plan-tree carriers shared by planning and execution.
//!
//! This boundary owns the `custom_private`, `custom_exprs`, and scan-tuple
//! contracts. It must contain only data that PostgreSQL can copy or rewrite as
//! part of a plan tree; provider runtime state belongs in `execution`.

use pgrx::pg_sys;

pub mod custom_exprs;
pub mod custom_private;
pub(crate) mod path_private;
mod purpose;
pub(crate) mod tuple_layout;

pub use purpose::ScanPurpose;

/// Errors for the framework-owned `custom_private` envelope and tuple layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum EnvelopeError {
    #[error(
        "custom_private cell {field} has wrong NodeTag: found {found:?}, expected {expected:?}"
    )]
    WrongNodeTag {
        field: i32,
        expected: pg_sys::NodeTag,
        found: pg_sys::NodeTag,
    },
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
}
