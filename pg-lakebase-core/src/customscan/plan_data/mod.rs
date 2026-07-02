//! Plan-tree carriers shared by planning and execution.
//!
//! This boundary owns the `custom_private`, `custom_exprs`, and scan-tuple
//! contracts. It must contain only data that PostgreSQL can copy or rewrite as
//! part of a plan tree; provider runtime state belongs in `execution`.

pub mod codec;
pub(crate) mod custom_exprs;
pub mod custom_private;
mod purpose;
pub(crate) mod tuple_layout;

pub use purpose::ScanPurpose;
