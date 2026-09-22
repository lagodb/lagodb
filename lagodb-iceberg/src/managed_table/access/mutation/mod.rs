//! Iceberg mutation operations.
//!
//! The module facade exposes the provider's modify/query state. Runtime
//! callbacks live in [`state`], immutable command decisions in [`plan`], and
//! shared write sinks in [`crate::write`].

mod cursor;
mod plan;
mod row_identity;
mod scan;
mod state;

pub(crate) use cursor::ManagedMutationCursor;
pub use row_identity::{
    IcebergFileSource, IcebergModifyQueryState, IcebergModifyScanContext,
};
pub(crate) use scan::PreparedManagedMutationScan;
pub use state::IcebergModifyState;
