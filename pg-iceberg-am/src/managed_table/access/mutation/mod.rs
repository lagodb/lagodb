//! Iceberg mutation operations.
//!
//! The module facade exposes the provider's modify/query state. Runtime
//! callbacks live in [`state`], immutable command decisions in [`plan`], and
//! shared write sinks in [`crate::engine::write`].

mod plan;
mod row_identity;
mod state;

pub use row_identity::{
    IcebergFileSource, IcebergModifyQueryState, IcebergModifyScanContext,
};
pub use state::IcebergModifyState;
