//! Iceberg mutation operations.
//!
//! The module facade exposes the provider's modify/query state. Runtime
//! callbacks live in [`state`], immutable command decisions in [`plan`], and
//! row-delete backends in [`row_delete`].

mod data_file_sink;
mod plan;
mod row_delete;
mod row_identity;
mod state;

pub use row_identity::{
    IcebergFileSource, IcebergModifyQueryState, IcebergModifyScanContext,
};
pub use state::IcebergModifyState;
