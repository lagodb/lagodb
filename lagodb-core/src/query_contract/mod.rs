//! Shared contracts for provider-neutral query planning and table scanning.
//!
//! This module contains only values that cross crate or provider-runtime
//! boundaries. Query-plan structure and execution-engine types belong in
//! `lagodb-query`.

mod estimate;
mod identity;

pub use estimate::{ScanCost, ScanCostError};
pub use identity::{OutputId, ScanId, TableScanRoute, TableScanRouteKind};
