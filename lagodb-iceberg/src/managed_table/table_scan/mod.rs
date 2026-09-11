//! Managed-Iceberg table-scan lifecycle for query-subtree offload.
//!
//! Planner state, Begin-owned schema binding, run-local task plans, and
//! DataFusion cursors are deliberately separate. None of these types reuse the
//! relation CustomScan slot/cursor state.

mod error;
mod lifecycle;
mod plan;
mod provider;
mod runtime_predicate;
mod stream;

use error::IcebergTableScanError;
pub(crate) use lifecycle::{BoundIcebergTableScan, PlannedIcebergTableScan};
pub(crate) use plan::{IcebergScanPlan, IcebergScanPlanError};
use stream::IcebergArrowStream;

pub(crate) use provider::register;
