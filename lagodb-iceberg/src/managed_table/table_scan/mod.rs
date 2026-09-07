//! Managed-Iceberg table-scan lifecycle for query-subtree offload.
//!
//! Planner state, Begin-owned prepared metadata, and run-local
//! DataFusion cursors are deliberately separate. None of these types reuse the
//! relation CustomScan slot/cursor state.

mod error;
mod plan;
mod prepared;
mod provider;
mod stream;

use error::IcebergTableScanError;
pub(crate) use plan::{IcebergScanPlan, IcebergScanPlanError};
pub(crate) use prepared::PreparedIcebergTableScan;
use stream::IcebergArrowStream;

pub(crate) use provider::register;
