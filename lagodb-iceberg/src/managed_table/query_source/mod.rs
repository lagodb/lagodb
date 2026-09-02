//! Managed-Iceberg source lifecycle for query-subtree offload.
//!
//! Planner state, Begin-owned prepared metadata, and run-local
//! DataFusion cursors are deliberately separate. None of these types reuse the
//! relation CustomScan slot/cursor state.

mod error;
mod plan;
mod prepared;
mod provider;
mod stream;

use error::IcebergQuerySourceError;
pub(crate) use plan::{IcebergSourcePlan, IcebergSourcePlanError};
pub(crate) use prepared::PreparedIcebergSource;
use stream::IcebergArrowStream;

pub(crate) use provider::register;
