//! Generic PostgreSQL CustomPath/CustomScan framework.
//!
//! Downstream extensions register providers during `_PG_init`; registration
//! stages the relation and modify planner facets published with the DSO's
//! runtime registration transaction.

mod error;
mod execution;
pub use error::CustomScanError;
mod filter;
mod gucs;
mod methods;
mod plan_data;
mod planning;
pub mod provider;

use provider::RelationContext;

pub use methods::{
    BeginCustomScan, CreateCustomScanState, CustomScanMethodTables, EndCustomScan,
    ExecCustomScan, ExplainCustomScan, ParallelCustomScanCallbacks, PlanCustomPath,
    ReScanCustomScan, SerialCustomScanCallbacks,
};
pub use plan_data::ScanPurpose;

pub mod modify;

pub(crate) fn has_modify_provider_for(context: &RelationContext<'_>) -> bool {
    modify::has_provider_for(context)
}
