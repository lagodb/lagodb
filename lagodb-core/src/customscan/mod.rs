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
mod plan_data;
mod planning;
pub mod provider;

use provider::RelationContext;

pub use plan_data::ScanPurpose;

// Backend tests live in `pg-backend-tests`; these are only the production
// modules they exercise, exposed through the normal public facade.
pub use execution::{exec, explain, state};
pub use plan_data::{custom_exprs, custom_private};
pub use planning::{candidate, router, tuple_planner};

pub mod modify;

pub(crate) fn has_modify_provider_for(context: &RelationContext<'_>) -> bool {
    modify::has_provider_for(context)
}
