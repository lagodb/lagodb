//! Generic PostgreSQL CustomPath/CustomScan framework.
//!
//! Downstream extensions call [`provider::register_provider`] then [`init`]
//! from `_PG_init`. The `modify` module owns the PG17 wrapper around
//! `ModifyTable`.

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
pub use planning::{candidate, hook, tuple_planner};

pub mod modify;

pub(crate) fn has_modify_provider_for(context: &RelationContext<'_>) -> bool {
    modify::has_provider_for(context)
}

/// Install the `set_rel_pathlist_hook` router. Shared GUCs are runtime-owned.
pub fn init() {
    // SAFETY: call from `_PG_init` (single-threaded hook slot update).
    unsafe {
        planning::hook::install_set_rel_pathlist_hook();
    }
}
