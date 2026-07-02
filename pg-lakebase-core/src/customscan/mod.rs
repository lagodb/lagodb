//! Generic PostgreSQL CustomPath/CustomScan framework.
//!
//! Downstream extensions call [`provider::register_provider`] then [`init`]
//! from `_PG_init`. The `modify` module owns the PG17 wrapper around
//! `ModifyTable`.

mod error;
mod execution;
pub use error::CustomScanError;
mod gucs;
mod plan_data;
mod planning;
pub mod provider;

pub use execution::{exec, explain, state};
pub use plan_data::ScanPurpose;
pub use plan_data::{codec, custom_private};
pub use planning::{builder, hook};

pub(crate) use execution::exec_params;
pub(crate) use plan_data::{custom_exprs, tuple_layout};
pub(crate) use planning::{candidate, parameterized, paths};

#[cfg(test)]
mod test_support;

#[cfg(feature = "pg17")]
pub mod modify;

#[cfg(feature = "pg17")]
pub(crate) fn has_modify_provider_for(
    context: &crate::customscan::provider::RelPathContext,
) -> bool {
    modify::has_provider_for(context)
}

#[cfg(not(feature = "pg17"))]
pub(crate) fn has_modify_provider_for(
    _context: &crate::customscan::provider::RelPathContext,
) -> bool {
    false
}

/// Register GUCs and install the `set_rel_pathlist_hook` router. Idempotent.
pub fn init() {
    gucs::init();

    // SAFETY: call from `_PG_init` (single-threaded hook slot update).
    unsafe {
        hook::install_set_rel_pathlist_hook();
    }
}
