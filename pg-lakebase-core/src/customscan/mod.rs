//! Generic CustomScan framework: planner/executor pushdown for TableAM lake providers.
//!
//! Downstream extensions call [`provider::register_provider`] then [`init`] from `_PG_init`.

mod error;
mod execution;
pub use error::CustomScanError;
mod gucs;
mod plan_data;
mod planning;
pub mod provider;

// Stable facade paths for provider crates. Internally, ownership follows the
// planning / plan-data / execution lifecycle boundaries above.
pub use execution::{exec, explain, state};
pub use plan_data::{codec, custom_private};
pub use planning::{builder, hook};

pub(crate) use execution::exec_params;
pub(crate) use plan_data::{custom_exprs, tuple_layout};
pub(crate) use planning::{candidate, parameterized, paths};

#[cfg(test)]
mod test_support;

/// Register GUCs and install the `set_rel_pathlist_hook` router. Idempotent.
pub fn init() {
    gucs::init();

    // SAFETY: call from `_PG_init` (single-threaded hook slot update).
    unsafe {
        hook::install_set_rel_pathlist_hook();
    }
}
