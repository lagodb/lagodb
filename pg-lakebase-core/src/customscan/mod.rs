//! Generic CustomScan framework: planner/executor pushdown for TableAM lake providers.
//!
//! Downstream extensions call [`provider::register_provider`] then [`init`] from `_PG_init`.

pub mod builder;
pub mod codec;
mod custom_exprs;
pub mod custom_private;
mod error;
pub use error::CustomScanError;
pub mod exec;
mod exec_params;
pub mod explain;
mod gucs;
pub mod hook;
mod param_path;
mod path_clause;
mod path_gate;
mod path_router;
pub mod provider;
pub mod state;

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

#[cfg(test)]
mod pbt_param_info;
