//! Public provider boundary for the generic CustomScan framework.
//!
//! `api` defines the typed SPI implemented by storage providers. `registry`
//! owns process-wide type erasure used only by planner routing.

mod api;
mod methods;
mod registry;

pub use api::*;
pub use methods::{ProviderMethodTables, method_tables_for};
pub use registry::{ErasedProvider, find_matching_provider, register_provider};
