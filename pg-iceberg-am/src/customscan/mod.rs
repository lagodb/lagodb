//! Iceberg CustomScan provider and predicate pushdown implementation.

pub mod provider;

#[cfg(feature = "pg_test")]
mod pg_test;

pub use provider::{IcebergCustomScanProvider, IcebergScanState};

/// Register the Iceberg provider once from `_PG_init`.
pub fn register() {
    pg_lakebase_core::customscan::provider::register_provider::<
        IcebergCustomScanProvider,
    >();
}
