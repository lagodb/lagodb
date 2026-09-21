//! Iceberg foreign-data-wrapper integration.

use std::sync::OnceLock;

mod analyze;
pub mod catalog;
mod ddl;
mod error;
mod filter;
mod import;
mod modify;
mod options;
mod planning_source;
mod provider;
mod relation;
mod scan;
mod schema;
mod source_identity;
mod transaction;

pub(crate) use error::IcebergFdwError;
pub(crate) use options::{ForeignTableIdentity, ForeignTableMode};
pub(crate) use planning_source::ForeignPlanningSource;
pub(crate) use provider::LagodbIceberg;
pub(crate) use relation::RestForeignTable;
pub(crate) use schema::ForeignSchemaBinding;
pub(crate) use source_identity::PlanSourceIdentity;
pub(crate) use transaction::ForeignTransaction;

static RUSTLS_CRYPTO_PROVIDER: OnceLock<()> = OnceLock::new();

pub(crate) fn initialize_crypto_provider() {
    RUSTLS_CRYPTO_PROVIDER.get_or_init(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("the rustls crypto provider must be installed only once")
    });
}

pub(crate) fn register() {
    ddl::register();
}
