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
mod provider;
mod relation;
mod scan;
mod schema;
mod source_identity;
mod transaction;

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
