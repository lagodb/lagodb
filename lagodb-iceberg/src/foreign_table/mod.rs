//! Iceberg foreign-data-wrapper integration.
//!
//! # Upstream partition metadata boundaries
//!
//! REST table metadata is deserialized by iceberg-lite before LagoDB receives a
//! `Table`. Its transform parser maps only the literal `"unknown"` to
//! `Transform::Unknown`; an actual unrecognized transform name fails metadata
//! deserialization first, so the local predicate-projection fallback for
//! `Transform::Unknown` cannot handle future transforms. Upstream issue #2789
//! and PR #2790 track preserving and accepting the original unknown transform.
//!
//! Iceberg v3 partition fields may also use `source-ids` for multi-source
//! transforms. `UnboundPartitionField` accepts this form for transport, but
//! binding rejects multiple sources. Table metadata uses bound `PartitionField`,
//! which still requires a single `source-id`; `source-ids`-only fields therefore
//! fail at the same table metadata load boundary.
//! Upstream issue #2801 and PR #2802 track that model change. Both fixes belong
//! in iceberg-rust/iceberg-lite rather than REST JSON preprocessing in LagoDB.
//!
//! https://github.com/apache/iceberg-rust/issues/2789
//! https://github.com/apache/iceberg-rust/pull/2790
//! https://github.com/apache/iceberg-rust/issues/2801
//! https://github.com/apache/iceberg-rust/pull/2802

use std::sync::OnceLock;

mod analyze;
pub mod catalog;
mod ddl;
mod error;
mod executor_cache;
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
mod type_mapping;

pub(crate) use error::IcebergFdwError;
pub(crate) use options::ForeignTableIdentity;
pub(crate) use planning_source::ForeignPlanningSource;
pub(crate) use provider::LagodbIceberg;
pub(crate) use relation::ResolvedForeignTableBinding;
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
    ForeignTransaction::register_invalidation_observer();
    ddl::register();
}
