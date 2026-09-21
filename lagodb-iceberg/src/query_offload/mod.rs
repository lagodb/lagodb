//! Query-offload provider for Iceberg-backed PostgreSQL relations.
//!
//! This layer owns query-subtree offload, PostgreSQL AM/FDW routing, and source
//! binding. Shared statement and worker scan mechanics live in [`crate::scan`]
//! and do not depend on either PostgreSQL relation adapter.

mod error;
mod filter;
mod plan;
mod provider;
mod stream;
mod worker;

pub(crate) use provider::register;
