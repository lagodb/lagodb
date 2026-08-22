//! Object storage created from Iceberg REST table configuration.

mod cache;
mod config;
mod factory;
mod routes;

pub(crate) use factory::PgStorageFactory;
