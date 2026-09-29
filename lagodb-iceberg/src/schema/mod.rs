//! Shared PostgreSQL/Iceberg schema binding and conversion plans.

pub(crate) mod column_plan;
#[cfg(feature = "pg_test")]
mod pg_test;
pub(crate) mod projection;
pub(crate) mod relation;
pub(crate) mod type_mapping;
