//! PostgreSQL planner integration for CustomPath discovery, gating, costing,
//! and final CustomScan plan construction.

pub mod builder;
pub(crate) mod candidate;
pub mod hook;
pub(crate) mod parameterized;
pub(crate) mod paths;
