//! PostgreSQL planner integration for CustomPath discovery, gating, costing,
//! and final CustomScan plan construction.

pub mod builder;
pub mod candidate;
pub(crate) mod final_plan;
pub mod hook;
pub(crate) mod parameterized;
pub(crate) mod paths;
pub mod tuple_planner;
