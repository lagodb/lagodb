//! CustomScan executor lifecycle and EXPLAIN.

pub mod exec;
pub mod explain;
pub(crate) mod lifecycle;
pub(crate) mod parallel;
pub(crate) mod scan;
pub(crate) mod start;
pub mod state;
