//! CustomScan executor lifecycle, runtime parameter handling, and EXPLAIN.

pub mod exec;
pub mod exec_params;
pub mod explain;
pub(crate) mod lifecycle;
pub(crate) mod scan;
pub mod state;
