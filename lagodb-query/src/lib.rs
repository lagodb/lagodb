//! DataFusion-backed query offload framework for LagoDB.

pub mod datafusion;
pub mod plan;

mod profile;

pub use profile::{
    DEFAULT_MAXIMUM_BATCH_ROWS, ExecutionProfile, ExecutionProfileError,
    MAXIMUM_BATCH_ROWS_LIMIT,
};
