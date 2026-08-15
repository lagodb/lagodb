//! Optional foreign-table `ANALYZE` and `TRUNCATE` capabilities.
//!
//! PostgreSQL exposes both operations through `FdwRoutine`, but their
//! lifecycles are independent: `ANALYZE` negotiates a later sampling callback,
//! while `TRUNCATE` receives a same-server batch of open relations.

mod callbacks;
mod contract;
mod error;

pub(crate) use callbacks::{
    acquire_sample_rows, analyze_foreign_table, exec_foreign_truncate,
};
pub use contract::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignAnalyzeSupport,
    ForeignSampleContext, ForeignSampleStatistics, ForeignTruncateBehavior,
    ForeignTruncateContext,
};
pub use error::{ForeignTableMaintenanceError, ForeignTableMaintenancePhase};
