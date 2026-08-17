//! PostgreSQL ForeignScan planning and execution capability.

mod callbacks;
mod context;
mod contract;
mod error;
mod executor;
mod explain;
mod filter;
mod parameterized;
mod path_builder;
mod pathkeys;
mod pg;
mod plan_filter;
mod planning;
mod private;
mod projection;
mod pushdown;
mod slot;
mod state;

pub use context::{
    ForeignFilterEstimate, ForeignPathContext, ForeignPathSpec, ForeignPlanContext,
    ForeignPlanPrivate, ForeignPlanSpec, ForeignRelContext, ForeignRelSize,
    ForeignRelSizeContext, PathVariantKind, Relids,
};
pub use contract::FdwScan;
pub use error::{ForeignScanError, ForeignScanPhase};
pub use path_builder::ForeignPathBuilder;
pub use pathkeys::{ForeignPathKey, ForeignPathKeys};
pub use plan_filter::{
    ForeignFilterExplainValues, ForeignPlanFilter, ForeignPlanFilters,
    ForeignPlanQualLocation,
};
pub use projection::{ColumnRequirements, ScanProjection, ScanProjectionPolicy};
pub use pushdown::{
    BeginForeignScanContext, ForeignExpressionValue, ForeignExprs,
    ReScanForeignScanContext, RuntimeExpressionValues,
};
pub use slot::{ScanDatumWriter, ScanOutputColumn, ScanSlotWriter};

pub(crate) use callbacks::{
    begin_foreign_scan, end_foreign_scan, iterate_foreign_scan, rescan_foreign_scan,
};
pub(crate) use explain::explain_foreign_scan;
pub(crate) use planning::{
    get_foreign_paths, get_foreign_plan, get_foreign_rel_size,
};
