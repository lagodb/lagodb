//! Generic PostgreSQL Foreign Data Wrapper framework.
//!
//! Scan, modify, analyze, and truncate are optional capability facets of one
//! provider identity. Their PostgreSQL callback lifecycles remain separate.

mod maintenance;
mod modify;
mod payload;
mod provider;
mod routine;
mod row_identity;
mod scan;
mod system_column;
mod validation;

pub use crate::plan_data::{
    PlanDataReader as ForeignPrivateReader, PlanDataWriter as ForeignPrivateWriter,
};
pub use maintenance::{
    FdwAnalyze, FdwTruncate, ForeignAnalyzeContext, ForeignAnalyzeSupport,
    ForeignSampleContext, ForeignSampleStatistics, ForeignTableMaintenanceError,
    ForeignTableMaintenancePhase, ForeignTruncateBehavior, ForeignTruncateContext,
};
pub use modify::{
    FdwModify, ForeignInsertBatch, ForeignInsertBeginContext,
    ForeignModifyBeginContext, ForeignModifyCapabilities, ForeignModifyError,
    ForeignModifyOperation, ForeignModifyOutcome, ForeignModifyPhase,
    ForeignModifyPlanContext, ForeignModifyPlanSpec, ForeignModifyPrivate,
    ForeignModifyRelationContext, ForeignModifyState, ForeignReturnedIdentity,
    ForeignRowIdentity, ForeignRowIdentityKind, ForeignUpdateTargetContext,
    ModifyPlanSlot, ModifySlot,
};
pub use provider::ForeignDataWrapper;
pub use routine::{
    FdwRoutine, register_analyze, register_modify, register_scan, register_truncate,
};
pub use row_identity::{ForeignRowIdentityError, ForeignRowIdentityRequirement};
pub use scan::{
    BeginForeignScanContext, ColumnRequirements, FdwScan, ForeignExpressionValue,
    ForeignExprs, ForeignPathBuilder, ForeignPathContext, ForeignPathKey,
    ForeignPathKeys, ForeignPathSpec, ForeignPlanContext, ForeignPlanFilter,
    ForeignPlanFilters, ForeignPlanPrivate, ForeignPlanQualLocation,
    ForeignPlanSpec, ForeignFilterEstimate, ForeignFilterExplainValues,
    ForeignRelContext, ForeignRelSize, ForeignRelSizeContext, ForeignScanError,
    ForeignScanPhase, PathVariantKind, ReScanForeignScanContext, Relids,
    RuntimeExpressionValues, ScanDatumWriter, ScanOutputColumn, ScanProjection,
    ScanProjectionPolicy, ScanSlotWriter,
};
pub use validation::ForeignValidationError;

/// Entry point used by the generated `#[pg_fdw]` registration code.
#[doc(hidden)]
pub mod __private {
    pub use super::routine::new_routine;
}

/// Common imports for FDW providers.
pub mod prelude {
    pub use super::{
        FdwAnalyze, FdwModify, FdwRoutine, FdwScan, FdwTruncate, ForeignDataWrapper,
        ForeignInsertBatch, ForeignValidationError, register_analyze,
        register_modify, register_scan, register_truncate,
    };
}
