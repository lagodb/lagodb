//! Generic PostgreSQL Foreign Data Wrapper framework.
//!
//! Scan and modify are optional capability facets of one provider identity.
//! Their PostgreSQL callback lifecycles remain separate because PostgreSQL
//! stores their executor state in different node fields.

mod codec;
mod modify;
mod payload;
mod provider;
mod routine;
mod row_identity;
mod scan;
mod system_column;
mod validation;

pub use codec::{ForeignPrivateReader, ForeignPrivateWriter, PrivateCodecError};
pub use modify::{
    FdwModify, ForeignInsertBeginContext, ForeignModifyBeginContext,
    ForeignModifyCapabilities, ForeignModifyError, ForeignModifyOperation,
    ForeignModifyOutcome, ForeignModifyPhase, ForeignModifyPlanContext,
    ForeignModifyPlanSpec, ForeignModifyPrivate, ForeignModifyRelationContext,
    ForeignModifyState, ForeignReturnedIdentity, ForeignRowIdentity,
    ForeignRowIdentityKind, ForeignUpdateTargetContext, ModifyPlanSlot, ModifySlot,
};
pub use provider::ForeignDataWrapper;
pub use routine::{FdwRoutine, register_modify, register_scan};
pub use row_identity::{ForeignRowIdentityError, ForeignRowIdentityRequirement};
pub use scan::{
    BeginForeignScanContext, ColumnRequirements, FdwScan, ForeignExprList,
    ForeignExpressionValue, ForeignExprs, ForeignPathBuilder, ForeignPathContext,
    ForeignPathKey, ForeignPathKeys, ForeignPathSpec, ForeignPlanContext,
    ForeignPlanPrivate, ForeignPlanSpec, ForeignPushdown, ForeignRelContext,
    ForeignRelSize, ForeignRelSizeContext, ForeignScanError, ForeignScanPhase,
    PathVariantKind, ReScanForeignScanContext, Relids, RuntimeExpressionValues,
    ScanDatumWriter, ScanOutputColumn, ScanProjection, ScanProjectionPolicy,
    ScanSlotWriter,
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
        FdwModify, FdwRoutine, FdwScan, ForeignDataWrapper, ForeignValidationError,
        register_modify, register_scan,
    };
}
