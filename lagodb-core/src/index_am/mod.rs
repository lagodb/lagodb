//! Index access method contract and its PostgreSQL `IndexAmRoutine` adapter.
//!
//! Every access method must call [`register_index_am`] from its extension's
//! `_PG_init`: `amoptions` parses through the relopt kind allocated there.

mod build;
mod insert;
mod opclass;
mod options;
mod planning;
mod predicate;
mod properties;
mod routine;
mod scan;
mod vacuum;
mod values;

use pgrx::{AllocatedByPostgres, PgBox, pg_sys};

use crate::diag::PgReportError;

pub use build::{
    IndexBuild, IndexBuildContext, IndexBuildPhases, IndexBuildStats, IndexBuildTuple,
};
#[cfg(any(feature = "pg17", feature = "pg18"))]
pub use insert::IndexInsertCleanupContext;
pub use insert::{
    HeapConflictProbe, HeapTupleWait, IndexInsertContext, IndexInsertRow,
    IndexInsertSession, IndexUniqueCheck,
};
#[cfg(feature = "pg18")]
pub use opclass::IndexStrategyTranslation;
pub use opclass::{
    IndexAdjustMembers, IndexOpclass, OpFamilyMemberRef, OpFamilyMembers,
};
pub use options::{
    IndexEnumMember, IndexOption, IndexOptionDef, IndexOptionLock, IndexOptionValue,
    IndexOptions, register_index_am,
};
#[cfg(feature = "pg18")]
pub use planning::IndexTreeHeight;
pub use planning::{IndexCost, IndexCostContext, IndexPlanning, IndexReturnable};
pub use predicate::{IndexPredicateReads, IndexPredicateWrites};
pub use properties::{
    IndexAmProperties, IndexPropertyProvider, ParallelVacuumOptions, PropertyAnswer,
};
pub use routine::{IndexAmRoutineBuilder, index_am_routine};
pub use scan::{
    IndexBitmap, IndexBitmapScan, IndexMarkRestore, IndexMatch, IndexOrderByDistance,
    IndexParallelRegion, IndexParallelScan, IndexParallelShared, IndexReturnedValues,
    IndexScanContext, IndexScanKeys, IndexScanSession, IndexTupleOutput,
    IndexTupleScan, ReturnedTupleDesc,
};
pub use vacuum::{
    BulkDeleteCallbackHandle, IndexVacuum, IndexVacuumContext, IndexVacuumStats,
};
pub use values::IndexValues;

/// Reported at the callback's FFI boundary.
pub type IndexAmResult<T> = Result<T, PgReportError>;

pub type IndexAmRoutine = PgBox<pg_sys::IndexAmRoutine, AllocatedByPostgres>;

/// Root trait of an index access method.
pub trait IndexAccessMethod:
    IndexBuild + IndexVacuum + IndexPlanning + IndexOpclass + Sized + 'static
{
    const PROPERTIES: IndexAmProperties;

    /// Reloptions; read them through keys made by [`IndexOption`]'s `*_at`
    /// constructors from positions in this list.
    const OPTIONS: &'static [IndexOptionDef] = &[];

    /// `AccessMethod = Self` types the options the session's contexts hand
    /// out.
    type InsertSession: IndexInsertSession<AccessMethod = Self>;

    type ScanSession: IndexScanSession<AccessMethod = Self>;

    /// Checks beyond PostgreSQL's type and range checks, when `amoptions`
    /// validates.
    fn validate_options(options: &IndexOptions<'_, Self>) -> IndexAmResult<()> {
        let _ = options;
        Ok(())
    }

    /// Register optional callbacks.
    fn register(routine: &mut IndexAmRoutineBuilder<Self>) {
        let _ = routine;
    }
}
