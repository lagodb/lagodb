//! Optional scan capability implemented by an FDW provider.

use crate::expr::pushdown::{FilterPlanningContext, FilterPushdown};
use pgrx::pg_sys;

use super::super::provider::ForeignDataWrapper;
use super::context::{
    ForeignPathContext, ForeignPlanContext, ForeignPlanSpec, ForeignRelContext,
    ForeignRelSize, ForeignRelSizeContext,
};
use super::error::ForeignScanError;
use super::path_builder::ForeignPathBuilder;
use super::pathkeys::ForeignPathKeys;
use super::plan_filter::ForeignFilterExplainValues;
use super::pushdown::{
    BeginForeignScanContext, ReScanForeignScanContext, StartForeignScanContext,
};
use super::slot::{ForeignScanResult, ScanSlotWriter};

/// Optional scan capability of an FDW provider.
pub trait FdwScan: ForeignDataWrapper + FilterPushdown + 'static {
    type PlannerState: 'static;
    type PrivateData: super::context::ForeignPlanPrivate;
    type State: 'static;

    /// Whether this FDW supplies the native-parallel DSM callback family and
    /// may offer unparameterized partial paths. Individual complete paths must
    /// independently opt into worker-local reconstruction through
    /// [`ForeignPathSpec`].
    const NATIVE_PARALLEL: bool = false;

    fn init_planner(
        ctx: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError>;

    /// Start relation-scoped filter planning with access to the matching
    /// provider planner state. Providers that do not share planning metadata
    /// retain the ordinary [`FilterPushdown`] construction path.
    fn begin_scan_filter_planning(
        _state: &mut Self::PlannerState,
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        <Self as FilterPushdown>::begin_filter_planning(context)
    }

    /// Borrow the provider state installed by this FDW's `GetForeignRelSize`.
    ///
    /// # Safety
    ///
    /// `relation` must be the live foreign base relation initialized by this
    /// exact provider's `GetForeignRelSize` callback. The returned reference
    /// may only be used synchronously while no mutable FDW callback is active.
    unsafe fn planning_state(relation: &pg_sys::RelOptInfo) -> &Self::PlannerState
    where
        Self: Sized,
    {
        // SAFETY: the caller supplies the provider type witness and exclusive
        // callback-phase invariant documented by this method.
        unsafe { super::planning::PlannerState::<Self>::provider(relation) }
    }

    fn estimate(
        state: &mut Self::PlannerState,
        ctx: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ForeignScanError>;

    /// Called once for each framework path variant. Submit every independent
    /// unordered and ordered alternative that the provider wants PostgreSQL to
    /// compare for this variant.
    fn build_paths(
        state: &Self::PlannerState,
        ctx: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<Self::PrivateData>,
    ) -> Result<(), ForeignScanError>;

    /// Decide whether the provider can guarantee the ordering described by a
    /// candidate foreign path. During path creation, the framework has already
    /// collected non-system-column EC members local to the scanned relation
    /// and validated PostgreSQL's relation-target pathkey contract. During
    /// plan creation it rebuilds that candidate view from the selected path's
    /// EC members without using a persisted candidate index.
    /// Providers must additionally validate remote expression, operator-family,
    /// collation, NULL-ordering, and deparse semantics, then select one member
    /// candidate for every pathkey when more than one is available.
    /// This callback validates provider-level ordering semantics; it does not
    /// infer whether a particular `PrivateData` alternative actually executes
    /// that ordering. Each ordered spec submitted by `build_paths` must satisfy
    /// that contract independently.
    ///
    /// The framework calls this during both `GetForeignPaths` and
    /// `GetForeignPlan`. The provider must apply the same remote validation and
    /// candidate selection in both phases; path private data does not preserve
    /// a PostgreSQL EC member index across those phases.
    ///
    /// The default rejects ordered paths. Unordered paths do not call this
    /// method and retain the ordinary scan planning path.
    fn supports_pathkeys(
        _state: &Self::PlannerState,
        _ctx: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ForeignScanError> {
        Ok(false)
    }

    /// Compose provider-specific final plan data from core's finalized filter
    /// plan and the selected non-filter path state. Filter structure has
    /// already been accepted or rejected by `try_plan_filter`; implementations
    /// must not repeat that decision from PostgreSQL expression trees here.
    fn build_plan(
        state: &mut Self::PlannerState,
        ctx: &ForeignPlanContext<'_, Self>,
    ) -> Result<ForeignPlanSpec<Self::PrivateData>, ForeignScanError>
    where
        Self: Sized;

    /// Build an EXPLAIN-ready description from the provider predicate accepted
    /// during planning. The framework persists the returned text separately
    /// from executor expressions and never calls this method at execution time.
    fn explain_filter(
        _predicate: &Self::PlannedPredicate,
        _values: ForeignFilterExplainValues<'_>,
    ) -> Result<Option<String>, ForeignScanError> {
        Ok(None)
    }

    /// Initialize stable provider state during PostgreSQL's BeginForeignScan.
    fn begin(
        ctx: BeginForeignScanContext<'_, Self>,
    ) -> Result<Self::State, ForeignScanError>;

    /// Bind the first valid dynamic parameter set and open the provider cursor.
    fn start(
        state: &mut Self::State,
        ctx: StartForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError>;

    /// Produce the next row and finalize it through [`ScanSlotWriter::finish`].
    ///
    /// A produced result requires either one datum representation or one
    /// provider-owned HeapTuple representation. Datum output obtains
    /// [`super::slot::ScanDatumWriter`] once for the row and writes every
    /// [`super::slot::ScanOutputColumn`] exactly once; the requested row
    /// identity must also be supplied. A synthetic-null projection has no
    /// provider column to write. An end-of-scan result is cleared by the
    /// framework; providers that must probe PostgreSQL-owned output buffers
    /// before discovering EOF may leave those unpublished writes in the writer.
    fn next_slot<'a>(
        state: &mut Self::State,
        output: &'a mut ScanSlotWriter<'_>,
    ) -> Result<ForeignScanResult<'a>, ForeignScanError>;

    fn rescan(
        state: &mut Self::State,
        ctx: ReScanForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError>;

    fn end(state: &mut Self::State) -> Result<(), ForeignScanError>;

    fn estimate_dsm(
        _state: &mut Self::State,
    ) -> Result<pg_sys::Size, ForeignScanError> {
        Err(ForeignScanError::framework(
            "FDW enabled native parallel scan without EstimateDSM support",
        ))
    }

    /// # Safety
    /// `coordinate` is PostgreSQL DSM storage sized by `estimate_dsm`.
    unsafe fn initialize_dsm(
        _state: &mut Self::State,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        Err(ForeignScanError::framework(
            "FDW enabled native parallel scan without InitializeDSM support",
        ))
    }

    /// # Safety
    ///
    /// `coordinate` must be the live PostgreSQL DSM storage previously
    /// initialized for this scan and sized according to `estimate_dsm`.
    unsafe fn reinitialize_dsm(
        _state: &mut Self::State,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        Err(ForeignScanError::framework(
            "FDW enabled native parallel scan without ReInitializeDSM support",
        ))
    }

    /// # Safety
    ///
    /// `toc` and `coordinate` must be the live PostgreSQL DSM objects for the
    /// current parallel scan and must remain valid until worker shutdown.
    unsafe fn initialize_worker(
        _state: &mut Self::State,
        _toc: *mut pg_sys::shm_toc,
        _coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        Err(ForeignScanError::framework(
            "FDW enabled native parallel scan without worker initialization support",
        ))
    }

    fn shutdown_parallel(_state: &mut Self::State) -> Result<(), ForeignScanError> {
        Ok(())
    }
}
