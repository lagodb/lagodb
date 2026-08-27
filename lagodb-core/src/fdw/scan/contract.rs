//! Optional scan capability implemented by an FDW provider.

use crate::expr::pushdown::FilterPushdown;

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
use super::slot::ScanSlotWriter;

/// Optional scan capability of an FDW provider.
pub trait FdwScan: ForeignDataWrapper + FilterPushdown + 'static {
    type PlannerState: 'static;
    type PrivateData: super::context::ForeignPlanPrivate;
    type State: 'static;

    fn init_planner(
        ctx: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError>;

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

    /// Produce the next row.
    ///
    /// Returning `true` requires either one datum representation or one
    /// provider-owned HeapTuple representation. Datum output obtains
    /// [`super::slot::ScanDatumWriter`] once for the row and writes every
    /// [`super::slot::ScanOutputColumn`] exactly once; the requested row
    /// identity must also be supplied. A
    /// synthetic-null projection has no provider column to write. Returning
    /// `false` requires leaving `output` untouched.
    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError>;

    fn rescan(
        state: &mut Self::State,
        ctx: ReScanForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError>;

    fn end(state: &mut Self::State) -> Result<(), ForeignScanError>;
}
