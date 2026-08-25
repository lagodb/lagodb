//! PostgreSQL planner adapter for the shared Iceberg reader.

use pg_lakebase_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignFilterExplainValues, ForeignPathBuilder,
    ForeignPathContext, ForeignPathKeys, ForeignPathSpec, ForeignPlanContext,
    ForeignPlanSpec, ForeignRelContext, ForeignRelSize, ForeignRelSizeContext,
    ForeignScanError, ReScanForeignScanContext, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use super::super::error::IcebergFdwError;
use super::super::options::ForeignTableIdentity;
use super::super::provider::LagodbIceberg;
use super::private::IcebergFdwScanPrivate;
use super::state::IcebergFdwScanState;

const UNANALYZED_FALLBACK_PAGES: pg_sys::BlockNumber = 10;
const REST_SCAN_STARTUP_COST: f64 = 100.0;

pub(crate) struct IcebergFdwScanPlanner {
    identity: ForeignTableIdentity,
    base_tuples: f64,
    pages: f64,
}

impl FdwScan for LagodbIceberg {
    type PlannerState = IcebergFdwScanPlanner;
    type PrivateData = IcebergFdwScanPrivate;
    type State = IcebergFdwScanState;

    fn init_planner(
        context: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError> {
        Ok(IcebergFdwScanPlanner {
            identity: ForeignTableIdentity::resolve(context.relation_oid())?,
            base_tuples: 0.0,
            pages: 0.0,
        })
    }

    fn estimate(
        state: &mut Self::PlannerState,
        context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ForeignScanError> {
        let estimate = context.local_statistics_estimate(UNANALYZED_FALLBACK_PAGES);
        state.base_tuples = context.relation().base_tuples().max(estimate.rows);
        state.pages = context.relation().base_pages().max(0.0);
        Ok(estimate)
    }

    fn build_paths(
        state: &Self::PlannerState,
        context: &ForeignPathContext<'_>,
        paths: &mut ForeignPathBuilder<Self::PrivateData>,
    ) -> Result<(), ForeignScanError> {
        let rows = context.rows();
        let pruning = context.pruning_estimate();
        let retrieved_rows = (state.base_tuples * pruning.selectivity).max(rows);
        let startup = REST_SCAN_STARTUP_COST + pruning.startup_cost;
        // SAFETY: PostgreSQL initializes planner cost GUCs before invoking
        // GetForeignPaths, matching the existing Parquet FDW cost path.
        let total = startup
            + state.pages * unsafe { pg_sys::seq_page_cost }
            + state.base_tuples * pruning.per_tuple_cost;
        let mut path = ForeignPathSpec::new(
            rows,
            startup,
            total,
            IcebergFdwScanPrivate::new(state.identity.clone()),
        );
        path.retrieved_rows = retrieved_rows;
        paths.push(path);
        Ok(())
    }

    fn supports_pathkeys(
        _state: &Self::PlannerState,
        _context: &ForeignPathContext<'_>,
        _pathkeys: &mut ForeignPathKeys,
    ) -> Result<bool, ForeignScanError> {
        Ok(false)
    }

    fn build_plan(
        state: &mut Self::PlannerState,
        context: &ForeignPlanContext<'_, Self>,
    ) -> Result<ForeignPlanSpec<Self::PrivateData>, ForeignScanError> {
        let mut filters = context.filters().iter();
        let source = filters
            .next()
            .map(|filter| filter.predicate().source().clone());
        if let Some(expected) = source.as_ref()
            && filters.any(|filter| filter.predicate().source() != expected)
        {
            return Err(IcebergFdwError::InvalidPlan {
                detail: "filter plan contains more than one source identity",
            }
            .into());
        }
        Ok(ForeignPlanSpec::new(IcebergFdwScanPrivate::with_source(
            state.identity.clone(),
            source,
        )))
    }

    fn explain_filter(
        predicate: &super::super::filter::FdwPlannedPredicate,
        values: ForeignFilterExplainValues<'_>,
    ) -> Result<Option<String>, ForeignScanError> {
        Ok(Some(predicate.explain(values)))
    }

    fn begin(
        context: BeginForeignScanContext<'_, Self>,
    ) -> Result<Self::State, ForeignScanError> {
        IcebergFdwScanState::begin(context)
    }

    fn next_slot(
        state: &mut Self::State,
        output: &mut ScanSlotWriter<'_>,
    ) -> Result<bool, ForeignScanError> {
        state.next_slot(output)
    }

    fn start(
        state: &mut Self::State,
        context: StartForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        state.start(context)
    }

    fn rescan(
        state: &mut Self::State,
        context: ReScanForeignScanContext<'_, Self>,
    ) -> Result<(), ForeignScanError> {
        state.rescan(context)
    }

    fn end(_state: &mut Self::State) -> Result<(), ForeignScanError> {
        Ok(())
    }
}
