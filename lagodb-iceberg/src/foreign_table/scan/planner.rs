//! PostgreSQL planner adapter for the shared Iceberg reader.

use std::rc::Rc;

use lagodb_core::expr::pushdown::FilterPlanningContext;
use lagodb_core::fdw::{
    BeginForeignScanContext, FdwScan, ForeignFilterExplainValues, ForeignPathBuilder,
    ForeignPathContext, ForeignPathKeys, ForeignPathSpec, ForeignPlanContext,
    ForeignPlanSpec, ForeignRelContext, ForeignRelSize, ForeignRelSizeContext,
    ForeignScanError, ForeignScanResult, ReScanForeignScanContext, ScanSlotWriter,
    StartForeignScanContext,
};
use pgrx::pg_sys;

use crate::config::scan_fraction;

use super::super::error::IcebergFdwError;
use super::super::filter::IcebergFdwFilterPlanner;
use super::super::planning_source::ForeignPlanningSource;
use super::super::provider::LagodbIceberg;
use super::private::IcebergFdwScanPrivate;
use super::state::IcebergFdwScanState;

const UNANALYZED_FALLBACK_PAGES: pg_sys::BlockNumber = 10;
const REST_SCAN_STARTUP_COST: f64 = 100.0;

pub(crate) struct IcebergFdwScanPlanner {
    source: Rc<ForeignPlanningSource>,
    base_tuples: f64,
    pages: f64,
}

impl IcebergFdwScanPlanner {
    pub(crate) fn planning_source(&self) -> &ForeignPlanningSource {
        &self.source
    }
}

impl FdwScan for LagodbIceberg {
    type PlannerState = IcebergFdwScanPlanner;
    type PrivateData = IcebergFdwScanPrivate;
    type State = IcebergFdwScanState;
    const NATIVE_PARALLEL: bool = true;

    fn init_planner(
        context: &ForeignRelContext<'_>,
    ) -> Result<Self::PlannerState, ForeignScanError> {
        Ok(IcebergFdwScanPlanner {
            source: Rc::new(ForeignPlanningSource::new(
                context.relation_oid(),
                context.effective_user_id(),
            )?),
            base_tuples: 0.0,
            pages: 0.0,
        })
    }

    fn begin_scan_filter_planning(
        state: &mut Self::PlannerState,
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        Ok(IcebergFdwFilterPlanner::new(
            context,
            Rc::clone(&state.source),
        ))
    }

    fn estimate(
        state: &mut Self::PlannerState,
        context: &ForeignRelSizeContext<'_>,
    ) -> Result<ForeignRelSize, ForeignScanError> {
        // TODO(fdw-unanalyzed-statistics): An unANALYZEd foreign table has no
        // pg_class row/page statistics, so `local_statistics_estimate` uses
        // the fixed ten-page fallback and derives rows from tuple width. Use
        // the current Iceberg snapshot summary (`total-records` and
        // `total-files-size`) for that case, while keeping the ANALYZEd path
        // entirely local. The planner source shared with filter negotiation
        // retains any REST table resolved for predicate classification, so a
        // future remote-statistics estimate can reuse it. BeginForeignScan must
        // continue to resolve independently so it can validate the planned
        // identity and source against execution-time catalog state.
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
        let scanned_pages = state.pages * scan_fraction(pruning.selectivity);
        let retrieved_rows = (state.base_tuples * pruning.selectivity).max(rows);
        let startup = REST_SCAN_STARTUP_COST + pruning.startup_cost;
        // SAFETY: PostgreSQL initializes planner cost GUCs before invoking
        // GetForeignPaths, matching the existing Parquet FDW cost path.
        let total = startup
            + scanned_pages * unsafe { pg_sys::seq_page_cost }
            + state.base_tuples * pruning.per_tuple_cost;
        let mut path = ForeignPathSpec::new(
            rows,
            startup,
            total,
            IcebergFdwScanPrivate::new(state.source.identity().clone()),
        );
        // A read-write table can carry a backend-local transaction overlay.
        // Until that overlay is part of the worker payload, neither a partial
        // worker nor an independently rebuilt complete path may read it.
        path.set_native_parallel_partial(
            !state.source.identity().mode().is_writable(),
        );
        path.set_scanned_pages(scanned_pages);
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
            state.source.identity().clone(),
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

    fn next_slot<'a>(
        state: &mut Self::State,
        output: &'a mut ScanSlotWriter<'_>,
    ) -> Result<ForeignScanResult<'a>, ForeignScanError> {
        let produced = state.next_slot(output)?;
        output.finish(produced)
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

    fn end(state: &mut Self::State) -> Result<(), ForeignScanError> {
        state.end();
        Ok(())
    }

    fn estimate_dsm(
        state: &mut Self::State,
    ) -> Result<pg_sys::Size, ForeignScanError> {
        state.estimate_dsm()
    }

    unsafe fn initialize_dsm(
        state: &mut Self::State,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { state.initialize_dsm(coordinate) }
    }

    unsafe fn reinitialize_dsm(
        state: &mut Self::State,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { state.reinitialize_dsm(coordinate) }
    }

    unsafe fn initialize_worker(
        state: &mut Self::State,
        _toc: *mut pg_sys::shm_toc,
        coordinate: *mut core::ffi::c_void,
    ) -> Result<(), ForeignScanError> {
        unsafe { state.initialize_worker(coordinate) }
    }

    fn shutdown_parallel(state: &mut Self::State) -> Result<(), ForeignScanError> {
        state.shutdown_parallel();
        Ok(())
    }
}
