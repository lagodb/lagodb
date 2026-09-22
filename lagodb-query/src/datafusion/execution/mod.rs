//! Current-thread DataFusion lifecycle for query offload.

mod output;
mod state;

use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use pgrx::pg_sys;

use crate::plan::{PlanExplainNode, PlannedTableScan, QueryFragment, QueryPlanData};

use super::error::QueryExecutionError;
use super::memory::QueryExecutionLimits;
use super::metrics::{
    ExecutionMetrics, ExecutionMetricsSnapshot, QueryExecutionMode,
};
use super::parallel::ParallelQueryOptions;
use super::scan_callbacks::TableScanCallbacks;
use output::QueryOutputDecoder;
use state::QueryExecutionState;

/// Whether LagoDB records counters and retains physical-plan metrics for
/// PostgreSQL instrumentation.
///
/// This switch does not control DataFusion 55's intrinsic operator metrics:
/// standard physical operators create their own timers and update them while
/// executing. `Disabled` guarantees that ordinary PostgreSQL execution adds no
/// LagoDB scan counters or physical-plan metric retention. EXPLAIN `TIMING OFF`
/// suppresses DataFusion duration metrics when the retained plan is rendered;
/// eliminating DataFusion's internal timer reads requires upstream engine
/// support rather than a LagoDB execution-mode flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMetricsMode {
    /// Do not collect LagoDB counters or retain physical-plan metrics.
    Disabled,
    /// Collect and retain metrics requested by PostgreSQL instrumentation.
    Enabled,
}

/// Begin-owned query state with statement resources and at most one lazy run.
pub struct QueryExecution {
    output: QueryOutputDecoder,
    metrics: Option<Arc<ExecutionMetrics>>,
    state: Option<QueryExecutionState>,
    backend_thread: PhantomData<Rc<()>>,
}

/// Begin-time inputs selected by the PG host for one complete query fragment.
pub struct QueryExecutionRequest<'a> {
    pub query: QueryPlanData,
    pub scans: &'a [PlannedTableScan<'a>],
    pub limits: QueryExecutionLimits,
    pub metrics_mode: ExecutionMetricsMode,
    pub callbacks: &'a [TableScanCallbacks],
    pub parallel: Option<ParallelQueryOptions>,
    pub runtime_exprs: *mut pg_sys::List,
    pub parent: *mut pg_sys::PlanState,
}

impl QueryExecution {
    pub fn try_new(
        request: QueryExecutionRequest<'_>,
    ) -> Result<Self, QueryExecutionError> {
        let metrics = (request.metrics_mode == ExecutionMetricsMode::Enabled)
            .then(|| Arc::new(ExecutionMetrics::new(request.scans.len())));
        let (state, output) =
            QueryExecutionState::prepare(request, metrics.as_ref())?;
        Ok(Self {
            output,
            metrics,
            state: Some(state),
            backend_thread: PhantomData,
        })
    }

    /// Write the next query result through the pre-bound Arrow decoder.
    ///
    /// # Safety
    ///
    /// `slot` must be the live scan slot created by PostgreSQL from this
    /// CustomScan's target list.
    pub unsafe fn next_into_slot(
        &mut self,
        slot: *mut pg_sys::TupleTableSlot,
        datum_context: pg_sys::MemoryContext,
    ) -> Result<bool, QueryExecutionError> {
        let state = self
            .state
            .as_mut()
            .expect("query execution state is active");
        let produced =
            unsafe { state.next_into_slot(&self.output, slot, datum_context) }?;
        Ok(produced)
    }

    /// # Safety
    ///
    /// `changed_parameters` is NULL or the live `PlanState::chgParam` bitmap
    /// supplied during PostgreSQL's `ExecReScan` callback.
    pub unsafe fn rescan(
        &mut self,
        changed_parameters: *mut pg_sys::Bitmapset,
    ) -> Result<(), QueryExecutionError> {
        unsafe {
            self.state
                .as_mut()
                .expect("query execution state is active")
                .rescan(changed_parameters)
        }
    }

    pub fn close(mut self) -> Result<(), QueryExecutionError> {
        self.state
            .take()
            .expect("query execution state is active")
            .close()
    }

    pub fn abort(mut self) {
        if let Some(state) = self.state.take() {
            state.abort();
        }
    }

    pub fn metrics(&self) -> Option<ExecutionMetricsSnapshot> {
        self.metrics.as_ref().map(|metrics| {
            metrics.snapshot(
                self.state
                    .as_ref()
                    .expect("query execution state is active")
                    .peak_reserved(),
            )
        })
    }

    pub fn physical_plan_analyze(
        &self,
        include_timing: bool,
    ) -> Option<PlanExplainNode> {
        self.state
            .as_ref()
            .expect("query execution state is active")
            .physical_plan_analyze(include_timing)
    }

    pub fn physical_plan_explain(&self) -> PlanExplainNode {
        self.state
            .as_ref()
            .expect("query execution state is active")
            .physical_plan_explain()
    }

    pub fn planned_mode(&self) -> QueryExecutionMode {
        if self
            .state
            .as_ref()
            .expect("query execution state is active")
            .planned_parallel()
        {
            QueryExecutionMode::Parallel
        } else {
            QueryExecutionMode::Serial
        }
    }

    pub fn fragment(&self) -> &QueryFragment {
        self.state
            .as_ref()
            .expect("query execution state is active")
            .fragment()
    }
}

impl Drop for QueryExecution {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            let _ = state.close();
        }
    }
}
