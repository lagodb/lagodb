//! Query-offload EXPLAIN lifecycle and presentation policy.

mod expression;
mod plan;
mod renderer;

use std::ffi::CStr;
use std::ptr;

use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::{ExecutionMetricsSnapshot, QueryExecutionMode};
use lagodb_query::plan::{PlanExplainNode, QueryFragment, SelectedQueryPlan};
use pgrx::pg_sys;

use super::error::QueryHostError;
use plan::{QueryExplainPlan, ScanExplainMetadata};
use renderer::PgExplainTree;

const PROP_ENGINE: &CStr = c"Engine";
const PROP_MODE: &CStr = c"Execution Mode";
const PROP_MAXIMUM_BATCH_ROWS: &CStr = c"Maximum Batch Rows";
const PROP_POSTGRES_EXPR_FALLBACKS: &CStr = c"PostgreSQL Expression Fallbacks";
const PROP_LOCAL_ENGINE_PEAK_MEMORY: &CStr = c"Local Engine Peak Memory Bytes";
const PROP_MAX_WORKER_FRAGMENT_PEAK_MEMORY: &CStr =
    c"Maximum Worker Fragment Peak Memory Bytes";
const PROP_MAX_WORKER_PARTICIPANT_PEAK_MEMORY: &CStr =
    c"Maximum Worker Participant Peak Memory Bytes";
const PROP_PARALLEL_WORKERS: &CStr = c"Maximum Launched Workers";
const PROP_PARALLEL_RUNS: &CStr = c"Parallel Runs";
const PROP_WORKER_METRIC_REPORTS: &CStr = c"Worker Metric Reports";
const PROP_EXPECTED_WORKER_METRIC_REPORTS: &CStr = c"Expected Worker Metric Reports";
const GROUP_ENGINE_PLAN: &CStr = c"Engine Plan";
const ENGINE_NAME: &CStr = c"DataFusion";

#[derive(Clone, Copy)]
pub(super) struct ExplainOptions {
    pub(super) verbose: bool,
    pub(super) costs: bool,
    pub(super) analyze: bool,
    pub(super) timing: bool,
}

impl ExplainOptions {
    /// # Safety
    ///
    /// `explain` must be PostgreSQL's live ExplainState for the callback.
    pub(super) unsafe fn from_state(explain: *mut pg_sys::ExplainState) -> Self {
        Self {
            verbose: unsafe { (*explain).verbose },
            costs: unsafe { (*explain).costs },
            analyze: unsafe { (*explain).analyze },
            timing: unsafe { (*explain).timing },
        }
    }

    pub(super) const fn engine_diagnostics(self) -> bool {
        self.verbose && self.analyze
    }
}

pub(super) struct QueryOffloadExplain {
    plan: Option<ExplainPlanSnapshot>,
}

struct ExplainPlanSnapshot {
    fragment: QueryFragment,
    scans: Box<[ScanExplainMetadata]>,
    execution: ExecutionProfile,
}

impl QueryOffloadExplain {
    pub(super) const fn new() -> Self {
        Self { plan: None }
    }

    /// Capture plan metadata only after PostgreSQL requests EXPLAIN output.
    ///
    /// # Safety
    ///
    /// The selected plan's relation OIDs must retain the locks held by the
    /// current statement while PostgreSQL catalog names are copied.
    pub(super) unsafe fn record_plan(&mut self, selected: &SelectedQueryPlan<'_>) {
        let fragment = selected.query().fragment();
        self.plan = Some(ExplainPlanSnapshot {
            fragment: fragment.clone(),
            scans: unsafe {
                ScanExplainMetadata::capture(selected.scans(), fragment)
            },
            execution: selected.execution_profile(),
        });
    }

    pub(super) const fn has_plan(&self) -> bool {
        self.plan.is_some()
    }

    /// Render one primary logical plan and, only for ANALYZE VERBOSE, the
    /// secondary engine diagnostic plan.
    ///
    /// # Safety
    ///
    /// `explain` must be the live `ExplainState` passed to the CustomScan
    /// callback by PostgreSQL.
    pub(super) unsafe fn emit(
        &self,
        fragment: Option<&QueryFragment>,
        metrics: Option<&ExecutionMetricsSnapshot>,
        planned_mode: Option<QueryExecutionMode>,
        physical_plan: Option<&PlanExplainNode>,
        options: ExplainOptions,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        let plan = self.plan.as_ref().ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        let fragment = fragment.unwrap_or(&plan.fragment);
        let logical_plan =
            QueryExplainPlan::new(fragment, &plan.scans, options, metrics).build();
        let mode = if options.analyze {
            let mode = metrics
                .map(ExecutionMetricsSnapshot::execution_mode)
                .filter(|mode| *mode != QueryExecutionMode::NotStarted)
                .or(planned_mode)
                .ok_or(QueryHostError::ExecutorContract(
                    "EXPLAIN ANALYZE has no query execution mode",
                ))?;
            Some(match mode {
                QueryExecutionMode::Serial => c"Serial",
                QueryExecutionMode::Parallel => c"Parallel",
                QueryExecutionMode::Mixed => c"Serial and Parallel",
                QueryExecutionMode::NotStarted => {
                    return Err(QueryHostError::ExecutorContract(
                        "EXPLAIN ANALYZE did not start query execution",
                    ));
                }
            })
        } else {
            None
        };
        unsafe {
            pg_sys::ExplainPropertyText(
                PROP_ENGINE.as_ptr(),
                ENGINE_NAME.as_ptr(),
                explain,
            );
            if let Some(mode) = mode {
                pg_sys::ExplainPropertyText(
                    PROP_MODE.as_ptr(),
                    mode.as_ptr(),
                    explain,
                );
            }
            if options.verbose {
                pg_sys::ExplainPropertyUInteger(
                    PROP_MAXIMUM_BATCH_ROWS.as_ptr(),
                    ptr::null(),
                    u64::try_from(plan.execution.maximum_batch_rows().get())
                        .expect("validated batch-row limit fits u64"),
                    explain,
                );
            }
            let fallbacks = fragment.postgres_fallback_count();
            if fallbacks != 0 {
                pg_sys::ExplainPropertyUInteger(
                    PROP_POSTGRES_EXPR_FALLBACKS.as_ptr(),
                    ptr::null(),
                    u64::try_from(fallbacks)
                        .expect("validated plan fallback count fits in u64"),
                    explain,
                );
            }
            if options.analyze
                && let Some(metrics) = metrics
            {
                if metrics.parallel_runs != 0 {
                    pg_sys::ExplainPropertyUInteger(
                        PROP_PARALLEL_WORKERS.as_ptr(),
                        ptr::null(),
                        metrics.maximum_workers,
                        explain,
                    );
                    pg_sys::ExplainPropertyUInteger(
                        PROP_PARALLEL_RUNS.as_ptr(),
                        ptr::null(),
                        metrics.parallel_runs,
                        explain,
                    );
                    pg_sys::ExplainPropertyUInteger(
                        PROP_WORKER_METRIC_REPORTS.as_ptr(),
                        ptr::null(),
                        metrics.worker_metric_reports,
                        explain,
                    );
                    pg_sys::ExplainPropertyUInteger(
                        PROP_EXPECTED_WORKER_METRIC_REPORTS.as_ptr(),
                        ptr::null(),
                        metrics.expected_worker_metric_reports,
                        explain,
                    );
                    pg_sys::ExplainPropertyUInteger(
                        PROP_MAX_WORKER_FRAGMENT_PEAK_MEMORY.as_ptr(),
                        ptr::null(),
                        metrics.maximum_worker_fragment_peak_memory_bytes,
                        explain,
                    );
                    pg_sys::ExplainPropertyUInteger(
                        PROP_MAX_WORKER_PARTICIPANT_PEAK_MEMORY.as_ptr(),
                        ptr::null(),
                        metrics.maximum_worker_participant_peak_memory_bytes,
                        explain,
                    );
                }
                pg_sys::ExplainPropertyUInteger(
                    PROP_LOCAL_ENGINE_PEAK_MEMORY.as_ptr(),
                    ptr::null(),
                    metrics.local_peak_memory_bytes,
                    explain,
                );
            }
            PgExplainTree::new(&logical_plan).emit_plan(explain)?;
            if let Some(physical_plan) = physical_plan {
                PgExplainTree::new(physical_plan)
                    .emit_diagnostic(GROUP_ENGINE_PLAN, explain)?;
            }
        }
        Ok(())
    }
}
