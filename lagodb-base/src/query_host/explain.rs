//! Query-offload EXPLAIN lifecycle and presentation policy.

mod expression;
mod plan;
mod renderer;

use std::ffi::CStr;
use std::ptr;

use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::ExecutionMetricsSnapshot;
use lagodb_query::plan::{PlanExplainNode, QueryFragment, SelectedQueryPlan};
use pgrx::pg_sys;

use super::error::QueryHostError;
use plan::{QueryExplainPlan, ScanExplainMetadata};
use renderer::PgExplainTree;

const PROP_ENGINE: &CStr = c"Engine";
const PROP_MODE: &CStr = c"Execution Mode";
const PROP_MAXIMUM_BATCH_ROWS: &CStr = c"Maximum Batch Rows";
const PROP_POSTGRES_EXPR_FALLBACKS: &CStr = c"PostgreSQL Expression Fallbacks";
const PROP_ENGINE_PEAK_MEMORY: &CStr = c"Engine Peak Memory Bytes";
const GROUP_ENGINE_PLAN: &CStr = c"Engine Plan";
const ENGINE_NAME: &CStr = c"DataFusion";
const MODE_NAME: &CStr = c"Serial";

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
    scans: Option<Box<[ScanExplainMetadata]>>,
    execution: Option<ExecutionProfile>,
    explain_only_fragment: Option<QueryFragment>,
}

impl QueryOffloadExplain {
    pub(super) const fn new() -> Self {
        Self {
            scans: None,
            execution: None,
            explain_only_fragment: None,
        }
    }

    /// Capture plan metadata only after PostgreSQL requests EXPLAIN output.
    ///
    /// # Safety
    ///
    /// The selected plan's relation OIDs must retain the locks held by the
    /// current statement while PostgreSQL catalog names are copied.
    pub(super) unsafe fn record_plan(
        &mut self,
        selected: &SelectedQueryPlan<'_>,
        retain_fragment: bool,
    ) {
        let fragment = selected.query().fragment();
        self.scans =
            Some(unsafe { ScanExplainMetadata::capture(selected.scans(), fragment) });
        self.execution = Some(selected.execution_profile());
        self.explain_only_fragment = retain_fragment.then(|| fragment.clone());
    }

    pub(super) const fn has_plan(&self) -> bool {
        self.scans.is_some()
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
        physical_plan: Option<&PlanExplainNode>,
        options: ExplainOptions,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        let scans = self.scans.as_ref().ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        let execution = self.execution.ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        let fragment = fragment.or(self.explain_only_fragment.as_ref()).ok_or(
            QueryHostError::ExecutorContract(
                "ExplainCustomScan was invoked before query-offload Begin",
            ),
        )?;
        let logical_plan =
            QueryExplainPlan::new(fragment, scans, options, metrics).build();

        unsafe {
            pg_sys::ExplainPropertyText(
                PROP_ENGINE.as_ptr(),
                ENGINE_NAME.as_ptr(),
                explain,
            );
            pg_sys::ExplainPropertyText(
                PROP_MODE.as_ptr(),
                MODE_NAME.as_ptr(),
                explain,
            );
            if options.verbose {
                pg_sys::ExplainPropertyUInteger(
                    PROP_MAXIMUM_BATCH_ROWS.as_ptr(),
                    ptr::null(),
                    u64::try_from(execution.maximum_batch_rows().get())
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
                pg_sys::ExplainPropertyUInteger(
                    PROP_ENGINE_PEAK_MEMORY.as_ptr(),
                    ptr::null(),
                    metrics.engine_peak_memory_bytes,
                    explain,
                );
            }
            PgExplainTree::new(&logical_plan).emit_plan(explain)?;
            if options.engine_diagnostics()
                && let Some(physical_plan) = physical_plan
            {
                PgExplainTree::new(physical_plan)
                    .emit_diagnostic(GROUP_ENGINE_PLAN, explain)?;
            }
        }
        Ok(())
    }
}
