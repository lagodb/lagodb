//! Query-offload execution timing and EXPLAIN rendering.

use std::ffi::{CStr, CString};
use std::ptr;
use std::time::{Duration, Instant};

use lagodb_core::expr::explain::{FILTER, PUSHED_FILTER, PUSHED_FILTER_CONSERVATIVE};
use lagodb_core::query_contract::{ProviderId, ScanEstimate, ScanId};
use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::ExecutionMetricsSnapshot;
use lagodb_query::plan::{PlannedTableScan, QueryPlanSummary};
use pgrx::pg_sys;

use super::error::QueryHostError;

const PROP_ENGINE: &CStr = c"Engine";
const PROP_MODE: &CStr = c"Offload";
const PROP_PROVIDER_ID: &CStr = c"Provider ID";
const PROP_SCAN_ID: &CStr = c"Scan ID";
const PROP_ESTIMATED_ROWS: &CStr = c"Estimated Scan Rows";
const PROP_ESTIMATED_SCAN_BYTES: &CStr = c"Estimated Scan Bytes";
const PROP_MAXIMUM_BATCH_ROWS: &CStr = c"Maximum Batch Rows";
const PROP_GROUP_KEYS: &CStr = c"Group Keys";
const PROP_DISTINCT_KEYS: &CStr = c"DISTINCT Keys";
const PROP_COUNT_STAR: &CStr = c"COUNT(*) Aggregates";
const PROP_COUNT_EXPR: &CStr = c"COUNT(expr) Aggregates";
const PROP_MIN: &CStr = c"MIN Aggregates";
const PROP_MAX: &CStr = c"MAX Aggregates";
const PROP_SUM: &CStr = c"SUM Aggregates";
const PROP_AVG: &CStr = c"AVG Aggregates";
const PROP_VARIANCE: &CStr = c"VARIANCE Aggregates";
const PROP_STDDEV: &CStr = c"STDDEV Aggregates";
const PROP_BOOLEAN: &CStr = c"Boolean Aggregates";
const PROP_ARRAY_AGG: &CStr = c"ARRAY_AGG Aggregates";
const PROP_STRING_AGG: &CStr = c"STRING_AGG Aggregates";
const PROP_DISTINCT_AGGREGATES: &CStr = c"DISTINCT Aggregates";
const PROP_ORDERED_AGGREGATES: &CStr = c"Ordered Aggregates";
const PROP_AGGREGATE_FILTERS: &CStr = c"Aggregate FILTERs";
const PROP_HAVING_FILTERS: &CStr = c"HAVING Filters";
const PROP_POSTGRES_EXPR_FALLBACKS: &CStr = c"PostgreSQL Expression Fallbacks";
const PROP_INPUT_BATCHES: &CStr = c"Input Batches";
const PROP_INPUT_ROWS: &CStr = c"Input Rows";
const PROP_ARROW_BATCH_BYTES: &CStr = c"Arrow Batch Bytes";
const PROP_OUTPUT_ROWS: &CStr = c"Output Rows";
const PROP_ENGINE_PEAK_MEMORY: &CStr = c"Engine Peak Memory Bytes";
const PROP_ENGINE_OPERATORS: &CStr = c"Engine Operators";
const PROP_OFFLOAD_WALL_TIME: &CStr = c"Offload Wall Time";
const PROP_OFFLOAD_CPU_TIME: &CStr = c"Offload CPU Time";
const GROUP_SCAN: &CStr = c"Table Scan";
const GROUP_SCANS: &CStr = c"Table Scans";
const MILLISECONDS: &CStr = c"ms";
const ENGINE_NAME: &CStr = c"DataFusion";
const MODE_NAME: &CStr = c"Serial Query";

struct ScanPlanSummary {
    provider: ProviderId,
    scan: ScanId,
    estimate: ScanEstimate,
    filter: Option<ScanFilterSummary>,
}

struct ScanFilterSummary {
    exact_expression: CString,
    pushed_expression: Option<CString>,
}

impl ScanFilterSummary {
    /// Emit the exact query-engine residual and the separately negotiated
    /// provider pruning predicate.
    unsafe fn emit(&self, verbose: bool, explain: *mut pg_sys::ExplainState) {
        unsafe {
            pg_sys::ExplainPropertyText(
                FILTER.as_ptr(),
                self.exact_expression.as_ptr(),
                explain,
            );
        }
        if let Some(pushed_expression) = &self.pushed_expression {
            let label = if verbose {
                PUSHED_FILTER_CONSERVATIVE
            } else {
                PUSHED_FILTER
            };
            unsafe {
                pg_sys::ExplainPropertyText(
                    label.as_ptr(),
                    pushed_expression.as_ptr(),
                    explain,
                );
            }
        }
    }
}

struct ExecutionTimer {
    wall_started: Instant,
    cpu_started: Option<Duration>,
}

impl ExecutionTimer {
    fn start() -> Self {
        Self {
            wall_started: Instant::now(),
            cpu_started: Self::thread_cpu_time(),
        }
    }

    fn wall_millis(&self) -> f64 {
        self.wall_started.elapsed().as_secs_f64() * 1_000.0
    }

    fn cpu_millis(&self) -> Option<f64> {
        let elapsed = Self::thread_cpu_time()?.checked_sub(self.cpu_started?)?;
        Some(elapsed.as_secs_f64() * 1_000.0)
    }

    fn thread_cpu_time() -> Option<Duration> {
        let mut value = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `value` is writable and the clock identifier has no
        // ownership or lifetime requirements.
        let status =
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) };
        (status == 0 && value.tv_sec >= 0 && value.tv_nsec >= 0)
            .then(|| Duration::new(value.tv_sec as u64, value.tv_nsec as u32))
    }
}

pub(super) struct QueryOffloadExplain {
    scans: Option<Box<[ScanPlanSummary]>>,
    execution: Option<ExecutionProfile>,
    plan: Option<QueryPlanSummary>,
    timer: Option<ExecutionTimer>,
}

impl QueryOffloadExplain {
    pub(super) const fn new() -> Self {
        Self {
            scans: None,
            execution: None,
            plan: None,
            timer: None,
        }
    }

    pub(super) fn start_execution(&mut self) {
        self.timer = Some(ExecutionTimer::start());
    }

    pub(super) fn record_plan(
        &mut self,
        scans: &[PlannedTableScan<'_>],
        execution: ExecutionProfile,
        plan: QueryPlanSummary,
    ) {
        self.scans = Some(
            scans
                .iter()
                .map(|scan| ScanPlanSummary {
                    provider: scan.provider(),
                    scan: scan.scan(),
                    estimate: scan.estimate(),
                    filter: scan.filter_explain().map(|filter| ScanFilterSummary {
                        exact_expression: filter.exact_expression().to_owned(),
                        pushed_expression: filter
                            .pushed_expression()
                            .map(CStr::to_owned),
                    }),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        self.execution = Some(execution);
        self.plan = Some(plan);
    }

    /// Render planned properties for plain EXPLAIN and add actual properties
    /// only when normal execution installed the engine.
    ///
    /// # Safety
    ///
    /// `explain` must be the live `ExplainState` passed to the CustomScan
    /// callback by PostgreSQL.
    pub(super) unsafe fn emit(
        &self,
        metrics: Option<&ExecutionMetricsSnapshot>,
        physical_operators: Option<&CStr>,
        explain: *mut pg_sys::ExplainState,
    ) -> Result<(), QueryHostError> {
        let scans = self.scans.as_ref().ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        let execution = self.execution.ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        let plan = self.plan.ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before query-offload Begin",
        ))?;
        // SAFETY: the callback supplies the live ExplainState and all names
        // and values remain live for each synchronous PostgreSQL call.
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
            pg_sys::ExplainPropertyUInteger(
                PROP_MAXIMUM_BATCH_ROWS.as_ptr(),
                ptr::null(),
                u64::try_from(execution.maximum_batch_rows().get())
                    .expect("validated batch-row limit fits u64"),
                explain,
            );
            Self::emit_plan(plan, explain);
            self.emit_scans(scans, explain);
            if let Some(metrics) = metrics {
                self.emit_actual(metrics, explain);
            }
            if let Some(physical_operators) = physical_operators {
                pg_sys::ExplainPropertyText(
                    PROP_ENGINE_OPERATORS.as_ptr(),
                    physical_operators.as_ptr(),
                    explain,
                );
            }
            if let Some(timer) = &self.timer {
                pg_sys::ExplainPropertyFloat(
                    PROP_OFFLOAD_WALL_TIME.as_ptr(),
                    MILLISECONDS.as_ptr(),
                    timer.wall_millis(),
                    3,
                    explain,
                );
                if let Some(cpu_millis) = timer.cpu_millis() {
                    pg_sys::ExplainPropertyFloat(
                        PROP_OFFLOAD_CPU_TIME.as_ptr(),
                        MILLISECONDS.as_ptr(),
                        cpu_millis,
                        3,
                        explain,
                    );
                }
            }
        }
        Ok(())
    }

    unsafe fn emit_plan(plan: QueryPlanSummary, explain: *mut pg_sys::ExplainState) {
        let properties = [
            (PROP_GROUP_KEYS, plan.group_keys()),
            (PROP_DISTINCT_KEYS, plan.distinct_keys()),
            (PROP_COUNT_STAR, plan.count_star()),
            (PROP_COUNT_EXPR, plan.count_expr()),
            (PROP_MIN, plan.min()),
            (PROP_MAX, plan.max()),
            (PROP_SUM, plan.sum()),
            (PROP_AVG, plan.avg()),
            (PROP_VARIANCE, plan.variance()),
            (PROP_STDDEV, plan.stddev()),
            (PROP_BOOLEAN, plan.boolean()),
            (PROP_ARRAY_AGG, plan.array_agg()),
            (PROP_STRING_AGG, plan.string_agg()),
            (PROP_DISTINCT_AGGREGATES, plan.distinct_aggregates()),
            (PROP_ORDERED_AGGREGATES, plan.ordered_aggregates()),
            (PROP_AGGREGATE_FILTERS, plan.aggregate_filters()),
            (PROP_HAVING_FILTERS, plan.having_filters()),
            (
                PROP_POSTGRES_EXPR_FALLBACKS,
                plan.postgres_expression_fallbacks(),
            ),
        ];
        for (name, value) in properties {
            if value != 0 || name == PROP_POSTGRES_EXPR_FALLBACKS {
                unsafe {
                    pg_sys::ExplainPropertyUInteger(
                        name.as_ptr(),
                        ptr::null(),
                        value as u64,
                        explain,
                    );
                }
            }
        }
    }

    unsafe fn emit_scans(
        &self,
        scans: &[ScanPlanSummary],
        explain: *mut pg_sys::ExplainState,
    ) {
        let verbose = unsafe { (*explain).verbose };
        unsafe {
            pg_sys::ExplainOpenGroup(
                GROUP_SCAN.as_ptr(),
                GROUP_SCANS.as_ptr(),
                false,
                explain,
            );
            for summary in scans {
                pg_sys::ExplainOpenGroup(
                    GROUP_SCAN.as_ptr(),
                    ptr::null(),
                    true,
                    explain,
                );
                pg_sys::ExplainPropertyUInteger(
                    PROP_PROVIDER_ID.as_ptr(),
                    ptr::null(),
                    summary.provider.index() as u64,
                    explain,
                );
                pg_sys::ExplainPropertyUInteger(
                    PROP_SCAN_ID.as_ptr(),
                    ptr::null(),
                    summary.scan.index() as u64,
                    explain,
                );
                pg_sys::ExplainPropertyFloat(
                    PROP_ESTIMATED_ROWS.as_ptr(),
                    ptr::null(),
                    summary.estimate.estimated_rows(),
                    0,
                    explain,
                );
                pg_sys::ExplainPropertyFloat(
                    PROP_ESTIMATED_SCAN_BYTES.as_ptr(),
                    ptr::null(),
                    summary.estimate.estimated_scan_bytes(),
                    0,
                    explain,
                );
                if let Some(filter) = &summary.filter {
                    filter.emit(verbose, explain);
                }
                pg_sys::ExplainCloseGroup(
                    GROUP_SCAN.as_ptr(),
                    ptr::null(),
                    true,
                    explain,
                );
            }
            pg_sys::ExplainCloseGroup(
                GROUP_SCAN.as_ptr(),
                GROUP_SCANS.as_ptr(),
                false,
                explain,
            );
        }
    }

    unsafe fn emit_actual(
        &self,
        metrics: &ExecutionMetricsSnapshot,
        explain: *mut pg_sys::ExplainState,
    ) {
        // SAFETY: caller upholds the same live ExplainState contract as emit.
        unsafe {
            pg_sys::ExplainPropertyUInteger(
                PROP_INPUT_BATCHES.as_ptr(),
                ptr::null(),
                metrics.input_batches,
                explain,
            );
            pg_sys::ExplainPropertyUInteger(
                PROP_INPUT_ROWS.as_ptr(),
                ptr::null(),
                metrics.input_rows,
                explain,
            );
            pg_sys::ExplainPropertyUInteger(
                PROP_ARROW_BATCH_BYTES.as_ptr(),
                ptr::null(),
                metrics.arrow_batch_bytes,
                explain,
            );
            pg_sys::ExplainPropertyUInteger(
                PROP_OUTPUT_ROWS.as_ptr(),
                ptr::null(),
                metrics.output_rows,
                explain,
            );
            pg_sys::ExplainPropertyUInteger(
                PROP_ENGINE_PEAK_MEMORY.as_ptr(),
                ptr::null(),
                metrics.engine_peak_memory_bytes,
                explain,
            );
        }
    }
}
