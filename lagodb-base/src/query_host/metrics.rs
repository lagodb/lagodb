//! AggregateScan plan identity, execution metrics, and EXPLAIN rendering.

use std::ffi::CStr;
use std::ptr;
use std::time::{Duration, Instant};

use lagodb_core::query_contract::{ProviderId, SourceEstimate, SourceId};
use lagodb_query::ExecutionProfile;
use lagodb_query::datafusion::ExecutionMetricsSnapshot;
use pgrx::pg_sys;

use super::error::QueryHostError;

const PROP_ENGINE: &CStr = c"Engine";
const PROP_MODE: &CStr = c"Offload";
const PROP_PROVIDER_ID: &CStr = c"Provider ID";
const PROP_SOURCE_ID: &CStr = c"Source ID";
const PROP_ESTIMATED_ROWS: &CStr = c"Estimated Source Rows";
const PROP_ESTIMATED_SCAN_BYTES: &CStr = c"Estimated Source Scan Bytes";
const PROP_MAXIMUM_BATCH_ROWS: &CStr = c"Maximum Batch Rows";
const PROP_INPUT_BATCHES: &CStr = c"Input Batches";
const PROP_INPUT_ROWS: &CStr = c"Input Rows";
const PROP_ARROW_BATCH_BYTES: &CStr = c"Arrow Batch Bytes";
const PROP_OUTPUT_ROWS: &CStr = c"Output Rows";
const PROP_ENGINE_PEAK_MEMORY: &CStr = c"Engine Peak Memory Bytes";
const PROP_ENGINE_OPERATORS: &CStr = c"Engine Operators";
const PROP_ENGINE_WALL_TIME: &CStr = c"Engine Wall Time";
const PROP_ENGINE_CPU_TIME: &CStr = c"Engine CPU Time";
const MILLISECONDS: &CStr = c"ms";
const ENGINE_NAME: &CStr = c"DataFusion";
const MODE_NAME: &CStr = c"Scalar COUNT(*)";

#[derive(Clone, Copy)]
struct AggregatePlanSummary {
    provider: ProviderId,
    source: SourceId,
    estimate: SourceEstimate,
    execution: ExecutionProfile,
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

pub(super) struct AggregateExplain {
    summary: Option<AggregatePlanSummary>,
    timer: Option<ExecutionTimer>,
}

impl AggregateExplain {
    pub(super) const fn new() -> Self {
        Self {
            summary: None,
            timer: None,
        }
    }

    pub(super) fn start_execution(&mut self) {
        self.timer = Some(ExecutionTimer::start());
    }

    pub(super) fn record_plan(
        &mut self,
        provider: ProviderId,
        source: SourceId,
        estimate: SourceEstimate,
        execution: ExecutionProfile,
    ) {
        self.summary = Some(AggregatePlanSummary {
            provider,
            source,
            estimate,
            execution,
        });
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
        let summary = self.summary.ok_or(QueryHostError::ExecutorContract(
            "ExplainCustomScan was invoked before AggregateScan Begin",
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
                PROP_PROVIDER_ID.as_ptr(),
                ptr::null(),
                summary.provider.index() as u64,
                explain,
            );
            pg_sys::ExplainPropertyUInteger(
                PROP_SOURCE_ID.as_ptr(),
                ptr::null(),
                summary.source.index() as u64,
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
            pg_sys::ExplainPropertyUInteger(
                PROP_MAXIMUM_BATCH_ROWS.as_ptr(),
                ptr::null(),
                u64::try_from(summary.execution.maximum_batch_rows().get())
                    .expect("validated batch-row limit fits u64"),
                explain,
            );
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
                    PROP_ENGINE_WALL_TIME.as_ptr(),
                    MILLISECONDS.as_ptr(),
                    timer.wall_millis(),
                    3,
                    explain,
                );
                if let Some(cpu_millis) = timer.cpu_millis() {
                    pg_sys::ExplainPropertyFloat(
                        PROP_ENGINE_CPU_TIME.as_ptr(),
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
