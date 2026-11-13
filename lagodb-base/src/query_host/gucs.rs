//! GUCs and work-memory budget for query-subtree offload.
//!
//! A cached plan keeps the settings it was planned and costed with, while
//! executor settings follow the session values in effect when the scan starts.

use lagodb_query::datafusion::QueryExecutionLimits;
use lagodb_query::{
    DEFAULT_MAXIMUM_BATCH_ROWS, ExecutionProfile, MAXIMUM_BATCH_ROWS_LIMIT,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum, pg_sys};

use super::error::QueryHostError;

static QUERY_OFFLOAD_MODE: GucSetting<QueryOffloadMode> =
    GucSetting::<QueryOffloadMode>::new(QueryOffloadMode::Off);
static QUERY_BATCH_ROWS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_MAXIMUM_BATCH_ROWS);
// Setup 40 and multiplier 0.05 follow the x86-64 measurements in
// docs/query-offload-auto-cost-model.md (sections 6.2 and 6.3).
static QUERY_OFFLOAD_SETUP_COST: GucSetting<f64> = GucSetting::<f64>::new(40.0);
static QUERY_OFFLOAD_CPU_COST_MULTIPLIER: GucSetting<f64> =
    GucSetting::<f64>::new(0.05);
static QUERY_OFFLOAD_PARALLEL_MIN_SCAN_ROWS: GucSetting<i32> =
    GucSetting::<i32>::new(500_000);
static QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE: GucSetting<i32> =
    GucSetting::<i32>::new(8 * 1_024 * 1_024);
static QUERY_OFFLOAD_SPILL: GucSetting<bool> = GucSetting::<bool>::new(true);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
pub(super) enum QueryOffloadMode {
    Off,
    Auto,
    Force,
}

impl QueryOffloadMode {
    pub(super) fn current() -> Self {
        QUERY_OFFLOAD_MODE.get()
    }
}

/// Settings read when one offload path is costed and encoded into its plan.
#[derive(Clone, Copy)]
pub(super) struct QueryPlanSettings {
    pub(super) execution: ExecutionProfile,
    pub(super) setup_cost: f64,
    pub(super) cpu_cost_multiplier: f64,
    pub(super) spill: bool,
}

impl QueryPlanSettings {
    pub(super) fn current() -> Self {
        let maximum_batch_rows = usize::try_from(QUERY_BATCH_ROWS.get())
            .expect("query_batch_rows GUC enforces a positive i32 value");
        Self {
            execution: ExecutionProfile::try_new(maximum_batch_rows)
                .expect("query_batch_rows GUC range matches ExecutionProfile"),
            setup_cost: QUERY_OFFLOAD_SETUP_COST.get(),
            cpu_cost_multiplier: QUERY_OFFLOAD_CPU_COST_MULTIPLIER.get(),
            spill: QUERY_OFFLOAD_SPILL.get(),
        }
    }
}

/// Settings read when an offload scan starts executing.
#[derive(Clone, Copy)]
pub(super) struct QueryExecutionSettings {
    pub(super) spill: bool,
    pub(super) parallel_min_scan_rows: f64,
    pub(super) parallel_queue_size: usize,
}

impl QueryExecutionSettings {
    pub(super) fn current() -> Self {
        Self {
            spill: QUERY_OFFLOAD_SPILL.get(),
            parallel_min_scan_rows: f64::from(
                QUERY_OFFLOAD_PARALLEL_MIN_SCAN_ROWS.get(),
            ),
            parallel_queue_size: usize::try_from(
                QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE.get(),
            )
            .expect(
                "query_offload_parallel_queue_size GUC enforces a positive i32 value",
            ),
        }
    }

    /// The complete budget belongs to the DataFusion execution pool; provider
    /// libraries are outside the engine's memory-accounting contract.
    pub(super) fn limits(
        self,
        execution: ExecutionProfile,
    ) -> Result<QueryExecutionLimits, QueryHostError> {
        // SAFETY: PostgreSQL derives this limit from work_mem and
        // hash_mem_multiplier; both are backend-local.
        let hash_memory_bytes = unsafe { pg_sys::get_hash_memory_limit() };
        QueryExecutionLimits::try_new(
            WorkMemBudget::bytes(),
            hash_memory_bytes,
            execution,
            self.spill,
        )
        .map_err(QueryHostError::from)
    }
}

pub(super) struct WorkMemBudget;

impl WorkMemBudget {
    /// PostgreSQL defines `work_mem` in KiB and bounds it to
    /// `64..=MAX_KILOBYTES` (`utils/guc.h`), so the byte count fits a `size_t`.
    pub(super) fn bytes() -> usize {
        // SAFETY: `work_mem` is a backend-local GUC read on the backend thread.
        let kib = unsafe { pg_sys::work_mem };
        kib as usize * 1_024
    }
}

pub(super) fn define() {
    GucRegistry::define_enum_guc(
        c"lagodb.query_offload_mode",
        c"Path-emission mode for LagoDB query-subtree offload",
        c"off disables query offload, auto uses cost, and force only biases otherwise legal paths for tests.",
        &QUERY_OFFLOAD_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.query_batch_rows",
        c"Maximum rows in one LagoDB query-engine batch",
        c"The value is captured when an offload path is planned and remains fixed for execution of that plan.",
        &QUERY_BATCH_ROWS,
        1,
        MAXIMUM_BATCH_ROWS_LIMIT,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        c"lagodb.query_offload_setup_cost",
        c"Planner's estimate of the cost of setting up the query-offload engine",
        c"Charged once per execution to the startup cost of every query-offload path.",
        &QUERY_OFFLOAD_SETUP_COST,
        0.0,
        f64::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        c"lagodb.query_offload_cpu_cost_multiplier",
        c"Multiple of cpu_tuple_cost and cpu_operator_cost charged by query-offload paths",
        c"Scales the per-row and per-operator CPU costs of query-offload paths relative to the session's PostgreSQL CPU costs; page I/O costs are not scaled.",
        &QUERY_OFFLOAD_CPU_COST_MULTIPLIER,
        0.0,
        1_000.0,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.query_offload_parallel_min_scan_rows",
        c"Minimum estimated table-scan row count for parallel query-offload execution",
        c"Parallel query offload engages only when the largest table scan is estimated to examine at least this many rows after provider pruning; set to 0 to disable the size gate.",
        &QUERY_OFFLOAD_PARALLEL_MIN_SCAN_ROWS,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb.query_offload_parallel_queue_size",
        c"Transport budget for parallel query-offload shuffles",
        c"Target size of the shared-memory rings one parallel run allocates, divided across the 2*W*W rings of its launch width; a floor per ring keeps small budgets usable, so the run may allocate more than this.",
        &QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE,
        64 * 1_024,
        1_024 * 1_024 * 1_024,
        GucContext::Userset,
        GucFlags::UNIT_BYTE,
    );
    GucRegistry::define_bool_guc(
        c"lagodb.query_offload_spill",
        c"Allow query offload operators to spill to PostgreSQL temporary files",
        c"When off, query offload never writes spill files and its memory has no limit, so a large query can exhaust server memory and restart every backend.",
        &QUERY_OFFLOAD_SPILL,
        GucContext::Suset,
        GucFlags::default(),
    );
}
