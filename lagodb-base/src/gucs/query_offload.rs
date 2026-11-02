//! GUCs that control query-subtree offload planning and execution.

use lagodb_query::{
    DEFAULT_MAXIMUM_BATCH_ROWS, ExecutionProfile, MAXIMUM_BATCH_ROWS_LIMIT,
};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};

static QUERY_OFFLOAD_MODE: GucSetting<QueryOffloadMode> =
    GucSetting::<QueryOffloadMode>::new(QueryOffloadMode::Off);
static QUERY_BATCH_ROWS: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_MAXIMUM_BATCH_ROWS);
// Defaults measured on PostgreSQL 18. Serial Engine Setup Time was 0.5-0.95 ms
// (median 0.66 ms) on an Apple M5 and 0.29-0.87 ms (median 0.55 ms) on an
// x86-64 server, where one cost unit of serial PostgreSQL plans took 7.1 us and
// 12-16 us, so setup is about 93 and 35-45 units; setup 40 follows the x86-64
// server. There, with multiplier 0.05 and the Iceberg scan defaults, the
// cheaper estimate matched the faster path on 157 of 162 managed-table points
// (aggregate, filtered aggregate, DISTINCT and join; serial and parallel; with
// and without spill) and 97 of 104 partitioned foreign-table points (LIST
// equality and day ranges), and every miss differed by under 2 ms. Setup 90
// with multiplier 0.1 matched 149 and 96, and 18 of its 21 misses chose
// PostgreSQL where offload ran faster.
static QUERY_OFFLOAD_SETUP_COST: GucSetting<f64> = GucSetting::<f64>::new(40.0);
static QUERY_OFFLOAD_CPU_COST_MULTIPLIER: GucSetting<f64> =
    GucSetting::<f64>::new(0.05);
static QUERY_OFFLOAD_PARALLEL_MIN_SCAN_ROWS: GucSetting<i32> =
    GucSetting::<i32>::new(500_000);
static QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE: GucSetting<i32> =
    GucSetting::<i32>::new(8 * 1_024 * 1_024);
static QUERY_OFFLOAD_PARALLEL_DEBUG: GucSetting<bool> =
    GucSetting::<bool>::new(false);
static QUERY_OFFLOAD_SPILL: GucSetting<bool> = GucSetting::<bool>::new(true);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PostgresGucEnum)]
pub(crate) enum QueryOffloadMode {
    Off,
    Auto,
    Force,
}

pub(crate) fn query_offload_mode() -> QueryOffloadMode {
    QUERY_OFFLOAD_MODE.get()
}

pub(crate) fn query_execution_profile() -> ExecutionProfile {
    let maximum_batch_rows = usize::try_from(QUERY_BATCH_ROWS.get())
        .expect("query_batch_rows GUC enforces a positive i32 value");
    ExecutionProfile::try_new(maximum_batch_rows)
        .expect("query_batch_rows GUC range matches ExecutionProfile")
}

pub(crate) fn query_offload_setup_cost() -> f64 {
    QUERY_OFFLOAD_SETUP_COST.get()
}

pub(crate) fn query_offload_cpu_cost_multiplier() -> f64 {
    QUERY_OFFLOAD_CPU_COST_MULTIPLIER.get()
}

pub(crate) fn query_offload_parallel_min_scan_rows() -> f64 {
    f64::from(QUERY_OFFLOAD_PARALLEL_MIN_SCAN_ROWS.get())
}

pub(crate) fn query_offload_parallel_queue_size() -> usize {
    usize::try_from(QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE.get())
        .expect("query_offload_parallel_queue_size GUC enforces a positive i32 value")
}

pub(crate) fn query_offload_parallel_debug() -> bool {
    QUERY_OFFLOAD_PARALLEL_DEBUG.get()
}

pub(crate) fn query_offload_spill() -> bool {
    QUERY_OFFLOAD_SPILL.get()
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
        c"Per-inbox ring size for parallel query-offload shuffles",
        c"Each parallel query-offload run allocates one inbox per participant; frames larger than an inbox stream through it in chunks.",
        &QUERY_OFFLOAD_PARALLEL_QUEUE_SIZE,
        64 * 1_024,
        1_024 * 1_024 * 1_024,
        GucContext::Userset,
        GucFlags::UNIT_BYTE,
    );
    GucRegistry::define_bool_guc(
        c"lagodb.query_offload_parallel_debug",
        c"Enable diagnostic safeguards for parallel query-offload execution",
        c"Enables the 30-second worker fragment deadlock detector; intended for diagnosis, not normal query timeouts.",
        &QUERY_OFFLOAD_PARALLEL_DEBUG,
        GucContext::Userset,
        GucFlags::default(),
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
