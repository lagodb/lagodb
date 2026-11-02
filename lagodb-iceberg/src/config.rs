//! Configuration shared by the Iceberg AM and foreign-table adapters.

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Buffered-row memory threshold (in MiB) that triggers a mutation write flush.
///
/// This bounds the shared write engine's row buffer; rolling Parquet file size
/// remains the responsibility of the file writer.
static MUTATION_BUFFER_FLUSH_MB: GucSetting<i32> = GucSetting::<i32>::new(64);

/// Floor on the estimated fraction of an Iceberg relation that a scan cost
/// model assumes will be read.
///
/// At path stage both the managed-table and FDW adapters estimate physical
/// scan volume from the costed-pruning predicates (see
/// [`crate::scan::ScanVolume`]). PostgreSQL
/// selectivity can become implausibly small when statistics are absent or
/// weak, so the floor prevents those estimates from making an Iceberg scan
/// look almost free. It is a costing guard, not an output-row correction.
static SCAN_MIN_FRACTION: GucSetting<f64> = GucSetting::<f64>::new(0.02);

/// Multiple of PostgreSQL's CPU unit prices charged for each row the Iceberg
/// reader decodes and filters before it returns rows to PostgreSQL.
///
/// Measured on PostgreSQL 18 against query offload: selective scans of 1-4
/// million rows ran 15-30% slower through this reader than through the
/// offload engine. Of 0.1, 0.3 and 1.0, 0.3 matched the faster path on the
/// most evaluated points in 6 of 8 combinations of offload setup cost
/// (20-90) and CPU multiplier (0.05-0.1), and on at most 3 fewer in the
/// others; at the offload defaults it matched 11 more points than 0.1.
static SCAN_CPU_COST_MULTIPLIER: GucSetting<f64> = GucSetting::<f64>::new(0.3);

pub(crate) fn init() {
    GucRegistry::define_int_guc(
        c"lagodb_iceberg.mutation_buffer_flush_mb",
        c"Buffered-row memory threshold (MiB) that triggers a mutation write flush",
        c"Controls in-process memory pressure on the shared row buffer used by the Iceberg AM and writable Iceberg foreign tables. This does not control produced Parquet file size.",
        &MUTATION_BUFFER_FLUSH_MB,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        c"lagodb_iceberg.scan_min_fraction",
        c"Floor on the estimated fraction read by Iceberg scan cost models",
        c"The managed-table and foreign-table path cost models multiply their relation baseline by the share costed-pruning predicates leave to scan. This GUC clamps that fraction from below so weak or absent PostgreSQL statistics cannot make a scan look almost free. Raise it for more conservative costing; lower it toward 0.0 to trust raw selectivity.",
        &SCAN_MIN_FRACTION,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_float_guc(
        c"lagodb_iceberg.scan_cpu_cost_multiplier",
        c"Multiple of cpu_tuple_cost and cpu_operator_cost charged per row the Iceberg reader reads",
        c"Managed-table and foreign-table scan paths charge each row the reader decodes, and the pushed filters it evaluates, at this multiple of the session's PostgreSQL CPU costs; rows returned to PostgreSQL keep the full cpu_tuple_cost.",
        &SCAN_CPU_COST_MULTIPLIER,
        0.0,
        1_000.0,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Returns the range-checked mutation buffer threshold in bytes.
pub(crate) fn mutation_buffer_flush_bytes() -> usize {
    MUTATION_BUFFER_FLUSH_MB.get() as usize * 1024 * 1024
}

/// `lagodb_iceberg.scan_min_fraction`.
pub(crate) fn scan_min_fraction() -> f64 {
    SCAN_MIN_FRACTION.get()
}

/// `lagodb_iceberg.scan_cpu_cost_multiplier`.
pub(crate) fn scan_cpu_cost_multiplier() -> f64 {
    SCAN_CPU_COST_MULTIPLIER.get()
}
