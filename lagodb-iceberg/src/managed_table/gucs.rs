use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Maximum number of retries for optimistic concurrency control (CAS) loops.
static MAX_COMMIT_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(100);

static VACUUM_COMPACT_DATA_FILES: GucSetting<bool> = GucSetting::<bool>::new(true);
static VACUUM_ORPHAN_RETENTION_S: GucSetting<i32> = GucSetting::<i32>::new(259_200);
static AUTO_MAINTENANCE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static AUTO_MAINTENANCE_NAPTIME_S: GucSetting<i32> = GucSetting::<i32>::new(300);
static AUTO_MAINTENANCE_MAX_TABLES: GucSetting<i32> = GucSetting::<i32>::new(32);

/// Maximum number of Iceberg data files opened by one bounded ANALYZE sample.
/// Rows within the selected files are sampled using manifest record counts.
static ANALYZE_MAX_DATA_FILES: GucSetting<i32> = GucSetting::<i32>::new(32);

/// Floor on the estimated fraction of a relation that the CustomScan cost
/// model assumes will be scanned (see `customscan::provider::create_path`).
///
/// ## What the fraction is
///
/// At path stage the provider estimates how much of a relation survives
/// pushdown as `clauselist_selectivity(costed-pruning pushed clauses)` and multiplies the
/// relation's baseline `(pages, tuples)` by it to produce
/// `scanned_pages` / `scanned_tuples`. This mirrors PG's own IndexScan cost
/// model (`scanned = selectivity * total`).
///
/// ## Why a floor is needed
///
/// The selectivity is only as trustworthy as PG's local statistics, and in
/// several common cases it collapses to a value with no factual basis:
///
/// - the relation has never been ANALYZEd (no `pg_statistic` rows), so
///   `clauselist_selectivity` returns PG's hard-coded defaults
///   (`DEFAULT_EQ_SEL = 0.005`, etc.);
/// - several pushed clauses are multiplied under PG's independence
///   assumption and the product collapses toward zero (e.g. `0.005^3`);
/// - a constant falls outside the histogram range and the estimate trends
///   to zero.
///
/// Using such a value directly would drive the CustomScan disk cost to
/// (near) zero and make the planner pick it on a pure guess. This floor
/// clamps the fraction from below so a bogus selectivity can never make the
/// scan look almost free:
///
/// ```text
/// fraction = clamp(selectivity, min_scan_fraction, 1.0)
/// ```
///
/// It is a worst-case guard, not a model correction: it only bites when the
/// selectivity is implausibly small. Raise it to be more conservative about
/// selecting CustomScan on weak statistics; lower it toward `0.0` to trust
/// the raw selectivity. ANALYZE improves selectivity quality for Iceberg
/// tables, so this floor should rarely trigger after statistics are present,
/// but it remains cheap insurance for stale or weak statistics.
static MIN_SCAN_FRACTION: GucSetting<f64> = GucSetting::<f64>::new(0.02);

pub fn init() {
    GucRegistry::define_int_guc(
        c"lagodb_iceberg.analyze_max_data_files",
        c"Maximum Iceberg data files sampled by ANALYZE",
        c"ANALYZE uses manifest record counts to build a fixed-size self-weighting sample while bounding file-level I/O locality.",
        &ANALYZE_MAX_DATA_FILES,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"lagodb_iceberg.auto_maintenance_enabled",
        c"Enable Iceberg logical-table automatic maintenance",
        c"The maintenance worker uses one short transaction per selected table.",
        &AUTO_MAINTENANCE_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb_iceberg.auto_maintenance_naptime_s",
        c"Delay before changed Iceberg tables become eligible for maintenance",
        c"Also provides the retry delay after a skipped or failed table attempt.",
        &AUTO_MAINTENANCE_NAPTIME_S,
        10,
        86_400,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb_iceberg.auto_maintenance_max_tables",
        c"Maximum Iceberg tables processed in one maintenance invocation",
        c"Bounds transient worker duration and memory use.",
        &AUTO_MAINTENANCE_MAX_TABLES,
        1,
        10_000,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"lagodb_iceberg.vacuum_compact_data_files",
        c"Compact eligible data files during ordinary VACUUM",
        c"VACUUM FULL always uses the exhaustive compaction profile.",
        &VACUUM_COMPACT_DATA_FILES,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"lagodb_iceberg.vacuum_orphan_retention_s",
        c"Minimum age in seconds for VACUUM FULL orphan removal",
        c"The hard minimum is one day; newly-created and reachable objects are preserved.",
        &VACUUM_ORPHAN_RETENTION_S,
        86_400,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb_iceberg.max_commit_retries",
        c"Maximum number of retries for optimistic concurrency control commits",
        c"When concurrent updates occur, lagodb-iceberg retries the commit. This GUC limits the number of retries.",
        &MAX_COMMIT_RETRIES,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"lagodb_iceberg.customscan_min_scan_fraction",
        c"Floor on the estimated scanned fraction used by the CustomScan cost model",
        c"At path stage the CustomScan cost model multiplies the relation's baseline \
          (pages, tuples) by the costed-pruning pushed-clause selectivity to estimate the scanned \
          volume (like PG's IndexScan model). This GUC clamps that fraction from below \
          so an implausibly small selectivity - e.g. from a never-ANALYZEd table, from \
          multiplying several clauses under PG's independence assumption, or from a \
          constant outside the histogram range - cannot drive the CustomScan disk cost \
          to near zero and make the planner pick it on a pure guess. It is a worst-case \
          guard, not a model correction: it only bites when the selectivity is \
          implausibly small. Raise it to be more conservative on weak statistics; lower \
          it toward 0.0 to trust the raw selectivity.",
        &MIN_SCAN_FRACTION,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );
}

pub(crate) fn auto_maintenance_enabled() -> bool {
    AUTO_MAINTENANCE_ENABLED.get()
}

pub(crate) fn analyze_max_data_files() -> usize {
    ANALYZE_MAX_DATA_FILES.get() as usize
}

pub(crate) fn auto_maintenance_naptime() -> std::time::Duration {
    std::time::Duration::from_secs(AUTO_MAINTENANCE_NAPTIME_S.get() as u64)
}

pub(crate) fn auto_maintenance_max_tables() -> usize {
    AUTO_MAINTENANCE_MAX_TABLES.get() as usize
}

pub fn vacuum_compact_data_files() -> bool {
    VACUUM_COMPACT_DATA_FILES.get()
}

pub fn vacuum_orphan_retention_ms() -> i64 {
    i64::from(VACUUM_ORPHAN_RETENTION_S.get())
        .checked_mul(1_000)
        .expect("vacuum_orphan_retention_s is range checked")
}

pub fn max_commit_retries() -> i32 {
    MAX_COMMIT_RETRIES.get()
}

/// Clamp a raw costed-pruning selectivity into the scanned fraction used by
/// the CustomScan cost model.
///
/// The fraction is the selectivity itself (matching PG's IndexScan cost
/// model, `scanned = selectivity * total`), floored by
/// `lagodb_iceberg.customscan_min_scan_fraction` so an implausibly small
/// selectivity cannot make the scan look almost free. See the
/// [`MIN_SCAN_FRACTION`] docs for the rationale.
///
/// `sel` is expected in `[0.0, 1.0]`; the result is always in `[0.0, 1.0]`.
pub fn scan_fraction(sel: f64) -> f64 {
    clamp_scan_fraction(sel, MIN_SCAN_FRACTION.get())
}

/// Pure clamp used by [`scan_fraction`], split out so the cost-model math is
/// unit-testable without a live GUC runtime.
fn clamp_scan_fraction(sel: f64, min_fraction: f64) -> f64 {
    let sel = sel.clamp(0.0, 1.0);
    let min_fraction = min_fraction.clamp(0.0, 1.0);
    sel.clamp(min_fraction, 1.0)
}

#[cfg(test)]
mod tests {
    use super::clamp_scan_fraction;

    #[test]
    fn selectivity_passes_through_above_floor() {
        // A plausible selectivity above the floor is used unchanged.
        assert!((clamp_scan_fraction(0.4, 0.02) - 0.4).abs() < 1e-9);
    }

    #[test]
    fn floor_clamps_tiny_selectivity() {
        // A bogus near-zero selectivity is lifted to the floor so the scan
        // never looks almost free.
        assert!((clamp_scan_fraction(1e-7, 0.02) - 0.02).abs() < 1e-9);
    }

    #[test]
    fn zero_floor_trusts_raw_selectivity() {
        // With the floor disabled, the raw selectivity passes through.
        assert!((clamp_scan_fraction(1e-7, 0.0) - 1e-7).abs() < 1e-12);
    }

    #[test]
    fn out_of_range_inputs_are_clamped() {
        assert!((clamp_scan_fraction(2.0, 0.02) - 1.0).abs() < 1e-9);
        assert!(clamp_scan_fraction(-1.0, 0.02) >= 0.0);
        // A floor above any selectivity still caps at 1.0, never above.
        assert!(clamp_scan_fraction(0.5, 2.0) <= 1.0);
    }
}
