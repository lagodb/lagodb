use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Maximum number of retries for optimistic concurrency control (CAS) loops.
static MAX_COMMIT_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(100);

/// Buffered-row memory threshold (in MiB) that triggers a mutation write flush.
///
/// This controls in-process memory pressure on the row buffer; it does NOT
/// control the size of produced Parquet data files (the rolling writer owns
/// that).
static MUTATION_BUFFER_FLUSH_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static VACUUM_COMPACT_DATA_FILES: GucSetting<bool> = GucSetting::<bool>::new(true);
static VACUUM_ORPHAN_RETENTION_S: GucSetting<i32> = GucSetting::<i32>::new(259_200);

/// Injection point for testing purposes.
static INJECTION_POINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

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
/// the raw selectivity. Once ANALYZE is implemented for Iceberg tables the
/// selectivity quality improves and this floor will rarely trigger, but it
/// remains a cheap insurance.
static MIN_SCAN_FRACTION: GucSetting<f64> = GucSetting::<f64>::new(0.02);

pub fn init() {
    GucRegistry::define_bool_guc(
        c"pg_iceberg_am.vacuum_compact_data_files",
        c"Compact eligible data files during ordinary VACUUM",
        c"VACUUM FULL always uses the exhaustive compaction profile.",
        &VACUUM_COMPACT_DATA_FILES,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_iceberg_am.vacuum_orphan_retention_s",
        c"Minimum age in seconds for VACUUM FULL orphan removal",
        c"The hard minimum is one day; newly-created and reachable objects are preserved.",
        &VACUUM_ORPHAN_RETENTION_S,
        86_400,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_iceberg_am.max_commit_retries",
        c"Maximum number of retries for optimistic concurrency control commits",
        c"When concurrent updates occur, pg-iceberg-am retries the commit. This GUC limits the number of retries.",
        &MAX_COMMIT_RETRIES,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_iceberg_am.mutation_buffer_flush_mb",
        c"Buffered-row memory threshold (MiB) that triggers a mutation write flush",
        c"Controls in-process memory pressure on the row buffer used during INSERT. \
          When the estimated size of buffered rows reaches this many MiB, the buffer \
          is converted to an Arrow batch and handed to the Parquet writer. \
          This does NOT control the size of produced Parquet data files: the rolling \
          file writer rolls files based on its own target size.",
        &MUTATION_BUFFER_FLUSH_MB,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_iceberg_am.injection_point",
        c"Injection point for testing purposes",
        c"Allows triggering specific failures or panics at predefined points in the code.",
        &INJECTION_POINT,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"pg_iceberg_am.customscan_min_scan_fraction",
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

/// Returns the configured mutation buffer flush threshold in bytes.
///
/// This is the memory-pressure trigger for the row buffer in
/// `IcebergModifyState::buffer_row`, not a Parquet file-size target. The GUC value
/// is in MiB and is clamped to `>= 1` at registration time, so the conversion
/// to bytes cannot overflow `usize` on any supported platform.
pub fn mutation_buffer_flush_bytes() -> usize {
    let mb = MUTATION_BUFFER_FLUSH_MB.get().max(1) as usize;
    mb * 1024 * 1024
}

pub fn injection_point() -> Option<String> {
    INJECTION_POINT
        .get()
        .map(|s| s.to_string_lossy().to_string())
}

pub fn injection_point_matches(name: &str) -> bool {
    INJECTION_POINT
        .get()
        .is_some_and(|s| s.as_c_str().to_bytes() == name.as_bytes())
}

/// Clamp a raw costed-pruning selectivity into the scanned fraction used by
/// the CustomScan cost model.
///
/// The fraction is the selectivity itself (matching PG's IndexScan cost
/// model, `scanned = selectivity * total`), floored by
/// `pg_iceberg_am.customscan_min_scan_fraction` so an implausibly small
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
