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
/// scan volume from the selectivity of costed-pruning predicates. PostgreSQL
/// selectivity can become implausibly small when statistics are absent or
/// weak, so the floor prevents those estimates from making an Iceberg scan
/// look almost free. It is a costing guard, not an output-row correction.
static SCAN_MIN_FRACTION: GucSetting<f64> = GucSetting::<f64>::new(0.02);

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
        c"The managed-table and foreign-table path cost models multiply their relation baseline by the selectivity of costed-pruning predicates. This GUC clamps that fraction from below so weak or absent PostgreSQL statistics cannot make a scan look almost free. Raise it for more conservative costing; lower it toward 0.0 to trust raw selectivity.",
        &SCAN_MIN_FRACTION,
        0.0,
        1.0,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Returns the range-checked mutation buffer threshold in bytes.
pub(crate) fn mutation_buffer_flush_bytes() -> usize {
    MUTATION_BUFFER_FLUSH_MB.get() as usize * 1024 * 1024
}

/// Clamp a costed-pruning selectivity into the physical scan fraction shared
/// by the managed-table and FDW cost models.
///
/// `selectivity` is expected in `[0.0, 1.0]`; the result is always in that
/// range and is floored by `lagodb_iceberg.scan_min_fraction`.
pub(crate) fn scan_fraction(selectivity: f64) -> f64 {
    clamp_scan_fraction(selectivity, SCAN_MIN_FRACTION.get())
}

fn clamp_scan_fraction(selectivity: f64, min_fraction: f64) -> f64 {
    let selectivity = selectivity.clamp(0.0, 1.0);
    let min_fraction = min_fraction.clamp(0.0, 1.0);
    selectivity.clamp(min_fraction, 1.0)
}

#[cfg(test)]
mod tests {
    use super::clamp_scan_fraction;

    #[test]
    fn selectivity_passes_through_above_floor() {
        assert!((clamp_scan_fraction(0.4, 0.02) - 0.4).abs() < 1e-9);
    }

    #[test]
    fn floor_clamps_tiny_selectivity() {
        assert!((clamp_scan_fraction(1e-7, 0.02) - 0.02).abs() < 1e-9);
    }

    #[test]
    fn zero_floor_trusts_raw_selectivity() {
        assert!((clamp_scan_fraction(1e-7, 0.0) - 1e-7).abs() < 1e-12);
    }

    #[test]
    fn out_of_range_inputs_are_clamped() {
        assert!((clamp_scan_fraction(2.0, 0.02) - 1.0).abs() < 1e-9);
        assert!(clamp_scan_fraction(-1.0, 0.02) >= 0.0);
        assert!(clamp_scan_fraction(0.5, 2.0) <= 1.0);
    }
}
