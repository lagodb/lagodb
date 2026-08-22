//! Configuration shared by the Iceberg AM and foreign-table adapters.

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Buffered-row memory threshold (in MiB) that triggers a mutation write flush.
///
/// This bounds the shared write engine's row buffer; rolling Parquet file size
/// remains the responsibility of the file writer.
static MUTATION_BUFFER_FLUSH_MB: GucSetting<i32> = GucSetting::<i32>::new(64);

pub(crate) fn init() {
    GucRegistry::define_int_guc(
        c"pg_iceberg_am.mutation_buffer_flush_mb",
        c"Buffered-row memory threshold (MiB) that triggers a mutation write flush",
        c"Controls in-process memory pressure on the shared row buffer used by the Iceberg AM and writable Iceberg foreign tables. This does not control produced Parquet file size.",
        &MUTATION_BUFFER_FLUSH_MB,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Returns the range-checked mutation buffer threshold in bytes.
pub(crate) fn mutation_buffer_flush_bytes() -> usize {
    MUTATION_BUFFER_FLUSH_MB.get() as usize * 1024 * 1024
}
