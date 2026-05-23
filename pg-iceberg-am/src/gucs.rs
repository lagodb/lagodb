use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Maximum number of retries for optimistic concurrency control (CAS) loops.
static MAX_COMMIT_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(100);

/// Buffered-row memory threshold (in MiB) that triggers a DML write flush.
///
/// This controls in-process memory pressure on the row buffer; it does NOT
/// control the size of produced Parquet data files (the rolling writer owns
/// that).
static DML_BUFFER_FLUSH_MB: GucSetting<i32> = GucSetting::<i32>::new(64);

/// Injection point for testing purposes.
static INJECTION_POINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

pub fn init() {
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
        c"pg_iceberg_am.dml_buffer_flush_mb",
        c"Buffered-row memory threshold (MiB) that triggers a DML write flush",
        c"Controls in-process memory pressure on the row buffer used during INSERT. \
          When the estimated size of buffered rows reaches this many MiB, the buffer \
          is converted to an Arrow batch and handed to the Parquet writer. \
          This does NOT control the size of produced Parquet data files: the rolling \
          file writer rolls files based on its own target size.",
        &DML_BUFFER_FLUSH_MB,
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
}

pub fn max_commit_retries() -> i32 {
    MAX_COMMIT_RETRIES.get()
}

/// Returns the configured DML buffer flush threshold in bytes.
///
/// This is the memory-pressure trigger for the row buffer in
/// `IcebergModify::buffer_row`, not a Parquet file-size target. The GUC value
/// is in MiB and is clamped to `>= 1` at registration time, so the conversion
/// to bytes cannot overflow `usize` on any supported platform.
pub fn dml_buffer_flush_bytes() -> usize {
    let mb = DML_BUFFER_FLUSH_MB.get().max(1) as usize;
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
