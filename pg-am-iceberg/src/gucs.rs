use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Maximum number of retries for optimistic concurrency control (CAS) loops.
static MAX_COMMIT_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(100);

/// Injection point for testing purposes.
static INJECTION_POINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

pub fn init() {
    GucRegistry::define_int_guc(
        c"pg_am_iceberg.max_commit_retries",
        c"Maximum number of retries for optimistic concurrency control commits",
        c"When concurrent updates occur, pg-am-iceberg retries the commit. This GUC limits the number of retries.",
        &MAX_COMMIT_RETRIES,
        0,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_am_iceberg.injection_point",
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

pub fn injection_point() -> Option<String> {
    INJECTION_POINT
        .get()
        .map(|s| s.to_string_lossy().to_string())
}
