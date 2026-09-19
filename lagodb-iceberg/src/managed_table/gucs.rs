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
