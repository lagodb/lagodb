//! Connector read/write configuration captured once per operation.

use std::ffi::CString;
use std::num::{NonZeroU64, NonZeroUsize};

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

const MEBIBYTE: u64 = 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE_MB: i32 = 256;
const DEFAULT_JSON_MAX_RECORD_SIZE_MB: i32 = 16;
const MAX_JSON_RECORD_SIZE_MB: i32 = {
    let max_mebibytes = usize::MAX / MEBIBYTE as usize;
    if max_mebibytes > i32::MAX as usize {
        i32::MAX
    } else {
        max_mebibytes as i32
    }
};

static TARGET_FILE_SIZE_MB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_TARGET_FILE_SIZE_MB);
static JSON_MAX_RECORD_SIZE_MB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_JSON_MAX_RECORD_SIZE_MB);
static DEFAULT_S3_SERVER: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static DEFAULT_GCS_SERVER: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);
static DEFAULT_AZURE_SERVER: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) fn init() {
    GucRegistry::define_int_guc(
        c"lagodb_connectors.target_file_size_mb",
        c"Target size of files written to an object prefix (MiB)",
        c"COPY TO and foreign-table INSERT roll prefix output after a complete format-specific write unit reaches this approximate encoded size. Exact-object COPY TO ignores this setting.",
        &TARGET_FILE_SIZE_MB,
        1,
        i32::MAX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"lagodb_connectors.json_max_record_size_mb",
        c"Maximum size of one NDJSON record (MiB)",
        c"Schema inference, foreign-table scans, and COPY FROM reject a single JSON record larger than this limit.",
        &JSON_MAX_RECORD_SIZE_MB,
        1,
        MAX_JSON_RECORD_SIZE_MB,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"lagodb_connectors.default_s3_server",
        c"Default foreign server for s3:// object URIs",
        c"Used when COPY or cache invalidation does not specify a server.",
        &DEFAULT_S3_SERVER,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"lagodb_connectors.default_gcs_server",
        c"Default foreign server for gs:// object URIs",
        c"Used when COPY or cache invalidation does not specify a server.",
        &DEFAULT_GCS_SERVER,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"lagodb_connectors.default_azure_server",
        c"Default foreign server for az:// object URIs",
        c"Used when COPY or cache invalidation does not specify a server.",
        &DEFAULT_AZURE_SERVER,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Scheme-specific default-server setting used by object URI operations.
#[derive(Clone, Copy)]
pub(crate) struct DefaultServerConfig {
    guc_name: &'static str,
    setting: &'static GucSetting<Option<CString>>,
}

impl DefaultServerConfig {
    pub(crate) const fn s3() -> Self {
        Self {
            guc_name: "lagodb_connectors.default_s3_server",
            setting: &DEFAULT_S3_SERVER,
        }
    }

    pub(crate) const fn gcs() -> Self {
        Self {
            guc_name: "lagodb_connectors.default_gcs_server",
            setting: &DEFAULT_GCS_SERVER,
        }
    }

    pub(crate) const fn azure() -> Self {
        Self {
            guc_name: "lagodb_connectors.default_azure_server",
            setting: &DEFAULT_AZURE_SERVER,
        }
    }

    pub(crate) const fn guc_name(self) -> &'static str {
        self.guc_name
    }

    pub(crate) fn server_name(self) -> Option<CString> {
        self.setting
            .get()
            .filter(|server| !server.as_bytes().is_empty())
    }
}

/// Immutable read settings captured at the start of one operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadConfig {
    json_max_record_bytes: NonZeroUsize,
}

impl ReadConfig {
    pub(crate) fn from_guc() -> Self {
        let megabytes = JSON_MAX_RECORD_SIZE_MB.get() as usize;
        let bytes = megabytes
            .checked_mul(MEBIBYTE as usize)
            .expect("the GUC upper bound guarantees an addressable byte count");
        Self {
            json_max_record_bytes: NonZeroUsize::new(bytes)
                .expect("the GUC lower bound guarantees a non-zero byte count"),
        }
    }

    pub(crate) const fn json_max_record_bytes(self) -> NonZeroUsize {
        self.json_max_record_bytes
    }
}

/// Immutable write settings captured at the start of one statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WriteConfig {
    target_file_bytes: NonZeroU64,
}

impl WriteConfig {
    pub(crate) fn from_guc() -> Self {
        let megabytes = u64::try_from(TARGET_FILE_SIZE_MB.get())
            .expect("the GUC lower bound guarantees a positive value");
        let bytes = megabytes
            .checked_mul(MEBIBYTE)
            .expect("an i32 MiB value fits in u64 bytes");
        Self {
            target_file_bytes: NonZeroU64::new(bytes)
                .expect("the GUC lower bound guarantees a non-zero byte count"),
        }
    }

    pub(crate) const fn target_file_bytes(self) -> NonZeroU64 {
        self.target_file_bytes
    }
}
