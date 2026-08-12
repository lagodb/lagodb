//! Connector write configuration captured once per statement.

use std::num::NonZeroU64;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

const MEBIBYTE: u64 = 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE_MB: i32 = 256;

static TARGET_FILE_SIZE_MB: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_TARGET_FILE_SIZE_MB);

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
