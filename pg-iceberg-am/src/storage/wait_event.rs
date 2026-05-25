use pgrx::pg_sys;
#[cfg(feature = "pg17")]
use std::{ffi::CStr, sync::OnceLock};

// Keep wait-event reporting in the PostgreSQL AM layer. The storage client and
// staging-file crate stay reusable outside a backend process.
#[cfg(feature = "pg17")]
static STAGING_FILE_WRITE: OnceLock<u32> = OnceLock::new();
#[cfg(feature = "pg17")]
static STAGING_FILE_SYNC: OnceLock<u32> = OnceLock::new();
#[cfg(feature = "pg17")]
static OBJECT_READ: OnceLock<u32> = OnceLock::new();
#[cfg(feature = "pg17")]
static OBJECT_UPLOAD: OnceLock<u32> = OnceLock::new();

#[derive(Clone, Copy)]
pub(super) enum StorageWaitEvent {
    StagingFileWrite,
    StagingFileSync,
    ObjectRead,
    ObjectUpload,
}

impl StorageWaitEvent {
    #[cfg(feature = "pg17")]
    fn info(self) -> Option<u32> {
        Some(match self {
            Self::StagingFileWrite => *STAGING_FILE_WRITE.get_or_init(|| {
                register_extension_event(c"PgLakebaseStagingFileWrite")
            }),
            Self::StagingFileSync => *STAGING_FILE_SYNC.get_or_init(|| {
                register_extension_event(c"PgLakebaseStagingFileSync")
            }),
            Self::ObjectRead => *OBJECT_READ
                .get_or_init(|| register_extension_event(c"PgLakebaseObjectRead")),
            Self::ObjectUpload => *OBJECT_UPLOAD
                .get_or_init(|| register_extension_event(c"PgLakebaseObjectUpload")),
        })
    }

    #[cfg(not(feature = "pg17"))]
    fn info(self) -> Option<u32> {
        match self {
            // Staging files are local disk I/O. Before PG17 named extension
            // events, the built-in data-file events are the closest available
            // wait-event class.
            Self::StagingFileWrite => {
                Some(pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE)
            }
            Self::StagingFileSync => {
                Some(pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_SYNC)
            }
            // PostgreSQL 16 has an Extension wait class but no named extension
            // event registry. Do not misclassify remote object operations as
            // data-file I/O there.
            Self::ObjectRead | Self::ObjectUpload => None,
        }
    }
}

pub(super) struct StorageWaitGuard {
    active: bool,
}

impl StorageWaitGuard {
    pub(super) fn start(event: StorageWaitEvent) -> Self {
        let Some(wait_event_info) = event.info() else {
            return Self { active: false };
        };

        unsafe {
            pg_sys::pgstat_report_wait_start(wait_event_info);
        }
        Self { active: true }
    }
}

impl Drop for StorageWaitGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        unsafe {
            pg_sys::pgstat_report_wait_end();
        }
    }
}

#[cfg(feature = "pg17")]
fn register_extension_event(name: &'static CStr) -> u32 {
    unsafe { pg_sys::WaitEventExtensionNew(name.as_ptr()) }
}
