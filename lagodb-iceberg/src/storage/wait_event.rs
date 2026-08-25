use pgrx::pg_sys;
use std::{ffi::CStr, sync::OnceLock};

// Keep wait-event reporting in the PostgreSQL adapter layer. The storage client and
// staging-file crate stay reusable outside a backend process.
static STAGING_FILE_WRITE: OnceLock<u32> = OnceLock::new();
static STAGING_FILE_SYNC: OnceLock<u32> = OnceLock::new();
static OBJECT_READ: OnceLock<u32> = OnceLock::new();
static OBJECT_UPLOAD: OnceLock<u32> = OnceLock::new();

#[derive(Clone, Copy)]
pub(crate) enum StorageWaitEvent {
    StagingFileWrite,
    StagingFileSync,
    ObjectRead,
    ObjectUpload,
}

impl StorageWaitEvent {
    fn info(self) -> u32 {
        match self {
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
        }
    }
}

pub(crate) struct StorageWaitGuard;

impl StorageWaitGuard {
    pub(crate) fn start(event: StorageWaitEvent) -> Self {
        unsafe {
            pg_sys::pgstat_report_wait_start(event.info());
        }
        Self
    }
}

impl Drop for StorageWaitGuard {
    fn drop(&mut self) {
        unsafe {
            pg_sys::pgstat_report_wait_end();
        }
    }
}

fn register_extension_event(name: &'static CStr) -> u32 {
    unsafe { pg_sys::WaitEventExtensionNew(name.as_ptr()) }
}
