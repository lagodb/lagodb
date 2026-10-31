use lagodb_core::wait_event::ExtensionWaitEvent;
use pgrx::pg_sys;
use std::sync::OnceLock;

// Keep wait-event reporting in the PostgreSQL adapter layer. The storage client and
// staging-file crate stay reusable outside a backend process.
static STAGING_FILE_WRITE: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static STAGING_FILE_SYNC: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static OBJECT_READ: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static OBJECT_UPLOAD: OnceLock<ExtensionWaitEvent> = OnceLock::new();

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
            Self::StagingFileWrite => STAGING_FILE_WRITE.get_or_init(|| {
                ExtensionWaitEvent::register(c"PgLagoDBStagingFileWrite")
            }),
            Self::StagingFileSync => STAGING_FILE_SYNC.get_or_init(|| {
                ExtensionWaitEvent::register(c"PgLagoDBStagingFileSync")
            }),
            Self::ObjectRead => OBJECT_READ
                .get_or_init(|| ExtensionWaitEvent::register(c"PgLagoDBObjectRead")),
            Self::ObjectUpload => OBJECT_UPLOAD.get_or_init(|| {
                ExtensionWaitEvent::register(c"PgLagoDBObjectUpload")
            }),
        }
        .info()
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
