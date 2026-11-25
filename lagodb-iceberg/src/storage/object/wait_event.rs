use std::sync::OnceLock;

use lagodb_core::backend::{ExtensionWaitEvent, ExtensionWaitGuard};

// Keep wait-event reporting in the PostgreSQL adapter layer. The storage client and
// staging-file crate stay reusable outside a backend process.
static STAGING_FILE_WRITE: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static STAGING_FILE_SYNC: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static OBJECT_READ: OnceLock<ExtensionWaitEvent> = OnceLock::new();
static OBJECT_UPLOAD: OnceLock<ExtensionWaitEvent> = OnceLock::new();

#[derive(Clone, Copy)]
pub(super) enum ObjectWaitEvent {
    StagingFileWrite,
    StagingFileSync,
    ObjectRead,
    ObjectUpload,
}

impl ObjectWaitEvent {
    pub(super) fn enter(self) -> ExtensionWaitGuard {
        let event = match self {
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
        };
        event.enter()
    }
}
