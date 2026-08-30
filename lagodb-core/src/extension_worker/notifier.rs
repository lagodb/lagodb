use std::ffi::CStr;

use crate::runtime_api::{RuntimeApiError, RuntimeClient, WorkerWakeupError};

/// Static provider-side locator used to resolve a worker's current catalog ID.
///
/// `worker_name` is globally unique. `extension_name` verifies that the named
/// worker is still owned by the provider before a wakeup is staged. The stable
/// runtime identity is the database-local `worker_id`, not this locator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerLocator {
    extension_name: &'static CStr,
    worker_name: &'static CStr,
}

impl WorkerLocator {
    pub const fn new(
        extension_name: &'static CStr,
        worker_name: &'static CStr,
    ) -> Self {
        Self {
            extension_name,
            worker_name,
        }
    }
}

/// Transaction-aware wakeup client for a runtime-owned extension worker.
#[derive(Clone, Copy, Debug)]
pub struct WorkerNotifier {
    locator: WorkerLocator,
}

impl WorkerNotifier {
    pub const fn new(locator: WorkerLocator) -> Self {
        Self { locator }
    }

    /// Stage one wakeup for publication by the runtime after top-level commit.
    pub fn stage_wakeup(self) -> Result<(), WorkerNotificationError> {
        RuntimeClient::connect()?
            .stage_worker_wakeup(
                self.locator.extension_name,
                self.locator.worker_name,
            )
            .map_err(WorkerNotificationError::Wakeup)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerNotificationError {
    #[error(transparent)]
    RuntimeApi(#[from] RuntimeApiError),
    #[error(transparent)]
    Wakeup(WorkerWakeupError),
}
