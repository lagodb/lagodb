use std::ffi::CStr;

use crate::runtime_api::{RuntimeApiError, RuntimeClient, WorkerWakeupError};

/// Stable catalog identity of a worker owned by an extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerIdentity {
    extension_name: &'static CStr,
    worker_name: &'static CStr,
}

impl WorkerIdentity {
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
    identity: WorkerIdentity,
}

impl WorkerNotifier {
    pub const fn new(identity: WorkerIdentity) -> Self {
        Self { identity }
    }

    /// Stage one wakeup for publication by the runtime after top-level commit.
    pub fn stage_wakeup(self) -> Result<(), WorkerNotificationError> {
        RuntimeClient::connect()?
            .stage_worker_wakeup(
                self.identity.extension_name,
                self.identity.worker_name,
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
