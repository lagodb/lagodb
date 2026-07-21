use super::bgworker::{
    HandleStatus, LauncherWorkerHandle, ReconcilerToken, WorkerToken,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) enum ProcessToken {
    Reconciler(ReconcilerToken),
    Worker(WorkerToken),
}

pub(super) struct OwnedProcess {
    token: ProcessToken,
    handle: LauncherWorkerHandle,
    termination_requested: bool,
}

impl OwnedProcess {
    pub(super) const fn new(
        token: ProcessToken,
        handle: LauncherWorkerHandle,
    ) -> Self {
        Self {
            token,
            handle,
            termination_requested: false,
        }
    }

    pub(super) const fn token(&self) -> ProcessToken {
        self.token
    }

    pub(super) fn status(&self) -> HandleStatus {
        self.handle.status()
    }

    pub(super) const fn raw_handle(
        &self,
    ) -> *mut pgrx::pg_sys::BackgroundWorkerHandle {
        self.handle.raw_ptr()
    }

    pub(super) fn request_termination(&mut self) {
        if self.termination_requested {
            return;
        }
        self.handle.request_termination();
        self.termination_requested = true;
    }

    pub(super) fn release_after_stopped(self) {
        self.handle.release_after_stopped();
    }
}
