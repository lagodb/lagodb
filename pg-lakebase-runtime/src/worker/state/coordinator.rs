use pgrx::{PGRXSharedMemory, pg_sys};

use super::ProcessState;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinatorStopDisposition {
    /// The coordinator completed with no work published during its exit.
    Settled,
    /// A concurrent request must be handed to the supervisor immediately.
    HandoffNow,
    /// The coordinator failed; PostgreSQL's worker-exit notification drives
    /// recovery.
    Failed,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct CoordinatorSlot {
    pub(crate) database_oid: u32,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) process_state: ProcessState,
    stop_requested: u8,
    needs_restart: u8,
    pub(crate) _padding: [u8; 1],
}

// SAFETY: CoordinatorSlot is repr(C), Copy, and consists only
// of PostgreSQL scalar process/state fields with no process-local pointers.
unsafe impl PGRXSharedMemory for CoordinatorSlot {}

impl CoordinatorSlot {
    pub(crate) const fn new(database_oid: u32) -> Self {
        Self {
            database_oid,
            pid: 0,
            proc_number: pg_sys::INVALID_PROC_NUMBER,
            process_state: ProcessState::Stopped,
            stop_requested: 0,
            needs_restart: 0,
            _padding: [0; 1],
        }
    }

    pub(crate) const fn process(&self) -> ProcessState {
        self.process_state
    }

    pub(crate) const fn is_stop_requested(&self) -> bool {
        self.stop_requested != 0
    }

    pub(crate) const fn needs_restart(&self) -> bool {
        self.needs_restart != 0
    }

    /// Returns whether this coordinator may apply catalog state and launch
    /// workers for its current reconciliation pass.
    ///
    /// `needs_restart` deliberately does not revoke authority: ordinary wake
    /// and RunAfter work may be consumed by the current live coordinator.
    pub(crate) const fn has_reconciliation_authority(&self) -> bool {
        self.process().is_active() && !self.is_stop_requested()
    }

    pub(crate) fn reserve(&mut self) {
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.process_state = ProcessState::Starting;
        self.stop_requested = 0;
        self.needs_restart = 0;
    }

    pub(crate) fn mark_running(&mut self, pid: i32, proc_number: i32) -> bool {
        let current = self.process();
        if current != ProcessState::Starting
            || self.stop_requested != 0
            || self.needs_restart != 0
        {
            return false;
        }
        self.process_state = ProcessState::Running;
        self.pid = pid;
        self.proc_number = proc_number;
        true
    }

    pub(crate) fn request_restart(&mut self) {
        self.needs_restart = 1;
    }

    pub(crate) fn request_stop(&mut self) {
        self.needs_restart = 0;
        self.stop_requested = 1;
        let current = self.process();
        if matches!(current, ProcessState::Starting | ProcessState::Running) {
            self.process_state = ProcessState::Restarting;
        }
    }

    /// Invalidates the current coordinator while preserving work for its
    /// successor.
    pub(crate) fn request_handoff(&mut self) {
        self.request_stop();
        self.needs_restart = 1;
    }

    pub(crate) fn prepare_for_drop(&mut self) {
        self.request_handoff();
    }

    pub(crate) fn confirm_stopped(
        &mut self,
        exit_code: i32,
    ) -> Option<CoordinatorStopDisposition> {
        if !self.process().is_active() {
            return None;
        }
        let disposition = if self.stop_requested == 0 && exit_code != 0 {
            // Failure takes precedence over a concurrent request. The request
            // remains durable, while PostgreSQL notifies the registering
            // supervisor through bgw_notify_pid when this worker exits.
            self.needs_restart = 1;
            CoordinatorStopDisposition::Failed
        } else if self.needs_restart != 0 {
            CoordinatorStopDisposition::HandoffNow
        } else {
            CoordinatorStopDisposition::Settled
        };
        self.process_state = ProcessState::Stopped;
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        Some(disposition)
    }

    pub(crate) fn reset_after_registration_failure(&mut self) {
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.process_state = ProcessState::Stopped;
        self.stop_requested = 0;
        self.needs_restart = 1;
    }
}
