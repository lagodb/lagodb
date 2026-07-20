use std::fmt;

use pg_lakebase_core::extension_worker::WorkerExit;
use pgrx::{PGRXSharedMemory, pg_sys};

pub(crate) const RUNTIME_MAGIC: u64 = 0x5047_4c41_4b45_4257;
// These bound concurrent reconciler processes and tracked registrations, not
// the number of PostgreSQL databases discovered by the launcher.
pub(crate) const MAX_RECONCILERS: usize = 64;
pub(crate) const MAX_WORKERS: usize = 512;
pub(crate) const MAX_WORKER_NAME_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeStateKind {
    Reconciler,
    Worker,
}

impl fmt::Display for RuntimeStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reconciler => "reconciler",
            Self::Worker => "worker",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid {kind} state transition: {current} -> {next}")]
pub(crate) struct RuntimeStateTransitionError {
    kind: RuntimeStateKind,
    current: &'static str,
    next: &'static str,
}

impl RuntimeStateTransitionError {
    const fn reconciler(current: ReconcilerState, next: ReconcilerState) -> Self {
        Self {
            kind: RuntimeStateKind::Reconciler,
            current: current.as_str(),
            next: next.as_str(),
        }
    }

    const fn worker(current: WorkerState, next: WorkerState) -> Self {
        Self {
            kind: RuntimeStateKind::Worker,
            current: current.as_str(),
            next: next.as_str(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ReconcilerState {
    Empty = 0,
    Starting = 1,
    Running = 2,
    Stopping = 3,
    Finished = 4,
    Retry = 5,
}

impl ReconcilerState {
    pub(crate) fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::Stopping,
            4 => Self::Finished,
            5 => Self::Retry,
            _ => Self::Empty,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Finished => "finished",
            Self::Retry => "retry",
        }
    }

    pub(crate) const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Starting,
                Self::Running | Self::Stopping | Self::Finished | Self::Retry
            ) | (Self::Running, Self::Stopping | Self::Finished | Self::Retry)
                | (Self::Stopping, Self::Finished)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum WorkerState {
    Empty = 0,
    PendingRegistration = 1,
    Dormant = 2,
    Starting = 3,
    Running = 4,
    Stopping = 5,
    Scheduled = 6,
    Backoff = 7,
}

impl WorkerState {
    pub(crate) fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::PendingRegistration,
            2 => Self::Dormant,
            3 => Self::Starting,
            4 => Self::Running,
            5 => Self::Stopping,
            6 => Self::Scheduled,
            7 => Self::Backoff,
            _ => Self::Empty,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::PendingRegistration => "pending_registration",
            Self::Dormant => "dormant",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Scheduled => "scheduled",
            Self::Backoff => "backoff",
        }
    }

    pub(crate) const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Dormant,
                Self::Dormant
                    | Self::PendingRegistration
                    | Self::Starting
                    | Self::Stopping
            ) | (Self::PendingRegistration, Self::Dormant | Self::Stopping)
                | (
                    Self::Starting,
                    Self::Running | Self::Stopping | Self::Backoff
                )
                | (
                    Self::Running,
                    Self::Dormant | Self::Stopping | Self::Scheduled | Self::Backoff
                )
                | (
                    Self::Stopping,
                    Self::PendingRegistration | Self::Dormant | Self::Stopping
                )
                | (
                    Self::Scheduled,
                    Self::Dormant | Self::Stopping | Self::Scheduled
                )
                | (
                    Self::Backoff,
                    Self::Dormant | Self::Stopping | Self::Backoff
                )
        )
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct ReconcilerSlot {
    pub(crate) database_oid: u32,
    pub(crate) generation: u32,
    pub(crate) startup_deadline_ms: i64,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) state: u8,
    pub(crate) _padding: [u8; 7],
}

impl ReconcilerSlot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: 0,
        generation: 0,
        startup_deadline_ms: 0,
        pid: 0,
        proc_number: pg_sys::INVALID_PROC_NUMBER,
        state: ReconcilerState::Empty as u8,
        _padding: [0; 7],
    };

    pub(crate) fn transition_to(
        &mut self,
        next: ReconcilerState,
    ) -> Result<(), RuntimeStateTransitionError> {
        let current = ReconcilerState::from_raw(self.state);
        if !current.can_transition_to(next) {
            return Err(RuntimeStateTransitionError::reconciler(current, next));
        }
        self.state = next as u8;
        Ok(())
    }

    // The caller must own this slot under the runtime exclusive LWLock so this
    // recovery and begin_reconciler() cannot both claim the same generation.
    pub(crate) fn recover_timed_out_start(
        &mut self,
        now_ms: i64,
    ) -> Result<bool, RuntimeStateTransitionError> {
        if ReconcilerState::from_raw(self.state) != ReconcilerState::Starting
            || self.pid != 0
            || self.startup_deadline_ms == 0
            || self.startup_deadline_ms > now_ms
        {
            return Ok(false);
        }
        self.transition_to(ReconcilerState::Retry)?;
        self.generation = self.generation.wrapping_add(1).max(1);
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;
        Ok(true)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct WorkerSlot {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    pub(crate) generation: u32,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) restart_at_ms: i64,
    pub(crate) startup_deadline_ms: i64,
    pub(crate) worker_name_len: u16,
    pub(crate) state: u8,
    pub(crate) wake_requested: u8,
    pub(crate) _padding: [u8; 4],
    pub(crate) worker_name: [u8; MAX_WORKER_NAME_BYTES],
}

impl WorkerSlot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: 0,
        extension_oid: 0,
        generation: 0,
        pid: 0,
        proc_number: pg_sys::INVALID_PROC_NUMBER,
        restart_at_ms: 0,
        startup_deadline_ms: 0,
        worker_name_len: 0,
        state: WorkerState::Empty as u8,
        wake_requested: 0,
        _padding: [0; 4],
        worker_name: [0; MAX_WORKER_NAME_BYTES],
    };

    pub(crate) fn transition_to(
        &mut self,
        next: WorkerState,
    ) -> Result<(), RuntimeStateTransitionError> {
        let current = WorkerState::from_raw(self.state);
        if !current.can_transition_to(next) {
            return Err(RuntimeStateTransitionError::worker(current, next));
        }
        self.state = next as u8;
        Ok(())
    }

    // The caller must own this slot under the runtime exclusive LWLock so this
    // recovery and begin_worker() cannot both claim the same generation.
    pub(crate) fn recover_timed_out_start(
        &mut self,
        now_ms: i64,
        retry_at_ms: i64,
    ) -> Result<bool, RuntimeStateTransitionError> {
        if WorkerState::from_raw(self.state) != WorkerState::Starting
            || self.pid != 0
            || self.startup_deadline_ms == 0
            || self.startup_deadline_ms > now_ms
        {
            return Ok(false);
        }
        self.transition_to(WorkerState::Backoff)?;
        self.generation = self.generation.wrapping_add(1).max(1);
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;
        self.restart_at_ms = retry_at_ms;
        Ok(true)
    }

    pub(crate) fn worker_name(&self) -> &[u8] {
        let len = usize::from(self.worker_name_len).min(MAX_WORKER_NAME_BYTES);
        &self.worker_name[..len]
    }

    pub(crate) fn worker_name_str(&self) -> &str {
        std::str::from_utf8(self.worker_name()).unwrap_or("<invalid utf8>")
    }

    fn set_worker_name(&mut self, worker_name: &str) -> bool {
        let bytes = worker_name.as_bytes();
        let Ok(len) = u16::try_from(bytes.len()) else {
            return false;
        };
        if bytes.len() > MAX_WORKER_NAME_BYTES {
            return false;
        }
        self.worker_name = [0; MAX_WORKER_NAME_BYTES];
        self.worker_name[..bytes.len()].copy_from_slice(bytes);
        self.worker_name_len = len;
        true
    }

    fn matches_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        self.database_oid == database_oid
            && self.extension_oid == extension_oid
            && self.worker_name() == worker_name.as_bytes()
    }

    pub(crate) fn request_wakeup(&mut self) {
        self.wake_requested = 1;
    }

    pub(crate) fn request_wakeup_preempting_schedule(
        &mut self,
    ) -> Result<(), RuntimeStateTransitionError> {
        if WorkerState::from_raw(self.state) == WorkerState::Scheduled {
            self.transition_to(WorkerState::Dormant)?;
            self.restart_at_ms = 0;
        }
        self.request_wakeup();
        Ok(())
    }

    pub(crate) fn finish_invocation(
        &mut self,
        directive: WorkerExit,
        now_ms: i64,
    ) -> Result<(), RuntimeStateTransitionError> {
        let current = WorkerState::from_raw(self.state);
        self.pid = 0;
        self.proc_number = pg_sys::INVALID_PROC_NUMBER;
        self.startup_deadline_ms = 0;

        if current == WorkerState::Stopping {
            self.transition_to(WorkerState::Dormant)?;
            self.restart_at_ms = 0;
            return Ok(());
        }

        match directive {
            WorkerExit::Dormant => {
                self.transition_to(WorkerState::Dormant)?;
                self.restart_at_ms = 0;
            }
            WorkerExit::RestartImmediately => {
                self.transition_to(WorkerState::Dormant)?;
                self.request_wakeup();
                self.restart_at_ms = 0;
            }
            // A wake published after this invocation started is newer than the
            // worker's own deadline decision.
            WorkerExit::RestartAfter(_) if self.wake_requested != 0 => {
                self.transition_to(WorkerState::Dormant)?;
                self.restart_at_ms = 0;
            }
            WorkerExit::RestartAfter(delay) => {
                self.transition_to(WorkerState::Scheduled)?;
                self.restart_at_ms = now_ms.saturating_add(
                    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct RuntimeSharedState {
    pub(crate) magic: u64,
    pub(crate) struct_size: u32,
    pub(crate) launcher_pid: i32,
    pub(crate) launcher_proc_number: i32,
    pub(crate) rescan_all: u8,
    pub(crate) _padding: [u8; 3],
    pub(crate) worker_cursor: u32,
    pub(crate) last_capacity_warning_ms: i64,
    pub(crate) reconcilers: [ReconcilerSlot; MAX_RECONCILERS],
    pub(crate) workers: [WorkerSlot; MAX_WORKERS],
}

impl Default for RuntimeSharedState {
    fn default() -> Self {
        Self {
            magic: RUNTIME_MAGIC,
            struct_size: u32::try_from(std::mem::size_of::<Self>())
                .expect("runtime shared state exceeds u32"),
            launcher_pid: 0,
            launcher_proc_number: pg_sys::INVALID_PROC_NUMBER,
            rescan_all: 1,
            _padding: [0; 3],
            worker_cursor: 0,
            last_capacity_warning_ms: 0,
            reconcilers: [ReconcilerSlot::EMPTY; MAX_RECONCILERS],
            workers: [WorkerSlot::EMPTY; MAX_WORKERS],
        }
    }
}

impl RuntimeSharedState {
    pub(crate) fn validate_layout(&self) -> bool {
        self.magic == RUNTIME_MAGIC
            && usize::try_from(self.struct_size).ok()
                == Some(std::mem::size_of::<Self>())
    }

    pub(crate) fn reconciler_slot(&self, database_oid: u32) -> Option<usize> {
        self.reconcilers.iter().position(|slot| {
            slot.database_oid == database_oid
                && ReconcilerState::from_raw(slot.state) != ReconcilerState::Empty
        })
    }

    pub(crate) fn reserve_reconciler_slot(
        &mut self,
        database_oid: u32,
        startup_deadline_ms: i64,
    ) -> Option<usize> {
        let index = self.reconcilers.iter().position(|slot| {
            ReconcilerState::from_raw(slot.state) == ReconcilerState::Empty
        })?;
        let generation = self.reconcilers[index].generation.wrapping_add(1).max(1);
        self.reconcilers[index] = ReconcilerSlot {
            database_oid,
            generation,
            startup_deadline_ms,
            pid: 0,
            proc_number: pg_sys::INVALID_PROC_NUMBER,
            state: ReconcilerState::Starting as u8,
            _padding: [0; 7],
        };
        Some(index)
    }

    pub(crate) fn worker_slot(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> Option<usize> {
        self.workers.iter().position(|slot| {
            slot.matches_worker(database_oid, extension_oid, worker_name)
                && WorkerState::from_raw(slot.state) != WorkerState::Empty
        })
    }

    pub(crate) fn ensure_worker_slot(
        &mut self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> Option<usize> {
        if let Some(index) =
            self.worker_slot(database_oid, extension_oid, worker_name)
        {
            return Some(index);
        }
        if worker_name.is_empty() || worker_name.len() > MAX_WORKER_NAME_BYTES {
            return None;
        }
        let index = self.workers.iter().position(|slot| {
            WorkerState::from_raw(slot.state) == WorkerState::Empty
        })?;
        let generation = self.workers[index].generation.wrapping_add(1).max(1);
        self.workers[index] = WorkerSlot {
            database_oid,
            extension_oid,
            generation,
            state: WorkerState::Dormant as u8,
            ..WorkerSlot::EMPTY
        };
        if !self.workers[index].set_worker_name(worker_name) {
            self.workers[index] = WorkerSlot {
                generation,
                ..WorkerSlot::EMPTY
            };
            return None;
        }
        Some(index)
    }

    pub(crate) fn clear_reconciler_slot(&mut self, index: usize) {
        let generation = self.reconcilers[index].generation;
        self.reconcilers[index] = ReconcilerSlot {
            generation,
            ..ReconcilerSlot::EMPTY
        };
    }

    pub(crate) fn clear_worker_slot(&mut self, index: usize) {
        let generation = self.workers[index].generation;
        self.workers[index] = WorkerSlot {
            generation,
            ..WorkerSlot::EMPTY
        };
    }
}

// SAFETY: RuntimeSharedState is repr(C), Copy, contains only fixed-size scalar
// fields and arrays, and contains no references or process-local pointers.
unsafe impl PGRXSharedMemory for RuntimeSharedState {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_validation_rejects_mismatch() {
        let mut state = RuntimeSharedState::default();
        assert!(state.validate_layout());
        state.struct_size += 1;
        assert!(!state.validate_layout());
    }

    #[test]
    fn slots_are_stable_and_generation_fenced() {
        let mut state = RuntimeSharedState::default();
        let reconciler = state.reserve_reconciler_slot(42, 100).unwrap();
        let old_reconciler_generation = state.reconcilers[reconciler].generation;
        state.clear_reconciler_slot(reconciler);
        let reused_reconciler = state.reserve_reconciler_slot(43, 100).unwrap();
        assert_eq!(reused_reconciler, reconciler);
        assert_ne!(
            state.reconcilers[reused_reconciler].generation,
            old_reconciler_generation
        );

        let worker = state.ensure_worker_slot(42, 8, "worker").unwrap();
        let old_worker_generation = state.workers[worker].generation;
        state.clear_worker_slot(worker);
        let reused_worker = state.ensure_worker_slot(42, 8, "other").unwrap();
        assert_eq!(reused_worker, worker);
        assert_ne!(
            state.workers[reused_worker].generation,
            old_worker_generation
        );
    }

    #[test]
    fn state_transition_tables_reject_invalid_edges() {
        assert!(
            ReconcilerState::Starting.can_transition_to(ReconcilerState::Running)
        );
        assert!(
            ReconcilerState::Starting.can_transition_to(ReconcilerState::Finished)
        );
        assert!(ReconcilerState::Running.can_transition_to(ReconcilerState::Retry));
        assert!(
            ReconcilerState::Stopping.can_transition_to(ReconcilerState::Finished)
        );
        assert!(
            !ReconcilerState::Finished.can_transition_to(ReconcilerState::Running)
        );

        assert!(WorkerState::Dormant.can_transition_to(WorkerState::Starting));
        assert!(WorkerState::Starting.can_transition_to(WorkerState::Running));
        assert!(WorkerState::Running.can_transition_to(WorkerState::Scheduled));
        assert!(WorkerState::Running.can_transition_to(WorkerState::Backoff));
        assert!(WorkerState::Scheduled.can_transition_to(WorkerState::Dormant));
        assert!(WorkerState::Stopping.can_transition_to(WorkerState::Dormant));
        assert!(
            WorkerState::Stopping.can_transition_to(WorkerState::PendingRegistration)
        );
        assert!(
            !WorkerState::PendingRegistration
                .can_transition_to(WorkerState::Starting)
        );
        assert!(!WorkerState::Dormant.can_transition_to(WorkerState::Running));
        assert!(!WorkerState::Scheduled.can_transition_to(WorkerState::Starting));
        assert!(!WorkerState::Backoff.can_transition_to(WorkerState::Running));
    }

    #[test]
    fn registration_capacity_is_bounded() {
        let mut state = RuntimeSharedState::default();
        for index in 1..=MAX_WORKERS {
            assert!(
                state
                    .ensure_worker_slot(42, 8, &format!("worker-{index}"))
                    .is_some()
            );
        }
        assert!(state.ensure_worker_slot(42, 8, "one-too-many").is_none());
    }

    #[test]
    fn invalid_transitions_do_not_mutate_state() {
        let mut worker = WorkerSlot {
            state: WorkerState::Dormant as u8,
            ..WorkerSlot::EMPTY
        };
        assert!(worker.transition_to(WorkerState::Running).is_err());
        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Dormant);

        let mut reconciler = ReconcilerSlot {
            state: ReconcilerState::Finished as u8,
            ..ReconcilerSlot::EMPTY
        };
        assert!(reconciler.transition_to(ReconcilerState::Running).is_err());
        assert_eq!(
            ReconcilerState::from_raw(reconciler.state),
            ReconcilerState::Finished
        );
    }

    #[test]
    fn timed_out_worker_start_is_generation_fenced_and_backed_off() {
        let mut worker = WorkerSlot {
            database_oid: 42,
            extension_oid: 8,
            generation: 7,
            state: WorkerState::Starting as u8,
            startup_deadline_ms: 100,
            ..WorkerSlot::EMPTY
        };

        assert!(!worker.recover_timed_out_start(99, 200).unwrap());
        assert_eq!(worker.generation, 7);
        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Starting);

        assert!(worker.recover_timed_out_start(100, 200).unwrap());
        assert_eq!(worker.generation, 8);
        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Backoff);
        assert_eq!(worker.startup_deadline_ms, 0);
        assert_eq!(worker.restart_at_ms, 200);
    }

    #[test]
    fn timed_out_reconciler_start_is_generation_fenced_for_retry() {
        let mut reconciler = ReconcilerSlot {
            database_oid: 42,
            generation: 7,
            state: ReconcilerState::Starting as u8,
            startup_deadline_ms: 100,
            ..ReconcilerSlot::EMPTY
        };

        assert!(!reconciler.recover_timed_out_start(99).unwrap());
        assert_eq!(reconciler.generation, 7);
        assert_eq!(
            ReconcilerState::from_raw(reconciler.state),
            ReconcilerState::Starting
        );

        assert!(reconciler.recover_timed_out_start(100).unwrap());
        assert_eq!(reconciler.generation, 8);
        assert_eq!(
            ReconcilerState::from_raw(reconciler.state),
            ReconcilerState::Retry
        );
        assert_eq!(reconciler.startup_deadline_ms, 0);
    }

    #[test]
    fn starting_slot_with_pid_is_not_reclaimed_after_deadline() {
        let mut worker = WorkerSlot {
            generation: 7,
            pid: 123,
            state: WorkerState::Starting as u8,
            startup_deadline_ms: 100,
            ..WorkerSlot::EMPTY
        };

        assert!(!worker.recover_timed_out_start(100, 200).unwrap());
        assert_eq!(worker.generation, 7);
        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Starting);

        let mut reconciler = ReconcilerSlot {
            generation: 7,
            pid: 123,
            state: ReconcilerState::Starting as u8,
            startup_deadline_ms: 100,
            ..ReconcilerSlot::EMPTY
        };

        assert!(!reconciler.recover_timed_out_start(100).unwrap());
        assert_eq!(reconciler.generation, 7);
        assert_eq!(
            ReconcilerState::from_raw(reconciler.state),
            ReconcilerState::Starting
        );
    }

    #[test]
    fn running_wakeup_survives_dormant_worker_exit() {
        let mut worker = WorkerSlot {
            state: WorkerState::Running as u8,
            wake_requested: 1,
            pid: 123,
            proc_number: 7,
            ..WorkerSlot::EMPTY
        };

        worker.finish_invocation(WorkerExit::Dormant, 100).unwrap();

        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Dormant);
        assert_eq!(worker.wake_requested, 1);
        assert_eq!(worker.restart_at_ms, 0);
        assert_eq!(worker.pid, 0);
        assert_eq!(worker.proc_number, pg_sys::INVALID_PROC_NUMBER);
    }

    #[test]
    fn running_wakeup_preempts_worker_restart_deadline() {
        let mut worker = WorkerSlot {
            state: WorkerState::Running as u8,
            wake_requested: 1,
            ..WorkerSlot::EMPTY
        };

        worker
            .finish_invocation(
                WorkerExit::RestartAfter(std::time::Duration::from_secs(60)),
                100,
            )
            .unwrap();

        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Dormant);
        assert_eq!(worker.wake_requested, 1);
        assert_eq!(worker.restart_at_ms, 0);
    }

    #[test]
    fn restart_deadline_is_kept_without_a_running_wakeup() {
        let mut worker = WorkerSlot {
            state: WorkerState::Running as u8,
            ..WorkerSlot::EMPTY
        };

        worker
            .finish_invocation(
                WorkerExit::RestartAfter(std::time::Duration::from_secs(2)),
                100,
            )
            .unwrap();

        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Scheduled);
        assert_eq!(worker.wake_requested, 0);
        assert_eq!(worker.restart_at_ms, 2_100);
    }

    #[test]
    fn explicit_wakeup_preempts_scheduled_deadline() {
        let mut worker = WorkerSlot {
            state: WorkerState::Scheduled as u8,
            restart_at_ms: 60_000,
            ..WorkerSlot::EMPTY
        };

        worker.request_wakeup_preempting_schedule().unwrap();

        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Dormant);
        assert_eq!(worker.wake_requested, 1);
        assert_eq!(worker.restart_at_ms, 0);
    }

    #[test]
    fn explicit_wakeup_preserves_crash_backoff() {
        let mut worker = WorkerSlot {
            state: WorkerState::Backoff as u8,
            restart_at_ms: 60_000,
            ..WorkerSlot::EMPTY
        };

        worker.request_wakeup_preempting_schedule().unwrap();

        assert_eq!(WorkerState::from_raw(worker.state), WorkerState::Backoff);
        assert_eq!(worker.wake_requested, 1);
        assert_eq!(worker.restart_at_ms, 60_000);
    }
}
