use std::collections::HashSet;
use std::time::Duration;

use pgrx::PgLwLock;
use pgrx::prelude::*;

use crate::error::{LakebaseError, LakebaseResult};
use crate::state::{
    DispatchState, INVALID_OID, ProcessState, RecoveryState, RuntimeSharedState,
    RuntimeStateDecodeError, RuntimeStateTransitionError, WorkerIdentity,
};

use super::CAPACITY_WARNING_INTERVAL;
use super::bgworker::{ReconcilerToken, WorkerToken, timestamp_ms};
use super::reconcile::DatabaseReconcileState;

mod control;
mod process_state;
mod registration;
mod status;

pub(super) use control::StopTarget;

pub(crate) static RUNTIME_STATE: PgLwLock<RuntimeSharedState> =
    unsafe { PgLwLock::new(c"pg_lakebase_runtime worker runtime") };

pub(super) struct RuntimeStore;

pub(super) struct RegistrationReconciliation {
    pub(super) registration_capacity_exhausted: bool,
}

pub(super) struct WorkerStart {
    pub(super) database_oid: u32,
    pub(super) extension_oid: u32,
    pub(super) worker_name: String,
}

pub(super) struct WorkerLaunch {
    pub(super) token: WorkerToken,
    pub(super) identity: WorkerIdentity,
}

#[derive(Default)]
struct TransitionWarning {
    first: Option<RuntimeStateTransitionError>,
}

impl TransitionWarning {
    fn capture(&mut self, error: RuntimeStateTransitionError) {
        self.first.get_or_insert(error);
    }

    fn report(self) {
        if let Some(error) = self.first {
            warn_transition_error(error);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegistrationToken {
    index: usize,
    generation: u32,
}

impl RegistrationToken {
    const fn new(index: usize, generation: u32) -> Self {
        Self { index, generation }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationReservation {
    New(RegistrationToken),
    Replacement(RegistrationToken),
}

impl RegistrationReservation {
    const fn token(self) -> RegistrationToken {
        match self {
            Self::New(token) | Self::Replacement(token) => token,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistrationCompletion {
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReconcilerReservation {
    Reserved(ReconcilerToken),
    AlreadyActive,
    AtCapacity,
    Recovering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReconcilerRetry {
    None,
    Immediate,
    Backoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StoppedProcess {
    Reconciler {
        database_oid: u32,
        retry: ReconcilerRetry,
    },
    Worker,
    Stale,
}

impl RuntimeStore {
    pub(super) const fn new() -> Self {
        Self
    }

    #[cfg(feature = "pg_test")]
    pub(crate) fn set_runtime_test_injection(&self, injection: u8) {
        let mut state = RUNTIME_STATE.exclusive();
        state.test_injection = injection;
        state.test_barrier_reached = 0;
    }

    #[cfg(feature = "pg_test")]
    pub(crate) fn runtime_test_injection(&self) -> u8 {
        RUNTIME_STATE.share().test_injection
    }

    #[cfg(feature = "pg_test")]
    pub(crate) fn mark_test_barrier_reached(&self) {
        RUNTIME_STATE.exclusive().test_barrier_reached = 1;
    }

    #[cfg(feature = "pg_test")]
    pub(crate) fn test_barrier_reached(&self) -> bool {
        RUNTIME_STATE.share().test_barrier_reached != 0
    }

    pub(super) fn signal_launcher(&self) {
        let proc_number = RUNTIME_STATE.share().launcher_proc_number;
        if proc_number != pg_sys::INVALID_PROC_NUMBER {
            // SAFETY: the launcher publishes a ProcNumber while alive. A stale
            // value can only produce a harmless spurious latch wakeup.
            unsafe { pg_sys::ProcSendSignal(proc_number) };
        }
    }

    pub(super) fn claim_launcher(&self) -> LakebaseResult<u64> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        state.launcher_epoch = state.launcher_epoch.wrapping_add(1).max(1);
        state.launcher_pid = unsafe { pg_sys::MyProcPid };
        state.launcher_proc_number = unsafe { pg_sys::MyProcNumber };
        state.recovery_state = RecoveryState::Recovering as u8;
        state.recovery_backend_count = 0;
        for slot in &mut state.reconcilers {
            slot.reset_after_lost_owner();
        }
        for slot in &mut state.workers {
            if !slot.is_empty() {
                slot.reset_after_lost_owner();
            }
        }
        state.rescan_all = 1;
        Ok(state.launcher_epoch)
    }

    pub(super) fn begin_reconciliation(&self) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        if state.launcher_pid != unsafe { pg_sys::MyProcPid }
            || RecoveryState::decode(state.recovery_state)
                != Ok(RecoveryState::Recovering)
            || state.recovery_backend_count != 0
        {
            return false;
        }
        state.recovery_state = RecoveryState::Reconciling as u8;
        state.rescan_all = 1;
        true
    }

    pub(super) fn update_recovery_backend_count(&self, count: usize) {
        let mut state = RUNTIME_STATE.exclusive();
        if state.launcher_pid == unsafe { pg_sys::MyProcPid }
            && RecoveryState::decode(state.recovery_state)
                == Ok(RecoveryState::Recovering)
        {
            state.recovery_backend_count = u32::try_from(count).unwrap_or(u32::MAX);
        }
    }

    pub(super) fn complete_recovery(&self) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        if state.launcher_pid != unsafe { pg_sys::MyProcPid }
            || RecoveryState::decode(state.recovery_state)
                != Ok(RecoveryState::Reconciling)
        {
            return false;
        }
        state.recovery_state = RecoveryState::Ready as u8;
        true
    }

    pub(super) fn clear_launcher(&self) {
        let mut state = RUNTIME_STATE.exclusive();
        if state.launcher_pid == unsafe { pg_sys::MyProcPid } {
            state.launcher_pid = 0;
            state.launcher_proc_number = pg_sys::INVALID_PROC_NUMBER;
        }
    }

    pub(super) fn take_full_scan_request(&self) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        if !RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_reconciliation)
        {
            return false;
        }
        let requested = state.rescan_all != 0;
        state.rescan_all = 0;
        requested
    }

    pub(super) fn remove_dropped_databases(&self, live: &HashSet<u32>) {
        let mut state = RUNTIME_STATE.exclusive();
        let mut warning = TransitionWarning::default();
        for slot in &mut state.reconcilers {
            if slot.database_oid != INVALID_OID
                && !live.contains(&slot.database_oid)
                && let Err(error) = slot.request_stop()
            {
                warning.capture(error);
            }
        }
        for index in 0..state.workers.len() {
            if state.workers[index].database_oid == INVALID_OID
                || live.contains(&state.workers[index].database_oid)
            {
                continue;
            }
            if state.workers[index].process() == Ok(ProcessState::Stopped) {
                state.clear_worker_slot(index);
            } else if let Err(error) = state.workers[index].mark_removing() {
                warning.capture(error);
            }
        }
        for intent in &mut state.database_reconciles {
            if intent.database_oid != INVALID_OID
                && !live.contains(&intent.database_oid)
            {
                *intent = DatabaseReconcileState::EMPTY;
            }
        }
        drop(state);
        warning.report();
    }

    pub(super) fn next_deadline_delay(&self) -> Option<Duration> {
        let now = timestamp_ms();
        let state = RUNTIME_STATE.share();
        let recovery = RecoveryState::decode(state.recovery_state).ok()?;
        if !recovery.allows_reconciliation() {
            return None;
        }
        let reconciler_deadlines = state.reconcilers.iter().filter_map(|slot| {
            (slot.process() == Ok(ProcessState::Starting)
                && slot.startup_deadline_ms > 0)
                .then_some(slot.startup_deadline_ms)
        });
        let worker_deadlines = state.workers.iter().filter_map(|slot| {
            if slot.process() == Ok(ProcessState::Starting)
                && slot.startup_deadline_ms > 0
            {
                Some(slot.startup_deadline_ms)
            } else if recovery.allows_dispatch()
                && slot.dispatch() == Ok(DispatchState::Delayed)
            {
                Some(slot.not_before_ms)
            } else {
                None
            }
        });
        let deadline = reconciler_deadlines.chain(worker_deadlines).min()?;
        Some(Duration::from_millis(
            u64::try_from(deadline.saturating_sub(now)).unwrap_or(u64::MAX),
        ))
    }

    pub(super) fn warn_capacity_exhausted(&self, message: &str) {
        let now = timestamp_ms();
        let should_warn = {
            let mut state = RUNTIME_STATE.exclusive();
            let elapsed = now.saturating_sub(state.last_capacity_warning_ms);
            if state.last_capacity_warning_ms == 0
                || elapsed >= CAPACITY_WARNING_INTERVAL.as_millis() as i64
            {
                state.last_capacity_warning_ms = now;
                true
            } else {
                false
            }
        };
        if should_warn {
            crate::diag::warning(message);
        }
    }
}

fn validate_state(state: &RuntimeSharedState) -> LakebaseResult<()> {
    if state.validate_layout() {
        Ok(())
    } else {
        Err(LakebaseError::SharedMemoryLayoutMismatch)
    }
}

impl From<RuntimeStateDecodeError> for LakebaseError {
    fn from(_source: RuntimeStateDecodeError) -> Self {
        Self::SharedMemoryLayoutMismatch
    }
}

fn warn_transition_error(error: RuntimeStateTransitionError) {
    crate::diag::warning(format_args!("{error}"));
}
