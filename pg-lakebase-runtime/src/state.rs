use pgrx::{PGRXSharedMemory, pg_sys};
use std::fmt;

use crate::runtime::reconcile::{DatabaseReconcileState, ReconcilerSlot};

mod worker;

pub(crate) use worker::{WorkerIdentity, WorkerSlot};

// Registration, dispatch, and physical process state are independent, and
// physical identity is launcher-confirmed. Changing this shared-memory layout
// requires updating RUNTIME_MAGIC and restarting PostgreSQL.
pub(crate) const RUNTIME_MAGIC: u64 = 0x5047_4c41_4b45_425c;
pub(crate) const INVALID_OID: u32 = pg_sys::InvalidOid.to_u32();
pub(crate) const MAX_RECONCILERS: usize = 64;
pub(crate) const MAX_WORKERS: usize = 512;
pub(crate) const MAX_DATABASE_RECONCILES: usize = MAX_WORKERS;
pub(crate) const MAX_WORKER_NAME_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeStateKind {
    Registration,
    Dispatch,
    Process,
    PendingDirective,
    Recovery,
    Worker,
    Reconciler,
}

impl fmt::Display for RuntimeStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Registration => "registration",
            Self::Dispatch => "dispatch",
            Self::Process => "process",
            Self::PendingDirective => "pending directive",
            Self::Recovery => "recovery",
            Self::Worker => "worker",
            Self::Reconciler => "reconciler",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid {kind} state value {raw}")]
pub(crate) struct RuntimeStateDecodeError {
    pub(crate) kind: RuntimeStateKind,
    pub(crate) raw: u8,
}

impl RuntimeStateDecodeError {
    const fn new(kind: RuntimeStateKind, raw: u8) -> Self {
        Self { kind, raw }
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
    const fn worker(current: &'static str, next: &'static str) -> Self {
        Self {
            kind: RuntimeStateKind::Worker,
            current,
            next,
        }
    }

    pub(crate) const fn reconciler(
        current: &'static str,
        next: &'static str,
    ) -> Self {
        Self {
            kind: RuntimeStateKind::Reconciler,
            current,
            next,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RegistrationState {
    Empty = 0,
    PendingCommit = 1,
    Registered = 2,
    Removing = 3,
}

impl RegistrationState {
    pub(crate) fn decode(raw: u8) -> Result<Self, RuntimeStateDecodeError> {
        match raw {
            0 => Ok(Self::Empty),
            1 => Ok(Self::PendingCommit),
            2 => Ok(Self::Registered),
            3 => Ok(Self::Removing),
            _ => Err(RuntimeStateDecodeError::new(
                RuntimeStateKind::Registration,
                raw,
            )),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::PendingCommit => "pending_commit",
            Self::Registered => "registered",
            Self::Removing => "removing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum DispatchState {
    Idle = 0,
    Ready = 1,
    Delayed = 2,
}

impl DispatchState {
    pub(crate) fn decode(raw: u8) -> Result<Self, RuntimeStateDecodeError> {
        match raw {
            0 => Ok(Self::Idle),
            1 => Ok(Self::Ready),
            2 => Ok(Self::Delayed),
            _ => Err(RuntimeStateDecodeError::new(
                RuntimeStateKind::Dispatch,
                raw,
            )),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Ready => "ready",
            Self::Delayed => "delayed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ProcessState {
    Stopped = 0,
    Starting = 1,
    Running = 2,
    Exiting = 3,
}

impl ProcessState {
    pub(crate) fn decode(raw: u8) -> Result<Self, RuntimeStateDecodeError> {
        match raw {
            0 => Ok(Self::Stopped),
            1 => Ok(Self::Starting),
            2 => Ok(Self::Running),
            3 => Ok(Self::Exiting),
            _ => Err(RuntimeStateDecodeError::new(RuntimeStateKind::Process, raw)),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Exiting => "exiting",
        }
    }

    pub(crate) const fn is_active(self) -> bool {
        !matches!(self, Self::Stopped)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RecoveryState {
    Ready = 0,
    Recovering = 1,
    Reconciling = 2,
}

impl RecoveryState {
    pub(crate) fn decode(raw: u8) -> Result<Self, RuntimeStateDecodeError> {
        match raw {
            0 => Ok(Self::Ready),
            1 => Ok(Self::Recovering),
            2 => Ok(Self::Reconciling),
            _ => Err(RuntimeStateDecodeError::new(
                RuntimeStateKind::Recovery,
                raw,
            )),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Recovering => "recovering",
            Self::Reconciling => "reconciling",
        }
    }

    pub(crate) const fn allows_reconciliation(self) -> bool {
        matches!(self, Self::Ready | Self::Reconciling)
    }

    pub(crate) const fn allows_dispatch(self) -> bool {
        matches!(self, Self::Ready | Self::Reconciling)
    }

    pub(crate) const fn allows_stop_completion(self) -> bool {
        matches!(self, Self::Ready | Self::Reconciling)
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct RuntimeSharedState {
    pub(crate) magic: u64,
    pub(crate) struct_size: u32,
    pub(crate) launcher_pid: i32,
    pub(crate) launcher_proc_number: i32,
    pub(crate) launcher_epoch: u64,
    pub(crate) recovery_state: u8,
    pub(crate) rescan_all: u8,
    #[cfg(feature = "pg_test")]
    pub(crate) test_injection: u8,
    #[cfg(feature = "pg_test")]
    pub(crate) test_barrier_reached: u8,
    #[cfg(not(feature = "pg_test"))]
    pub(crate) _padding: [u8; 2],
    pub(crate) worker_cursor: u32,
    pub(crate) last_capacity_warning_ms: i64,
    /// Previous-generation dynamic backends visible during the latest recovery
    /// scan. This is diagnostic only; the recovery phase is the dispatch and
    /// DDL-completion gate.
    pub(crate) recovery_backend_count: u32,
    pub(crate) _recovery_padding: u32,
    pub(crate) database_reconciles: [DatabaseReconcileState; MAX_DATABASE_RECONCILES],
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
            launcher_epoch: 0,
            recovery_state: RecoveryState::Ready as u8,
            rescan_all: 1,
            #[cfg(feature = "pg_test")]
            test_injection: 0,
            #[cfg(feature = "pg_test")]
            test_barrier_reached: 0,
            #[cfg(not(feature = "pg_test"))]
            _padding: [0; 2],
            worker_cursor: 0,
            last_capacity_warning_ms: 0,
            recovery_backend_count: 0,
            _recovery_padding: 0,
            database_reconciles: [DatabaseReconcileState::EMPTY;
                MAX_DATABASE_RECONCILES],
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
            && RecoveryState::decode(self.recovery_state).is_ok()
            && self.reconcilers.iter().all(|slot| slot.process().is_ok())
            && self.workers.iter().all(|slot| slot.validate().is_ok())
    }

    pub(crate) fn reconciler_slot(&self, database_oid: u32) -> Option<usize> {
        self.reconcilers.iter().position(|slot| {
            slot.database_oid == database_oid
                && slot.process().is_ok_and(ProcessState::is_active)
        })
    }

    pub(crate) fn database_reconcile_slot(&self, database_oid: u32) -> Option<usize> {
        self.database_reconciles
            .iter()
            .position(|intent| intent.database_oid == database_oid)
    }

    pub(crate) fn empty_database_reconcile_slot(&self) -> Option<usize> {
        self.database_reconciles
            .iter()
            .position(|intent| intent.database_oid == INVALID_OID)
    }

    pub(crate) fn empty_reconciler_slot(&self) -> Option<usize> {
        self.reconcilers.iter().position(|slot| {
            slot.database_oid == INVALID_OID
                && slot.process() == Ok(ProcessState::Stopped)
        })
    }

    pub(crate) fn worker_slot(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> Option<usize> {
        self.workers.iter().position(|slot| {
            slot.matches_worker(database_oid, extension_oid, worker_name)
        })
    }

    pub(crate) fn empty_worker_slot(&self) -> Option<usize> {
        self.workers.iter().position(WorkerSlot::is_empty)
    }

    pub(crate) fn allows_stop_completion(&self) -> bool {
        RecoveryState::decode(self.recovery_state)
            .is_ok_and(RecoveryState::allows_stop_completion)
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
    fn layout_validation_rejects_mismatch_and_invalid_discriminants() {
        let mut state = RuntimeSharedState::default();
        assert!(state.validate_layout());
        state.struct_size += 1;
        assert!(!state.validate_layout());

        let mut state = RuntimeSharedState::default();
        state.workers[0].registration_state = u8::MAX;
        assert!(!state.validate_layout());

        let state = RuntimeSharedState {
            recovery_state: u8::MAX,
            ..RuntimeSharedState::default()
        };
        assert!(!state.validate_layout());
        assert!(RegistrationState::decode(u8::MAX).is_err());
        assert!(DispatchState::decode(u8::MAX).is_err());
        assert!(ProcessState::decode(u8::MAX).is_err());
        assert!(RecoveryState::decode(u8::MAX).is_err());
    }

    #[test]
    fn recovery_phase_controls_dispatch_and_ddl_completion() {
        assert!(RecoveryState::Reconciling.allows_reconciliation());
        assert!(RecoveryState::Reconciling.allows_dispatch());
        assert!(RecoveryState::Reconciling.allows_stop_completion());
        assert_ne!(RecoveryState::Reconciling, RecoveryState::Ready);
        assert!(!RecoveryState::Recovering.allows_reconciliation());
        assert!(!RecoveryState::Recovering.allows_dispatch());
        assert!(!RecoveryState::Recovering.allows_stop_completion());
        let mut state = RuntimeSharedState::default();
        assert!(state.allows_stop_completion());
        state.recovery_state = RecoveryState::Recovering as u8;
        assert!(!state.allows_stop_completion());
    }
}
