use std::fmt;

use pgrx::PGRXSharedMemory;

pub(crate) const RUNTIME_MAGIC: u64 = 0x5047_4c41_4b45_4257;
pub(crate) const MAX_DATABASES: usize = 128;
pub(crate) const MAX_WORKERS: usize = 512;
pub(crate) const MAX_WORKER_NAME_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeStateKind {
    Database,
    Worker,
}

impl fmt::Display for RuntimeStateKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Database => "database",
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
    const fn database(current: DatabaseState, next: DatabaseState) -> Self {
        Self {
            kind: RuntimeStateKind::Database,
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
pub(crate) enum DatabaseState {
    Empty = 0,
    Dirty = 1,
    Reconciling = 2,
    Clean = 3,
    Stopping = 4,
}

impl DatabaseState {
    pub(crate) fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Dirty,
            2 => Self::Reconciling,
            3 => Self::Clean,
            4 => Self::Stopping,
            _ => Self::Empty,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Dirty => "dirty",
            Self::Reconciling => "reconciling",
            Self::Clean => "clean",
            Self::Stopping => "stopping",
        }
    }

    pub(crate) const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Dirty,
                Self::Dirty | Self::Reconciling | Self::Stopping
            ) | (
                Self::Reconciling,
                Self::Clean | Self::Dirty | Self::Stopping
            ) | (Self::Clean, Self::Dirty | Self::Stopping)
                | (Self::Stopping, Self::Dirty | Self::Stopping)
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
                | (Self::Stopping, Self::Dormant | Self::Stopping)
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
pub(crate) struct DatabaseSlot {
    pub(crate) database_oid: u32,
    pub(crate) generation: u32,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) state: u8,
    pub(crate) _padding: [u8; 3],
}

impl DatabaseSlot {
    pub(crate) const EMPTY: Self = Self {
        database_oid: 0,
        generation: 0,
        pid: 0,
        proc_number: 0,
        state: DatabaseState::Empty as u8,
        _padding: [0; 3],
    };

    pub(crate) fn transition_to(
        &mut self,
        next: DatabaseState,
    ) -> Result<(), RuntimeStateTransitionError> {
        let current = DatabaseState::from_raw(self.state);
        if !current.can_transition_to(next) {
            return Err(RuntimeStateTransitionError::database(current, next));
        }
        self.state = next as u8;
        Ok(())
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct WorkerSlot {
    pub(crate) database_oid: u32,
    pub(crate) extension_oid: u32,
    pub(crate) function_oid: u32,
    pub(crate) generation: u32,
    pub(crate) pid: i32,
    pub(crate) proc_number: i32,
    pub(crate) restart_at_ms: i64,
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
        function_oid: 0,
        generation: 0,
        pid: 0,
        proc_number: 0,
        restart_at_ms: 0,
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
    pub(crate) database_cursor: u32,
    pub(crate) worker_cursor: u32,
    pub(crate) last_capacity_warning_ms: i64,
    pub(crate) databases: [DatabaseSlot; MAX_DATABASES],
    pub(crate) workers: [WorkerSlot; MAX_WORKERS],
}

impl Default for RuntimeSharedState {
    fn default() -> Self {
        Self {
            magic: RUNTIME_MAGIC,
            struct_size: u32::try_from(std::mem::size_of::<Self>())
                .expect("runtime shared state exceeds u32"),
            launcher_pid: 0,
            launcher_proc_number: 0,
            rescan_all: 1,
            _padding: [0; 3],
            database_cursor: 0,
            worker_cursor: 0,
            last_capacity_warning_ms: 0,
            databases: [DatabaseSlot::EMPTY; MAX_DATABASES],
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

    pub(crate) fn database_slot(&self, database_oid: u32) -> Option<usize> {
        self.databases
            .iter()
            .position(|slot| slot.database_oid == database_oid)
    }

    pub(crate) fn ensure_database_slot(
        &mut self,
        database_oid: u32,
    ) -> Option<usize> {
        if let Some(index) = self.database_slot(database_oid) {
            return Some(index);
        }
        let index = self.databases.iter().position(|slot| {
            DatabaseState::from_raw(slot.state) == DatabaseState::Empty
        })?;
        let generation = self.databases[index].generation.wrapping_add(1).max(1);
        self.databases[index] = DatabaseSlot {
            database_oid,
            generation,
            pid: 0,
            proc_number: 0,
            state: DatabaseState::Dirty as u8,
            _padding: [0; 3],
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
        if worker_name.len() > MAX_WORKER_NAME_BYTES {
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

    pub(crate) fn clear_database_slot(&mut self, index: usize) {
        let generation = self.databases[index].generation;
        self.databases[index] = DatabaseSlot {
            generation,
            ..DatabaseSlot::EMPTY
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
        let database = state.ensure_database_slot(42).unwrap();
        assert_eq!(state.ensure_database_slot(42), Some(database));
        let worker = state.ensure_worker_slot(42, 8, "worker").unwrap();
        assert_eq!(state.ensure_worker_slot(42, 8, "worker"), Some(worker));
        assert_ne!(state.workers[worker].generation, 0);

        let old_generation = state.workers[worker].generation;
        state.clear_worker_slot(worker);
        let reused = state.ensure_worker_slot(42, 8, "other").unwrap();
        assert_eq!(reused, worker);
        assert_ne!(state.workers[reused].generation, old_generation);
    }

    #[test]
    fn state_transition_tables_reject_invalid_edges() {
        assert!(WorkerState::Dormant.can_transition_to(WorkerState::Starting));
        assert!(WorkerState::Starting.can_transition_to(WorkerState::Running));
        assert!(WorkerState::Running.can_transition_to(WorkerState::Scheduled));
        assert!(WorkerState::Running.can_transition_to(WorkerState::Backoff));
        assert!(WorkerState::Scheduled.can_transition_to(WorkerState::Dormant));
        assert!(WorkerState::Stopping.can_transition_to(WorkerState::Dormant));
        assert!(!WorkerState::Dormant.can_transition_to(WorkerState::Running));
        assert!(!WorkerState::Scheduled.can_transition_to(WorkerState::Starting));
        assert!(!WorkerState::Backoff.can_transition_to(WorkerState::Running));

        assert!(DatabaseState::Dirty.can_transition_to(DatabaseState::Reconciling));
        assert!(DatabaseState::Reconciling.can_transition_to(DatabaseState::Clean));
        assert!(DatabaseState::Stopping.can_transition_to(DatabaseState::Dirty));
        assert!(!DatabaseState::Clean.can_transition_to(DatabaseState::Reconciling));
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

        let mut database = DatabaseSlot {
            state: DatabaseState::Clean as u8,
            ..DatabaseSlot::EMPTY
        };
        assert!(database.transition_to(DatabaseState::Reconciling).is_err());
        assert_eq!(
            DatabaseState::from_raw(database.state),
            DatabaseState::Clean
        );
    }
}
