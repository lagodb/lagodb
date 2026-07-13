use std::collections::HashSet;

use pgrx::PgLwLock;
use pgrx::prelude::*;

use crate::error::{LakebaseError, LakebaseResult};
use crate::gucs;
use crate::registry::WorkerRegistration;
use crate::state::{
    DatabaseState, MAX_DATABASES, RuntimeSharedState, RuntimeStateTransitionError,
    WorkerState,
};
use pg_lakebase_core::extension_worker::WorkerExit;

use super::bgworker::{SlotToken, timestamp_ms};
use super::status::{ProcessStatus, WorkerStatus};
use super::{CAPACITY_WARNING_INTERVAL, CRASH_BACKOFF};

pub(crate) static RUNTIME_STATE: PgLwLock<RuntimeSharedState> =
    unsafe { PgLwLock::new(c"pg_lakebase_runtime worker runtime") };

pub(super) struct RuntimeStore;

pub(super) struct RegistrationReconciliation {
    pub(super) starts: Vec<SlotToken>,
    pub(super) capacity_exhausted: bool,
}

pub(super) struct WorkerStart {
    pub(super) database_oid: u32,
    pub(super) extension_oid: u32,
    pub(super) worker_name: String,
}

impl RuntimeStore {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) fn reserve_registration(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> LakebaseResult<()> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        let active = state
            .workers
            .iter()
            .filter(|slot| WorkerState::from_raw(slot.state) != WorkerState::Empty)
            .count();
        if active >= gucs::max_registrations() {
            return Err(LakebaseError::MaxWorkerRegistrationsExhausted);
        }
        state
            .ensure_database_slot(database_oid)
            .ok_or(LakebaseError::DatabaseRuntimeStateExhausted)?;
        let index = state
            .ensure_worker_slot(database_oid, extension_oid, worker_name)
            .ok_or(LakebaseError::WorkerRegistrationStateExhausted)?;
        state.workers[index].transition_to(WorkerState::PendingRegistration)?;
        Ok(())
    }

    pub(super) fn finish_registration(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
        committed: bool,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(index) = state.worker_slot(database_oid, extension_oid, worker_name)
        else {
            return false;
        };
        if committed {
            if let Err(error) =
                state.workers[index].transition_to(WorkerState::Dormant)
            {
                warn_transition_error(error);
                return false;
            }
            state.workers[index].wake_requested = 1;
            if let Err(error) = mark_database_dirty_locked(&mut state, database_oid) {
                warn_transition_error(error);
                return false;
            }
            true
        } else if WorkerState::from_raw(state.workers[index].state)
            == WorkerState::PendingRegistration
        {
            state.clear_worker_slot(index);
            true
        } else {
            false
        }
    }

    pub(super) fn wake_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        if let Some(index) =
            state.worker_slot(database_oid, extension_oid, worker_name)
        {
            if WorkerState::from_raw(state.workers[index].state)
                == WorkerState::Scheduled
            {
                if let Err(error) =
                    state.workers[index].transition_to(WorkerState::Dormant)
                {
                    warn_transition_error(error);
                    return false;
                }
                state.workers[index].restart_at_ms = 0;
            }
            state.workers[index].wake_requested = 1;
        }
        if let Err(error) = mark_database_dirty_locked(&mut state, database_oid) {
            warn_transition_error(error);
            return false;
        }
        true
    }

    pub(super) fn mark_database_dirty(&self, database_oid: u32) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        match mark_database_dirty_locked(&mut state, database_oid) {
            Ok(()) => true,
            Err(error) => {
                warn_transition_error(error);
                false
            }
        }
    }

    pub(super) fn request_full_rescan(&self) -> bool {
        RUNTIME_STATE.exclusive().rescan_all = 1;
        true
    }

    pub(super) fn signal_launcher(&self) {
        let proc_number = RUNTIME_STATE.share().launcher_proc_number;
        if proc_number > 0 {
            // SAFETY: proc_number was published by a live PostgreSQL process. A
            // stale value is harmless; ProcSendSignal validates the process slot.
            unsafe { pg_sys::ProcSendSignal(proc_number) };
        }
    }

    pub(super) fn worker_status(&self) -> Vec<WorkerStatus> {
        let state = RUNTIME_STATE.share();
        state
            .workers
            .iter()
            .filter_map(|slot| {
                let worker_state = WorkerState::from_raw(slot.state);
                (worker_state != WorkerState::Empty).then_some(WorkerStatus {
                    database_oid: slot.database_oid,
                    extension_oid: slot.extension_oid,
                    worker_name: slot.worker_name_str().to_owned(),
                    state: worker_state.as_str(),
                    pid: (slot.pid > 0).then_some(slot.pid),
                    restart_at_ms: matches!(
                        worker_state,
                        WorkerState::Scheduled | WorkerState::Backoff
                    )
                    .then_some(slot.restart_at_ms),
                })
            })
            .collect()
    }

    pub(super) fn process_status(&self) -> Vec<ProcessStatus> {
        let state = RUNTIME_STATE.share();
        let mut statuses = Vec::with_capacity(1 + MAX_DATABASES);
        statuses.push(ProcessStatus {
            process_kind: "launcher",
            database_oid: None,
            state: if state.launcher_pid > 0 {
                "running"
            } else {
                "stopped"
            },
            pid: (state.launcher_pid > 0).then_some(state.launcher_pid),
        });
        statuses.extend(state.databases.iter().filter_map(|slot| {
            let database_state = DatabaseState::from_raw(slot.state);
            (database_state != DatabaseState::Empty).then_some(ProcessStatus {
                process_kind: "database_reconciler",
                database_oid: Some(slot.database_oid),
                state: database_state.as_str(),
                pid: (slot.pid > 0).then_some(slot.pid),
            })
        }));
        statuses
    }

    pub(super) fn set_launcher_running(&self) {
        let mut state = RUNTIME_STATE.exclusive();
        state.launcher_pid = unsafe { pg_sys::MyProcPid };
        state.launcher_proc_number = unsafe { pg_sys::MyProcNumber };
        state.rescan_all = 1;
    }

    pub(super) fn clear_launcher(&self) {
        let mut state = RUNTIME_STATE.exclusive();
        state.launcher_pid = 0;
        state.launcher_proc_number = 0;
    }

    pub(super) fn take_full_scan_request(&self, interval_elapsed: bool) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let requested = state.rescan_all != 0 || interval_elapsed;
        if requested {
            state.rescan_all = 0;
        }
        requested
    }

    pub(super) fn apply_database_scan(
        &self,
        known_databases: &mut HashSet<u32>,
        databases: Vec<u32>,
    ) -> bool {
        let live: HashSet<_> = databases.iter().copied().collect();
        let mut state = RUNTIME_STATE.exclusive();
        let mut capacity_exhausted = false;
        for database_oid in databases {
            if !known_databases.contains(&database_oid) {
                if state.ensure_database_slot(database_oid).is_some() {
                    known_databases.insert(database_oid);
                } else {
                    state.rescan_all = 1;
                    capacity_exhausted = true;
                }
            }
        }
        known_databases.retain(|database_oid| live.contains(database_oid));
        let mut dropped = Vec::new();
        for index in 0..state.databases.len() {
            let database_oid = state.databases[index].database_oid;
            if database_oid != 0 && !live.contains(&database_oid) {
                dropped.push(database_oid);
                state.clear_database_slot(index);
            }
        }
        for index in 0..state.workers.len() {
            let worker = state.workers[index];
            if dropped.contains(&worker.database_oid) && worker.pid == 0 {
                state.clear_worker_slot(index);
            }
        }
        capacity_exhausted
    }

    pub(super) fn schedule_due_workers(&self) {
        let now = timestamp_ms();
        let mut state = RUNTIME_STATE.exclusive();
        let mut dirty = [0_u32; MAX_DATABASES];
        let mut dirty_count = 0_usize;
        let mut overflowed = false;
        for worker in &mut state.workers {
            if matches!(
                WorkerState::from_raw(worker.state),
                WorkerState::Scheduled | WorkerState::Backoff
            ) && worker.restart_at_ms <= now
            {
                worker.wake_requested = 1;
                if let Err(error) = worker.transition_to(WorkerState::Dormant) {
                    warn_transition_error(error);
                    continue;
                }
                if dirty[..dirty_count].contains(&worker.database_oid) {
                    continue;
                }
                if dirty_count < dirty.len() {
                    dirty[dirty_count] = worker.database_oid;
                    dirty_count += 1;
                } else {
                    overflowed = true;
                }
            }
        }
        for database_oid in dirty.into_iter().take(dirty_count) {
            if let Err(error) = mark_database_dirty_locked(&mut state, database_oid) {
                warn_transition_error(error);
            }
        }
        if overflowed {
            state.rescan_all = 1;
        }
    }

    pub(super) fn mark_all_worker_databases_dirty(&self) {
        let mut state = RUNTIME_STATE.exclusive();
        for index in 0..state.workers.len() {
            let worker = state.workers[index];
            let database_oid = worker.database_oid;
            if matches!(
                WorkerState::from_raw(worker.state),
                WorkerState::Empty
                    | WorkerState::PendingRegistration
                    | WorkerState::Stopping
            ) || database_oid == 0
            {
                continue;
            }
            if state.database_slot(database_oid).is_some_and(|index| {
                DatabaseState::from_raw(state.databases[index].state)
                    == DatabaseState::Stopping
            }) {
                continue;
            }
            if let Err(error) = mark_database_dirty_locked(&mut state, database_oid) {
                warn_transition_error(error);
            }
        }
    }

    pub(super) fn next_dirty_reconciler(&self) -> Option<SlotToken> {
        let mut state = RUNTIME_STATE.exclusive();
        let active = state
            .databases
            .iter()
            .filter(|slot| {
                DatabaseState::from_raw(slot.state) == DatabaseState::Reconciling
            })
            .count();
        if active >= gucs::max_database_reconcilers() {
            return None;
        }
        let start = state.database_cursor as usize % MAX_DATABASES;
        let candidate = (0..MAX_DATABASES)
            .map(|offset| (start + offset) % MAX_DATABASES)
            .find(|&index| {
                DatabaseState::from_raw(state.databases[index].state)
                    == DatabaseState::Dirty
            });
        candidate.and_then(|index| {
            if let Err(error) =
                state.databases[index].transition_to(DatabaseState::Reconciling)
            {
                warn_transition_error(error);
                return None;
            }
            state.database_cursor = ((index + 1) % MAX_DATABASES) as u32;
            Some(SlotToken::new(index, state.databases[index].generation))
        })
    }

    pub(super) fn reconciler_start_failed(&self, token: SlotToken) {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.databases.get_mut(token.index()) else {
            return;
        };
        if slot.generation == token.generation()
            && DatabaseState::from_raw(slot.state) == DatabaseState::Reconciling
            && let Err(error) = slot.transition_to(DatabaseState::Dirty)
        {
            warn_transition_error(error);
        }
    }

    pub(super) fn begin_reconciler(&self, token: SlotToken) -> Option<u32> {
        let mut state = RUNTIME_STATE.exclusive();
        let slot = state.databases.get_mut(token.index())?;
        if slot.generation != token.generation()
            || DatabaseState::from_raw(slot.state) != DatabaseState::Reconciling
        {
            return None;
        }
        slot.pid = unsafe { pg_sys::MyProcPid };
        slot.proc_number = unsafe { pg_sys::MyProcNumber };
        Some(slot.database_oid)
    }

    pub(super) fn finish_reconciler(
        &self,
        token: SlotToken,
        needs_retry: bool,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.databases.get(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let current = DatabaseState::from_raw(slot.state);
        {
            let slot = &mut state.databases[token.index()];
            slot.pid = 0;
            slot.proc_number = 0;
            if current == DatabaseState::Reconciling
                && let Err(error) = slot.transition_to(if needs_retry {
                    DatabaseState::Dirty
                } else {
                    DatabaseState::Clean
                })
            {
                warn_transition_error(error);
            }
        }
        current == DatabaseState::Dirty
            || has_dirty_database_except(&state, token.index())
    }

    pub(super) fn reconciler_exit(&self, token: SlotToken) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.databases.get(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let current = DatabaseState::from_raw(slot.state);
        {
            let slot = &mut state.databases[token.index()];
            slot.pid = 0;
            slot.proc_number = 0;
            if current == DatabaseState::Reconciling
                && let Err(error) = slot.transition_to(DatabaseState::Dirty)
            {
                warn_transition_error(error);
            }
        }
        current == DatabaseState::Reconciling
            && has_dirty_database_except(&state, token.index())
    }

    pub(super) fn reconcile_registrations(
        &self,
        database_oid: u32,
        registrations: &[WorkerRegistration],
    ) -> RegistrationReconciliation {
        let live: HashSet<(u32, &str)> = registrations
            .iter()
            .map(|registration| {
                (
                    registration.extension_oid.to_u32(),
                    registration.worker_name.as_str(),
                )
            })
            .collect();
        let mut starts = Vec::new();
        let mut capacity_exhausted = false;
        {
            let mut state = RUNTIME_STATE.exclusive();
            let mut active = state
                .workers
                .iter()
                .filter(|slot| {
                    WorkerState::from_raw(slot.state) != WorkerState::Empty
                })
                .count();
            let start = if registrations.is_empty() {
                0
            } else {
                state.worker_cursor as usize % registrations.len()
            };
            for offset in 0..registrations.len() {
                let registration =
                    &registrations[(start + offset) % registrations.len()];
                let extension_oid = registration.extension_oid.to_u32();
                let existing = state.worker_slot(
                    database_oid,
                    extension_oid,
                    &registration.worker_name,
                );
                if existing.is_none() && active >= gucs::max_registrations() {
                    capacity_exhausted = true;
                    continue;
                }
                let Some(index) = state.ensure_worker_slot(
                    database_oid,
                    extension_oid,
                    &registration.worker_name,
                ) else {
                    capacity_exhausted = true;
                    continue;
                };
                if existing.is_none() {
                    active += 1;
                }
                let slot = &mut state.workers[index];
                slot.extension_oid = extension_oid;
                slot.function_oid = registration.function_oid.to_u32();
                if WorkerState::from_raw(slot.state) == WorkerState::Stopping
                    && slot.pid == 0
                    && let Err(error) = slot.transition_to(WorkerState::Dormant)
                {
                    warn_transition_error(error);
                    continue;
                }
                let should_start = existing.is_none() || slot.wake_requested != 0;
                if should_start
                    && matches!(
                        WorkerState::from_raw(slot.state),
                        WorkerState::Dormant | WorkerState::PendingRegistration
                    )
                {
                    slot.wake_requested = 0;
                    if let Err(error) = slot.transition_to(WorkerState::Starting) {
                        warn_transition_error(error);
                        continue;
                    }
                    slot.generation = slot.generation.wrapping_add(1).max(1);
                    starts.push(SlotToken::new(index, slot.generation));
                }
            }
            if !registrations.is_empty() {
                state.worker_cursor = ((start + 1) % registrations.len()) as u32;
            }
            for index in 0..state.workers.len() {
                let should_clear = {
                    let slot = &state.workers[index];
                    slot.database_oid == database_oid
                        && WorkerState::from_raw(slot.state) != WorkerState::Empty
                        && WorkerState::from_raw(slot.state)
                            != WorkerState::PendingRegistration
                        && !live
                            .contains(&(slot.extension_oid, slot.worker_name_str()))
                        && slot.pid == 0
                };
                if should_clear {
                    state.clear_worker_slot(index);
                }
            }
        }
        RegistrationReconciliation {
            starts,
            capacity_exhausted,
        }
    }

    pub(super) fn worker_start_failed(&self, token: SlotToken) {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.workers.get_mut(token.index()) else {
            return;
        };
        if slot.generation == token.generation()
            && WorkerState::from_raw(slot.state) == WorkerState::Starting
        {
            if let Err(error) = slot.transition_to(WorkerState::Backoff) {
                warn_transition_error(error);
                return;
            }
            slot.restart_at_ms = timestamp_ms() + CRASH_BACKOFF.as_millis() as i64;
        }
    }

    pub(super) fn remove_database_state(&self, token: SlotToken, database_oid: u32) {
        let mut state = RUNTIME_STATE.exclusive();
        for index in 0..state.workers.len() {
            if state.workers[index].database_oid == database_oid
                && state.workers[index].pid == 0
            {
                state.clear_worker_slot(index);
            }
        }
        if state
            .databases
            .get(token.index())
            .is_some_and(|slot| slot.generation == token.generation())
        {
            state.clear_database_slot(token.index());
        }
    }

    pub(super) fn begin_worker(&self, token: SlotToken) -> Option<WorkerStart> {
        let mut state = RUNTIME_STATE.exclusive();
        let slot = state.workers.get_mut(token.index())?;
        if slot.generation != token.generation()
            || WorkerState::from_raw(slot.state) != WorkerState::Starting
        {
            return None;
        }
        slot.pid = unsafe { pg_sys::MyProcPid };
        slot.proc_number = unsafe { pg_sys::MyProcNumber };
        if let Err(error) = slot.transition_to(WorkerState::Running) {
            warn_transition_error(error);
            return None;
        }
        Some(WorkerStart {
            database_oid: slot.database_oid,
            extension_oid: slot.extension_oid,
            worker_name: slot.worker_name_str().to_owned(),
        })
    }

    pub(super) fn worker_exit(&self, token: SlotToken, code: i32) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.workers.get_mut(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let previous = WorkerState::from_raw(slot.state);
        slot.pid = 0;
        slot.proc_number = 0;
        match previous {
            WorkerState::Running | WorkerState::Starting => {
                if let Err(error) = slot.transition_to(WorkerState::Backoff) {
                    warn_transition_error(error);
                    return false;
                }
                slot.restart_at_ms =
                    timestamp_ms().saturating_add(CRASH_BACKOFF.as_millis() as i64);
                true
            }
            WorkerState::Stopping => {
                if let Err(error) = slot.transition_to(WorkerState::Dormant) {
                    warn_transition_error(error);
                    return false;
                }
                true
            }
            _ if code != 0 => true,
            _ => false,
        }
    }

    pub(super) fn finish_worker(&self, token: SlotToken, directive: WorkerExit) {
        let mut state = RUNTIME_STATE.exclusive();
        let database_oid;
        let extension_oid;
        let worker_name;
        let previous;
        {
            let Some(slot) = state.workers.get_mut(token.index()) else {
                return;
            };
            if slot.generation != token.generation() {
                return;
            }
            slot.pid = 0;
            slot.proc_number = 0;
            database_oid = slot.database_oid;
            extension_oid = slot.extension_oid;
            worker_name = slot.worker_name_str().to_owned();
            previous = WorkerState::from_raw(slot.state);
        }
        if previous == WorkerState::Stopping {
            let slot = &mut state.workers[token.index()];
            if let Err(error) = slot.transition_to(WorkerState::Dormant) {
                warn_transition_error(error);
                return;
            }
            slot.restart_at_ms = 0;
            drop(state);
            crate::diag::info(format_args!(
                "finished stopping Lakebase extension worker: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}, ignored_directive={directive:?}",
                token.generation()
            ));
            self.signal_launcher();
            return;
        }
        match directive {
            WorkerExit::Dormant => {
                let slot = &mut state.workers[token.index()];
                if let Err(error) = slot.transition_to(WorkerState::Dormant) {
                    warn_transition_error(error);
                    return;
                }
                slot.restart_at_ms = 0;
            }
            WorkerExit::RestartImmediately => {
                let slot = &mut state.workers[token.index()];
                if let Err(error) = slot.transition_to(WorkerState::Dormant) {
                    warn_transition_error(error);
                    return;
                }
                slot.wake_requested = 1;
                if let Err(error) =
                    mark_database_dirty_locked(&mut state, database_oid)
                {
                    warn_transition_error(error);
                    return;
                }
            }
            WorkerExit::RestartAfter(delay) => {
                let slot = &mut state.workers[token.index()];
                if let Err(error) = slot.transition_to(WorkerState::Scheduled) {
                    warn_transition_error(error);
                    return;
                }
                slot.restart_at_ms = timestamp_ms().saturating_add(
                    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
                );
            }
        }
        drop(state);
        crate::diag::info(format_args!(
            "finished Lakebase extension worker: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}, directive={directive:?}",
            token.generation()
        ));
        self.signal_launcher();
    }

    pub(super) fn stop_database_step(
        &self,
        database_oid: u32,
    ) -> LakebaseResult<Vec<i32>> {
        let mut state = RUNTIME_STATE.exclusive();
        let mut pids = Vec::new();
        if let Some(index) = state.database_slot(database_oid) {
            let database = &mut state.databases[index];
            database.transition_to(DatabaseState::Stopping)?;
            if database.pid > 0 {
                pids.push(database.pid);
            }
        }
        for slot in &mut state.workers {
            if slot.database_oid == database_oid
                && WorkerState::from_raw(slot.state) != WorkerState::Empty
            {
                if WorkerState::from_raw(slot.state) == WorkerState::Starting
                    && slot.pid == 0
                {
                    slot.generation = slot.generation.wrapping_add(1).max(1);
                }
                if WorkerState::from_raw(slot.state) != WorkerState::Stopping {
                    slot.transition_to(WorkerState::Stopping)?;
                }
                slot.wake_requested = 1;
                if slot.pid > 0 {
                    pids.push(slot.pid);
                }
            }
        }
        Ok(pids)
    }

    pub(super) fn database_has_running_processes(&self, database_oid: u32) -> bool {
        let state = RUNTIME_STATE.share();
        let reconciler_running = state
            .database_slot(database_oid)
            .is_some_and(|index| state.databases[index].pid > 0);
        reconciler_running
            || state
                .workers
                .iter()
                .any(|slot| slot.database_oid == database_oid && slot.pid > 0)
    }

    pub(super) fn stop_extension_step(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> LakebaseResult<Vec<i32>> {
        let mut state = RUNTIME_STATE.exclusive();
        let mut pids = Vec::new();
        for slot in &mut state.workers {
            if slot.database_oid == database_oid
                && slot.extension_oid == extension_oid
                && WorkerState::from_raw(slot.state) != WorkerState::Empty
            {
                if WorkerState::from_raw(slot.state) == WorkerState::Starting
                    && slot.pid == 0
                {
                    slot.generation = slot.generation.wrapping_add(1).max(1);
                }
                if WorkerState::from_raw(slot.state) != WorkerState::Stopping {
                    slot.transition_to(WorkerState::Stopping)?;
                }
                slot.wake_requested = 1;
                if slot.pid > 0 {
                    pids.push(slot.pid);
                }
            }
        }
        Ok(pids)
    }

    pub(super) fn extension_has_running_workers(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> bool {
        RUNTIME_STATE.share().workers.iter().any(|slot| {
            slot.database_oid == database_oid
                && slot.extension_oid == extension_oid
                && slot.pid > 0
        })
    }

    pub(super) fn pause_reconciliation_step(
        &self,
        database_oid: u32,
    ) -> LakebaseResult<Option<i32>> {
        let mut state = RUNTIME_STATE.exclusive();
        if let Some(index) = state.database_slot(database_oid) {
            let slot = &mut state.databases[index];
            if DatabaseState::from_raw(slot.state) != DatabaseState::Stopping {
                slot.transition_to(DatabaseState::Stopping)?;
            }
            Ok((slot.pid > 0).then_some(slot.pid))
        } else {
            Ok(None)
        }
    }

    pub(super) fn stop_worker_step(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> LakebaseResult<Option<i32>> {
        let mut state = RUNTIME_STATE.exclusive();
        if let Some(index) =
            state.worker_slot(database_oid, extension_oid, worker_name)
        {
            let slot = &mut state.workers[index];
            slot.wake_requested = 1;
            if WorkerState::from_raw(slot.state) == WorkerState::Starting
                && slot.pid == 0
            {
                slot.generation = slot.generation.wrapping_add(1).max(1);
            }
            if WorkerState::from_raw(slot.state) != WorkerState::Stopping {
                slot.transition_to(WorkerState::Stopping)?;
            }
            Ok((slot.pid > 0).then_some(slot.pid))
        } else {
            Ok(None)
        }
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

fn mark_database_dirty_locked(
    state: &mut RuntimeSharedState,
    database_oid: u32,
) -> Result<(), RuntimeStateTransitionError> {
    if let Some(index) = state.ensure_database_slot(database_oid) {
        state.databases[index].transition_to(DatabaseState::Dirty)?;
    } else {
        state.rescan_all = 1;
    }
    Ok(())
}

fn validate_state(state: &RuntimeSharedState) -> LakebaseResult<()> {
    if state.validate_layout() {
        Ok(())
    } else {
        Err(LakebaseError::SharedMemoryLayoutMismatch)
    }
}

fn warn_transition_error(error: RuntimeStateTransitionError) {
    crate::diag::warning(format_args!("{error}"));
}

fn has_dirty_database_except(
    state: &RuntimeSharedState,
    except_index: usize,
) -> bool {
    state.databases.iter().enumerate().any(|(index, slot)| {
        index != except_index
            && DatabaseState::from_raw(slot.state) == DatabaseState::Dirty
    })
}
