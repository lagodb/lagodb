use std::collections::HashSet;

use pgrx::PgLwLock;
use pgrx::prelude::*;

use crate::error::{LakebaseError, LakebaseResult};
use crate::gucs;
use crate::registry::WorkerRegistration;
use crate::state::{
    ReconcilerState, RuntimeSharedState, RuntimeStateTransitionError, WorkerState,
};
use pg_lakebase_core::extension_worker::WorkerExit;

use super::bgworker::{ReconcilerToken, WorkerToken, timestamp_ms};
use super::status::{ProcessStatus, WorkerStatus};
use super::{BGWORKER_START_TIMEOUT, CAPACITY_WARNING_INTERVAL, CRASH_BACKOFF};

pub(crate) static RUNTIME_STATE: PgLwLock<RuntimeSharedState> =
    unsafe { PgLwLock::new(c"pg_lakebase_runtime worker runtime") };

pub(super) struct RuntimeStore;

pub(super) struct RegistrationReconciliation {
    pub(super) starts: Vec<WorkerToken>,
    pub(super) registration_capacity_exhausted: bool,
    pub(super) worker_capacity_exhausted: bool,
}

pub(super) struct WorkerStart {
    pub(super) database_oid: u32,
    pub(super) extension_oid: u32,
    pub(super) worker_name: String,
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
    ) -> LakebaseResult<RegistrationReservation> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        if let Some(index) =
            state.worker_slot(database_oid, extension_oid, worker_name)
        {
            let slot = &mut state.workers[index];
            let current = WorkerState::from_raw(slot.state);
            if slot.pid != 0
                || !matches!(current, WorkerState::Dormant | WorkerState::Stopping)
            {
                return Err(LakebaseError::WorkerReplacementNotQuiescent);
            }
            slot.transition_to(WorkerState::PendingRegistration)?;
            slot.generation = slot.generation.wrapping_add(1).max(1);
            slot.proc_number = pg_sys::INVALID_PROC_NUMBER;
            slot.restart_at_ms = 0;
            slot.startup_deadline_ms = 0;
            slot.wake_requested = 0;
            return Ok(RegistrationReservation::Replacement(
                RegistrationToken::new(index, slot.generation),
            ));
        }
        let active = state
            .workers
            .iter()
            .filter(|slot| WorkerState::from_raw(slot.state) != WorkerState::Empty)
            .count();
        if active >= gucs::max_registrations() {
            return Err(LakebaseError::MaxWorkerRegistrationsExhausted);
        }
        let index = state
            .ensure_worker_slot(database_oid, extension_oid, worker_name)
            .ok_or(LakebaseError::WorkerRegistrationStateExhausted)?;
        state.workers[index].transition_to(WorkerState::PendingRegistration)?;
        Ok(RegistrationReservation::New(RegistrationToken::new(
            index,
            state.workers[index].generation,
        )))
    }

    pub(super) fn finish_registration(
        &self,
        reservation: RegistrationReservation,
        completion: RegistrationCompletion,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let token = reservation.token();
        let Some(slot) = state.workers.get(token.index) else {
            return false;
        };
        if slot.generation != token.generation
            || WorkerState::from_raw(slot.state) != WorkerState::PendingRegistration
        {
            return false;
        }
        if matches!(
            (reservation, completion),
            (
                RegistrationReservation::New(_),
                RegistrationCompletion::Abort
            )
        ) {
            state.clear_worker_slot(token.index);
            return false;
        }
        let slot = &mut state.workers[token.index];
        if let Err(error) = slot.transition_to(WorkerState::Dormant) {
            warn_transition_error(error);
            return false;
        }
        slot.proc_number = pg_sys::INVALID_PROC_NUMBER;
        slot.restart_at_ms = 0;
        slot.startup_deadline_ms = 0;
        slot.request_wakeup();
        true
    }

    pub(super) fn wake_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(index) = state.worker_slot(database_oid, extension_oid, worker_name)
        else {
            state.rescan_all = 1;
            return true;
        };
        let slot = &mut state.workers[index];
        if let Err(error) = slot.request_wakeup_preempting_schedule() {
            warn_transition_error(error);
            return false;
        }
        true
    }

    pub(super) fn wake_database_workers(&self, database_oid: u32) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let mut found = false;
        for slot in &mut state.workers {
            if slot.database_oid == database_oid
                && WorkerState::from_raw(slot.state) != WorkerState::Empty
            {
                if let Err(error) = slot.request_wakeup_preempting_schedule() {
                    warn_transition_error(error);
                    continue;
                }
                found = true;
            }
        }
        if !found {
            state.rescan_all = 1;
        }
        true
    }

    pub(super) fn request_database_reconcile(&self, database_oid: u32) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let mut found = false;
        for slot in &mut state.workers {
            if slot.database_oid == database_oid
                && WorkerState::from_raw(slot.state) != WorkerState::Empty
            {
                slot.request_wakeup();
                found = true;
            }
        }
        if !found {
            state.rescan_all = 1;
        }
        true
    }

    pub(super) fn request_full_rescan(&self) -> bool {
        RUNTIME_STATE.exclusive().rescan_all = 1;
        true
    }

    pub(super) fn signal_launcher(&self) {
        let proc_number = RUNTIME_STATE.share().launcher_proc_number;
        if proc_number != pg_sys::INVALID_PROC_NUMBER {
            // SAFETY: proc_number is either PostgreSQL's invalid sentinel (handled
            // above) or a ProcNumber published by the launcher. ProcSendSignal
            // only sets the target PGPROC latch; an exit-time stale value can at
            // worst cause a harmless spurious latch wakeup in a reused slot.
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
        let mut statuses = Vec::with_capacity(1 + state.reconcilers.len());
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
        statuses.extend(state.reconcilers.iter().filter_map(|slot| {
            let reconciler_state = ReconcilerState::from_raw(slot.state);
            (reconciler_state != ReconcilerState::Empty).then_some(ProcessStatus {
                process_kind: "database_reconciler",
                database_oid: Some(slot.database_oid),
                state: reconciler_state.as_str(),
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
        state.launcher_proc_number = pg_sys::INVALID_PROC_NUMBER;
    }

    pub(super) fn take_full_scan_request(&self) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let requested = state.rescan_all != 0;
        state.rescan_all = 0;
        requested
    }

    pub(super) fn remove_dropped_databases(&self, live: &HashSet<u32>) -> Vec<i32> {
        let mut state = RUNTIME_STATE.exclusive();
        let mut pids = Vec::new();
        for index in 0..state.reconcilers.len() {
            let slot = state.reconcilers[index];
            if slot.database_oid == 0 || live.contains(&slot.database_oid) {
                continue;
            }
            match ReconcilerState::from_raw(slot.state) {
                ReconcilerState::Starting => {
                    let generation = slot.generation.wrapping_add(1).max(1);
                    state.reconcilers[index].generation = generation;
                    state.reconcilers[index].startup_deadline_ms = 0;
                    if let Err(error) = state.reconcilers[index]
                        .transition_to(ReconcilerState::Finished)
                    {
                        warn_transition_error(error);
                    }
                }
                ReconcilerState::Running => {
                    if let Err(error) = state.reconcilers[index]
                        .transition_to(ReconcilerState::Stopping)
                    {
                        warn_transition_error(error);
                    }
                    if slot.pid > 0 {
                        pids.push(slot.pid);
                    }
                }
                ReconcilerState::Stopping => {
                    if slot.pid > 0 {
                        pids.push(slot.pid);
                    }
                }
                ReconcilerState::Finished | ReconcilerState::Retry => {
                    state.clear_reconciler_slot(index);
                }
                ReconcilerState::Empty => {}
            }
        }
        for index in 0..state.workers.len() {
            let slot = state.workers[index];
            if slot.database_oid == 0 || live.contains(&slot.database_oid) {
                continue;
            }
            if slot.pid > 0 {
                if WorkerState::from_raw(slot.state) != WorkerState::Stopping
                    && let Err(error) =
                        state.workers[index].transition_to(WorkerState::Stopping)
                {
                    warn_transition_error(error);
                }
                pids.push(slot.pid);
            } else {
                if WorkerState::from_raw(slot.state) == WorkerState::Starting {
                    state.workers[index].generation =
                        slot.generation.wrapping_add(1).max(1);
                }
                state.clear_worker_slot(index);
            }
        }
        pids
    }

    pub(super) fn schedule_due_workers(&self) {
        let now = timestamp_ms();
        let retry_at_ms = now.saturating_add(
            i64::try_from(CRASH_BACKOFF.as_millis()).unwrap_or(i64::MAX),
        );
        let mut state = RUNTIME_STATE.exclusive();
        let mut timed_out = Vec::new();
        for worker in &mut state.workers {
            let previous_generation = worker.generation;
            match worker.recover_timed_out_start(now, retry_at_ms) {
                Ok(true) => timed_out.push((
                    worker.database_oid,
                    worker.extension_oid,
                    worker.worker_name_str().to_owned(),
                    previous_generation,
                    worker.generation,
                )),
                Ok(false) => {}
                Err(error) => {
                    warn_transition_error(error);
                    continue;
                }
            }
            if matches!(
                WorkerState::from_raw(worker.state),
                WorkerState::Scheduled | WorkerState::Backoff
            ) && worker.restart_at_ms <= now
            {
                worker.request_wakeup();
                if let Err(error) = worker.transition_to(WorkerState::Dormant) {
                    warn_transition_error(error);
                    continue;
                }
                worker.restart_at_ms = 0;
            }
        }
        drop(state);
        for (
            database_oid,
            extension_oid,
            worker_name,
            previous_generation,
            generation,
        ) in timed_out
        {
            crate::diag::warning(format_args!(
                "Lakebase worker start timed out; canceled the stale start and scheduled retry: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, previous_generation={previous_generation}, generation={generation}, retry_at_ms={retry_at_ms}"
            ));
        }
    }

    pub(super) fn requested_databases(&self) -> Vec<u32> {
        let state = RUNTIME_STATE.share();
        Self::unique_databases(state.workers.iter().filter(|slot| {
            slot.wake_requested != 0
                && slot.pid == 0
                && matches!(
                    WorkerState::from_raw(slot.state),
                    WorkerState::Dormant | WorkerState::Stopping
                )
        }))
    }

    pub(super) fn all_worker_databases(&self) -> Vec<u32> {
        let state = RUNTIME_STATE.share();
        Self::unique_databases(state.workers.iter().filter(|slot| {
            !matches!(
                WorkerState::from_raw(slot.state),
                WorkerState::Empty
                    | WorkerState::PendingRegistration
                    | WorkerState::Stopping
            )
        }))
    }

    pub(super) fn drain_completed_reconcilers(&self) -> Vec<u32> {
        let mut state = RUNTIME_STATE.exclusive();
        let mut retries = Vec::new();
        for index in 0..state.reconcilers.len() {
            match ReconcilerState::from_raw(state.reconcilers[index].state) {
                ReconcilerState::Finished => state.clear_reconciler_slot(index),
                ReconcilerState::Retry => {
                    retries.push(state.reconcilers[index].database_oid);
                    state.clear_reconciler_slot(index);
                }
                _ => {}
            }
        }
        retries
    }

    pub(super) fn recover_timed_out_reconcilers(&self) {
        let now = timestamp_ms();
        let mut state = RUNTIME_STATE.exclusive();
        let mut timed_out = Vec::new();
        for reconciler in &mut state.reconcilers {
            let previous_generation = reconciler.generation;
            match reconciler.recover_timed_out_start(now) {
                Ok(true) => timed_out.push((
                    reconciler.database_oid,
                    previous_generation,
                    reconciler.generation,
                )),
                Ok(false) => {}
                Err(error) => warn_transition_error(error),
            }
        }
        drop(state);
        for (database_oid, previous_generation, generation) in timed_out {
            crate::diag::warning(format_args!(
                "Lakebase reconciler start timed out; canceled the stale start and scheduled retry: database_oid={database_oid}, previous_generation={previous_generation}, generation={generation}"
            ));
        }
    }

    pub(super) fn reserve_reconciler(
        &self,
        database_oid: u32,
    ) -> ReconcilerReservation {
        let startup_deadline_ms = timestamp_ms().saturating_add(
            i64::try_from(BGWORKER_START_TIMEOUT.as_millis()).unwrap_or(i64::MAX),
        );
        let mut state = RUNTIME_STATE.exclusive();
        if state.reconciler_slot(database_oid).is_some() {
            return ReconcilerReservation::AlreadyActive;
        }
        let active = state
            .reconcilers
            .iter()
            .filter(|slot| {
                matches!(
                    ReconcilerState::from_raw(slot.state),
                    ReconcilerState::Starting
                        | ReconcilerState::Running
                        | ReconcilerState::Stopping
                )
            })
            .count();
        if active >= gucs::max_database_reconcilers() {
            return ReconcilerReservation::AtCapacity;
        }
        let Some(index) =
            state.reserve_reconciler_slot(database_oid, startup_deadline_ms)
        else {
            return ReconcilerReservation::AtCapacity;
        };
        ReconcilerReservation::Reserved(ReconcilerToken::new(
            index,
            state.reconcilers[index].generation,
        ))
    }

    pub(super) fn reconciler_start_failed(&self, token: ReconcilerToken) {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.reconcilers.get_mut(token.index()) else {
            return;
        };
        if slot.generation == token.generation()
            && ReconcilerState::from_raw(slot.state) == ReconcilerState::Starting
        {
            if let Err(error) = slot.transition_to(ReconcilerState::Retry) {
                warn_transition_error(error);
                return;
            }
            slot.startup_deadline_ms = 0;
        }
    }

    pub(super) fn begin_reconciler(&self, token: ReconcilerToken) -> Option<u32> {
        let mut state = RUNTIME_STATE.exclusive();
        let slot = state.reconcilers.get_mut(token.index())?;
        if slot.generation != token.generation()
            || ReconcilerState::from_raw(slot.state) != ReconcilerState::Starting
        {
            return None;
        }
        if let Err(error) = slot.transition_to(ReconcilerState::Running) {
            warn_transition_error(error);
            return None;
        }
        slot.pid = unsafe { pg_sys::MyProcPid };
        slot.proc_number = unsafe { pg_sys::MyProcNumber };
        slot.startup_deadline_ms = 0;
        Some(slot.database_oid)
    }

    pub(super) fn finish_reconciler(
        &self,
        token: ReconcilerToken,
        needs_retry: bool,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.reconcilers.get_mut(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let current = ReconcilerState::from_raw(slot.state);
        slot.pid = 0;
        slot.proc_number = pg_sys::INVALID_PROC_NUMBER;
        slot.startup_deadline_ms = 0;
        let next = match current {
            ReconcilerState::Running if needs_retry => ReconcilerState::Retry,
            ReconcilerState::Running | ReconcilerState::Stopping => {
                ReconcilerState::Finished
            }
            _ => return false,
        };
        if let Err(error) = slot.transition_to(next) {
            warn_transition_error(error);
            return false;
        }
        true
    }

    pub(super) fn reconciler_exit(&self, token: ReconcilerToken) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.reconcilers.get_mut(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let current = ReconcilerState::from_raw(slot.state);
        slot.pid = 0;
        slot.proc_number = pg_sys::INVALID_PROC_NUMBER;
        slot.startup_deadline_ms = 0;
        let next = match current {
            ReconcilerState::Starting | ReconcilerState::Running => {
                ReconcilerState::Retry
            }
            ReconcilerState::Stopping => ReconcilerState::Finished,
            _ => return false,
        };
        if let Err(error) = slot.transition_to(next) {
            warn_transition_error(error);
            return false;
        }
        true
    }

    pub(super) fn reconcile_registrations(
        &self,
        database_oid: u32,
        registrations: &[WorkerRegistration],
    ) -> RegistrationReconciliation {
        let startup_deadline_ms = timestamp_ms().saturating_add(
            i64::try_from(BGWORKER_START_TIMEOUT.as_millis()).unwrap_or(i64::MAX),
        );
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
        let mut registration_capacity_exhausted = false;
        let mut worker_capacity_exhausted = false;
        {
            let mut state = RUNTIME_STATE.exclusive();
            let mut registrations_tracked = state
                .workers
                .iter()
                .filter(|slot| {
                    WorkerState::from_raw(slot.state) != WorkerState::Empty
                })
                .count();
            let mut active_workers = state
                .workers
                .iter()
                .filter(|slot| {
                    matches!(
                        WorkerState::from_raw(slot.state),
                        WorkerState::Starting | WorkerState::Running
                    ) || (WorkerState::from_raw(slot.state) == WorkerState::Stopping
                        && slot.pid > 0)
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
                if existing.is_none()
                    && registrations_tracked >= gucs::max_registrations()
                {
                    registration_capacity_exhausted = true;
                    continue;
                }
                let Some(index) = state.ensure_worker_slot(
                    database_oid,
                    extension_oid,
                    &registration.worker_name,
                ) else {
                    registration_capacity_exhausted = true;
                    continue;
                };
                if existing.is_none() {
                    registrations_tracked += 1;
                    state.workers[index].request_wakeup();
                }
                let slot = &mut state.workers[index];
                if WorkerState::from_raw(slot.state) == WorkerState::Stopping
                    && slot.pid == 0
                    && let Err(error) = slot.transition_to(WorkerState::Dormant)
                {
                    warn_transition_error(error);
                    continue;
                }
                let should_start = slot.wake_requested != 0;
                // PendingRegistration is finalized to Dormant by the commit
                // callback; reconciliation must not bypass that transaction fence.
                if should_start
                    && WorkerState::from_raw(slot.state) == WorkerState::Dormant
                {
                    if active_workers >= gucs::max_active_workers() {
                        worker_capacity_exhausted = true;
                        continue;
                    }
                    if let Err(error) = slot.transition_to(WorkerState::Starting) {
                        warn_transition_error(error);
                        continue;
                    }
                    slot.wake_requested = 0;
                    slot.generation = slot.generation.wrapping_add(1).max(1);
                    slot.startup_deadline_ms = startup_deadline_ms;
                    starts.push(WorkerToken::new(index, slot.generation));
                    active_workers += 1;
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
            registration_capacity_exhausted,
            worker_capacity_exhausted,
        }
    }

    pub(super) fn clear_database_workers(&self, database_oid: u32) {
        let mut state = RUNTIME_STATE.exclusive();
        for index in 0..state.workers.len() {
            if state.workers[index].database_oid == database_oid
                && state.workers[index].pid == 0
            {
                state.clear_worker_slot(index);
            }
        }
    }

    pub(super) fn worker_start_failed(&self, token: WorkerToken) {
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
            slot.startup_deadline_ms = 0;
            slot.restart_at_ms = timestamp_ms().saturating_add(
                i64::try_from(CRASH_BACKOFF.as_millis()).unwrap_or(i64::MAX),
            );
        }
    }

    pub(super) fn begin_worker(&self, token: WorkerToken) -> Option<WorkerStart> {
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
        slot.startup_deadline_ms = 0;
        Some(WorkerStart {
            database_oid: slot.database_oid,
            extension_oid: slot.extension_oid,
            worker_name: slot.worker_name_str().to_owned(),
        })
    }

    pub(super) fn worker_exit(&self, token: WorkerToken, code: i32) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.workers.get_mut(token.index()) else {
            return false;
        };
        if slot.generation != token.generation() {
            return false;
        }
        let previous = WorkerState::from_raw(slot.state);
        slot.pid = 0;
        slot.proc_number = pg_sys::INVALID_PROC_NUMBER;
        slot.startup_deadline_ms = 0;
        match previous {
            WorkerState::Running | WorkerState::Starting => {
                if let Err(error) = slot.transition_to(WorkerState::Backoff) {
                    warn_transition_error(error);
                    return false;
                }
                slot.restart_at_ms = timestamp_ms().saturating_add(
                    i64::try_from(CRASH_BACKOFF.as_millis()).unwrap_or(i64::MAX),
                );
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

    pub(super) fn finish_worker(&self, token: WorkerToken, directive: WorkerExit) {
        let mut state = RUNTIME_STATE.exclusive();
        let database_oid;
        let extension_oid;
        let worker_name;
        let previous;
        let now_ms = timestamp_ms();
        {
            let Some(slot) = state.workers.get_mut(token.index()) else {
                return;
            };
            if slot.generation != token.generation() {
                return;
            }
            database_oid = slot.database_oid;
            extension_oid = slot.extension_oid;
            worker_name = slot.worker_name_str().to_owned();
            previous = WorkerState::from_raw(slot.state);
            if let Err(error) = slot.finish_invocation(directive, now_ms) {
                warn_transition_error(error);
                return;
            }
        }
        if previous == WorkerState::Stopping {
            drop(state);
            crate::diag::info(format_args!(
                "finished stopping Lakebase extension worker: database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name}, generation={}, ignored_directive={directive:?}",
                token.generation()
            ));
            self.signal_launcher();
            return;
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
        for slot in &mut state.reconcilers {
            if slot.database_oid != database_oid {
                continue;
            }
            match ReconcilerState::from_raw(slot.state) {
                ReconcilerState::Starting if slot.pid == 0 => {
                    slot.generation = slot.generation.wrapping_add(1).max(1);
                    slot.startup_deadline_ms = 0;
                    slot.transition_to(ReconcilerState::Finished)?;
                }
                ReconcilerState::Running => {
                    slot.transition_to(ReconcilerState::Stopping)?;
                    if slot.pid > 0 {
                        pids.push(slot.pid);
                    }
                }
                ReconcilerState::Stopping if slot.pid > 0 => {
                    pids.push(slot.pid);
                }
                _ => {}
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
                slot.startup_deadline_ms = 0;
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
        state
            .reconcilers
            .iter()
            .any(|slot| slot.database_oid == database_oid && slot.pid > 0)
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
                slot.startup_deadline_ms = 0;
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
            slot.startup_deadline_ms = 0;
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

    fn unique_databases<'a>(
        workers: impl Iterator<Item = &'a crate::state::WorkerSlot>,
    ) -> Vec<u32> {
        let mut seen = HashSet::new();
        workers
            .filter_map(|slot| {
                seen.insert(slot.database_oid).then_some(slot.database_oid)
            })
            .collect()
    }
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
