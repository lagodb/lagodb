use std::collections::HashSet;

use pg_lakebase_core::extension_worker::WorkerDirective;
use pgrx::prelude::*;

use crate::gucs;
use crate::registry::WorkerRegistration;
use crate::state::{
    DispatchState, ProcessState, RecoveryState, RegistrationState, WorkerSlot,
};

use super::{
    RUNTIME_STATE, ReconcilerReservation, ReconcilerRetry,
    RegistrationReconciliation, RuntimeStore, StoppedProcess, TransitionWarning,
    WorkerLaunch, WorkerStart, warn_transition_error,
};
use crate::runtime::bgworker::{ReconcilerToken, WorkerToken, timestamp_ms};
use crate::runtime::process::ProcessToken;
use crate::runtime::reconcile::{DatabaseReconcileState, ReconcilerSlot};
use crate::runtime::{BGWORKER_START_TIMEOUT, CRASH_BACKOFF};

impl RuntimeStore {
    pub(in crate::runtime) fn promote_due_workers(&self) {
        let now = timestamp_ms();
        let mut state = RUNTIME_STATE.exclusive();
        for slot in &mut state.workers {
            slot.promote_due(now);
        }
    }

    pub(in crate::runtime) fn requested_databases(&self) -> Vec<u32> {
        let state = RUNTIME_STATE.share();
        state
            .database_reconciles
            .iter()
            .filter(|intent| intent.is_pending())
            .map(|intent| intent.database_oid)
            .collect()
    }

    pub(in crate::runtime) fn all_worker_databases(&self) -> Vec<u32> {
        let state = RUNTIME_STATE.share();
        Self::unique_databases(state.workers.iter().filter(|slot| {
            matches!(
                slot.registration(),
                Ok(RegistrationState::Registered | RegistrationState::Removing)
            )
        }))
    }

    pub(in crate::runtime) fn reserve_reconciler(
        &self,
        database_oid: u32,
        active_owned: usize,
    ) -> ReconcilerReservation {
        let mut state = RUNTIME_STATE.exclusive();
        if !RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_reconciliation)
        {
            return ReconcilerReservation::Recovering;
        }
        if state.reconciler_slot(database_oid).is_some() {
            return ReconcilerReservation::AlreadyActive;
        }
        let target_generation = state
            .database_reconcile_slot(database_oid)
            .map_or(0, |index| {
                state.database_reconciles[index].desired_generation
            });
        if active_owned >= gucs::max_database_reconcilers() {
            return ReconcilerReservation::AtCapacity;
        }
        let Some(index) = state.empty_reconciler_slot() else {
            return ReconcilerReservation::AtCapacity;
        };
        let generation = state.reconcilers[index].generation.wrapping_add(1).max(1);
        let deadline = timestamp_ms().saturating_add(
            i64::try_from(BGWORKER_START_TIMEOUT.as_millis()).unwrap_or(i64::MAX),
        );
        state.reconcilers[index] = ReconcilerSlot::reserve(
            database_oid,
            generation,
            target_generation,
            deadline,
        );
        ReconcilerReservation::Reserved(ReconcilerToken::new(index, generation))
    }

    pub(in crate::runtime) fn reconciler_registration_failed(
        &self,
        token: ReconcilerToken,
    ) -> Option<u32> {
        let mut state = RUNTIME_STATE.exclusive();
        let slot = state.reconcilers.get(token.index())?;
        if slot.generation != token.generation()
            || !slot.process().is_ok_and(ProcessState::is_active)
        {
            return None;
        }
        let database_oid = slot.database_oid;
        state.clear_reconciler_slot(token.index());
        Some(database_oid)
    }

    pub(in crate::runtime) fn begin_reconciler(
        &self,
        token: ReconcilerToken,
    ) -> Option<u32> {
        let mut state = RUNTIME_STATE.exclusive();
        if !RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_reconciliation)
        {
            return None;
        }
        let slot = state.reconcilers.get_mut(token.index())?;
        let result = slot.publish_running(
            token.generation(),
            unsafe { pg_sys::MyProcPid },
            unsafe { pg_sys::MyProcNumber },
        );
        drop(state);
        match result {
            Ok(Some(database_oid)) => Some(database_oid),
            Ok(None) => None,
            Err(error) => {
                warn_transition_error(error);
                None
            }
        }
    }

    pub(in crate::runtime) fn validate_reconciler_token(
        &self,
        token: ReconcilerToken,
    ) -> bool {
        let state = RUNTIME_STATE.share();
        RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_reconciliation)
            && state.reconcilers.get(token.index()).is_some_and(|slot| {
                slot.token_matches(token.generation())
                    && slot.process().is_ok_and(ProcessState::is_active)
                    && slot.stop_requested == 0
            })
    }

    pub(in crate::runtime) fn finish_reconciler(
        &self,
        token: ReconcilerToken,
        needs_retry: bool,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.reconcilers.get_mut(token.index()) else {
            return false;
        };
        let result = slot.publish_completion(token.generation(), needs_retry);
        drop(state);
        match result {
            Ok(published) => published,
            Err(error) => {
                warn_transition_error(error);
                false
            }
        }
    }

    pub(in crate::runtime) fn reconciler_exit(
        &self,
        token: ReconcilerToken,
        code: i32,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        state
            .reconcilers
            .get_mut(token.index())
            .is_some_and(|slot| slot.record_exit_callback(token.generation(), code))
    }

    pub(in crate::runtime) fn reconcile_registrations(
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
        let mut state = RUNTIME_STATE.exclusive();
        let mut warning = TransitionWarning::default();
        let mut tracked =
            state.workers.iter().filter(|slot| !slot.is_empty()).count();
        let mut exhausted = false;

        for registration in registrations {
            let extension_oid = registration.extension_oid.to_u32();
            if let Some(index) = state.worker_slot(
                database_oid,
                extension_oid,
                &registration.worker_name,
            ) {
                state.workers[index].reconcile_present();
                continue;
            }
            if tracked >= gucs::max_registrations() {
                exhausted = true;
                continue;
            }
            let Some(index) = state.empty_worker_slot() else {
                exhausted = true;
                continue;
            };
            let generation = state.workers[index].generation.wrapping_add(1).max(1);
            if !state.workers[index].initialize_registration(
                database_oid,
                extension_oid,
                &registration.worker_name,
                generation,
                RegistrationState::Registered,
            ) {
                exhausted = true;
                continue;
            }
            state.workers[index].publish_wakeup();
            tracked += 1;
        }

        for index in 0..state.workers.len() {
            let slot = state.workers[index];
            if slot.database_oid != database_oid
                || slot.is_empty()
                || slot.registration() == Ok(RegistrationState::PendingCommit)
                || live.contains(&(slot.extension_oid, slot.worker_name_str()))
            {
                continue;
            }
            if slot.process() == Ok(ProcessState::Stopped) {
                state.clear_worker_slot(index);
            } else if let Err(error) = state.workers[index].mark_removing() {
                warning.capture(error);
            }
        }
        let result = RegistrationReconciliation {
            registration_capacity_exhausted: exhausted,
        };
        drop(state);
        warning.report();
        result
    }

    pub(in crate::runtime) fn clear_database_workers(&self, database_oid: u32) {
        let mut state = RUNTIME_STATE.exclusive();
        let mut warning = TransitionWarning::default();
        for index in 0..state.workers.len() {
            if state.workers[index].database_oid != database_oid
                || state.workers[index].is_empty()
            {
                continue;
            }
            if state.workers[index].process() == Ok(ProcessState::Stopped) {
                state.clear_worker_slot(index);
            } else if let Err(error) = state.workers[index].mark_removing() {
                warning.capture(error);
            }
        }
        drop(state);
        warning.report();
    }

    pub(in crate::runtime) fn reserve_ready_workers(
        &self,
        limit: usize,
    ) -> Vec<WorkerLaunch> {
        if limit == 0 {
            return Vec::new();
        }
        let mut state = RUNTIME_STATE.exclusive();
        if !RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_dispatch)
        {
            return Vec::new();
        }
        let deadline = timestamp_ms().saturating_add(
            i64::try_from(BGWORKER_START_TIMEOUT.as_millis()).unwrap_or(i64::MAX),
        );
        let start = state.worker_cursor as usize % state.workers.len();
        let mut launches = Vec::new();
        let mut warning = TransitionWarning::default();
        let mut last_offset = 0;
        for offset in 0..state.workers.len() {
            if launches.len() == limit {
                break;
            }
            let index = (start + offset) % state.workers.len();
            let slot = &mut state.workers[index];
            if slot.registration() != Ok(RegistrationState::Registered)
                || slot.dispatch() != Ok(DispatchState::Ready)
                || slot.process() != Ok(ProcessState::Stopped)
                || slot.stop_requested != 0
            {
                continue;
            }
            let generation = match slot.prepare_start(deadline) {
                Ok(generation) => generation,
                Err(error) => {
                    warning.capture(error);
                    continue;
                }
            };
            launches.push(WorkerLaunch {
                token: WorkerToken::new(index, generation),
                identity: slot.identity(),
            });
            last_offset = offset + 1;
        }
        state.worker_cursor = ((start + last_offset) % state.workers.len()) as u32;
        drop(state);
        warning.report();
        launches
    }

    pub(in crate::runtime) fn worker_registration_failed(&self, token: WorkerToken) {
        let retry_at = timestamp_ms().saturating_add(
            i64::try_from(CRASH_BACKOFF.as_millis()).unwrap_or(i64::MAX),
        );
        let mut state = RUNTIME_STATE.exclusive();
        if let Some(slot) = state.workers.get_mut(token.index()) {
            slot.registration_failed(token.generation(), retry_at);
        }
    }

    pub(in crate::runtime) fn begin_worker(
        &self,
        token: WorkerToken,
    ) -> Option<WorkerStart> {
        let mut state = RUNTIME_STATE.exclusive();
        if !RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_dispatch)
        {
            return None;
        }
        let slot = state.workers.get_mut(token.index())?;
        let result = slot.publish_running(
            token.generation(),
            unsafe { pg_sys::MyProcPid },
            unsafe { pg_sys::MyProcNumber },
        );
        let identity = slot.identity();
        drop(state);
        let published = match result {
            Ok(published) => published,
            Err(error) => {
                warn_transition_error(error);
                false
            }
        };
        if !published {
            return None;
        }
        Some(WorkerStart {
            database_oid: identity.database_oid,
            extension_oid: identity.extension_oid,
            worker_name: identity.worker_name().to_owned(),
        })
    }

    pub(in crate::runtime) fn validate_worker_token(
        &self,
        token: WorkerToken,
    ) -> bool {
        let state = RUNTIME_STATE.share();
        RecoveryState::decode(state.recovery_state)
            .is_ok_and(RecoveryState::allows_dispatch)
            && state.workers.get(token.index()).is_some_and(|slot| {
                slot.token_matches(token.generation())
                    && slot.process().is_ok_and(ProcessState::is_active)
                    && slot.stop_requested == 0
            })
    }

    pub(in crate::runtime) fn worker_exit(
        &self,
        token: WorkerToken,
        code: i32,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        state
            .workers
            .get_mut(token.index())
            .is_some_and(|slot| slot.record_exit_callback(token.generation(), code))
    }

    pub(in crate::runtime) fn finish_worker(
        &self,
        token: WorkerToken,
        directive: WorkerDirective,
    ) {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(slot) = state.workers.get_mut(token.index()) else {
            return;
        };
        let identity = slot.identity();
        let result = slot.publish_directive(token.generation(), directive);
        drop(state);
        let published = match result {
            Ok(published) => published,
            Err(error) => {
                warn_transition_error(error);
                false
            }
        };
        if published {
            crate::diag::info(format_args!(
                "Lakebase worker published completion directive: database_oid={}, extension_oid={}, worker_name={}, generation={}, directive={directive:?}",
                identity.database_oid,
                identity.extension_oid,
                identity.worker_name(),
                token.generation(),
            ));
            self.signal_launcher();
        }
    }

    pub(in crate::runtime) fn termination_requests(&self) -> Vec<ProcessToken> {
        let now = timestamp_ms();
        let mut state = RUNTIME_STATE.exclusive();
        let mut requests = Vec::new();
        for (index, slot) in state.reconcilers.iter_mut().enumerate() {
            let timed_out = slot.mark_start_timed_out(now);
            if (timed_out || slot.stop_requested != 0)
                && slot.process().is_ok_and(ProcessState::is_active)
            {
                requests.push(ProcessToken::Reconciler(ReconcilerToken::new(
                    index,
                    slot.generation,
                )));
            }
        }
        for (index, slot) in state.workers.iter_mut().enumerate() {
            let timed_out = slot.mark_start_timed_out(now);
            if (timed_out || slot.stop_requested != 0)
                && slot.process().is_ok_and(ProcessState::is_active)
            {
                requests.push(ProcessToken::Worker(WorkerToken::new(
                    index,
                    slot.generation,
                )));
            }
        }
        requests
    }

    pub(in crate::runtime) fn confirm_process_stopped(
        &self,
        token: ProcessToken,
    ) -> StoppedProcess {
        let mut state = RUNTIME_STATE.exclusive();
        match token {
            ProcessToken::Reconciler(token) => {
                let Some(slot) = state.reconcilers.get_mut(token.index()) else {
                    return StoppedProcess::Stale;
                };
                let database_oid = slot.database_oid;
                let Some(completion) = slot.confirm_stopped(token.generation())
                else {
                    return StoppedProcess::Stale;
                };
                if let Some(target_generation) = completion.completed_target
                    && let Some(intent_index) =
                        state.database_reconcile_slot(database_oid)
                {
                    state.database_reconciles[intent_index]
                        .complete(target_generation);
                }
                let pending =
                    state.database_reconcile_slot(database_oid).is_some_and(
                        |index| state.database_reconciles[index].is_pending(),
                    );
                if !pending
                    && let Some(intent_index) =
                        state.database_reconcile_slot(database_oid)
                {
                    state.database_reconciles[intent_index] =
                        DatabaseReconcileState::EMPTY;
                }
                state.clear_reconciler_slot(token.index());
                StoppedProcess::Reconciler {
                    database_oid,
                    retry: if completion.retry {
                        ReconcilerRetry::Backoff
                    } else if pending {
                        ReconcilerRetry::Immediate
                    } else {
                        ReconcilerRetry::None
                    },
                }
            }
            ProcessToken::Worker(token) => {
                let Some(slot) = state.workers.get_mut(token.index()) else {
                    return StoppedProcess::Stale;
                };
                match slot.confirm_stopped(
                    token.generation(),
                    timestamp_ms(),
                    CRASH_BACKOFF,
                ) {
                    Ok(Some(_)) => {
                        if slot.registration() == Ok(RegistrationState::Removing) {
                            state.clear_worker_slot(token.index());
                        }
                        StoppedProcess::Worker
                    }
                    Ok(None) | Err(_) => StoppedProcess::Stale,
                }
            }
        }
    }

    fn unique_databases<'a>(
        workers: impl Iterator<Item = &'a WorkerSlot>,
    ) -> Vec<u32> {
        let mut seen = HashSet::new();
        workers
            .filter_map(|slot| {
                seen.insert(slot.database_oid).then_some(slot.database_oid)
            })
            .collect()
    }
}
