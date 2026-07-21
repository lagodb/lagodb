use crate::error::{LakebaseError, LakebaseResult};
use crate::gucs;
use crate::state::{INVALID_OID, ProcessState, RegistrationState};

use super::{
    RUNTIME_STATE, RegistrationCompletion, RegistrationReservation,
    RegistrationToken, RuntimeStore, validate_state,
};

impl RuntimeStore {
    pub(in crate::runtime) fn reserve_registration(
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
            if slot.process()? != ProcessState::Stopped {
                return Err(LakebaseError::WorkerReplacementNotQuiescent);
            }
            slot.begin_registration_replacement()?;
            return Ok(RegistrationReservation::Replacement(
                RegistrationToken::new(index, slot.generation),
            ));
        }

        let tracked = state.workers.iter().filter(|slot| !slot.is_empty()).count();
        if tracked >= gucs::max_registrations() {
            return Err(LakebaseError::MaxWorkerRegistrationsExhausted);
        }
        let index = state
            .empty_worker_slot()
            .ok_or(LakebaseError::WorkerRegistrationStateExhausted)?;
        let generation = state.workers[index].generation.wrapping_add(1).max(1);
        if !state.workers[index].initialize_registration(
            database_oid,
            extension_oid,
            worker_name,
            generation,
            RegistrationState::PendingCommit,
        ) {
            return Err(LakebaseError::InvalidWorkerName);
        }
        Ok(RegistrationReservation::New(RegistrationToken::new(
            index, generation,
        )))
    }

    pub(in crate::runtime) fn finish_registration(
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
            || slot.registration() != Ok(RegistrationState::PendingCommit)
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
        state.workers[token.index].finish_registration();
        true
    }

    pub(in crate::runtime) fn wake_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let Some(index) = state.worker_slot(database_oid, extension_oid, worker_name)
        else {
            return false;
        };
        state.workers[index].publish_wakeup();
        true
    }

    pub(in crate::runtime) fn wake_database_workers(
        &self,
        database_oid: u32,
    ) -> bool {
        let mut state = RUNTIME_STATE.exclusive();
        let mut found = false;
        for slot in &mut state.workers {
            if slot.database_oid == database_oid && !slot.is_empty() {
                slot.publish_wakeup();
                found = true;
            }
        }
        found
    }

    pub(in crate::runtime) fn request_database_reconcile(
        &self,
        database_oid: u32,
    ) -> bool {
        if database_oid == INVALID_OID {
            return false;
        }
        let mut state = RUNTIME_STATE.exclusive();
        let index = state
            .database_reconcile_slot(database_oid)
            .or_else(|| state.empty_database_reconcile_slot());
        let Some(index) = index else {
            state.rescan_all = 1;
            return true;
        };
        state.database_reconciles[index].request(database_oid);
        true
    }

    pub(in crate::runtime) fn request_full_rescan(&self) -> bool {
        RUNTIME_STATE.exclusive().rescan_all = 1;
        true
    }
}
