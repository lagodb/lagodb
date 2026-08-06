use pgrx::prelude::*;

use crate::worker::bgworker::DynamicWorkerRegistration;

use super::{COORDINATOR_TABLE, CoordinatorRegistration, SHARED_STATE, Store};

impl Store {
    pub(in crate::worker) fn register_coordinator(
        &self,
        database_oid: u32,
    ) -> CoordinatorRegistration {
        let _state = SHARED_STATE.exclusive();
        if let Some(slot) = COORDINATOR_TABLE.find(database_oid) {
            if slot.process().is_active() {
                return CoordinatorRegistration::AlreadyActive;
            }
            if !slot.needs_restart() {
                return CoordinatorRegistration::NoWork;
            }
        }
        let mut slot = COORDINATOR_TABLE.get_or_insert(database_oid);
        slot.request_restart();
        assert!(COORDINATOR_TABLE.replace(slot));
        let registration =
            match DynamicWorkerRegistration::register_coordinator(database_oid) {
                Ok(registration) => registration,
                Err(error) => return CoordinatorRegistration::Failed(error),
            };
        slot.reserve();
        assert!(COORDINATOR_TABLE.replace(slot));
        CoordinatorRegistration::Registered(registration)
    }

    pub(in crate::worker) fn coordinator_registration_failed(
        &self,
        database_oid: u32,
    ) {
        let _state = SHARED_STATE.exclusive();
        COORDINATOR_TABLE.with_mut(database_oid, |slot| {
            if slot.process().is_active() {
                slot.reset_after_registration_failure();
            }
        });
    }

    pub(in crate::worker) fn begin_coordinator(&self, database_oid: u32) -> bool {
        let state = SHARED_STATE.exclusive();
        let result = COORDINATOR_TABLE.with_mut(database_oid, |slot| {
            slot.mark_running(unsafe { pg_sys::MyProcPid }, unsafe {
                pg_sys::MyProcNumber
            })
        });
        let Some(result) = result else {
            return false;
        };
        drop(state);
        result
    }

    pub(in crate::worker) fn validate_coordinator(&self, database_oid: u32) -> bool {
        let _state = SHARED_STATE.share();
        COORDINATOR_TABLE
            .find(database_oid)
            .is_some_and(|slot| slot.has_reconciliation_authority())
    }
}
