use pgrx::pg_sys;

use crate::worker::state::{INVALID_OID, SharedState, WorkerKey};

use super::{COORDINATOR_TABLE, SHARED_STATE, Store, WORKER_TABLE};

impl Store {
    pub(in crate::worker) fn wake_worker(&self, key: WorkerKey) -> bool {
        let mut state = SHARED_STATE.exclusive();
        WORKER_TABLE.with_mut(key, |slot| slot.request_wakeup());
        Self::request_coordination_locked(&mut state, key.database_oid)
    }

    pub(in crate::worker) fn wake_database_workers(&self, database_oid: u32) -> bool {
        let mut state = SHARED_STATE.exclusive();
        WORKER_TABLE.for_each_mut(|slot| {
            if slot.database_oid == database_oid {
                slot.request_wakeup();
            }
        });
        Self::request_coordination_locked(&mut state, database_oid)
    }

    pub(in crate::worker) fn request_database_reconcile(
        &self,
        database_oid: u32,
    ) -> bool {
        let mut state = SHARED_STATE.exclusive();
        Self::request_coordination_locked(&mut state, database_oid)
    }

    /// Routes a database coordination request to one control process.
    ///
    /// Existing coordinators retain the request in their slot. When no live
    /// coordinator can receive it, a full database scan retains the request for
    /// the supervisor without creating an unqualified coordinator entry.
    /// Returns `true` when the caller must wake the supervisor after releasing
    /// `SHARED_STATE`.
    pub(super) fn request_coordination_locked(
        state: &mut SharedState,
        database_oid: u32,
    ) -> bool {
        if database_oid == INVALID_OID {
            return false;
        }
        let Some(mut coordinator) = COORDINATOR_TABLE.find(database_oid) else {
            state.rescan_all = 1;
            return true;
        };
        coordinator.request_restart();
        let proc_number = coordinator.proc_number;
        assert!(COORDINATOR_TABLE.replace(coordinator));
        if proc_number != pg_sys::INVALID_PROC_NUMBER {
            // SAFETY: the coordinator publishes this ProcNumber under the same
            // shared-state lock. Its exit callback cannot clear or recycle the
            // identity until this lock is released.
            unsafe { pg_sys::ProcSendSignal(proc_number) };
            return false;
        }
        state.rescan_all = 1;
        true
    }

    pub(in crate::worker) fn request_full_rescan(&self) -> bool {
        SHARED_STATE.exclusive().rescan_all = 1;
        true
    }
}
