use pgrx::pg_sys;

use crate::worker::state::WorkerKey;

use super::{COORDINATOR_TABLE, SHARED_STATE, Store, WORKER_TABLE};

impl Store {
    pub(in crate::worker) fn prepare_drop_database(&self, database_oid: u32) {
        let _state = SHARED_STATE.exclusive();
        for mut slot in COORDINATOR_TABLE.snapshots() {
            if slot.database_oid == database_oid {
                slot.prepare_for_drop();
                Self::terminate_coordinator(slot.pid);
                assert!(COORDINATOR_TABLE.replace(slot));
            }
        }
        for mut slot in WORKER_TABLE.snapshots() {
            if slot.database_oid == database_oid && slot.prepare_transactional_stop()
            {
                Self::signal_worker(slot.pid);
                assert!(WORKER_TABLE.replace(slot));
            }
        }
    }

    pub(in crate::worker) fn prepare_drop_extension(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) {
        let _state = SHARED_STATE.exclusive();
        for mut slot in COORDINATOR_TABLE.snapshots() {
            if slot.database_oid == database_oid {
                slot.prepare_for_drop();
                Self::terminate_coordinator(slot.pid);
                assert!(COORDINATOR_TABLE.replace(slot));
            }
        }
        for mut slot in WORKER_TABLE.snapshots() {
            if slot.database_oid == database_oid
                && slot.extension_oid == extension_oid
                && slot.prepare_transactional_stop()
            {
                Self::signal_worker(slot.pid);
                assert!(WORKER_TABLE.replace(slot));
            }
        }
    }

    /// Stops a deregistered worker and invalidates any coordinator snapshot
    /// that predates the catalog change.
    ///
    /// Returns `true` when an existing coordinator cannot receive the immediate
    /// handoff, so the caller must wake the supervisor after releasing
    /// `SHARED_STATE`. A missing coordinator is recovered by the database
    /// reconciliation that the deregistration caller stages for transaction
    /// commit or abort.
    pub(in crate::worker) fn request_stop_worker(
        &self,
        database_oid: u32,
        worker_id: i32,
    ) -> bool {
        let _state = SHARED_STATE.exclusive();
        let key = WorkerKey::new(database_oid, worker_id);
        if let Some(mut slot) = WORKER_TABLE.find(key)
            && slot.prepare_transactional_stop()
        {
            Self::signal_worker(slot.pid);
            assert!(WORKER_TABLE.replace(slot));
        }

        let Some(mut coordinator) = COORDINATOR_TABLE.find(database_oid) else {
            return false;
        };
        coordinator.request_handoff();
        let proc_number = coordinator.proc_number;
        assert!(COORDINATOR_TABLE.replace(coordinator));
        if proc_number != pg_sys::INVALID_PROC_NUMBER {
            // SAFETY: the coordinator publishes this ProcNumber under the same
            // shared-state lock. Its exit callback cannot clear or recycle the
            // identity until this lock is released. This is a reconciliation
            // handoff, so a latch wake is sufficient; it is not a DROP SIGTERM.
            unsafe { pg_sys::ProcSendSignal(proc_number) };
            return false;
        }
        true
    }

    fn terminate_coordinator(pid: i32) {
        if pid > 0 {
            // SAFETY: the coordinator publishes its PID before it becomes
            // stoppable. SIGTERM is PostgreSQL's standard background-worker
            // termination path and is harmless if the process is exiting.
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }

    fn signal_worker(pid: i32) {
        if pid > 0 {
            // SAFETY: a worker slot's PID is published by that worker and is
            // cleared only by its exit callback. SIGTERM is idempotent here.
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
}
