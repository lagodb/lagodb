use std::collections::HashSet;

use pgrx::PgLwLock;
use pgrx::prelude::*;

use super::state::{CoordinatorStopDisposition, Identity, SharedState, WorkerKey};
use crate::worker::bgworker::{DynamicWorkerRegistration, DynamicWorkerStartError};

use table::{CoordinatorTable, WorkerTable};

mod control;
mod coordinator;
#[cfg(feature = "pg_test")]
mod pg_test;
mod registration;
mod status;
mod table;
mod worker;

pub(crate) static SHARED_STATE: PgLwLock<SharedState> =
    unsafe { PgLwLock::new(c"lagodb worker state") };
pub(crate) static COORDINATOR_TABLE: CoordinatorTable = CoordinatorTable::new();
pub(crate) static WORKER_TABLE: WorkerTable = WorkerTable::new();

pub(super) struct Store;

pub(super) struct WorkerStart {
    pub(super) database_oid: u32,
    pub(super) worker_id: i32,
    pub(super) extension_oid: u32,
    pub(super) worker_name: String,
}

pub(super) struct WorkerLaunch {
    pub(super) key: WorkerKey,
    pub(super) identity: Identity,
}

pub(super) enum CoordinatorRegistration {
    Registered(DynamicWorkerRegistration),
    AlreadyActive,
    NoWork,
    Failed(DynamicWorkerStartError),
}

pub(super) enum WorkerLaunchRegistration {
    Registered {
        launch: WorkerLaunch,
        registration: DynamicWorkerRegistration,
    },
    Failed {
        launch: WorkerLaunch,
        error: DynamicWorkerStartError,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StoppedWorkerProcess {
    Coordinator(CoordinatorStopDisposition),
    Worker { reconcile: bool },
    Stale,
}

impl Store {
    pub(super) const fn new() -> Self {
        Self
    }

    pub(super) fn signal_supervisor(&self) {
        let proc_number = SHARED_STATE.share().supervisor_proc_number;
        if proc_number != pg_sys::INVALID_PROC_NUMBER {
            // SAFETY: the supervisor publishes a ProcNumber while alive. A stale
            // value can only produce a harmless spurious latch wakeup.
            unsafe { pg_sys::ProcSendSignal(proc_number) };
        }
    }

    pub(super) fn claim_supervisor(&self) {
        let mut state = SHARED_STATE.exclusive();
        state.supervisor_pid = unsafe { pg_sys::MyProcPid };
        state.supervisor_proc_number = unsafe { pg_sys::MyProcNumber };
        state.rescan_all = 1;
    }

    pub(super) fn clear_supervisor(&self) {
        let mut state = SHARED_STATE.exclusive();
        state.supervisor_pid = 0;
        state.supervisor_proc_number = pg_sys::INVALID_PROC_NUMBER;
    }

    pub(super) fn take_full_scan_request(&self) -> bool {
        let mut state = SHARED_STATE.exclusive();
        let requested = state.rescan_all != 0;
        state.rescan_all = 0;
        requested
    }

    pub(super) fn remove_dropped_databases(&self, live: &HashSet<u32>) {
        let _state = SHARED_STATE.exclusive();
        let mut dropped_coordinators = Vec::new();
        COORDINATOR_TABLE.for_each_mut(|slot| {
            if !live.contains(&slot.database_oid) {
                if slot.process().is_active() {
                    slot.request_stop();
                } else {
                    dropped_coordinators.push(slot.database_oid);
                }
            }
        });
        for database_oid in dropped_coordinators {
            COORDINATOR_TABLE.remove(database_oid);
        }
        let mut dropped_workers = Vec::new();
        WORKER_TABLE.for_each_mut(|slot| {
            if !live.contains(&slot.database_oid) {
                if !slot.has_active_process() {
                    dropped_workers.push(slot.key());
                } else {
                    slot.mark_removing();
                }
            }
        });
        for key in dropped_workers {
            WORKER_TABLE.remove(key);
        }
    }
}
