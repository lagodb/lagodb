use crate::worker::state::Identity;

use super::{COORDINATOR_TABLE, SHARED_STATE, Store, WORKER_TABLE};
use crate::worker::status::{ProcessStatus, WorkerStatus};

#[derive(Clone, Copy)]
struct WorkerStatusSnapshot {
    identity: Identity,
    registration_state: &'static str,
    process_state: &'static str,
    pid: Option<i32>,
    needs_restart: bool,
    restart_after_ms: Option<i64>,
    failure_count: i32,
    stop_requested: bool,
}

impl Store {
    pub(in crate::worker) fn worker_status(&self) -> Vec<WorkerStatus> {
        let snapshots: Vec<_> = {
            let _state = SHARED_STATE.share();
            WORKER_TABLE
                .snapshots()
                .into_iter()
                .map(|slot| WorkerStatusSnapshot {
                    identity: slot.identity(),
                    registration_state: slot.registration().as_str(),
                    process_state: slot.process().as_str(),
                    pid: (slot.pid > 0).then_some(slot.pid),
                    needs_restart: slot.needs_restart(),
                    restart_after_ms: (slot.needs_restart()
                        && slot.restart_after_ms > 0)
                        .then_some(slot.restart_after_ms),
                    failure_count: slot.failure_count,
                    stop_requested: slot.is_stop_requested(),
                })
                .collect()
        };
        snapshots
            .into_iter()
            .map(|snapshot| WorkerStatus {
                database_oid: snapshot.identity.database_oid,
                worker_id: snapshot.identity.worker_id,
                extension_oid: snapshot.identity.extension_oid,
                worker_name: snapshot.identity.worker_name().to_owned(),
                registration_state: snapshot.registration_state,
                process_state: snapshot.process_state,
                pid: snapshot.pid,
                needs_restart: snapshot.needs_restart,
                restart_after_ms: snapshot.restart_after_ms,
                failure_count: snapshot.failure_count,
                stop_requested: snapshot.stop_requested,
            })
            .collect()
    }

    pub(in crate::worker) fn process_status(&self) -> Vec<ProcessStatus> {
        let state = SHARED_STATE.share();
        let mut statuses = Vec::new();
        statuses.push(ProcessStatus {
            process_kind: "supervisor",
            database_oid: None,
            state: if state.supervisor_pid > 0 {
                "running"
            } else {
                "stopped"
            },
            pid: (state.supervisor_pid > 0).then_some(state.supervisor_pid),
            needs_restart: None,
        });
        statuses.extend(COORDINATOR_TABLE.snapshots().into_iter().map(|slot| {
            let process = slot.process();
            ProcessStatus {
                process_kind: "coordinator",
                database_oid: Some(slot.database_oid),
                state: process.as_str(),
                pid: (slot.pid > 0).then_some(slot.pid),
                needs_restart: Some(slot.needs_restart()),
            }
        }));
        statuses
    }
}
