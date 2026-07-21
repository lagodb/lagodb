use crate::state::{
    DispatchState, INVALID_OID, ProcessState, RecoveryState, RegistrationState,
    WorkerIdentity,
};

use super::{RUNTIME_STATE, RuntimeStore};
use crate::runtime::status::{ProcessStatus, WorkerStatus};

#[derive(Clone, Copy)]
struct WorkerStatusSnapshot {
    identity: WorkerIdentity,
    registration_state: &'static str,
    dispatch_state: &'static str,
    process_state: &'static str,
    pid: Option<i32>,
    generation: u32,
    not_before_ms: Option<i64>,
    stop_requested: bool,
    launcher_epoch: u64,
    recovery_state: &'static str,
}

impl RuntimeStore {
    pub(in crate::runtime) fn worker_status(&self) -> Vec<WorkerStatus> {
        let snapshots: Vec<_> = {
            let state = RUNTIME_STATE.share();
            let recovery_state = RecoveryState::decode(state.recovery_state)
                .map_or("corrupt", RecoveryState::as_str);
            state
                .workers
                .iter()
                .filter(|slot| !slot.is_empty())
                .map(|slot| WorkerStatusSnapshot {
                    identity: slot.identity(),
                    registration_state: slot
                        .registration()
                        .map_or("corrupt", RegistrationState::as_str),
                    dispatch_state: slot
                        .dispatch()
                        .map_or("corrupt", DispatchState::as_str),
                    process_state: slot
                        .process()
                        .map_or("corrupt", ProcessState::as_str),
                    pid: (slot.pid > 0).then_some(slot.pid),
                    generation: slot.generation,
                    not_before_ms: (slot.dispatch() == Ok(DispatchState::Delayed))
                        .then_some(slot.not_before_ms),
                    stop_requested: slot.stop_requested != 0,
                    launcher_epoch: state.launcher_epoch,
                    recovery_state,
                })
                .collect()
        };
        snapshots
            .into_iter()
            .map(|snapshot| WorkerStatus {
                database_oid: snapshot.identity.database_oid,
                extension_oid: snapshot.identity.extension_oid,
                worker_name: snapshot.identity.worker_name().to_owned(),
                registration_state: snapshot.registration_state,
                dispatch_state: snapshot.dispatch_state,
                process_state: snapshot.process_state,
                pid: snapshot.pid,
                generation: snapshot.generation,
                not_before_ms: snapshot.not_before_ms,
                stop_requested: snapshot.stop_requested,
                launcher_epoch: snapshot.launcher_epoch,
                recovery_state: snapshot.recovery_state,
            })
            .collect()
    }

    pub(in crate::runtime) fn process_status(&self) -> Vec<ProcessStatus> {
        let state = RUNTIME_STATE.share();
        let mut statuses = Vec::with_capacity(1 + state.reconcilers.len());
        statuses.push(ProcessStatus {
            process_kind: "launcher",
            database_oid: None,
            state: if state.launcher_pid > 0 {
                match RecoveryState::decode(state.recovery_state) {
                    Ok(RecoveryState::Ready) => "running",
                    Ok(RecoveryState::Recovering) => "recovering",
                    Ok(RecoveryState::Reconciling) => "reconciling",
                    Err(_) => "corrupt",
                }
            } else {
                "stopped"
            },
            pid: (state.launcher_pid > 0).then_some(state.launcher_pid),
            recovery_backend_count: Some(state.recovery_backend_count),
        });
        statuses.extend(state.reconcilers.iter().filter_map(|slot| {
            let process = slot.process().ok()?;
            (slot.database_oid != INVALID_OID && process.is_active()).then_some(
                ProcessStatus {
                    process_kind: "database_reconciler",
                    database_oid: Some(slot.database_oid),
                    state: process.as_str(),
                    pid: (slot.pid > 0).then_some(slot.pid),
                    recovery_backend_count: None,
                },
            )
        }));
        statuses
    }
}
