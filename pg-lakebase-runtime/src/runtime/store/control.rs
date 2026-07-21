use std::fmt::Write;

use crate::error::LakebaseResult;
use crate::state::{ProcessState, RecoveryState, WorkerIdentity, WorkerSlot};

use super::{RUNTIME_STATE, RuntimeStore, validate_state};
use crate::runtime::reconcile::ReconcilerSlot;

#[derive(Clone, Copy)]
pub(in crate::runtime) enum StopTarget<'a> {
    Database(u32),
    Extension {
        database_oid: u32,
        extension_oid: u32,
    },
    Worker {
        database_oid: u32,
        extension_oid: u32,
        worker_name: &'a str,
    },
}

impl StopTarget<'_> {
    fn includes_reconciler(self, slot: &ReconcilerSlot) -> bool {
        matches!(self, Self::Database(database_oid) if slot.database_oid == database_oid)
    }

    fn includes_worker(self, slot: &WorkerSlot) -> bool {
        match self {
            Self::Database(database_oid) => slot.database_oid == database_oid,
            Self::Extension {
                database_oid,
                extension_oid,
            } => {
                slot.database_oid == database_oid
                    && slot.extension_oid == extension_oid
            }
            Self::Worker {
                database_oid,
                extension_oid,
                worker_name,
            } => slot.matches_worker(database_oid, extension_oid, worker_name),
        }
    }
}

impl std::fmt::Display for StopTarget<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(database_oid) => {
                write!(formatter, "database(database_oid={database_oid})")
            }
            Self::Extension {
                database_oid,
                extension_oid,
            } => write!(
                formatter,
                "extension(database_oid={database_oid}, extension_oid={extension_oid})"
            ),
            Self::Worker {
                database_oid,
                extension_oid,
                worker_name,
            } => write!(
                formatter,
                "worker(database_oid={database_oid}, extension_oid={extension_oid}, worker_name={worker_name})"
            ),
        }
    }
}

#[derive(Clone, Copy)]
struct ReconcilerStopDiagnostic {
    database_oid: u32,
    generation: u32,
    process_state: &'static str,
    pid: i32,
    proc_number: i32,
    stop_requested: u8,
}

#[derive(Clone, Copy)]
struct WorkerStopDiagnostic {
    identity: WorkerIdentity,
    generation: u32,
    process_state: &'static str,
    pid: i32,
    proc_number: i32,
    stop_requested: u8,
}

struct StopDiagnosticsSnapshot {
    launcher_epoch: u64,
    recovery_state: &'static str,
    recovery_backend_count: u32,
    reconcilers: Vec<ReconcilerStopDiagnostic>,
    workers: Vec<WorkerStopDiagnostic>,
}

impl StopDiagnosticsSnapshot {
    fn render(self, target: StopTarget<'_>) -> String {
        let mut details = format!(
            "target={target}, launcher_epoch={}, recovery_state={}, recovery_backend_count={}, active_processes=[",
            self.launcher_epoch, self.recovery_state, self.recovery_backend_count
        );
        let mut separator = "";
        for process in self.reconcilers {
            write!(
                details,
                "{separator}reconciler(database_oid={}, generation={}, process_state={}, pid={}, proc_number={}, stop_requested={})",
                process.database_oid,
                process.generation,
                process.process_state,
                process.pid,
                process.proc_number,
                process.stop_requested,
            )
            .expect("writing to String cannot fail");
            separator = ", ";
        }
        for process in self.workers {
            write!(
                details,
                "{separator}worker(database_oid={}, extension_oid={}, worker_name={}, generation={}, process_state={}, pid={}, proc_number={}, stop_requested={})",
                process.identity.database_oid,
                process.identity.extension_oid,
                process.identity.worker_name(),
                process.generation,
                process.process_state,
                process.pid,
                process.proc_number,
                process.stop_requested,
            )
            .expect("writing to String cannot fail");
            separator = ", ";
        }
        details.push(']');
        details
    }
}

impl RuntimeStore {
    pub(in crate::runtime) fn request_stop_database(
        &self,
        database_oid: u32,
    ) -> LakebaseResult<()> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        for slot in &mut state.reconcilers {
            if slot.database_oid == database_oid {
                slot.request_stop()?;
            }
        }
        for slot in &mut state.workers {
            if slot.database_oid == database_oid && !slot.is_empty() {
                slot.request_stop()?;
            }
        }
        Ok(())
    }

    pub(in crate::runtime) fn database_is_stopped(&self, database_oid: u32) -> bool {
        let state = RUNTIME_STATE.share();
        state.allows_stop_completion()
            && !state.reconcilers.iter().any(|slot| {
                slot.database_oid == database_oid
                    && slot.process().is_ok_and(ProcessState::is_active)
            })
            && !state.workers.iter().any(|slot| {
                slot.database_oid == database_oid
                    && slot.process().is_ok_and(ProcessState::is_active)
            })
    }

    pub(in crate::runtime) fn request_stop_extension(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> LakebaseResult<()> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        for slot in &mut state.workers {
            if slot.database_oid == database_oid
                && slot.extension_oid == extension_oid
                && !slot.is_empty()
            {
                slot.request_stop()?;
            }
        }
        Ok(())
    }

    pub(in crate::runtime) fn extension_is_stopped(
        &self,
        database_oid: u32,
        extension_oid: u32,
    ) -> bool {
        let state = RUNTIME_STATE.share();
        state.allows_stop_completion()
            && !state.workers.iter().any(|slot| {
                slot.database_oid == database_oid
                    && slot.extension_oid == extension_oid
                    && slot.process().is_ok_and(ProcessState::is_active)
            })
    }

    pub(in crate::runtime) fn request_stop_worker(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> LakebaseResult<bool> {
        let mut state = RUNTIME_STATE.exclusive();
        validate_state(&state)?;
        let Some(index) = state.worker_slot(database_oid, extension_oid, worker_name)
        else {
            return Ok(false);
        };
        state.workers[index].request_stop()?;
        Ok(true)
    }

    pub(in crate::runtime) fn worker_is_stopped(
        &self,
        database_oid: u32,
        extension_oid: u32,
        worker_name: &str,
    ) -> bool {
        let state = RUNTIME_STATE.share();
        if !state.allows_stop_completion() {
            return false;
        }
        state
            .worker_slot(database_oid, extension_oid, worker_name)
            .is_none_or(|index| {
                state.workers[index].process() == Ok(ProcessState::Stopped)
            })
    }

    pub(in crate::runtime) fn stop_diagnostics(
        &self,
        target: StopTarget<'_>,
    ) -> String {
        let snapshot = {
            let state = RUNTIME_STATE.share();
            let recovery_state = RecoveryState::decode(state.recovery_state)
                .map_or("corrupt", RecoveryState::as_str);
            let mut reconcilers = Vec::new();
            for slot in &state.reconcilers {
                if target.includes_reconciler(slot)
                    && slot.process().is_ok_and(ProcessState::is_active)
                {
                    reconcilers.push(ReconcilerStopDiagnostic {
                        database_oid: slot.database_oid,
                        generation: slot.generation,
                        process_state: slot
                            .process()
                            .map_or("corrupt", ProcessState::as_str),
                        pid: slot.pid,
                        proc_number: slot.proc_number,
                        stop_requested: slot.stop_requested,
                    });
                }
            }
            let mut workers = Vec::new();
            for slot in &state.workers {
                if target.includes_worker(slot)
                    && slot.process().is_ok_and(ProcessState::is_active)
                {
                    workers.push(WorkerStopDiagnostic {
                        identity: slot.identity(),
                        generation: slot.generation,
                        process_state: slot
                            .process()
                            .map_or("corrupt", ProcessState::as_str),
                        pid: slot.pid,
                        proc_number: slot.proc_number,
                        stop_requested: slot.stop_requested,
                    });
                }
            }
            StopDiagnosticsSnapshot {
                launcher_epoch: state.launcher_epoch,
                recovery_state,
                recovery_backend_count: state.recovery_backend_count,
                reconcilers,
                workers,
            }
        };
        snapshot.render(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_stop_diagnostics_still_identifies_the_target() {
        let details = StopDiagnosticsSnapshot {
            launcher_epoch: 7,
            recovery_state: RecoveryState::Recovering.as_str(),
            recovery_backend_count: 2,
            reconcilers: Vec::new(),
            workers: Vec::new(),
        }
        .render(StopTarget::Worker {
            database_oid: 42,
            extension_oid: 8,
            worker_name: "maintenance",
        });

        assert!(details.contains(
            "target=worker(database_oid=42, extension_oid=8, worker_name=maintenance)"
        ));
        assert!(details.contains("recovery_state=recovering"));
        assert!(details.contains("recovery_backend_count=2"));
        assert!(details.ends_with("active_processes=[]"));
    }
}
