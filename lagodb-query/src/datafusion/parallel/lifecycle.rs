//! Partial launch ownership before a run reaches its stable running state.

use std::mem;

use super::host::ParallelWorkers;
use super::run::LeaderTransport;

/// Owns a launched worker group until transport initialization has completed.
/// Any partial-launch error follows the same non-blocking abort ownership as a
/// running query rather than invoking `DestroyParallelContext` from `Drop`.
pub(super) struct ParallelWorkerLaunch {
    state: ParallelWorkerLaunchState,
}

enum ParallelWorkerLaunchState {
    Workers(Box<dyn ParallelWorkers>),
    Transport {
        transport: LeaderTransport,
        workers: Box<dyn ParallelWorkers>,
    },
    Complete,
}

impl ParallelWorkerLaunch {
    pub(super) fn new(workers: Box<dyn ParallelWorkers>) -> Self {
        Self {
            state: ParallelWorkerLaunchState::Workers(workers),
        }
    }

    pub(super) fn workers(&self) -> &dyn ParallelWorkers {
        match &self.state {
            ParallelWorkerLaunchState::Workers(workers)
            | ParallelWorkerLaunchState::Transport { workers, .. } => {
                workers.as_ref()
            }
            ParallelWorkerLaunchState::Complete => {
                unreachable!("completed launch transferred the worker group")
            }
        }
    }

    pub(super) fn workers_mut(&mut self) -> &mut dyn ParallelWorkers {
        match &mut self.state {
            ParallelWorkerLaunchState::Workers(workers)
            | ParallelWorkerLaunchState::Transport { workers, .. } => {
                workers.as_mut()
            }
            ParallelWorkerLaunchState::Complete => {
                unreachable!("completed launch transferred the worker group")
            }
        }
    }

    pub(super) fn install_transport(&mut self, transport: LeaderTransport) {
        let state =
            mem::replace(&mut self.state, ParallelWorkerLaunchState::Complete);
        let ParallelWorkerLaunchState::Workers(workers) = state else {
            unreachable!("transport is installed once after worker launch")
        };
        self.state = ParallelWorkerLaunchState::Transport { transport, workers };
    }

    pub(super) fn complete(mut self) -> (LeaderTransport, Box<dyn ParallelWorkers>) {
        let state =
            mem::replace(&mut self.state, ParallelWorkerLaunchState::Complete);
        let ParallelWorkerLaunchState::Transport { transport, workers } = state
        else {
            unreachable!("completed launch owns transport and workers")
        };
        (transport, workers)
    }
}

impl Drop for ParallelWorkerLaunch {
    fn drop(&mut self) {
        let state =
            mem::replace(&mut self.state, ParallelWorkerLaunchState::Complete);
        match state {
            ParallelWorkerLaunchState::Workers(workers) => workers.abandon(),
            ParallelWorkerLaunchState::Transport { transport, workers } => {
                // Dropping the transport ends the run for every worker while the
                // mapping is still attached, then detaches the reactor.
                drop(transport);
                workers.abandon();
            }
            ParallelWorkerLaunchState::Complete => {}
        }
    }
}
