use std::collections::HashMap;
use std::ffi::CStr;

use pgrx::prelude::*;

use super::bgworker::{HandleStatus, LauncherWorkerHandle, ReconcilerToken};
use super::process::{OwnedProcess, ProcessToken};
use super::store::{ReconcilerRetry, RuntimeStore, StoppedProcess, WorkerLaunch};
use super::{RECONCILER_FUNCTION, RECONCILER_TYPE, WORKER_FUNCTION, WORKER_TYPE};

const RESTART_EMPTY_RECOVERY_SCANS: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackendIdentity {
    pid: i32,
    proc_number: i32,
    process_start: i64,
}

struct HandleRegistry {
    owned: HashMap<ProcessToken, OwnedProcess>,
}

pub(super) struct LauncherExitState {
    handles: Vec<*mut pg_sys::BackgroundWorkerHandle>,
}

impl LauncherExitState {
    pub(super) const fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    fn add(&mut self, handle: *mut pg_sys::BackgroundWorkerHandle) {
        self.handles.push(handle);
    }

    fn remove(&mut self, handle: *mut pg_sys::BackgroundWorkerHandle) {
        if let Some(index) = self
            .handles
            .iter()
            .position(|candidate| *candidate == handle)
        {
            self.handles.swap_remove(index);
        }
    }

    pub(super) fn request_all_terminations(&self) {
        for &handle in &self.handles {
            // SAFETY: entries are retained PostgreSQL handles that are removed
            // only after BGWH_STOPPED. Termination is idempotent and valid for
            // both running and not-yet-started dynamic workers.
            unsafe { pg_sys::TerminateBackgroundWorker(handle) };
        }
    }
}

impl HandleRegistry {
    fn new() -> Self {
        Self {
            owned: HashMap::new(),
        }
    }

    fn insert(&mut self, process: OwnedProcess) {
        let previous = self.owned.insert(process.token(), process);
        debug_assert!(previous.is_none());
    }

    fn active_reconcilers(&self) -> usize {
        self.owned
            .keys()
            .filter(|token| matches!(token, ProcessToken::Reconciler(_)))
            .count()
    }

    fn active_workers(&self) -> usize {
        self.owned
            .keys()
            .filter(|token| matches!(token, ProcessToken::Worker(_)))
            .count()
    }

    fn stopped_tokens(&self) -> Vec<ProcessToken> {
        self.owned
            .iter()
            .filter_map(|(&token, process)| {
                (process.status() == HandleStatus::Stopped).then_some(token)
            })
            .collect()
    }
}

pub(super) struct LauncherSupervisor {
    handles: HandleRegistry,
    reported_recovery_backends: Option<usize>,
    consecutive_empty_recovery_scans: u8,
    exit_state: *mut LauncherExitState,
}

impl LauncherSupervisor {
    pub(super) fn new(exit_state: *mut LauncherExitState) -> Self {
        Self {
            handles: HandleRegistry::new(),
            reported_recovery_backends: None,
            consecutive_empty_recovery_scans: 0,
            exit_state,
        }
    }

    fn exit_state(&mut self) -> &mut LauncherExitState {
        // SAFETY: launcher constructs one process-lifetime exit registry and
        // passes its stable pointer to this supervisor. Access is confined to
        // the launcher main thread before proc_exit callbacks run.
        unsafe { &mut *self.exit_state }
    }

    pub(super) fn active_reconcilers(&self) -> usize {
        self.handles.active_reconcilers()
    }

    pub(super) fn remaining_worker_capacity(&self, configured: usize) -> usize {
        configured.saturating_sub(self.handles.active_workers())
    }

    pub(super) fn start_reconciler(
        &mut self,
        store: &RuntimeStore,
        database_oid: u32,
        token: ReconcilerToken,
    ) -> bool {
        let name = format!(
            "pg-lakebase reconciler db={database_oid} g={}",
            token.generation()
        );
        match LauncherWorkerHandle::register_reconciler(
            RECONCILER_FUNCTION,
            &name,
            RECONCILER_TYPE,
            token,
        ) {
            Ok(handle) => {
                let process =
                    OwnedProcess::new(ProcessToken::Reconciler(token), handle);
                self.exit_state().add(process.raw_handle());
                self.handles.insert(process);
                true
            }
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to register pg-lakebase-runtime database reconciler: database_oid={database_oid}, generation={}, error={error}",
                    token.generation()
                ));
                store.reconciler_registration_failed(token);
                false
            }
        }
    }

    pub(super) fn start_worker(
        &mut self,
        store: &RuntimeStore,
        launch: WorkerLaunch,
    ) {
        let name = format!(
            "pg-lakebase worker db={} slot={} g={}",
            launch.identity.database_oid,
            launch.token.index(),
            launch.token.generation(),
        );
        match LauncherWorkerHandle::register_worker(
            WORKER_FUNCTION,
            &name,
            WORKER_TYPE,
            launch.token,
        ) {
            Ok(handle) => {
                let process =
                    OwnedProcess::new(ProcessToken::Worker(launch.token), handle);
                self.exit_state().add(process.raw_handle());
                self.handles.insert(process);
                crate::diag::info(format_args!(
                    "registered Lakebase extension worker: database_oid={}, extension_oid={}, worker_name={}, generation={}",
                    launch.identity.database_oid,
                    launch.identity.extension_oid,
                    launch.identity.worker_name(),
                    launch.token.generation(),
                ));
            }
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to register Lakebase extension worker: database_oid={}, extension_oid={}, worker_name={}, generation={}, error={error}",
                    launch.identity.database_oid,
                    launch.identity.extension_oid,
                    launch.identity.worker_name(),
                    launch.token.generation(),
                ));
                store.worker_registration_failed(launch.token);
            }
        }
    }

    pub(super) fn reap_stopped(
        &mut self,
        store: &RuntimeStore,
    ) -> Vec<(u32, ReconcilerRetry)> {
        let stopped = self.handles.stopped_tokens();
        let mut retries = Vec::new();
        for token in stopped {
            let process = self
                .handles
                .owned
                .remove(&token)
                .expect("stopped token came from handle registry");
            self.exit_state().remove(process.raw_handle());
            match store.confirm_process_stopped(token) {
                StoppedProcess::Reconciler {
                    database_oid,
                    retry,
                } => {
                    if retry != ReconcilerRetry::None {
                        retries.push((database_oid, retry));
                    }
                }
                StoppedProcess::Worker | StoppedProcess::Stale => {}
            }
            process.release_after_stopped();
        }
        retries
    }

    pub(super) fn request_terminations(&mut self, store: &RuntimeStore) {
        for token in store.termination_requests() {
            let Some(process) = self.handles.owned.get_mut(&token) else {
                crate::diag::warning(format_args!(
                    "runtime process has no launcher-owned handle: token={token:?}"
                ));
                continue;
            };
            process.request_termination();
        }
    }

    /// Terminate every previous-generation runtime backend visible in a fresh
    /// PostgreSQL activity snapshot. Recovery remains closed to dispatch until
    /// the restart path observes a stable empty result and no backend can still
    /// execute extension code.
    pub(super) fn drain_previous_generation(
        &mut self,
        store: &RuntimeStore,
        launcher_epoch: u64,
    ) -> bool {
        debug_assert!(self.handles.owned.is_empty());
        let identities = Self::category_backends();
        let count = identities.len();
        let count_changed = self.reported_recovery_backends != Some(count);
        if count_changed {
            self.reported_recovery_backends = Some(count);
            store.update_recovery_backend_count(count);
        }
        if identities.is_empty() {
            self.consecutive_empty_recovery_scans =
                self.consecutive_empty_recovery_scans.saturating_add(1);
            let required_empty_scans = if launcher_epoch == 1 {
                1
            } else {
                RESTART_EMPTY_RECOVERY_SCANS
            };
            if self.consecutive_empty_recovery_scans >= required_empty_scans {
                crate::diag::info(
                    "previous-generation Lakebase backends are no longer executing",
                );
                return true;
            }
            return false;
        }
        self.consecutive_empty_recovery_scans = 0;
        for identity in &identities {
            Self::terminate_if_still_same(*identity);
        }
        if count_changed {
            crate::diag::info(format_args!(
                "Lakebase launcher is draining {count} previous-generation runtime backends: launcher_epoch={launcher_epoch}",
            ));
        }
        false
    }

    fn category_backends() -> Vec<BackendIdentity> {
        let mut identities = Vec::new();
        // SAFETY: these are PostgreSQL's local backend-status snapshot APIs.
        // Each returned entry is copied before the next call and is treated as
        // advisory until terminate_if_still_same revalidates it.
        unsafe { pg_sys::pgstat_clear_backend_activity_snapshot() };
        // SAFETY: the backend-status snapshot is initialized above and the
        // returned count bounds the one-based local-entry iteration.
        let count = unsafe { pg_sys::pgstat_fetch_stat_numbackends() };
        for index in 1..=count {
            // SAFETY: index is within the backend count from the same snapshot.
            let local = unsafe { pg_sys::pgstat_get_local_beentry_by_index(index) };
            let Some(local) = std::ptr::NonNull::new(local) else {
                continue;
            };
            // SAFETY: NonNull validated the snapshot entry pointer and it is
            // consumed before the next snapshot refresh.
            let status = unsafe { &local.as_ref().backendStatus };
            if status.st_procpid <= 0 || !Self::is_runtime_category(status.st_procpid)
            {
                continue;
            }
            identities.push(BackendIdentity {
                pid: status.st_procpid,
                // SAFETY: same validated local snapshot entry as `status`.
                proc_number: unsafe { local.as_ref().proc_number },
                process_start: status.st_proc_start_timestamp,
            });
        }
        identities
    }

    fn is_runtime_category(pid: i32) -> bool {
        // SAFETY: PostgreSQL returns null or a stable C string for a currently
        // visible background worker PID.
        let worker_type = unsafe { pg_sys::GetBackgroundWorkerTypeByPid(pid) };
        if worker_type.is_null() {
            return false;
        }
        // SAFETY: PostgreSQL returned a non-null, NUL-terminated worker type.
        let worker_type = unsafe { CStr::from_ptr(worker_type) };
        worker_type.to_bytes() == RECONCILER_TYPE.as_bytes()
            || worker_type.to_bytes() == WORKER_TYPE.as_bytes()
    }

    fn is_still_same(identity: BackendIdentity) -> bool {
        // Revalidate PID, ProcNumber, process start timestamp, and category
        // so PID reuse cannot target or retain an unrelated PostgreSQL backend.
        // SAFETY: BackendPidGetProc performs its own ProcArrayLock lookup. Use
        // only nullness here: the returned PGPROC is not protected after the
        // function releases that lock.
        let process = unsafe { pg_sys::BackendPidGetProc(identity.pid) };
        if process.is_null() {
            return false;
        }
        // SAFETY: discard the earlier advisory snapshot so process start time
        // and PID are read again immediately before signaling.
        unsafe { pg_sys::pgstat_clear_backend_activity_snapshot() };
        let local = unsafe {
            pg_sys::pgstat_get_local_beentry_by_proc_number(identity.proc_number)
        };
        if local.is_null() {
            return false;
        }
        // SAFETY: pgstat returned a non-null entry from the current local
        // backend-status snapshot.
        let status = unsafe { &(*local).backendStatus };
        status.st_procpid == identity.pid
            && status.st_proc_start_timestamp == identity.process_start
            && Self::is_runtime_category(identity.pid)
    }

    fn terminate_if_still_same(identity: BackendIdentity) {
        if !Self::is_still_same(identity) {
            return;
        }
        // SAFETY: the identity was just revalidated against PGPROC, the stats
        // snapshot, and the exact Lakebase bgw_type. SIGTERM is PostgreSQL's
        // normal background-worker shutdown signal.
        let result = unsafe { libc::kill(identity.pid, libc::SIGTERM) };
        if result != 0 {
            crate::diag::warning(format_args!(
                "failed to terminate old Lakebase runtime process: pid={}",
                identity.pid,
            ));
        }
    }
}
