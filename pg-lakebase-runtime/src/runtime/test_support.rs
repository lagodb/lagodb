use std::panic::AssertUnwindSafe;
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::state::{ProcessState, RegistrationState};

use super::locks::DatabaseLifecycleLock;
use super::store::{RUNTIME_STATE, RegistrationCompletion, RuntimeStore};

pub(crate) struct RuntimeTestInjection;

impl RuntimeTestInjection {
    pub(super) const HOLD_AFTER_EXITING: &'static str = "hold_after_exiting";
    pub(super) const FAIL_EXIT_CLEANUP: &'static str = "fail_exit_cleanup";
    pub(super) const HOLD_AFTER_RUNNING: &'static str = "hold_after_running";
    pub(super) const HOLD_BEFORE_START: &'static str = "hold_before_start";
    pub(super) const HOLD_RECONCILER_AFTER_COMPLETION: &'static str =
        "hold_reconciler_after_completion";
    const NONE: u8 = 0;
    const HOLD_AFTER_EXITING_CODE: u8 = 1;
    const FAIL_EXIT_CLEANUP_CODE: u8 = 2;
    const HOLD_AFTER_RUNNING_CODE: u8 = 3;
    const HOLD_BEFORE_START_CODE: u8 = 4;
    const HOLD_RECONCILER_AFTER_COMPLETION_CODE: u8 = 5;

    pub(crate) fn set(name: &str) {
        let code = match name {
            Self::HOLD_AFTER_EXITING => Self::HOLD_AFTER_EXITING_CODE,
            Self::FAIL_EXIT_CLEANUP => Self::FAIL_EXIT_CLEANUP_CODE,
            Self::HOLD_AFTER_RUNNING => Self::HOLD_AFTER_RUNNING_CODE,
            Self::HOLD_BEFORE_START => Self::HOLD_BEFORE_START_CODE,
            Self::HOLD_RECONCILER_AFTER_COMPLETION => {
                Self::HOLD_RECONCILER_AFTER_COMPLETION_CODE
            }
            _ => panic!("unknown runtime test injection: {name}"),
        };
        RuntimeStore::new().set_runtime_test_injection(code);
    }

    pub(crate) fn clear() {
        RuntimeStore::new().set_runtime_test_injection(Self::NONE);
    }

    pub(crate) fn barrier_reached() -> bool {
        RuntimeStore::new().test_barrier_reached()
    }

    pub(crate) fn reconcile_snapshot(
        database_oid: u32,
    ) -> Option<(u64, u64, Option<u64>)> {
        let state = RUNTIME_STATE.share();
        let intent = state
            .database_reconciles
            .iter()
            .find(|intent| intent.database_oid == database_oid)?;
        let active_target = state
            .reconcilers
            .iter()
            .find(|slot| {
                slot.database_oid == database_oid
                    && slot.process().is_ok_and(ProcessState::is_active)
            })
            .map(|slot| slot.target_generation);
        Some((
            intent.desired_generation,
            intent.completed_generation,
            active_target,
        ))
    }

    pub(crate) fn reconcile_is_complete(database_oid: u32) -> bool {
        let state = RUNTIME_STATE.share();
        let active = state.reconcilers.iter().any(|slot| {
            slot.database_oid == database_oid
                && slot.process().is_ok_and(ProcessState::is_active)
        });
        let pending = state
            .database_reconciles
            .iter()
            .find(|intent| intent.database_oid == database_oid)
            .is_some_and(|intent| intent.is_pending());
        !active && !pending
    }

    pub(super) fn before_worker_start() {
        if RuntimeStore::new().runtime_test_injection()
            == Self::HOLD_BEFORE_START_CODE
        {
            Self::hold_interruptibly(Self::HOLD_BEFORE_START_CODE);
        }
    }

    pub(super) fn after_directive(database_oid: u32) {
        match RuntimeStore::new().runtime_test_injection() {
            Self::HOLD_AFTER_EXITING_CODE => Self::hold(database_oid),
            Self::FAIL_EXIT_CLEANUP_CODE => {
                // SAFETY: this path is compiled only for pg_test and intentionally
                // simulates a failure after publishing a directive. proc_exit(1)
                // preserves PostgreSQL's required child-slot cleanup while the
                // nonzero callback code makes the normal directive invalid.
                unsafe { pg_sys::proc_exit(1) };
            }
            Self::NONE
            | Self::HOLD_AFTER_RUNNING_CODE
            | Self::HOLD_BEFORE_START_CODE
            | Self::HOLD_RECONCILER_AFTER_COMPLETION_CODE => {}
            code => panic!("invalid worker test injection code: {code}"),
        }
    }

    pub(super) fn after_running(database_oid: u32) {
        match RuntimeStore::new().runtime_test_injection() {
            Self::HOLD_AFTER_RUNNING_CODE => Self::hold(database_oid),
            Self::NONE
            | Self::HOLD_AFTER_EXITING_CODE
            | Self::FAIL_EXIT_CLEANUP_CODE
            | Self::HOLD_BEFORE_START_CODE
            | Self::HOLD_RECONCILER_AFTER_COMPLETION_CODE => {}
            code => panic!("invalid worker test injection code: {code}"),
        }
    }

    pub(super) fn after_reconciler_completion(database_oid: u32) {
        if RuntimeStore::new().runtime_test_injection()
            == Self::HOLD_RECONCILER_AFTER_COMPLETION_CODE
        {
            Self::hold(database_oid);
        }
    }

    pub(crate) fn register_capacity_workers(
        database_oid: u32,
        extension_oid: u32,
        count: usize,
    ) {
        DatabaseLifecycleLock::new(database_oid).acquire_drop();
        let store = RuntimeStore::new();
        for index in 0..count {
            let worker_name = format!("supervisor_capacity_{index}");
            let reservation = store
                .reserve_registration(database_oid, extension_oid, &worker_name)
                .expect("failed to reserve capacity-test worker");
            assert!(store.finish_registration(
                reservation,
                RegistrationCompletion::Commit,
            ));
        }
        store.signal_launcher();
    }

    pub(crate) fn clear_capacity_workers(database_oid: u32) {
        let mut state = RUNTIME_STATE.exclusive();
        for index in 0..state.workers.len() {
            let slot = state.workers[index];
            if slot.database_oid == database_oid
                && slot.registration() == Ok(RegistrationState::Registered)
                && slot.process() == Ok(ProcessState::Stopped)
                && slot.worker_name_str().starts_with("supervisor_capacity_")
            {
                state.clear_worker_slot(index);
            }
        }
    }

    pub(crate) fn request_capacity_worker_cleanup(database_oid: u32) {
        let mut state = RUNTIME_STATE.exclusive();
        for index in 0..state.workers.len() {
            let slot = state.workers[index];
            if slot.database_oid != database_oid
                || !slot.worker_name_str().starts_with("supervisor_capacity_")
            {
                continue;
            }
            if slot.process() == Ok(ProcessState::Stopped) {
                state.clear_worker_slot(index);
            } else {
                let _ = state.workers[index].mark_removing();
            }
        }
        drop(state);
        RuntimeStore::new().signal_launcher();
    }

    fn hold(database_oid: u32) {
        let lock_key = i64::from(database_oid);
        let query = format!("SELECT pg_catalog.pg_advisory_lock({lock_key})");
        let result =
            BackgroundWorker::transaction(AssertUnwindSafe(|| Spi::run(&query)));
        if let Err(error) = result {
            crate::diag::warning(format_args!(
                "worker test hold failed: database_oid={database_oid}, error={error}"
            ));
        }
    }

    fn hold_interruptibly(expected: u8) {
        RuntimeStore::new().mark_test_barrier_reached();
        while RuntimeStore::new().runtime_test_injection() == expected {
            super::bgworker::interruptible_sleep(Duration::from_millis(10));
        }
    }
}
