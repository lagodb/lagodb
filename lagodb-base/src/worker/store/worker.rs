use std::collections::HashSet;
use std::time::Duration;

use lagodb_core::extension_worker::WorkerSchedule;
use pgrx::prelude::*;

use crate::diag;
use crate::registry::WorkerRegistration;
use crate::worker::CAPACITY_RETRY;
use crate::worker::bgworker::{DynamicWorkerRegistration, timestamp_ms};
use crate::worker::state::{
    CoordinatorStopDisposition, ProcessState, RegistrationState, RestartPolicy,
    WorkerKey, WorkerStopDisposition,
};

use super::{
    COORDINATOR_TABLE, SHARED_STATE, StoppedWorkerProcess, Store, WORKER_TABLE,
    WorkerLaunch, WorkerLaunchRegistration, WorkerStart,
};

#[derive(Clone, Copy)]
enum ProcessKey {
    Coordinator(u32),
    Worker(WorkerKey),
}

impl Store {
    pub(in crate::worker) fn next_database_worker_start_delay(
        &self,
        database_oid: u32,
    ) -> Option<Duration> {
        let now = timestamp_ms();
        let _state = SHARED_STATE.share();
        let mut minimum = None;
        WORKER_TABLE.for_each(|slot| {
            if slot.database_oid != database_oid {
                return;
            }
            let Some(delay) = slot.restart_delay(now) else {
                return;
            };
            minimum =
                Some(minimum.map_or(delay, |current: Duration| current.min(delay)));
        });
        minimum
    }

    pub(in crate::worker) fn requested_databases(&self) -> Vec<u32> {
        let _state = SHARED_STATE.share();
        let mut requested = Vec::new();
        COORDINATOR_TABLE.for_each(|slot| {
            if slot.needs_restart() && slot.process() == ProcessState::Stopped {
                requested.push(slot.database_oid);
            }
        });
        requested
    }

    pub(in crate::worker) fn all_worker_databases(&self) -> Vec<u32> {
        let _state = SHARED_STATE.share();
        let mut seen = HashSet::new();
        WORKER_TABLE.for_each(|slot| {
            if matches!(
                slot.registration(),
                RegistrationState::Registered | RegistrationState::Removing
            ) {
                seen.insert(slot.database_oid);
            }
        });
        seen.into_iter().collect()
    }

    pub(in crate::worker) fn coordinator_exit(
        &self,
        database_oid: u32,
        code: i32,
    ) -> Option<CoordinatorStopDisposition> {
        match self
            .confirm_process_stopped(ProcessKey::Coordinator(database_oid), code)
        {
            StoppedWorkerProcess::Coordinator(disposition) => Some(disposition),
            StoppedWorkerProcess::Worker { .. } | StoppedWorkerProcess::Stale => None,
        }
    }

    /// Applies an owned catalog snapshot only while the current coordinator
    /// still has reconciliation authority.
    ///
    /// The authorization check and all worker-slot changes share one
    /// `SHARED_STATE` critical section, so a DROP stop request either precedes
    /// and rejects this snapshot or follows and remains authoritative.
    pub(in crate::worker) fn reconcile_registrations(
        &self,
        database_oid: u32,
        registrations: &[WorkerRegistration],
    ) -> bool {
        let live: HashSet<i32> = registrations
            .iter()
            .map(|registration| registration.worker_id)
            .collect();
        let _state = SHARED_STATE.exclusive();
        let coordinator_can_reconcile = COORDINATOR_TABLE
            .find(database_oid)
            .is_some_and(|slot| slot.has_reconciliation_authority());
        if !coordinator_can_reconcile {
            return false;
        }
        for registration in registrations {
            let key = WorkerKey::new(database_oid, registration.worker_id);
            if WORKER_TABLE
                .with_mut(key, |slot| slot.reconcile_present())
                .is_some()
            {
                continue;
            }
            let mut slot = WORKER_TABLE.get_or_insert(key);
            slot.initialize_registration(
                registration.registration_owner_oid.to_u32(),
                &registration.worker_name,
            );
            slot.request_wakeup();
            assert!(WORKER_TABLE.replace(slot));
        }

        let mut remove = Vec::new();
        WORKER_TABLE.for_each_mut(|slot| {
            if slot.database_oid == database_oid && !live.contains(&slot.worker_id) {
                if slot.has_active_process() {
                    slot.mark_removing();
                } else {
                    remove.push(slot.key());
                }
            }
        });
        for key in remove {
            WORKER_TABLE.remove(key);
        }
        true
    }

    pub(in crate::worker) fn clear_database_workers(&self, database_oid: u32) {
        let _state = SHARED_STATE.exclusive();
        let mut remove = Vec::new();
        WORKER_TABLE.for_each_mut(|slot| {
            if slot.database_oid == database_oid {
                if slot.has_active_process() {
                    slot.mark_removing();
                } else {
                    remove.push(slot.key());
                }
            }
        });
        for key in remove {
            WORKER_TABLE.remove(key);
        }
    }

    pub(in crate::worker) fn register_ready_worker(
        &self,
        database_oid: u32,
    ) -> Option<WorkerLaunchRegistration> {
        let state = SHARED_STATE.exclusive();
        let coordinator_can_launch = COORDINATOR_TABLE
            .find(database_oid)
            .is_some_and(|slot| slot.has_reconciliation_authority());
        if !coordinator_can_launch {
            return None;
        }
        let now = timestamp_ms();
        let mut launch = None;
        WORKER_TABLE.for_each(|slot| {
            if launch.is_some()
                || slot.database_oid != database_oid
                || slot.restart_delay(now) != Some(Duration::ZERO)
            {
                return;
            }
            launch = Some(WorkerLaunch {
                key: slot.key(),
                identity: slot.identity(),
            });
        });
        let launch = launch?;
        let registration = match DynamicWorkerRegistration::register_worker(
            launch.key,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                let delayed = WORKER_TABLE
                    .with_mut(launch.key, |slot| {
                        slot.registration_failed(timestamp_ms(), CAPACITY_RETRY)
                    })
                    .expect(
                        "eligible worker disappeared while the shared-state lock was held",
                    );
                assert!(
                    delayed,
                    "eligible worker rejected registration failure while the shared-state lock was held",
                );
                return Some(WorkerLaunchRegistration::Failed { launch, error });
            }
        };
        WORKER_TABLE
            .with_mut(launch.key, |slot| slot.prepare_start())
            .expect(
                "eligible worker disappeared while the shared-state lock was held",
            );
        drop(state);
        Some(WorkerLaunchRegistration::Registered {
            launch,
            registration,
        })
    }

    pub(in crate::worker) fn worker_registration_failed(&self, key: WorkerKey) {
        let _state = SHARED_STATE.exclusive();
        WORKER_TABLE.with_mut(key, |slot| {
            slot.registration_failed(timestamp_ms(), CAPACITY_RETRY)
        });
    }

    pub(in crate::worker) fn begin_worker(
        &self,
        key: WorkerKey,
    ) -> Option<WorkerStart> {
        let state = SHARED_STATE.exclusive();
        let (published, identity) = WORKER_TABLE.with_mut(key, |slot| {
            (
                slot.mark_running(unsafe { pg_sys::MyProcPid }, timestamp_ms()),
                slot.identity(),
            )
        })?;
        drop(state);
        published.then(|| WorkerStart {
            database_oid: identity.database_oid,
            worker_id: identity.worker_id,
            extension_oid: identity.extension_oid,
            worker_name: identity.worker_name().to_owned(),
        })
    }

    pub(in crate::worker) fn worker_registration_missing(&self, key: WorkerKey) {
        let _state = SHARED_STATE.exclusive();
        WORKER_TABLE.with_mut(key, |slot| {
            if slot.has_active_process() {
                slot.mark_removing();
            }
        });
    }

    pub(in crate::worker) fn validate_worker(&self, key: WorkerKey) -> bool {
        let _state = SHARED_STATE.share();
        WORKER_TABLE.find(key).is_some_and(|slot| {
            slot.has_active_process() && !slot.is_stop_requested()
        })
    }

    pub(in crate::worker) fn worker_exit(&self, key: WorkerKey, code: i32) -> bool {
        matches!(
            self.confirm_process_stopped(ProcessKey::Worker(key), code),
            StoppedWorkerProcess::Worker { reconcile: true }
        ) && self.request_database_reconcile(key.database_oid)
    }

    pub(in crate::worker) fn complete_worker(
        &self,
        key: WorkerKey,
        schedule: WorkerSchedule,
    ) -> bool {
        let mut state = SHARED_STATE.exclusive();
        let Some(identity) = WORKER_TABLE.with_mut(key, |slot| {
            let identity = slot.identity();
            slot.complete_run(schedule, timestamp_ms());
            identity
        }) else {
            return false;
        };
        let needs_supervisor_wake = match schedule {
            WorkerSchedule::Idle => false,
            WorkerSchedule::RunImmediately | WorkerSchedule::RunAfter(_) => {
                Self::request_coordination_locked(&mut state, key.database_oid)
            }
        };
        drop(state);
        diag::info(format_args!(
            "LagoDB worker published completion schedule: database_oid={}, extension_oid={}, worker_name={}, schedule={schedule:?}",
            identity.database_oid,
            identity.extension_oid,
            identity.worker_name(),
        ));
        needs_supervisor_wake
    }

    fn confirm_process_stopped(
        &self,
        key: ProcessKey,
        exit_code: i32,
    ) -> StoppedWorkerProcess {
        let _state = SHARED_STATE.exclusive();
        match key {
            ProcessKey::Coordinator(database_oid) => {
                let Some(completion) = COORDINATOR_TABLE
                    .with_mut(database_oid, |slot| slot.confirm_stopped(exit_code))
                    .flatten()
                else {
                    return StoppedWorkerProcess::Stale;
                };
                StoppedWorkerProcess::Coordinator(completion)
            }
            ProcessKey::Worker(key) => {
                let restart_policy = RestartPolicy::new(
                    crate::gucs::worker_restart_backoff_initial(),
                    crate::gucs::worker_restart_backoff_max(),
                    crate::gucs::worker_restart_healthy(),
                );
                let Some(result) = WORKER_TABLE.with_mut(key, |slot| {
                    let disposition = slot.confirm_stopped(
                        timestamp_ms(),
                        &restart_policy,
                        exit_code,
                    );
                    (disposition, slot.registration())
                }) else {
                    return StoppedWorkerProcess::Stale;
                };
                match result {
                    (Some(disposition), RegistrationState::Removing) => {
                        WORKER_TABLE.remove(key);
                        StoppedWorkerProcess::Worker {
                            reconcile: disposition
                                == WorkerStopDisposition::Reconcile,
                        }
                    }
                    (Some(disposition), _) => StoppedWorkerProcess::Worker {
                        reconcile: disposition == WorkerStopDisposition::Reconcile,
                    },
                    (None, _) => StoppedWorkerProcess::Stale,
                }
            }
        }
    }
}
