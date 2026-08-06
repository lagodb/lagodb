use std::time::Duration;

use pg_lakebase_core::diag::PgReportError;
use pg_lakebase_core::extension_worker::WorkerTransaction;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::diag;
use crate::registry;

use super::bgworker::{DynamicWorkerRegistration, DynamicWorkerStartResult};
use super::lock::DatabaseLifecycleLock;
use super::state::CoordinatorStopDisposition;
use super::store::{Store, WorkerLaunchRegistration};

const MIN_RETRY_SLEEP: Duration = Duration::from_millis(100);

pub(super) struct Coordinator;

impl Coordinator {
    pub(super) fn run(arg: pg_sys::Datum) {
        let database_oid = arg.value() as u32;
        unsafe { pg_sys::before_shmem_exit(Some(coordinator_exit_callback), arg) };
        super::signals::BackgroundWorkerSignals::install_coordinator();

        let store = Store::new();
        if !store.begin_coordinator(database_oid) {
            return;
        }

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        loop {
            super::signals::BackgroundWorkerSignals::process_config_reload();
            if !store.validate_coordinator(database_oid) {
                return;
            }

            let runtime_catalog_exists =
                WorkerTransaction::run(|| -> Result<_, PgReportError> {
                    DatabaseLifecycleLock::new(database_oid).acquire_reconciliation();
                    let exists = registry::runtime_catalog_exists()
                        .map_err(PgReportError::from)?;
                    Ok(exists)
                });
            match runtime_catalog_exists {
                Ok(true) => {}
                Ok(false) => {
                    store.clear_database_workers(database_oid);
                    return;
                }
                Err(error) => error.report(),
            }
            if !store.validate_coordinator(database_oid) {
                return;
            }

            let registrations =
                WorkerTransaction::run(|| -> Result<_, PgReportError> {
                    registry::load_if_runtime_installed().map_err(PgReportError::from)
                });
            match registrations {
                Ok(Some(registrations)) => {
                    if !store.reconcile_registrations(database_oid, &registrations) {
                        return;
                    }
                }
                Ok(None) => {
                    store.clear_database_workers(database_oid);
                    return;
                }
                Err(error) => error.report(),
            }
            if !store.validate_coordinator(database_oid) {
                return;
            }
            while let Some(true) = Self::start_ready_worker(&store, database_oid) {}
            let Some(retry_delay) =
                store.next_database_worker_start_delay(database_oid)
            else {
                return;
            };
            if !super::signals::BackgroundWorkerSignals::wait_latch(
                retry_delay.max(MIN_RETRY_SLEEP),
            ) {
                return;
            }
        }
    }

    fn start_ready_worker(store: &Store, database_oid: u32) -> Option<bool> {
        let mut started = None;
        DynamicWorkerRegistration::with_transient_context(|| {
            started = store.register_ready_worker(database_oid).map(
                |registration| match registration {
                    WorkerLaunchRegistration::Registered {
                        launch,
                        registration,
                    } => match registration.wait_for_startup() {
                        DynamicWorkerStartResult::Started(_) => {
                            diag::info(format_args!(
                                "registered Lakebase extension worker: database_oid={}, extension_oid={}, worker_name={}",
                                launch.identity.database_oid,
                                launch.identity.extension_oid,
                                launch.identity.worker_name(),
                            ));
                            true
                        }
                        DynamicWorkerStartResult::Stopped
                        | DynamicWorkerStartResult::PostmasterDied => {
                            store.worker_registration_failed(launch.key);
                            false
                        }
                    },
                    WorkerLaunchRegistration::Failed { launch, error } => {
                        diag::warning(format_args!(
                            "failed to register pg-lakebase extension worker: database_oid={}, extension_oid={}, worker_name={}, error={error}",
                            launch.identity.database_oid,
                            launch.identity.extension_oid,
                            launch.identity.worker_name(),
                        ));
                        false
                    }
                },
            );
        });
        started
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn coordinator_exit_callback(code: i32, arg: pg_sys::Datum) {
    let database_oid = arg.value() as u32;
    let store = Store::new();
    if store.coordinator_exit(database_oid, code)
        == Some(CoordinatorStopDisposition::HandoffNow)
    {
        store.signal_supervisor();
    }
}
