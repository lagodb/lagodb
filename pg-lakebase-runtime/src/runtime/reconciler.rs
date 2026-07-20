use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

use crate::error::LakebaseResult;
use crate::registry;

use super::WORKER_FUNCTION;
use super::bgworker::{
    DynamicWorkerSpawner, ReconcilerToken, install_terminating_sigterm_handler,
};
use super::locks::DatabaseLifecycleLock;
use super::store::RuntimeStore;

const EXTENSION_WORKER_NAME: &str = "pg-lakebase-runtime extension worker";

pub(super) struct DatabaseReconciler;

impl DatabaseReconciler {
    pub(super) fn run(arg: pg_sys::Datum) {
        BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
        install_terminating_sigterm_handler();
        let token = ReconcilerToken::from_datum(arg);
        unsafe { pg_sys::before_shmem_exit(Some(reconciler_exit_callback), arg) };

        let store = RuntimeStore::new();
        let Some(database_oid) = store.begin_reconciler(token) else {
            return;
        };

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        let reconciliation: LakebaseResult<Option<_>> =
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                DatabaseLifecycleLock::new(database_oid).acquire_reconciliation();
                match registry::load_if_runtime_installed()? {
                    Some(registrations) => Ok(Some(
                        store.reconcile_registrations(database_oid, &registrations),
                    )),
                    None => {
                        store.clear_database_workers(database_oid);
                        Ok(None)
                    }
                }
            }));
        let needs_retry = match reconciliation {
            Ok(Some(reconciliation)) => Self::start_workers(&store, reconciliation),
            Ok(None) => false,
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to load Lakebase worker registrations for database {database_oid}: {error}"
                ));
                true
            }
        };
        if store.finish_reconciler(token, needs_retry) {
            store.signal_launcher();
        }
    }

    fn start_workers(
        store: &RuntimeStore,
        reconciliation: super::store::RegistrationReconciliation,
    ) -> bool {
        for token in reconciliation.starts {
            if let Err(error) = DynamicWorkerSpawner::start_worker(
                WORKER_FUNCTION,
                EXTENSION_WORKER_NAME,
                token,
            ) {
                crate::diag::warning(format_args!(
                    "failed to start pg-lakebase-runtime extension worker: {error}"
                ));
                store.worker_start_failed(token);
            }
        }
        if reconciliation.registration_capacity_exhausted {
            store.warn_capacity_exhausted(
                "pg_lakebase.max_worker_registrations is exhausted; worker reconciliation remains pending",
            );
        }
        if reconciliation.worker_capacity_exhausted {
            store.warn_capacity_exhausted(
                "pg_lakebase.max_active_workers is exhausted; worker reconciliation remains pending",
            );
        }
        reconciliation.registration_capacity_exhausted
            || reconciliation.worker_capacity_exhausted
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn reconciler_exit_callback(_code: i32, arg: pg_sys::Datum) {
    let token = ReconcilerToken::from_datum(arg);
    let store = RuntimeStore::new();
    if store.reconciler_exit(token) {
        store.signal_launcher();
    }
}
