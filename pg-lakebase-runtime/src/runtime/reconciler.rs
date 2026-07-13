use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

use crate::registry;

use super::WORKER_FUNCTION;
use super::bgworker::{
    DynamicWorkerSpawner, SlotToken, install_terminating_sigterm_handler,
};
use super::store::RuntimeStore;

const EXTENSION_WORKER_NAME: &str = "pg-lakebase-runtime extension worker";

pub(super) struct DatabaseReconciler;

impl DatabaseReconciler {
    pub(super) fn run(arg: pg_sys::Datum) {
        BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
        install_terminating_sigterm_handler();
        let token = SlotToken::from_datum(arg);
        if !token.has_database_index() {
            return;
        }
        unsafe { pg_sys::before_shmem_exit(Some(reconciler_exit_callback), arg) };

        let store = RuntimeStore::new();
        let Some(database_oid) = store.begin_reconciler(token) else {
            return;
        };

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        let registrations = BackgroundWorker::transaction(AssertUnwindSafe(
            registry::load_if_runtime_installed,
        ));
        let needs_retry = match registrations {
            Ok(Some(registrations)) => {
                Self::reconcile_registrations(&store, database_oid, &registrations)
            }
            Ok(None) => {
                store.remove_database_state(token, database_oid);
                store.signal_launcher();
                return;
            }
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to load Lakebase worker registrations for database {database_oid}: {error}"
                ));
                store.remove_database_state(token, database_oid);
                store.signal_launcher();
                return;
            }
        };
        if store.finish_reconciler(token, needs_retry) {
            store.signal_launcher();
        }
    }

    fn reconcile_registrations(
        store: &RuntimeStore,
        database_oid: u32,
        registrations: &[registry::WorkerRegistration],
    ) -> bool {
        let reconciliation =
            store.reconcile_registrations(database_oid, registrations);
        for token in reconciliation.starts {
            if let Err(error) = DynamicWorkerSpawner::start(
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
        if reconciliation.capacity_exhausted {
            store.warn_capacity_exhausted(
                "pg_lakebase.max_worker_registrations is exhausted; worker reconciliation remains pending",
            );
        }
        reconciliation.capacity_exhausted
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn reconciler_exit_callback(_code: i32, arg: pg_sys::Datum) {
    let token = SlotToken::from_datum(arg);
    let store = RuntimeStore::new();
    if store.reconciler_exit(token) {
        store.signal_launcher();
    }
}
