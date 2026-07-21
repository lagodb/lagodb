use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;

use crate::error::LakebaseResult;
use crate::registry;

use super::bgworker::ReconcilerToken;
use super::locks::DatabaseLifecycleLock;
use super::store::RuntimeStore;

pub(super) struct DatabaseReconciler;

impl DatabaseReconciler {
    pub(super) fn run(arg: pg_sys::Datum) {
        let token = ReconcilerToken::from_datum(arg);
        unsafe { pg_sys::before_shmem_exit(Some(reconciler_exit_callback), arg) };
        super::signals::BackgroundWorkerSignals::install_dynamic_worker();

        let store = RuntimeStore::new();
        let Some(database_oid) = store.begin_reconciler(token) else {
            return;
        };

        BackgroundWorker::connect_worker_to_spi_by_oid(
            Some(pg_sys::Oid::from(database_oid)),
            None,
        );
        if !store.validate_reconciler_token(token) {
            return;
        }
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
            Ok(Some(reconciliation)) => {
                Self::finish_reconciliation(&store, reconciliation)
            }
            Ok(None) => false,
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to load Lakebase worker registrations for database {database_oid}: {error}"
                ));
                true
            }
        };
        let published = store.finish_reconciler(token, needs_retry);
        if published {
            store.signal_launcher();
        }
        #[cfg(feature = "pg_test")]
        if published {
            super::test_support::RuntimeTestInjection::after_reconciler_completion(
                database_oid,
            );
        }
    }

    fn finish_reconciliation(
        store: &RuntimeStore,
        reconciliation: super::store::RegistrationReconciliation,
    ) -> bool {
        if reconciliation.registration_capacity_exhausted {
            store.warn_capacity_exhausted(
                "pg_lakebase.max_worker_registrations is exhausted; worker reconciliation remains pending",
            );
        }
        reconciliation.registration_capacity_exhausted
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn reconciler_exit_callback(code: i32, arg: pg_sys::Datum) {
    let token = ReconcilerToken::from_datum(arg);
    let store = RuntimeStore::new();
    if store.reconciler_exit(token, code) {
        store.signal_launcher();
    }
}
