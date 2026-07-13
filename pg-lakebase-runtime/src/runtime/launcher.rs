use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

use crate::gucs;

use super::RECONCILER_FUNCTION;
use super::bgworker::DynamicWorkerSpawner;
use super::store::RuntimeStore;

const RECONCILER_WORKER_NAME: &str = "pg-lakebase-runtime database reconciler";

pub(super) struct Launcher;

impl Launcher {
    pub(super) fn run() {
        BackgroundWorker::attach_signal_handlers(
            SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
        );
        BackgroundWorker::connect_worker_to_spi(None, None);
        unsafe {
            pg_sys::before_shmem_exit(
                Some(launcher_exit_callback),
                pg_sys::Datum::from(0_usize),
            );
        }

        let store = RuntimeStore::new();
        store.set_launcher_running();

        let mut last_full_scan = Instant::now()
            .checked_sub(gucs::reconcile_interval())
            .unwrap_or_else(Instant::now);
        let mut known_databases = HashSet::new();
        while BackgroundWorker::worker_continue() {
            if BackgroundWorker::sighup_received() {
                unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
            }

            let full_scan = store.take_full_scan_request(
                last_full_scan.elapsed() >= gucs::reconcile_interval(),
            );
            if full_scan {
                Self::reconcile_database_list(&store, &mut known_databases);
                store.mark_all_worker_databases_dirty();
                last_full_scan = Instant::now();
            }
            store.schedule_due_workers();
            Self::start_dirty_reconcilers(&store);

            if !BackgroundWorker::wait_latch(Some(gucs::launcher_naptime())) {
                break;
            }
        }
    }

    fn reconcile_database_list(
        store: &RuntimeStore,
        known_databases: &mut HashSet<u32>,
    ) {
        let databases = match BackgroundWorker::transaction(AssertUnwindSafe(
            Self::scan_databases,
        )) {
            Ok(databases) => databases,
            Err(error) => {
                crate::diag::warning(format_args!(
                    "failed to scan pg_database for Lakebase workers: {error}"
                ));
                store.request_full_rescan();
                return;
            }
        };
        let capacity_exhausted =
            store.apply_database_scan(known_databases, databases);
        if capacity_exhausted {
            store.warn_capacity_exhausted(
                "Lakebase database runtime slots are exhausted; discovery remains pending",
            );
        }
    }

    fn scan_databases() -> Result<Vec<u32>, pg_lakebase_core::diag::PgError> {
        use pg_lakebase_core::catalog::{CatalogRelation, CatalogSnapshot};

        let relation = CatalogRelation::open(
            pg_sys::DatabaseRelationId,
            pg_sys::AccessShareLock as _,
        )?;
        let mut scan = relation.begin_scan(
            pg_sys::InvalidOid,
            false,
            CatalogSnapshot::Default,
            std::iter::empty(),
        )?;
        let mut databases = Vec::new();
        while let Some(tuple) = scan.get_next()? {
            // SAFETY: the catalog scan tuple is valid until the next scan call;
            // FormData_pg_database fixed fields are available through GETSTRUCT.
            let database = unsafe {
                &*(pg_sys::GETSTRUCT(tuple.as_raw()) as pg_sys::Form_pg_database)
            };
            if !database.datistemplate && database.datallowconn {
                databases.push(database.oid.to_u32());
            }
        }
        Ok(databases)
    }

    fn start_dirty_reconcilers(store: &RuntimeStore) {
        loop {
            let Some(token) = store.next_dirty_reconciler() else {
                return;
            };
            if let Err(error) = DynamicWorkerSpawner::start(
                RECONCILER_FUNCTION,
                RECONCILER_WORKER_NAME,
                token,
            ) {
                crate::diag::warning(format_args!(
                    "failed to start pg-lakebase-runtime database reconciler: {error}"
                ));
                store.reconciler_start_failed(token);
                return;
            }
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn launcher_exit_callback(_code: i32, _arg: pg_sys::Datum) {
    RuntimeStore::new().clear_launcher();
}
