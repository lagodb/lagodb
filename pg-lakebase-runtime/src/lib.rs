use pg_lakebase_core::maintenance::{MaintenanceItemId, MaintenanceQueue};
use pgrx::datum::{Internal, Uuid};
use pgrx::prelude::*;
use runtime::RUNTIME_STATE;
use storage::STORAGE_STATE;

mod diag;
mod error;
mod gucs;
mod hooks;
mod lifecycle;
mod registry;
mod runtime;
mod state;
mod storage;

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    pg_lakebase_core::maintenance::init_gucs();
    storage::init();

    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        pgrx::pg_shmem_init!(RUNTIME_STATE);
        pgrx::pg_shmem_init!(STORAGE_STATE);
        lifecycle::init();
        hooks::init();
        runtime::init();
    }
}

fn ensure_runtime_preloaded() {
    runtime::ensure_preloaded().unwrap_or_else(|error| error.report());
}

#[pg_schema]
mod lakebase {
    use super::*;

    #[pg_extern]
    fn register_worker_impl(worker_name: &str, entrypoint: pg_sys::Oid) {
        ensure_runtime_preloaded();
        let worker = registry::register(worker_name, entrypoint)
            .unwrap_or_else(|error| error.report());
        lifecycle::reserve_registration(
            worker.extension_oid.to_u32(),
            &worker.worker_name,
        )
        .unwrap_or_else(|error| error.report());
    }

    #[pg_extern]
    fn deregister_worker(worker_name: &str) {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        lifecycle::request_database_reconcile();
        runtime::pause_database_reconciliation(database_oid)
            .unwrap_or_else(|error| error.report());
        let extension_oid = unsafe { pg_sys::CurrentExtensionObject }.to_u32();
        if registry::deregister(worker_name).unwrap_or_else(|error| error.report()) {
            runtime::stop_worker(database_oid, extension_oid, worker_name)
                .unwrap_or_else(|error| error.report());
        }
    }

    #[pg_extern]
    fn request_worker_wakeup(extension_name: &str, worker_name: &str) {
        ensure_runtime_preloaded();
        let extension_oid =
            registry::registration_extension_oid(extension_name, worker_name)
                .unwrap_or_else(|error| error.report())
                .unwrap_or_else(|| {
                    error::LakebaseError::WorkerNotRegistered {
                        extension_name: extension_name.to_owned(),
                        worker_name: worker_name.to_owned(),
                    }
                    .report()
                });
        lifecycle::request_wakeup(extension_oid.to_u32(), worker_name);
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx names each returned SQL column in the Rust tuple.
    fn worker_runtime_status() -> TableIterator<
        'static,
        (
            name!(database_oid, pg_sys::Oid),
            name!(extension_oid, pg_sys::Oid),
            name!(worker_name, String),
            name!(state, &'static str),
            name!(pid, Option<i32>),
            name!(restart_at_ms, Option<i64>),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(runtime::worker_status().into_iter().map(|status| {
            (
                pg_sys::Oid::from(status.database_oid),
                pg_sys::Oid::from(status.extension_oid),
                status.worker_name,
                status.state,
                status.pid,
                status.restart_at_ms,
            )
        }))
    }

    #[pg_extern]
    fn process_runtime_status() -> TableIterator<
        'static,
        (
            name!(process_kind, &'static str),
            name!(database_oid, Option<pg_sys::Oid>),
            name!(state, &'static str),
            name!(pid, Option<i32>),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(runtime::process_status().into_iter().map(|status| {
            (
                status.process_kind,
                status.database_oid.map(pg_sys::Oid::from),
                status.state,
                status.pid,
            )
        }))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx names each returned SQL column in the Rust tuple.
    fn storage_runtime_status() -> TableIterator<
        'static,
        (
            name!(enabled, bool),
            name!(pid, Option<i32>),
            name!(state, &'static str),
            name!(socket_path, String),
            name!(cache_dir, String),
            name!(last_start_ms, Option<i64>),
            name!(last_stop_ms, Option<i64>),
            name!(last_reconcile_at_ms, Option<i64>),
            name!(last_reconcile_added, i64),
            name!(last_reconcile_removed, i64),
            name!(last_reconcile_replaced, i64),
            name!(last_reconcile_unchanged, i64),
            name!(last_error_at_ms, Option<i64>),
            name!(last_error, Option<String>),
        ),
    > {
        ensure_runtime_preloaded();
        let status = storage::runtime_status();
        TableIterator::new(std::iter::once((
            status.enabled,
            status.pid,
            status.state,
            status.socket_path,
            status.cache_dir,
            status.last_start_ms,
            status.last_stop_ms,
            status.last_reconcile_at_ms,
            status.last_reconcile_added,
            status.last_reconcile_removed,
            status.last_reconcile_replaced,
            status.last_reconcile_unchanged,
            status.last_error_at_ms,
            status.last_error,
        )))
    }

    #[pg_extern]
    fn maintenance_worker(worker_context: Internal) -> i64 {
        // SAFETY: this SQL-inaccessible `internal` argument is supplied only
        // by pg_lakebase_runtime_extension_worker_main.
        unsafe {
            pg_lakebase_core::extension_worker::WorkerContext::from_internal(
                &worker_context,
            )
        }
        .map_err(|source| error::LakebaseError::WorkerContext { source })
        .unwrap_or_else(|error| error.report());
        pg_lakebase_core::maintenance::run_database_worker().encode()
    }

    #[pg_extern]
    fn retry_maintenance_item(target_item_id: Uuid) -> bool {
        ensure_runtime_preloaded();
        MaintenanceQueue::retry_failed(MaintenanceItemId::from_pg_uuid(
            target_item_id,
        ))
        .map_err(|source| error::LakebaseError::RetryMaintenanceItem { source })
        .unwrap_or_else(|error| error.report())
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec!["shared_preload_libraries = 'pg_lakebase_runtime'"]
    }
}
