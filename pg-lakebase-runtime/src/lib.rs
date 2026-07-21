use pg_lakebase_core::maintenance::{MaintenanceItemId, MaintenanceQueue};
use pgrx::PgRelation;
use pgrx::datum::{Internal, Uuid};
use pgrx::prelude::*;
use runtime::RUNTIME_STATE;
use storage::STORAGE_STATE;

mod diag;
mod error;
mod gucs;
mod hooks;
mod lifecycle;
mod object_access;
mod process_utility;
mod registry;
mod runtime;
mod runtime_api;
mod state;
mod storage;

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    storage::init();
    runtime_api::init();

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
    #[allow(clippy::type_complexity)]
    fn table_maintenance_stats(
        relation: PgRelation,
    ) -> TableIterator<
        'static,
        (
            name!(provider, String),
            name!(format, Option<String>),
            name!(history_points, i64),
            name!(current_content_objects, i64),
            name!(current_content_bytes, i64),
            name!(retained_content_objects, i64),
            name!(retained_content_bytes, i64),
            name!(current_data_objects, i64),
            name!(current_data_bytes, i64),
            name!(retained_data_objects, i64),
            name!(retained_data_bytes, i64),
        ),
    > {
        use pg_lakebase_core::diag::{PgReportError, ReportableError};
        let relation = pg_lakebase_core::handles::RelationGuard::open(
            relation.oid(),
            pg_sys::AccessShareLock as _,
        )
        .map_err(PgReportError::from_domain_error)
        .report_unwrap();
        let stats =
            pg_lakebase_core::table_maintenance::TableMaintenanceRouter::inspect(
                &relation.as_handle(),
            )
            .map_err(PgReportError::from_domain_error)
            .report_unwrap();
        let sql_i64 = |value: u64, metric: &'static str| {
            i64::try_from(value).unwrap_or_else(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                    format!("{metric} exceeds PostgreSQL bigint"),
                )
                .report()
            })
        };
        TableIterator::new(std::iter::once((
            stats.provider,
            stats.format,
            sql_i64(stats.history_points, "history-point count"),
            sql_i64(
                stats.current_content_objects,
                "current content object count",
            ),
            sql_i64(stats.current_content_bytes, "current content byte count"),
            sql_i64(
                stats.retained_content_objects,
                "retained content object count",
            ),
            sql_i64(stats.retained_content_bytes, "retained content byte count"),
            sql_i64(stats.current_data_objects, "current data object count"),
            sql_i64(stats.current_data_bytes, "current data byte count"),
            sql_i64(stats.retained_data_objects, "retained data object count"),
            sql_i64(stats.retained_data_bytes, "retained data byte count"),
        )))
    }

    #[pg_extern]
    fn observe_object_tree(
        store_id: &str,
        namespace: &str,
        prefix: &str,
    ) -> TableIterator<'static, (name!(objects, i64), name!(bytes, i64))> {
        use pg_lakebase_core::diag::PgReportError;
        let stats = pg_lakebase_core::maintenance::ObjectTreeObserver::connect(
            std::time::Duration::from_secs(5),
        )
        .and_then(|observer| observer.observe(store_id, namespace, prefix))
        .unwrap_or_else(|error| {
            PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
                format!("failed to observe object tree: {error}"),
            )
            .report()
        });
        let sql_i64 = |value: u64, metric: &'static str| {
            i64::try_from(value).unwrap_or_else(|_| {
                PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
                    format!("object-tree {metric} exceeds PostgreSQL bigint"),
                )
                .report()
            })
        };
        TableIterator::new(std::iter::once((
            sql_i64(stats.objects, "object count"),
            sql_i64(stats.bytes, "byte count"),
        )))
    }

    #[pg_extern]
    fn register_worker_impl(worker_name: &str, entrypoint: pg_sys::Oid) {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        runtime::DatabaseLifecycleLock::new(database_oid).acquire_shared();
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
        runtime::DatabaseLifecycleLock::new(database_oid).acquire_shared();
        lifecycle::request_database_reconcile();
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
            name!(registration_state, &'static str),
            name!(dispatch_state, &'static str),
            name!(process_state, &'static str),
            name!(pid, Option<i32>),
            name!(generation, i64),
            name!(not_before_ms, Option<i64>),
            name!(stop_requested, bool),
            name!(launcher_epoch, i64),
            name!(recovery_state, &'static str),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(runtime::worker_status().into_iter().map(|status| {
            (
                pg_sys::Oid::from(status.database_oid),
                pg_sys::Oid::from(status.extension_oid),
                status.worker_name,
                status.registration_state,
                status.dispatch_state,
                status.process_state,
                status.pid,
                i64::from(status.generation),
                status.not_before_ms,
                status.stop_requested,
                i64::try_from(status.launcher_epoch).unwrap_or(i64::MAX),
                status.recovery_state,
            )
        }))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx names each returned SQL column in the Rust tuple.
    fn process_runtime_status() -> TableIterator<
        'static,
        (
            name!(process_kind, &'static str),
            name!(database_oid, Option<pg_sys::Oid>),
            name!(state, &'static str),
            name!(pid, Option<i32>),
            name!(recovery_backend_count, Option<i64>),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(runtime::process_status().into_iter().map(|status| {
            (
                status.process_kind,
                status.database_oid.map(pg_sys::Oid::from),
                status.state,
                status.pid,
                status.recovery_backend_count.map(i64::from),
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
        let worker_context = unsafe {
            pg_lakebase_core::extension_worker::WorkerContext::from_internal(
                &worker_context,
            )
        };
        let worker_context = worker_context
            .map_err(|source| error::LakebaseError::WorkerContext { source })
            .unwrap_or_else(|error| error.report());
        pg_lakebase_core::maintenance::run_database_worker(&worker_context).encode()
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

pgrx::extension_sql!(
    "REVOKE ALL ON FUNCTION lakebase.observe_object_tree(text, text, text) FROM PUBLIC;",
    name = "lock_down_observe_object_tree",
    requires = [lakebase::observe_object_tree],
);

// `#[pg_test]` host wrappers call `crate::pg_test::{setup,
// postgresql_conf_options}` under `cfg(test)`. The module gates its actual
// PostgreSQL-backed tests independently on the `pg_test` feature.
#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test;
