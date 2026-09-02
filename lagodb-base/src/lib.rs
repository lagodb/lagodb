use std::ffi::CStr;

use lagodb_core::object_cleanup::{
    ObjectCleanupItemId, ObjectCleanupQueue, ObjectTreeObserver,
    run_object_cleanup_worker,
};
use lagodb_core::table_maintenance::TableMaintenanceRouter;
use pgrx::datum::{Internal, Uuid};
use pgrx::prelude::*;
use pgrx::{PgRelation, pg_getarg, pg_getarg_datum_raw};

mod catalog;
mod descriptor_directory;
mod error;
mod gucs;
mod hooks;
mod lifecycle;
mod object_access;
mod planning_hooks;
mod process_utility;
mod provider_bootstrap;
mod query_host;
mod registry;
mod runtime_api;
mod storage;
mod utility_consumer;
mod worker;

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    storage::init();
    provider_bootstrap::init();
    runtime_api::init();

    if unsafe {
        pg_sys::process_shared_preload_libraries_in_progress
            && !pg_sys::IsBinaryUpgrade
    } {
        worker::init_shared_memory();
        storage::init_shared_memory();
        lifecycle::init();
        hooks::init();
        planning_hooks::init();
        query_host::init();
        worker::init();
        provider_bootstrap::load_configured();
    }
}

fn ensure_runtime_preloaded() {
    worker::ensure_preloaded().unwrap_or_else(|error| error.report());
}

#[pg_schema]
mod lagodb {
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
        use lagodb_core::diag::{PgReportError, ReportableError};
        let relation = lagodb_core::handles::RelationGuard::open(
            relation.oid(),
            pg_sys::AccessShareLock as _,
        )
        .map_err(PgReportError::from_domain_error)
        .report_unwrap();
        let stats = TableMaintenanceRouter::inspect(&relation.as_handle())
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
        volume_id: i64,
        namespace: &str,
        prefix: &str,
    ) -> TableIterator<'static, (name!(objects, i64), name!(bytes, i64))> {
        use lagodb_core::diag::PgReportError;
        let volume_id =
            lagodb_core::storage::volume::StorageVolumeId::try_from(volume_id)
                .unwrap_or_else(|_| pgrx::error!("invalid storage volume id"));
        let stats = ObjectTreeObserver::connect(std::time::Duration::from_secs(5))
            .and_then(|observer| observer.observe(volume_id, namespace, prefix))
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
    fn register_worker_impl(worker_name: &str, entrypoint: pg_sys::Oid) -> i32 {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        let worker_id = registry::register(worker_name, entrypoint)
            .unwrap_or_else(|error| error.report());
        if !registry::database_is_template(database_oid) {
            worker::DatabaseLifecycleLock::new(database_oid).acquire_shared();
            lifecycle::request_database_reconcile();
        }
        worker_id
    }

    #[pg_extern]
    fn deregister_worker(worker_name: &str, missing_ok: default!(bool, "false")) {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        let worker_id =
            registry::deregister(worker_name).unwrap_or_else(|error| error.report());
        if worker_id.is_none() && !missing_ok {
            error::LagodbError::WorkerNameNotRegistered {
                worker_name: worker_name.to_owned(),
            }
            .report();
        }
        if !registry::database_is_template(database_oid) {
            worker::DatabaseLifecycleLock::new(database_oid).acquire_shared();
            lifecycle::request_database_reconcile();
            if let Some(worker_id) = worker_id {
                worker::stop_worker(database_oid, worker_id);
            }
        }
    }

    #[pg_extern]
    fn deregister_worker_by_id(worker_id: i32, missing_ok: default!(bool, "false")) {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        let deregistered = registry::deregister_by_id(worker_id)
            .unwrap_or_else(|error| error.report());
        if !deregistered && !missing_ok {
            error::LagodbError::WorkerIdNotRegistered { worker_id }.report();
        }
        if !registry::database_is_template(database_oid) {
            worker::DatabaseLifecycleLock::new(database_oid).acquire_shared();
            lifecycle::request_database_reconcile();
            if deregistered {
                worker::stop_worker(database_oid, worker_id);
            }
        }
    }

    #[pg_extern(sql = r#"
CREATE FUNCTION lagodb.request_worker_wakeup(
    extension_name pg_catalog.text,
    worker_name pg_catalog.text
)
RETURNS void
STRICT
VOLATILE
PARALLEL UNSAFE
LANGUAGE c
AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
"#)]
    fn request_worker_wakeup(fcinfo: pg_sys::FunctionCallInfo) {
        ensure_runtime_preloaded();
        // SAFETY: the custom SQL declaration makes the second argument a
        // non-null PostgreSQL text value. Worker names are the UTF-8
        // application-name domain, so pgrx's &str conversion is intentional.
        let worker_name = unsafe { pg_getarg::<&str>(fcinfo, 1) }
            .expect("STRICT request_worker_wakeup receives a non-null worker name");
        // SAFETY: the custom SQL declaration makes the first argument a
        // non-null PostgreSQL text Datum. text_to_cstring follows PostgreSQL's
        // native text-to-name lookup pattern and preserves server-encoding
        // bytes while producing a palloc'd, NUL-terminated copy.
        let extension_name_ptr = unsafe {
            pg_sys::text_to_cstring(
                pg_getarg_datum_raw(fcinfo, 0).cast_mut_ptr::<pg_sys::text>(),
            )
        };
        // SAFETY: text_to_cstring returned a live, NUL-terminated allocation.
        let extension_name = unsafe { CStr::from_ptr(extension_name_ptr) };
        let worker_id = match registry::resolve_worker_id(extension_name, worker_name)
        {
            Ok(Some(worker_id)) => worker_id,
            Ok(None) => {
                let extension_name = extension_name.to_owned();
                error::LagodbError::WorkerNotRegistered {
                    extension_name,
                    worker_name: worker_name.to_owned(),
                }
                .report()
            }
            Err(error) => error.report(),
        };
        lifecycle::request_wakeup(worker_id);
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx requires named SQL columns in this tuple.
    fn worker_status() -> TableIterator<
        'static,
        (
            name!(database_oid, pg_sys::Oid),
            name!(worker_id, i32),
            name!(extension_oid, pg_sys::Oid),
            name!(worker_name, String),
            name!(registration_state, &'static str),
            name!(process_state, &'static str),
            name!(pid, Option<i32>),
            name!(needs_restart, bool),
            name!(restart_after_ms, Option<i64>),
            name!(failure_count, i32),
            name!(stop_requested, bool),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(worker::worker_status().into_iter().map(|status| {
            (
                pg_sys::Oid::from(status.database_oid),
                status.worker_id,
                pg_sys::Oid::from(status.extension_oid),
                status.worker_name,
                status.registration_state,
                status.process_state,
                status.pid,
                status.needs_restart,
                status.restart_after_ms,
                status.failure_count,
                status.stop_requested,
            )
        }))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx requires named SQL columns in this tuple.
    fn process_status() -> TableIterator<
        'static,
        (
            name!(process_kind, &'static str),
            name!(database_oid, Option<pg_sys::Oid>),
            name!(state, &'static str),
            name!(pid, Option<i32>),
            name!(needs_restart, Option<bool>),
        ),
    > {
        ensure_runtime_preloaded();
        TableIterator::new(worker::process_status().into_iter().map(|status| {
            (
                status.process_kind,
                status.database_oid.map(pg_sys::Oid::from),
                status.state,
                status.pid,
                status.needs_restart,
            )
        }))
    }

    #[pg_extern]
    #[allow(clippy::type_complexity)] // pgrx requires named SQL columns in this tuple.
    fn storage_service_status() -> TableIterator<
        'static,
        (
            name!(enabled, bool),
            name!(pid, Option<i32>),
            name!(state, &'static str),
            name!(socket_path, String),
            name!(cache_dir, String),
            name!(last_start_ms, Option<i64>),
            name!(last_stop_ms, Option<i64>),
            name!(last_reload_at_ms, Option<i64>),
            name!(reload_generation, i64),
            name!(last_reload_added, i64),
            name!(last_reload_removed, i64),
            name!(last_reload_replaced, i64),
            name!(last_reload_unchanged, i64),
            name!(desired_volume_count, i64),
            name!(loaded_volume_count, i64),
            name!(stale_volume_count, i64),
            name!(unavailable_volume_count, i64),
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
            status.last_reload_at_ms,
            status.reload_generation,
            status.last_reload_added,
            status.last_reload_removed,
            status.last_reload_replaced,
            status.last_reload_unchanged,
            status.desired_volume_count,
            status.loaded_volume_count,
            status.stale_volume_count,
            status.unavailable_volume_count,
            status.last_error_at_ms,
            status.last_error,
        )))
    }

    #[pg_extern]
    fn maintenance_worker(worker_context: Internal) -> i64 {
        // SAFETY: this SQL-inaccessible `internal` argument is supplied only
        // by the worker entry point.
        let worker_context = unsafe {
            lagodb_core::extension_worker::WorkerContext::from_internal(
                &worker_context,
            )
        };
        run_object_cleanup_worker(&worker_context).into_raw()
    }

    #[pg_extern]
    fn retry_maintenance_item(target_item_id: Uuid) -> bool {
        ensure_runtime_preloaded();
        ObjectCleanupQueue::retry_failed(ObjectCleanupItemId::from_pg_uuid(
            target_item_id,
        ))
        .map_err(|source| error::LagodbError::RetryMaintenanceItem { source })
        .unwrap_or_else(|error| error.report())
    }
}

pgrx::extension_sql!(
    "REVOKE ALL ON FUNCTION lagodb.observe_object_tree(bigint, text, text) FROM PUBLIC;",
    name = "lock_down_observe_object_tree",
    requires = [lagodb::observe_object_tree],
);

// `#[pg_test]` host wrappers call this module's runner configuration under
// `cfg(test)`. Worker framework tests live with `crate::worker`.
#[cfg(any(test, feature = "pg_test"))]
pub mod pg_test;
