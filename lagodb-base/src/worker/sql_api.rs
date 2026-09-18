//! PostgreSQL SQL adapters for worker registration, control, and status.

use std::ffi::CStr;

use pgrx::prelude::*;
use pgrx::{pg_getarg, pg_getarg_datum_raw};

use crate::ensure_runtime_preloaded;

use super::lifecycle::{request_database_reconcile, request_wakeup};
use super::registry;
use super::{
    DatabaseLifecycleLock, WorkerError, process_status as runtime_process_status,
    stop_worker, worker_status as runtime_worker_status,
};

#[pg_schema]
mod lagodb {
    use super::*;

    #[pg_extern]
    fn register_worker_impl(worker_name: &str, entrypoint: pg_sys::Oid) -> i32 {
        ensure_runtime_preloaded();
        let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
        let worker_id = registry::register(worker_name, entrypoint)
            .unwrap_or_else(|error| error.report());
        if !registry::database_is_template(database_oid) {
            DatabaseLifecycleLock::new(database_oid).acquire_shared();
            request_database_reconcile();
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
            WorkerError::WorkerNameNotRegistered {
                worker_name: worker_name.to_owned(),
            }
            .report();
        }
        if !registry::database_is_template(database_oid) {
            DatabaseLifecycleLock::new(database_oid).acquire_shared();
            request_database_reconcile();
            if let Some(worker_id) = worker_id {
                stop_worker(database_oid, worker_id);
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
            WorkerError::WorkerIdNotRegistered { worker_id }.report();
        }
        if !registry::database_is_template(database_oid) {
            DatabaseLifecycleLock::new(database_oid).acquire_shared();
            request_database_reconcile();
            if deregistered {
                stop_worker(database_oid, worker_id);
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
                WorkerError::WorkerNotRegistered {
                    extension_name,
                    worker_name: worker_name.to_owned(),
                }
                .report()
            }
            Err(error) => error.report(),
        };
        request_wakeup(worker_id);
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
        TableIterator::new(runtime_worker_status().into_iter().map(|status| {
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
        TableIterator::new(runtime_process_status().into_iter().map(|status| {
            (
                status.process_kind,
                status.database_oid.map(pg_sys::Oid::from),
                status.state,
                status.pid,
                status.needs_restart,
            )
        }))
    }
}
