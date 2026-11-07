use std::ffi::{CStr, c_char};
use std::time::Duration;

use lagodb_core::runtime_api::{
    STAGE_WORKER_WAKEUP_INVALID_REQUEST, STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND,
    STAGE_WORKER_WAKEUP_OK, STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED,
};
use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

use crate::runtime_is_preloaded;

mod bgworker;
mod catalog;
mod control;
mod coordinator;
mod error;
mod extension;
mod gucs;
mod injection;
mod lifecycle;
mod lock;
// The lifecycle harness drives workers through PostgreSQL injection points.
#[cfg(all(feature = "pg_test", any(feature = "pg17", feature = "pg18")))]
mod pg_test;
mod registry;
mod scheduler;
mod signals;
mod sql_api;
mod state;
mod status;
mod store;
mod supervisor;

use error::WorkerError;
pub(crate) use lifecycle::{
    drop_extension_workers, init as init_lifecycle, preflight,
};
use lock::DatabaseLifecycleLock;
use state::{INVALID_OID, MAX_WORKER_NAME_BYTES, WorkerKey};
use status::{ProcessStatus, WorkerStatus};
use store::{COORDINATOR_TABLE, SHARED_STATE, Store, WORKER_TABLE};

pub(crate) type WorkerResult<T> = Result<T, WorkerError>;

const SUPERVISOR_FUNCTION: &str = "lagodb_base_supervisor_main";
pub(super) const COORDINATOR_FUNCTION: &str = "lagodb_base_coordinator_main";
pub(super) const WORKER_FUNCTION: &str = "lagodb_base_worker_main";
pub(super) const LIBRARY_NAME: &str = "lagodb_base";
pub(super) const COORDINATOR_TYPE: &str = "lagodb coordinator";
pub(super) const WORKER_TYPE: &str = "lagodb worker";
pub(super) const CAPACITY_RETRY: Duration = Duration::from_millis(100);
pub(super) const SUPERVISOR_ERROR_RETRY: Duration = Duration::from_secs(5);

/// Define the worker GUCs on every load of the runtime library.
pub(crate) fn define_gucs() {
    gucs::define();
}

pub(crate) fn init_shared_memory() {
    pgrx::pg_shmem_init!(SHARED_STATE);
    pgrx::pg_shmem_init!(COORDINATOR_TABLE);
    pgrx::pg_shmem_init!(WORKER_TABLE);
}

pub(crate) fn init() {
    let _keep_supervisor = supervisor_main as extern "C-unwind" fn(pg_sys::Datum);
    let _keep_coordinator = coordinator_main as extern "C-unwind" fn(pg_sys::Datum);
    let _keep_worker = worker_main as extern "C-unwind" fn(pg_sys::Datum);

    BackgroundWorkerBuilder::new("lagodb supervisor")
        .set_type("lagodb supervisor")
        .set_library(LIBRARY_NAME)
        .set_function(SUPERVISOR_FUNCTION)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

fn wake_worker(key: WorkerKey) -> bool {
    store::Store::new().wake_worker(key)
}

fn wake_database_workers(database_oid: u32) -> bool {
    store::Store::new().wake_database_workers(database_oid)
}

fn request_database_reconcile(database_oid: u32) -> bool {
    store::Store::new().request_database_reconcile(database_oid)
}

fn request_full_rescan() -> bool {
    store::Store::new().request_full_rescan()
}

fn signal_supervisor() {
    store::Store::new().signal_supervisor();
}

fn worker_status() -> Vec<WorkerStatus> {
    Store::new().worker_status()
}

fn process_status() -> Vec<ProcessStatus> {
    Store::new().process_status()
}

fn prepare_database_drop(database_oid: u32) {
    control::StopController::new().prepare_database_drop(database_oid)
}

fn prepare_extension_drop(database_oid: u32, extension_oid: u32) {
    control::StopController::new().prepare_extension_drop(database_oid, extension_oid)
}

fn stop_worker(database_oid: u32, worker_id: i32) {
    control::StopController::new().stop_worker(database_oid, worker_id)
}

/// Runtime ABI entry that resolves a worker by extension and worker name and
/// stages its wakeup in the current database until the surrounding transaction
/// commits.
#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn stage_named_worker_wakeup(
    extension_name: *const c_char,
    worker_name: *const c_char,
) -> u32 {
    if !runtime_is_preloaded() {
        return STAGE_WORKER_WAKEUP_RUNTIME_NOT_PRELOADED;
    }
    // SAFETY: the exact-build ABI passes both arguments from `&CStr`, so they
    // point to live, NUL-terminated strings for this synchronous call.
    let extension_name = unsafe { CStr::from_ptr(extension_name) };
    // SAFETY: the same runtime ABI string contract applies to `worker_name`.
    let worker_name = unsafe { CStr::from_ptr(worker_name) };
    if extension_name.is_empty()
        || worker_name.is_empty()
        || worker_name.to_bytes().len() > MAX_WORKER_NAME_BYTES
    {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    }
    let Ok(worker_name) = worker_name.to_str() else {
        return STAGE_WORKER_WAKEUP_INVALID_REQUEST;
    };
    let Some(worker_id) = registry::resolve_worker_id(extension_name, worker_name)
        .unwrap_or_else(|error| error.report())
    else {
        return STAGE_WORKER_WAKEUP_LOCATOR_NOT_FOUND;
    };
    lifecycle::stage_worker_wakeup(worker_id);
    STAGE_WORKER_WAKEUP_OK
}

#[pg_guard]
#[unsafe(export_name = "lagodb_base_supervisor_main")]
pub extern "C-unwind" fn supervisor_main(_arg: pg_sys::Datum) {
    supervisor::Supervisor::run();
}

#[pg_guard]
#[unsafe(export_name = "lagodb_base_coordinator_main")]
pub extern "C-unwind" fn coordinator_main(arg: pg_sys::Datum) {
    coordinator::Coordinator::run(arg);
}

#[pg_guard]
#[unsafe(export_name = "lagodb_base_worker_main")]
pub extern "C-unwind" fn worker_main(arg: pg_sys::Datum) {
    extension::Worker::run(arg);
}
