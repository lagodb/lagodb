use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

use crate::error::{LagodbError, LagodbResult};

mod bgworker;
mod control;
mod coordinator;
mod extension;
mod injection;
mod lock;
#[cfg(feature = "pg_test")]
mod pg_test;
mod scheduler;
mod signals;
mod state;
mod status;
mod store;
mod supervisor;
#[cfg(test)]
mod tests;

pub(crate) use lock::DatabaseLifecycleLock;
pub(crate) use state::{INVALID_OID, MAX_WORKER_NAME_BYTES, WorkerKey};
pub(crate) use status::{ProcessStatus, WorkerStatus};
use store::{COORDINATOR_TABLE, SHARED_STATE, Store, WORKER_TABLE};

const SUPERVISOR_FUNCTION: &str = "lagodb_base_supervisor_main";
pub(super) const COORDINATOR_FUNCTION: &str = "lagodb_base_coordinator_main";
pub(super) const WORKER_FUNCTION: &str = "lagodb_base_worker_main";
pub(super) const LIBRARY_NAME: &str = "lagodb_base";
pub(super) const COORDINATOR_TYPE: &str = "lagodb coordinator";
pub(super) const WORKER_TYPE: &str = "lagodb worker";
pub(super) const CAPACITY_RETRY: Duration = Duration::from_millis(100);
pub(super) const SUPERVISOR_ERROR_RETRY: Duration = Duration::from_secs(5);
static PRELOADED: AtomicBool = AtomicBool::new(false);

pub(crate) fn init_shared_memory() {
    pgrx::pg_shmem_init!(SHARED_STATE);
    pgrx::pg_shmem_init!(COORDINATOR_TABLE);
    pgrx::pg_shmem_init!(WORKER_TABLE);
}

pub(crate) fn init() {
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }

    PRELOADED.store(true, Ordering::Release);

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

pub(crate) fn ensure_preloaded() -> LagodbResult<()> {
    if is_preloaded() {
        Ok(())
    } else {
        Err(LagodbError::RuntimeNotPreloaded)
    }
}

pub(crate) fn is_preloaded() -> bool {
    PRELOADED.load(Ordering::Acquire)
}

pub(crate) fn wake_worker(key: WorkerKey) -> bool {
    store::Store::new().wake_worker(key)
}

pub(crate) fn wake_database_workers(database_oid: u32) -> bool {
    store::Store::new().wake_database_workers(database_oid)
}

pub(crate) fn request_database_reconcile(database_oid: u32) -> bool {
    store::Store::new().request_database_reconcile(database_oid)
}

pub(crate) fn request_full_rescan() -> bool {
    store::Store::new().request_full_rescan()
}

pub(crate) fn signal_supervisor() {
    store::Store::new().signal_supervisor();
}

pub(crate) fn worker_status() -> Vec<WorkerStatus> {
    Store::new().worker_status()
}

pub(crate) fn process_status() -> Vec<ProcessStatus> {
    Store::new().process_status()
}

pub(crate) fn prepare_database_drop(database_oid: u32) {
    control::StopController::new().prepare_database_drop(database_oid)
}

pub(crate) fn prepare_extension_drop(database_oid: u32, extension_oid: u32) {
    control::StopController::new().prepare_extension_drop(database_oid, extension_oid)
}

pub(crate) fn stop_worker(database_oid: u32, worker_id: i32) {
    control::StopController::new().stop_worker(database_oid, worker_id)
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
