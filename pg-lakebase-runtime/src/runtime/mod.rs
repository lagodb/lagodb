use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pgrx::bgworkers::BackgroundWorkerBuilder;
use pgrx::prelude::*;

use crate::error::{LakebaseError, LakebaseResult};

mod bgworker;
mod control;
mod launcher;
mod reconciler;
mod status;
mod store;
mod worker;

pub(crate) use status::{ProcessStatus, WorkerStatus};
pub(crate) use store::RUNTIME_STATE;

const LAUNCHER_FUNCTION: &str = "pg_lakebase_runtime_launcher_main";
pub(super) const RECONCILER_FUNCTION: &str =
    "pg_lakebase_runtime_database_reconciler_main";
pub(super) const WORKER_FUNCTION: &str = "pg_lakebase_runtime_extension_worker_main";
pub(super) const LIBRARY_NAME: &str = "pg_lakebase_runtime";
pub(super) const CRASH_BACKOFF: Duration = Duration::from_secs(5);
pub(super) const CAPACITY_WARNING_INTERVAL: Duration = Duration::from_secs(60);

static RUNTIME_PRELOADED: AtomicBool = AtomicBool::new(false);

pub(crate) fn init() {
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }

    RUNTIME_PRELOADED.store(true, Ordering::Release);

    let _keep_launcher =
        pg_lakebase_runtime_launcher_main as extern "C-unwind" fn(pg_sys::Datum);
    let _keep_reconciler = pg_lakebase_runtime_database_reconciler_main
        as extern "C-unwind" fn(pg_sys::Datum);
    let _keep_worker = pg_lakebase_runtime_extension_worker_main
        as extern "C-unwind" fn(pg_sys::Datum);

    BackgroundWorkerBuilder::new("pg-lakebase-runtime launcher")
        .set_type("pg-lakebase-runtime launcher")
        .set_library(LIBRARY_NAME)
        .set_function(LAUNCHER_FUNCTION)
        .enable_spi_access()
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

pub(crate) fn ensure_preloaded() -> LakebaseResult<()> {
    if RUNTIME_PRELOADED.load(Ordering::Acquire) {
        Ok(())
    } else {
        Err(LakebaseError::RuntimeNotPreloaded)
    }
}

pub(crate) fn reserve_registration(
    database_oid: u32,
    extension_oid: u32,
    worker_name: &str,
) -> LakebaseResult<()> {
    store::RuntimeStore::new().reserve_registration(
        database_oid,
        extension_oid,
        worker_name,
    )
}

pub(crate) fn finish_registration(
    database_oid: u32,
    extension_oid: u32,
    worker_name: &str,
    committed: bool,
) -> bool {
    store::RuntimeStore::new().finish_registration(
        database_oid,
        extension_oid,
        worker_name,
        committed,
    )
}

pub(crate) fn wake_worker(
    database_oid: u32,
    extension_oid: u32,
    worker_name: &str,
) -> bool {
    store::RuntimeStore::new().wake_worker(database_oid, extension_oid, worker_name)
}

pub(crate) fn mark_database_dirty(database_oid: u32) -> bool {
    store::RuntimeStore::new().mark_database_dirty(database_oid)
}

pub(crate) fn request_full_rescan() -> bool {
    store::RuntimeStore::new().request_full_rescan()
}

pub(crate) fn signal_launcher() {
    store::RuntimeStore::new().signal_launcher();
}

pub(crate) fn worker_status() -> Vec<WorkerStatus> {
    status::worker_status()
}

pub(crate) fn process_status() -> Vec<ProcessStatus> {
    status::process_status()
}

pub(crate) fn stop_database(database_oid: u32) -> LakebaseResult<()> {
    control::StopController::new().stop_database(database_oid)
}

pub(crate) fn stop_extension(
    database_oid: u32,
    extension_oid: u32,
) -> LakebaseResult<()> {
    control::StopController::new().stop_extension(database_oid, extension_oid)
}

pub(crate) fn pause_database_reconciliation(database_oid: u32) -> LakebaseResult<()> {
    control::StopController::new().pause_reconciliation(database_oid)
}

pub(crate) fn stop_worker(
    database_oid: u32,
    extension_oid: u32,
    worker_name: &str,
) -> LakebaseResult<()> {
    control::StopController::new().stop_worker(
        database_oid,
        extension_oid,
        worker_name,
    )
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_runtime_launcher_main(_arg: pg_sys::Datum) {
    launcher::Launcher::run();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_runtime_database_reconciler_main(
    arg: pg_sys::Datum,
) {
    reconciler::DatabaseReconciler::run(arg);
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_runtime_extension_worker_main(
    arg: pg_sys::Datum,
) {
    worker::ExtensionWorker::run(arg);
}
