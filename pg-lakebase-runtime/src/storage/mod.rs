//! Runtime-owned storage background worker host.
//!
//! `lakebase.workers` stores database-local extension workers only. The storage
//! server is a cluster singleton owned by the `pg_lakebase_runtime` extension:
//! `_PG_init` registers its GUC backing statics and, when enabled, one static
//! postmaster-managed background worker.

mod catalog;
mod config;
pub(crate) mod gucs;
pub(crate) mod logging;
mod reconciler;
mod reload;
mod state;
mod supervisor;
pub(crate) mod volume_config;

use std::path::PathBuf;

use pg_lakebase_core::storage::service::StorageEndpoint;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;

pub(crate) use state::{STORAGE_STATE, StorageRuntimeStatus};

const LIBRARY_NAME: &str = "pg_lakebase_runtime";
const WORKER_FUNCTION: &str = "pg_lakebase_runtime_storage_bgworker_main";

/// Register storage GUCs and the static storage background worker.
///
/// Must be called from `_PG_init`. Only the runtime crate owns this host path;
/// AM crates discover the endpoint through PostgreSQL GUCs and never register
/// this worker themselves.
pub(crate) fn init() {
    gucs::init();

    // Static workers and postmaster-wide staging cleanup are valid only while
    // PostgreSQL is processing shared_preload_libraries. A backend that loads
    // the SQL extension must not disturb files owned by live transactions.
    if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }

    if !gucs::enabled() {
        return;
    }

    cleanup_staging_dir();

    // Force the linker to retain the entry-point symbol in the runtime cdylib.
    let _keep = pg_lakebase_runtime_storage_bgworker_main
        as extern "C-unwind" fn(pg_sys::Datum);

    BackgroundWorkerBuilder::new("pg-lakebase-storage")
        .set_type("pg-lakebase-storage")
        .set_library(LIBRARY_NAME)
        .set_function(WORKER_FUNCTION)
        // `enable_spi_access` sets BGWORKER_BACKEND_DATABASE_CONNECTION and
        // forces start time to RecoveryFinished, which supplies the normal
        // backend environment used by GUC reload and shared status reporting.
        .enable_spi_access()
        .set_restart_time(Some(std::time::Duration::from_secs(5)))
        .load();
}

pub(crate) fn runtime_status() -> StorageRuntimeStatus {
    let (enabled, socket_path, cache_dir) = resolved_endpoint().into_parts();
    state::snapshot(enabled, socket_path, cache_dir)
}

fn cleanup_staging_dir() {
    let (_, _, cache_dir) = resolved_endpoint().into_parts();
    let staging_dir =
        pg_lakebase_storage::StagingPathResolver::new(cache_dir).staging_dir();
    if !staging_dir.exists() {
        return;
    }
    match std::fs::remove_dir_all(&staging_dir) {
        Ok(()) => crate::diag::info(format_args!(
            "cleaned Lakebase staging directory at postmaster startup: {}",
            staging_dir.display()
        )),
        Err(error) => crate::diag::warning(format_args!(
            "failed to clean Lakebase staging directory {} at postmaster startup: {error}",
            staging_dir.display()
        )),
    }
}

fn resolved_endpoint() -> StorageEndpoint {
    StorageEndpoint::from_config(
        gucs::enabled(),
        gucs::socket_path().map(PathBuf::from),
        gucs::cache_dir().map(PathBuf::from),
    )
    .expect("PostgreSQL DataDir must be initialized before resolving storage paths")
}

/// Background worker entry point called by PostgreSQL after forking.
///
/// # Safety
///
/// This function is called by PostgreSQL's background worker infrastructure.
/// It must be `extern "C-unwind"`, `#[pg_guard]`, and `#[unsafe(no_mangle)]`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_runtime_storage_bgworker_main(
    _arg: pg_sys::Datum,
) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    let pid = unsafe { pg_sys::MyProcPid };
    let callback_arg = usize::try_from(pid)
        .expect("PostgreSQL background worker PID must be non-negative");
    // SAFETY: storage_exit_callback has PostgreSQL's before_shmem_exit ABI,
    // and callback_arg is this process's PID encoded as a scalar Datum.
    unsafe {
        pg_sys::before_shmem_exit(
            Some(storage_exit_callback),
            pg_sys::Datum::from(callback_arg),
        );
    }

    state::StorageStatusStore::new().mark_starting(pid);
    supervisor::StorageWorkerSupervisor::from_gucs().run();
}

/// Final shared-state cleanup for the storage background worker.
///
/// # Safety
///
/// PostgreSQL invokes this function through the `before_shmem_exit` callback
/// ABI with the scalar PID registered by the worker entry point above.
#[pg_guard]
unsafe extern "C-unwind" fn storage_exit_callback(code: i32, arg: pg_sys::Datum) {
    let Ok(pid) = i32::try_from(arg.value()) else {
        return;
    };
    state::StorageStatusStore::new().finish_process(pid, code);
}
