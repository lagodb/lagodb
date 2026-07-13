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
mod state;
mod supervisor;

use std::ffi::CStr;
use std::path::PathBuf;

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
        // forces start time to RecoveryFinished, which is what we need so the
        // tablespace catalog reconciler can scan `pg_tablespace` (a shared
        // catalog) before the storage server starts accepting requests.
        .enable_spi_access()
        .set_restart_time(Some(std::time::Duration::from_secs(5)))
        .load();
}

pub(crate) fn runtime_status() -> StorageRuntimeStatus {
    state::snapshot(
        gucs::enabled(),
        resolved_socket_path(),
        resolved_cache_dir(),
    )
}

fn cleanup_staging_dir() {
    let staging_dir =
        pg_lakebase_storage::StagingPathResolver::new(resolved_cache_dir())
            .staging_dir();
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

fn resolved_socket_path() -> PathBuf {
    if let Some(p) = gucs::socket_path() {
        return PathBuf::from(p);
    }
    data_dir_base().join("storage.sock")
}

fn resolved_cache_dir() -> PathBuf {
    if let Some(p) = gucs::cache_dir() {
        return PathBuf::from(p);
    }
    data_dir_base().join("storage-cache")
}

fn data_dir_base() -> PathBuf {
    let data_dir = unsafe {
        CStr::from_ptr(pg_sys::DataDir)
            .to_string_lossy()
            .into_owned()
    };
    PathBuf::from(data_dir).join("pg_lakebase")
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

    state::StorageStatusStore::new().mark_starting(unsafe { pg_sys::MyProcPid });
    supervisor::StorageWorkerSupervisor::from_gucs().run();
}
