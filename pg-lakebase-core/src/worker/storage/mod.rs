//! pg-lakebase storage background worker.
//!
//! Starts `pg-lakebase-storage` as a PostgreSQL background worker managed by the
//! postmaster.  The storage server runs inside a multi-thread Tokio runtime; the
//! bgworker main thread handles signals, lifecycle, and pumps log events into PG.
//!
//! # Usage
//!
//! An AM extension calls [`init_for_extension`] from its `_PG_init`:
//!
//! ```rust,ignore
//! pg_lakebase_core::worker::storage::init_for_extension("pg_iceberg_am");
//! ```
//!
//! This registers the GUCs and, when enabled, the static background worker.
//! The worker entry point is [`pg_lakebase_storage_bgworker_main`], which must
//! be exported from the final `.so` that PostgreSQL loads.

mod catalog;
mod config;
pub(crate) mod gucs;
pub(crate) mod logging;
mod reconciler;
mod supervisor;

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;

/// Register GUCs and (if enabled) a static storage background worker.
///
/// `library_name` must match the shared-library file that PostgreSQL loads
/// (e.g. `"pg_iceberg_am"`), because the bgworker entry point symbol lives in
/// that library.
///
/// Must be called from `_PG_init` while processing `shared_preload_libraries`.
pub fn init_for_extension(library_name: &str) {
    gucs::init();

    if !gucs::enabled() {
        return;
    }

    // Force the linker to retain the entry-point symbol in the final cdylib.
    let _keep =
        pg_lakebase_storage_bgworker_main as extern "C-unwind" fn(pg_sys::Datum);

    BackgroundWorkerBuilder::new("pg-lakebase-storage")
        .set_type("pg-lakebase-storage")
        .set_library(library_name)
        .set_function("pg_lakebase_storage_bgworker_main")
        // `enable_spi_access` sets BGWORKER_BACKEND_DATABASE_CONNECTION and
        // forces start time to RecoveryFinished, which is what we need so the
        // tablespace catalog reconciler can scan `pg_tablespace` (a shared
        // catalog) before the storage server starts accepting requests.
        .enable_spi_access()
        .set_restart_time(None)
        .load();
}

/// Background worker entry point called by PostgreSQL after forking.
///
/// # Safety
///
/// This function is called by PostgreSQL's background worker infrastructure.
/// It must be `extern "C-unwind"`, `#[pg_guard]`, and `#[unsafe(no_mangle)]`.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_lakebase_storage_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(
        SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM,
    );

    supervisor::StorageWorkerSupervisor::from_gucs().run();
}
