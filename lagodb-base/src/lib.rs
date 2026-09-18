use std::sync::atomic::{AtomicBool, Ordering};

use lagodb_core::diag::PgReportError;
use pgrx::prelude::*;

mod descriptor_registry;
mod gucs;
mod maintenance;
mod object_access;
mod planning_hooks;
mod process_utility;
mod provider_bootstrap;
mod query_host;
mod runtime_host;
mod storage;
mod worker;

static RUNTIME_PRELOADED: AtomicBool = AtomicBool::new(false);

pg_module_magic!();

extension_sql_file!("../sql/bootstrap.sql", bootstrap);
extension_sql_file!("../sql/finalize.sql", finalize);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    gucs::init();
    storage::init();
    provider_bootstrap::init();
    runtime_host::init();

    if unsafe {
        pg_sys::process_shared_preload_libraries_in_progress
            && !pg_sys::IsBinaryUpgrade
    } {
        worker::init_shared_memory();
        storage::init_shared_memory();
        worker::init_lifecycle();
        planning_hooks::init();
        query_host::init();
        RUNTIME_PRELOADED.store(true, Ordering::Release);
        worker::init();
        provider_bootstrap::load_configured();
    }
}

pub(crate) fn runtime_is_preloaded() -> bool {
    RUNTIME_PRELOADED.load(Ordering::Acquire)
}

pub(crate) fn ensure_runtime_preloaded() {
    if !runtime_is_preloaded() {
        PgReportError::from_message(
            PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            "lagodb_base must be loaded with shared_preload_libraries before use; add lagodb_base to shared_preload_libraries and restart PostgreSQL",
        )
        .report();
    }
}

// `#[pg_test]` host wrappers call this module's runner configuration under
// `cfg(test)`. Worker framework tests live with `crate::worker`.
#[cfg(test)]
pub mod pg_test {
    //! pgrx test-runner configuration.

    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'lagodb_base'",
            "max_worker_processes = 32",
        ]
    }
}
