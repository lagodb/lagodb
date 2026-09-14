//! Process-lifetime query-offload method tables owned by `lagodb-base`.

use std::ffi::CStr;
use std::sync::OnceLock;

use lagodb_core::customscan::{CustomScanMethodTables, SerialCustomScanCallbacks};
use pgrx::pg_sys;

use super::{execution, planning};

const QUERY_NAME: &CStr = c"LagoDB Query Offload";
static QUERY_TABLES: OnceLock<CustomScanMethodTables> = OnceLock::new();

pub(super) fn tables() -> &'static CustomScanMethodTables {
    QUERY_TABLES.get_or_init(|| {
        CustomScanMethodTables::serial(
            QUERY_NAME,
            SerialCustomScanCallbacks {
                plan: planning::plan_custom_path,
                reparameterize: None,
                create_state: execution::create_state,
                begin: execution::begin,
                execute: execution::exec,
                end: execution::end,
                rescan: execution::rescan,
                explain: execution::explain,
            },
        )
    })
}

pub(crate) fn register() {
    let scan = tables().scan();
    // SAFETY: the table is process-lifetime immutable storage and is
    // registered once during shared-preload initialization.
    unsafe { pg_sys::RegisterCustomScanMethods(scan) };
}
