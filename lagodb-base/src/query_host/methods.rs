//! Process-lifetime AggregateScan method tables owned by `lagodb-base`.

use std::sync::OnceLock;

use lagodb_core::customscan::{CustomScanMethodTables, SerialCustomScanCallbacks};
use pgrx::pg_sys;

use super::{execution, planning};

const NAME: &std::ffi::CStr = c"LagoDB Aggregate";

static TABLES: OnceLock<CustomScanMethodTables> = OnceLock::new();

pub(super) fn tables() -> &'static CustomScanMethodTables {
    TABLES.get_or_init(|| {
        CustomScanMethodTables::serial(
            NAME,
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
    // SAFETY: `scan` is process-lifetime immutable storage and registration is
    // performed once during shared-preload initialization.
    unsafe { pg_sys::RegisterCustomScanMethods(scan) };
}
