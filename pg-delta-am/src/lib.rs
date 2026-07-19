//! Delta table access method skeleton.
//!
//! The storage callbacks intentionally delegate to PostgreSQL heap today. This
//! keeps the extension loadable while the Delta implementation is built, and
//! gives the shared runtime a second real AM owner for cross-DSO registration
//! coverage. It must not be treated as a Delta storage implementation.

use pg_lakebase_core::table_maintenance::{
    LakebaseTableMaintenanceProvider, TableMaintenanceError, TableMaintenanceReport,
    TableMaintenanceRequest, TableMaintenanceStats,
};
use pgrx::prelude::*;

pgrx::pg_module_magic!();

#[cfg(feature = "pg_test")]
mod pg_test_support;

struct DeltaMaintenanceProvider;

impl LakebaseTableMaintenanceProvider for DeltaMaintenanceProvider {
    const NAME: &'static std::ffi::CStr = c"delta";
    const ACCESS_METHOD_NAME: &'static std::ffi::CStr = c"delta";

    fn access_method_oid() -> Option<pg_sys::Oid> {
        let oid = unsafe {
            pg_sys::get_table_am_oid(Self::ACCESS_METHOD_NAME.as_ptr(), true)
        };
        (oid != pg_sys::InvalidOid).then_some(oid)
    }

    fn execute(
        _request: TableMaintenanceRequest<'_>,
    ) -> Result<TableMaintenanceReport, TableMaintenanceError> {
        Err(TableMaintenanceError::framework(
            "Delta table maintenance is not implemented",
        ))
    }

    fn inspect(
        _relation: &pg_lakebase_core::handles::RelationHandle<'_>,
    ) -> Result<TableMaintenanceStats, TableMaintenanceError> {
        Ok(TableMaintenanceStats {
            format: Some("delta-skeleton".to_owned()),
            ..TableMaintenanceStats::default()
        })
    }
}

#[pg_extern(sql = "CREATE FUNCTION delta_table_am_handler(internal)
           RETURNS table_am_handler
           LANGUAGE c STRICT
           AS 'MODULE_PATHNAME', 'delta_table_am_handler_wrapper';")]
fn delta_table_am_handler() -> pg_lakebase_core::TableAmRoutine {
    let heap_handler_oid =
        unsafe { pg_sys::fmgr_internal_function(c"heap_tableam_handler".as_ptr()) };
    assert_ne!(
        heap_handler_oid,
        pg_sys::InvalidOid,
        "PostgreSQL heap table-AM handler is unavailable"
    );
    let routine = unsafe { pg_sys::GetTableAmRoutine(heap_handler_oid) };
    assert!(
        !routine.is_null(),
        "PostgreSQL returned a null heap table-AM routine"
    );
    unsafe { pg_lakebase_core::TableAmRoutine::from_pg(routine.cast_mut()) }
}

pgrx::extension_sql!(
    "CREATE ACCESS METHOD delta
     TYPE TABLE HANDLER delta_table_am_handler;",
    name = "create_delta_access_method",
    requires = [delta_table_am_handler],
);

#[pg_guard]
extern "C-unwind" fn _PG_init() {
    #[cfg(feature = "pg_test")]
    pg_test_support::init_hooks();
    pg_lakebase_core::table_maintenance::register_provider::<DeltaMaintenanceProvider>(
    );
}
