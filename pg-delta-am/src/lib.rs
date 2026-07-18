//! Delta table access method skeleton.
//!
//! The storage callbacks intentionally delegate to PostgreSQL heap today. This
//! keeps the extension loadable while the Delta implementation is built, and
//! gives the shared runtime a second real AM owner for cross-DSO registration
//! coverage. It must not be treated as a Delta storage implementation.

#[cfg(feature = "pg_test")]
use pg_lakebase_core::table_maintenance::abi::{
    MAINTENANCE_PROVIDER_VERSION, MaintenanceProviderV3, MaintenanceReportV1,
    MaintenanceRequestV1, MaintenanceStatsV1, REGISTER_DUPLICATE_ACCESS_METHOD,
    runtime_api,
};
use pg_lakebase_core::table_maintenance::{
    LakebaseTableMaintenanceProvider, TableMaintenanceError, TableMaintenanceReport,
    TableMaintenanceRequest, TableMaintenanceStats,
};
use pgrx::prelude::*;

pgrx::pg_module_magic!();

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
    pg_lakebase_core::table_maintenance::register_provider::<DeltaMaintenanceProvider>(
    );
}

#[cfg(feature = "pg_test")]
unsafe extern "C-unwind" fn duplicate_am_oid() -> pg_sys::Oid {
    pg_sys::InvalidOid
}

#[cfg(feature = "pg_test")]
unsafe extern "C-unwind" fn duplicate_execute(
    _request: *const MaintenanceRequestV1,
    _report: *mut MaintenanceReportV1,
) {
}

#[cfg(feature = "pg_test")]
unsafe extern "C-unwind" fn duplicate_inspect(
    _relation: pg_sys::Relation,
    _stats: *mut MaintenanceStatsV1,
) {
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod delta {
    use super::*;

    /// Ask the runtime to register a distinct provider for Iceberg.
    ///
    /// Returning a boolean lets regression tests prove that duplicate ownership
    /// is rejected by the runtime DSO, without panicking the backend.
    #[pg_extern]
    fn duplicate_iceberg_registration_rejected() -> bool {
        let api = runtime_api().expect("runtime API must be published");
        let descriptor = MaintenanceProviderV3 {
            abi_version: MAINTENANCE_PROVIDER_VERSION,
            struct_size: std::mem::size_of::<MaintenanceProviderV3>() as u32,
            name: c"delta-duplicate".as_ptr(),
            access_method_name: c"iceberg".as_ptr(),
            capability_flags: 0,
            access_method_oid: duplicate_am_oid,
            execute: duplicate_execute,
            inspect: duplicate_inspect,
        };
        let result = unsafe { (api.register_provider)(&descriptor) };
        result == REGISTER_DUPLICATE_ACCESS_METHOD
    }
}
