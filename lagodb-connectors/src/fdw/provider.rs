//! SQL-level FDW identity and callback registration.

use core::ffi::CStr;

use lagodb_core::fdw::{
    FdwRoutine, ForeignDataWrapper, ForeignValidationError, register_analyze,
    register_modify, register_scan, register_truncate,
};
use lagodb_core::pg_fdw;
use pgrx::pg_sys;

use super::options::validate_catalog_options;
use crate::CONNECTOR_FDW_NAME;

/// The single SQL-level provider for all LagoDB connector formats.
#[pg_fdw(
    version = "0.1.0",
    author = "LagoDB",
    website = "https://github.com/lagodb/lagodb"
)]
pub(crate) struct LagodbConnectors;

impl ForeignDataWrapper for LagodbConnectors {
    const NAME: &'static CStr = CONNECTOR_FDW_NAME;

    fn register(routine: &mut FdwRoutine) {
        register_scan::<Self>(routine);
        register_modify::<Self>(routine);
        register_analyze::<Self>(routine);
        register_truncate::<Self>(routine);
    }

    fn validate(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ForeignValidationError> {
        validate_catalog_options(options, catalog)?;
        Ok(())
    }
}

impl LagodbConnectors {
    pub(crate) fn server_uses_connectors(server_name: &CStr) -> bool {
        let server_oid =
            unsafe { pg_sys::get_foreign_server_oid(server_name.as_ptr(), true) };
        if server_oid == pg_sys::InvalidOid {
            return false;
        }
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let provider_oid = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(Self::NAME.as_ptr(), true)
        };
        server.fdwid == provider_oid
    }

    pub(crate) fn relation_uses_connectors(relation_oid: pg_sys::Oid) -> bool {
        let table = unsafe { &*pg_sys::GetForeignTable(relation_oid) };
        let server = unsafe { &*pg_sys::GetForeignServer(table.serverid) };
        let provider_oid = unsafe {
            pg_sys::get_foreign_data_wrapper_oid(Self::NAME.as_ptr(), true)
        };
        server.fdwid == provider_oid
    }
}
