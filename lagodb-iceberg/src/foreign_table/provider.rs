//! SQL-level Iceberg FDW identity and capability registration.

use std::ffi::{CStr, CString};

use lagodb_core::fdw::{
    FdwImportSchema, FdwRoutine, ForeignDataWrapper, ForeignImportError,
    ForeignImportSchemaContext, ForeignValidationError, register_analyze,
    register_import_schema, register_modify, register_scan,
};
use lagodb_core::pg_fdw;
use pgrx::pg_sys;

use super::import::IcebergSchemaImporter;
use super::options::IcebergFdwOptions;

#[pg_fdw(
    version = "0.1.0",
    author = "LagoDB",
    website = "https://github.com/robertmu/pg-lakebase"
)]
pub(crate) struct LagodbIceberg;

impl ForeignDataWrapper for LagodbIceberg {
    const NAME: &'static CStr = c"lagodb_iceberg";

    fn register(routine: &mut FdwRoutine) {
        register_scan::<Self>(routine);
        register_modify::<Self>(routine);
        register_analyze::<Self>(routine);
        register_import_schema::<Self>(routine);
    }

    fn validate(
        options: &[Option<String>],
        catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ForeignValidationError> {
        IcebergFdwOptions::validate_catalog(options, catalog)
    }
}

impl LagodbIceberg {
    /// Match the installed handler by its C entry-point identity. PostgreSQL
    /// renames do not change `pg_proc.prosrc`; replacing the FDW handler does.
    pub(crate) fn handles_server(server_oid: pg_sys::Oid) -> bool {
        let server = unsafe { &*pg_sys::GetForeignServer(server_oid) };
        let wrapper = unsafe { &*pg_sys::GetForeignDataWrapper(server.fdwid) };
        if wrapper.fdwhandler == pg_sys::InvalidOid {
            return false;
        }
        let tuple = unsafe {
            pg_sys::SearchSysCache1(
                pg_sys::SysCacheIdentifier::PROCOID as i32,
                pg_sys::Datum::from(wrapper.fdwhandler),
            )
        };
        if tuple.is_null() {
            return false;
        }
        let mut is_null = false;
        let datum = unsafe {
            pg_sys::SysCacheGetAttr(
                pg_sys::SysCacheIdentifier::PROCOID as i32,
                tuple,
                pg_sys::Anum_pg_proc_prosrc as i16,
                &mut is_null,
            )
        };
        let matches = if is_null {
            false
        } else {
            let source = unsafe {
                pg_sys::text_to_cstring(
                    pg_sys::DatumGetPointer(datum).cast::<pg_sys::text>(),
                )
            };
            unsafe { CStr::from_ptr(source) }.to_bytes()
                == b"lagodb_iceberg_fdw_handler_wrapper"
        };
        unsafe { pg_sys::ReleaseSysCache(tuple) };
        matches
    }
}

impl FdwImportSchema for LagodbIceberg {
    fn import_schema(
        context: &ForeignImportSchemaContext<'_>,
    ) -> Result<Vec<CString>, ForeignImportError> {
        IcebergSchemaImporter::import(context)
    }
}
