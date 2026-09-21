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
    website = "https://github.com/lagodb/lagodb"
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
    const HANDLER_MODULE: &'static CStr = c"$libdir/lagodb_iceberg";
    const HANDLER_SYMBOL: &'static CStr = c"lagodb_iceberg_fdw_handler_wrapper";

    /// Match the installed handler by the module and C entry point PostgreSQL
    /// uses to load it. Renaming the FDW does not change either field.
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
        let mut module_is_null = false;
        let module_datum = unsafe {
            pg_sys::SysCacheGetAttr(
                pg_sys::SysCacheIdentifier::PROCOID as i32,
                tuple,
                pg_sys::Anum_pg_proc_probin as i16,
                &mut module_is_null,
            )
        };
        if module_is_null {
            unsafe { pg_sys::ReleaseSysCache(tuple) };
            return false;
        }
        let symbol_datum = unsafe {
            pg_sys::SysCacheGetAttrNotNull(
                pg_sys::SysCacheIdentifier::PROCOID as i32,
                tuple,
                pg_sys::Anum_pg_proc_prosrc as i16,
            )
        };
        let module = unsafe {
            pg_sys::text_to_cstring(
                pg_sys::DatumGetPointer(module_datum).cast::<pg_sys::text>(),
            )
        };
        let symbol = unsafe {
            pg_sys::text_to_cstring(
                pg_sys::DatumGetPointer(symbol_datum).cast::<pg_sys::text>(),
            )
        };
        let matches = unsafe { CStr::from_ptr(module) } == Self::HANDLER_MODULE
            && unsafe { CStr::from_ptr(symbol) } == Self::HANDLER_SYMBOL;
        unsafe {
            pg_sys::pfree(module.cast());
            pg_sys::pfree(symbol.cast());
            pg_sys::ReleaseSysCache(tuple);
        }
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
