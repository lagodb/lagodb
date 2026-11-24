//! SQL-level Iceberg FDW identity and capability registration.

use std::ffi::{CStr, CString};

use lagodb_core::fdw::{
    FdwImportSchema, FdwRoutine, ForeignDataWrapper, ForeignImportError,
    ForeignImportSchemaContext, ForeignValidationError, register_analyze,
    register_import_schema, register_modify, register_scan,
};
use lagodb_core::handles::search_syscache1;
use lagodb_core::pg_fdw;
use lagodb_core::storage::foreign::ForeignOptionView;
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
        options: ForeignOptionView<'_>,
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
        let tuple = search_syscache1(
            pg_sys::SysCacheIdentifier::PROCOID as i32,
            pg_sys::Datum::from(wrapper.fdwhandler),
        )
        .expect("foreign-data wrapper handler must have a pg_proc entry");
        let Some(module_datum) = tuple.get_attr(pg_sys::Anum_pg_proc_probin as i16)
        else {
            return false;
        };
        // `prosrc` is a NOT NULL catalog column.
        let symbol_datum = tuple
            .get_attr(pg_sys::Anum_pg_proc_prosrc as i16)
            .expect("pg_proc.prosrc must not be NULL");
        let module = unsafe {
            pg_sys::text_to_cstring(module_datum.cast_mut_ptr::<pg_sys::text>())
        };
        let symbol = unsafe {
            pg_sys::text_to_cstring(symbol_datum.cast_mut_ptr::<pg_sys::text>())
        };
        let matches = unsafe { CStr::from_ptr(module) } == Self::HANDLER_MODULE
            && unsafe { CStr::from_ptr(symbol) } == Self::HANDLER_SYMBOL;
        unsafe {
            pg_sys::pfree(module.cast());
            pg_sys::pfree(symbol.cast());
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
