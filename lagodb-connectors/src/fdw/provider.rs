//! SQL-level FDW identity and callback registration.

use core::ffi::CStr;

use pg_lakebase_core::fdw::{
    FdwRoutine, ForeignDataWrapper, ForeignValidationError, register_modify,
    register_analyze, register_scan, register_truncate,
};
use pg_lakebase_core::pg_fdw;
use pgrx::pg_sys;

use super::options::validate_catalog_options;

/// The single SQL-level provider for all LagoDB connector formats.
#[pg_fdw(
    version = "0.1.0",
    author = "LagoDB",
    website = "https://github.com/robertmu/pg-lakebase"
)]
pub(crate) struct Lakebase;

impl ForeignDataWrapper for Lakebase {
    const NAME: &'static CStr = c"lakebase_fdw";

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
