//! SQL-level FDW identity and callback registration.

use core::ffi::CStr;

use lagodb_core::fdw::{
    FdwRoutine, ForeignDataWrapper, ForeignValidationError, register_analyze,
    register_modify, register_scan, register_truncate,
};
use lagodb_core::pg_fdw;
use pgrx::pg_sys;

use super::options::validate_catalog_options;

/// The single SQL-level provider for all LagoDB connector formats.
#[pg_fdw(
    version = "0.1.0",
    author = "LagoDB",
    website = "https://github.com/robertmu/pg-lakebase"
)]
pub(crate) struct LagodbConnectors;

impl ForeignDataWrapper for LagodbConnectors {
    const NAME: &'static CStr = c"lagodb_connectors";

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
