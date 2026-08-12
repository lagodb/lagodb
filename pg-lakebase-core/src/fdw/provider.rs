//! Provider identity and explicit routine registration shared by the optional
//! FDW capabilities.

use core::ffi::CStr;

use pgrx::pg_sys;

use super::routine::FdwRoutine;
use super::validation::ForeignValidationError;

/// Root contract implemented by every FDW provider.
pub trait ForeignDataWrapper: 'static {
    /// Stable UTF-8 provider name stored in PostgreSQL plan private data.
    ///
    /// The generated metadata function exposes the same value as a SQL text
    /// column, so a provider must not use bytes that are not valid UTF-8.
    const NAME: &'static CStr;

    /// Register the PostgreSQL callback groups supported by this provider.
    ///
    /// Implementations should call the registration function for every
    /// capability they implement: [`crate::fdw::register_scan`],
    /// [`crate::fdw::register_modify`], [`crate::fdw::register_analyze`], or
    /// [`crate::fdw::register_truncate`]. The generated `#[pg_fdw]` handler
    /// invokes this method once while constructing a fresh PostgreSQL-owned
    /// routine; it is not part of any planner or executor hot path.
    fn register(routine: &mut FdwRoutine);

    /// Validate options supplied for this FDW, server, user mapping, or table.
    ///
    /// PostgreSQL invokes the generated SQL validator only when it is named in
    /// `CREATE FOREIGN DATA WRAPPER` or a related command. The default accepts
    /// all options.
    fn validate(
        _options: &[Option<String>],
        _catalog: Option<pg_sys::Oid>,
    ) -> Result<(), ForeignValidationError> {
        Ok(())
    }
}
