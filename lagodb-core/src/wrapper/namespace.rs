use super::PgWrapper;
use crate::diag::PgError;
use pgrx::pg_sys::RVROption::RVR_MISSING_OK;
use pgrx::{PgTryBuilder, pg_sys};
use std::ffi::{CStr, c_void};
use std::panic::AssertUnwindSafe;
use std::ptr;

impl PgWrapper {
    pub(crate) fn current_database_name_bytes() -> Vec<u8> {
        unsafe {
            // PostgreSQL initializes `MyDatabaseId` before extension code can
            // run in a connected backend, so the lookup cannot return NULL.
            let ptr = pg_sys::get_database_name(pg_sys::MyDatabaseId);
            let name = CStr::from_ptr(ptr).to_bytes().to_vec();
            pg_sys::pfree(ptr.cast());
            name
        }
    }

    pub(crate) fn get_namespace_oid(
        nspname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgError> {
        let nspname_ptr = nspname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_namespace_oid(nspname_ptr, missing_ok))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn get_namespace_name_bytes(nspid: pg_sys::Oid) -> Option<Vec<u8>> {
        unsafe {
            let ptr = pg_sys::get_namespace_name(nspid);
            if ptr.is_null() {
                None
            } else {
                let name = CStr::from_ptr(ptr).to_bytes().to_vec();
                pg_sys::pfree(ptr.cast());
                Some(name)
            }
        }
    }

    pub(crate) fn get_relname_relid(
        relname: &CStr,
        relnamespace: pg_sys::Oid,
    ) -> pg_sys::Oid {
        unsafe { pg_sys::get_relname_relid(relname.as_ptr(), relnamespace) }
    }

    /// Resolves a `RangeVar` to a relation OID.
    ///
    /// # Safety
    ///
    /// `relation` must point to a valid PostgreSQL `RangeVar` for the duration
    /// of the call.
    pub(crate) unsafe fn range_var_get_relid(
        relation: *const pg_sys::RangeVar,
        lockmode: pg_sys::LOCKMODE,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgError> {
        let flags = if missing_ok { RVR_MISSING_OK } else { 0 };

        unsafe {
            Self::range_var_get_relid_extended(
                relation,
                lockmode,
                flags,
                None,
                ptr::null_mut(),
            )
        }
    }

    /// Calls PostgreSQL's callback-aware `RangeVarGetRelidExtended` and
    /// converts an unwind crossing the PostgreSQL FFI boundary into `PgError`.
    ///
    /// # Safety
    ///
    /// `relation` must remain valid for the duration of the call. If non-null,
    /// `callback_arg` must satisfy the contract established by `callback`, and
    /// `callback` must uphold PostgreSQL's callback contract.
    pub(crate) unsafe fn range_var_get_relid_extended(
        relation: *const pg_sys::RangeVar,
        lockmode: pg_sys::LOCKMODE,
        flags: u32,
        callback: pg_sys::RangeVarGetRelidCallback,
        callback_arg: *mut c_void,
    ) -> Result<pg_sys::Oid, PgError> {
        let relation = AssertUnwindSafe(relation);
        let callback_arg = AssertUnwindSafe(callback_arg);

        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::RangeVarGetRelidExtended(
                    *relation,
                    lockmode,
                    flags,
                    callback,
                    *callback_arg,
                ))
            })
            .catch_others(|error| Err(PgError::from_caught(error)))
            .execute()
        }
    }

    pub(crate) fn get_tablespace_oid(
        spcname: &CStr,
        missing_ok: bool,
    ) -> Result<pg_sys::Oid, PgError> {
        let spcname_ptr = spcname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_tablespace_oid(spcname_ptr, missing_ok))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
