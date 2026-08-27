use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};
use std::ffi::CStr;
use std::panic::AssertUnwindSafe;

impl PgWrapper {
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

    pub(crate) fn get_namespace_name(
        nspid: pg_sys::Oid,
    ) -> Result<Option<String>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let ptr = pg_sys::get_namespace_name(nspid);
                if ptr.is_null() {
                    Ok(None)
                } else {
                    let name = CStr::from_ptr(ptr).to_string_lossy().into_owned();
                    pg_sys::pfree(ptr as *mut core::ffi::c_void);
                    Ok(Some(name))
                }
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    pub(crate) fn get_relname_relid(
        relname: &CStr,
        relnamespace: pg_sys::Oid,
    ) -> Result<pg_sys::Oid, PgError> {
        let relname_ptr = relname.as_ptr();
        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::get_relname_relid(relname_ptr, relnamespace))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
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
        let relation = AssertUnwindSafe(relation);
        const RVR_MISSING_OK: u32 = 1;
        let flags = if missing_ok { RVR_MISSING_OK } else { 0 };

        unsafe {
            PgTryBuilder::new(move || {
                Ok(pg_sys::RangeVarGetRelidExtended(
                    *relation,
                    lockmode,
                    flags,
                    None,
                    std::ptr::null_mut(),
                ))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
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
