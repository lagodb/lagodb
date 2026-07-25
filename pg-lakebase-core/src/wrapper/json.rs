use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, fcinfo, pg_sys};
use std::ffi::c_char;

impl PgWrapper {
    /// Call PostgreSQL's validating `json_in` input function for a
    /// NUL-terminated string.
    ///
    /// This parses the input and lets PostgreSQL report invalid JSON.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn json_input_from_cstr(
        ptr: *const c_char,
    ) -> Result<Option<pg_sys::Datum>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                Ok(fcinfo::direct_function_call_as_datum(
                    pg_sys::json_in,
                    &[Some(pg_sys::Datum::from(ptr))],
                ))
            })
            .catch_others(|err| Err(PgError::from(err)))
            .execute()
        }
    }

    /// Call PostgreSQL's validating `jsonb_in` input function for a
    /// NUL-terminated string.
    ///
    /// # Safety
    ///
    /// `ptr` must point to a live NUL-terminated string for the duration of
    /// the PostgreSQL call.
    pub(crate) unsafe fn jsonb_input_from_cstr(
        ptr: *const c_char,
    ) -> Result<Option<pg_sys::Datum>, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                Ok(fcinfo::direct_function_call_as_datum(
                    pg_sys::jsonb_in,
                    &[Some(pg_sys::Datum::from(ptr))],
                ))
            })
            .catch_others(|err| Err(PgError::from(err)))
            .execute()
        }
    }
}
