use super::PgWrapper;
use crate::diag::PgError;
use pgrx::{PgTryBuilder, pg_sys};

impl PgWrapper {
    /// Converts already-validated JSON text bytes to a PostgreSQL JSON Datum.
    ///
    /// This is not a general replacement for PostgreSQL's `json_in` input
    /// function. PostgreSQL stores `json` with the same internal representation
    /// as `text`, so this helper constructs the text value directly and skips
    /// JSON parsing. Callers must only use it for bytes that PostgreSQL has
    /// already validated, or after equivalent validation has happened elsewhere.
    ///
    /// pg-iceberg-am uses this for its private json-as-Iceberg-string mapping.
    /// It is intentionally fast for the extension's own write/read path, but it
    /// is not a portable decoder for arbitrary external Iceberg data.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` points to `len` bytes of valid memory.
    pub(crate) unsafe fn json_in_from_bytes(
        ptr: *const u8,
        len: usize,
    ) -> Result<pg_sys::Datum, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let text_ptr = pg_sys::cstring_to_text_with_len(
                    ptr as *const std::os::raw::c_char,
                    len as i32,
                );
                Ok(pg_sys::Datum::from(text_ptr))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }

    /// Converts PostgreSQL-internal JSONB varlena bytes to a JSONB Datum.
    ///
    /// This is not PostgreSQL's `jsonb_in` input function. It does not parse
    /// JSON text and does not validate the JSONB structure. The input must
    /// already be a valid PostgreSQL JSONB varlena value, including the varlena
    /// header.
    ///
    /// pg-iceberg-am uses this for its private jsonb-as-Iceberg-binary codec:
    /// values written by PostgreSQL are copied into Iceberg binary and copied
    /// back here on read. These bytes are PostgreSQL-internal storage, not a
    /// portable Iceberg JSON encoding and not Iceberg v3 `variant`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `ptr` points to `len` bytes of valid memory
    /// representing a valid JSONB varlena.
    pub(crate) unsafe fn jsonb_in_from_bytes(
        ptr: *const u8,
        len: usize,
    ) -> Result<pg_sys::Datum, PgError> {
        unsafe {
            PgTryBuilder::new(move || {
                let new_ptr = pg_sys::palloc(len);
                std::ptr::copy_nonoverlapping(ptr, new_ptr as *mut u8, len);
                Ok(pg_sys::Datum::from(new_ptr))
            })
            .catch_others(|err| Err(PgError::from_caught(err)))
            .execute()
        }
    }
}
